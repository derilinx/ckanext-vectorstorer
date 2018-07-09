from settings import ogr, osr, db_encoding
from db_helpers import DB
import re
from psycopg2.extensions import adapt
import unicodedata as ud
SHAPEFILE = 'ESRI Shapefile'
KML = 'KML'
GEOJSON = 'GeoJSON'
GML = 'GML'
GPX = 'GPX'
CSV = 'CSV'
XLS = 'XLS'
SQLITE = 'SQLite'
GEOPACKAGE = 'GPKG'

import logging
log=logging.getLogger(__name__)

class Vector:
    _check_for_conversion = False
    default_epsg = 4326
    gdal_driver = None

    def __init__(self, gdal_driver, file_path, encoding = None, db_conn_params = None):
        self.gdal_driver = gdal_driver
        self.encoding = encoding
        self.db_conn_params = db_conn_params
        driver = ogr.GetDriverByName(gdal_driver)
        self.dataSource = driver.Open(file_path, 0)
        if self.dataSource is None:
            raise 'Could not open %s' % file_path

    def get_layer_count(self):
        return self.dataSource.GetLayerCount()

    def get_layer(self, layer_idx):
        return self.dataSource.GetLayer(layer_idx)

    def handle_layer(self, layer, geom_name, table_name):
        log.debug("handle_layer")
        srs = self.get_SRS(layer)
        featureCount = layer.GetFeatureCount()
        layerDefinition = layer.GetLayerDefn()
        self._db = DB(self.db_conn_params)
        fields = self._get_layer_fields(layerDefinition)
        layer.ResetReading()
        feat = layer.GetNextFeature()
        feat_geom = feat.GetGeometryRef()
        coordinate_dimension = feat_geom.GetCoordinateDimension()
        layer.ResetReading()
        log.debug(fields)
        if "parcel_id" in fields:
            table_name = self._db.create_table_and_view(table_name,
                                                        fields,
                                                        geom_name,
                                                        srs,
                                                        coordinate_dimension)
        else:
            self._db.create_table(table_name, fields, geom_name, srs, coordinate_dimension)
        self.write_to_db(table_name, layer, srs, geom_name)

    def get_SRS(self, layer):
        if not layer.GetSpatialRef() == None:
            prj = layer.GetSpatialRef().ExportToWkt()
            srs_osr = osr.SpatialReference()
            srs_osr.ImportFromESRI([prj])
            epsg = srs_osr.GetAuthorityCode(None)
            if epsg is None or epsg == 0:
                srs_osr.AutoIdentifyEPSG()
                epsg = srs_osr.GetAuthorityCode(None)
                if epsg is None or epsg == 0:
                    epsg = self.default_epsg
            return epsg
        else:
            return self.default_epsg

    def _get_layer_fields(self, layerDefinition):
        fields = []
        field_map = {
            0: 'integer',
            1: 'integer[]',
            2: 'real',
            3: 'real[]',
            4: 'varchar',
            5: 'varchar[]',
            6: 'varchar',
            7: 'varchar[]',
            8: 'bytea',
            9: 'date',
            10: 'time without time zone',
            11: 'timestamp without time zone',
        }

        for i in range(layerDefinition.GetFieldCount()):
            fname = layerDefinition.GetFieldDefn(i).GetName()
            ftype = layerDefinition.GetFieldDefn(i).GetType()
            if ftype in field_map:
                fields.append('"%s" %s' %(fname, field_map[ftype]))
        return ", ".join(fields)

    def get_geometry_name(self, layer):
        geometry_names = []
        for feat in layer:
            if not feat or not feat.GetGeometryRef():
                continue
            feat_geom = feat.GetGeometryRef().GetGeometryName()
            if feat_geom not in geometry_names:
                geometry_names.append(feat_geom)

        geometry_name = None
        if len(geometry_names) == 1:
            return geometry_names[0]
        if len(geometry_names) == 2:
            multi_geom = None
            simple_geom = None
            for gname in geometry_names:
                gname_upp = gname.upper()
                if 'MULTI' in gname_upp:
                    multi_geom = gname_upp
                else:
                    simple_geom = gname_upp

            if multi_geom and simple_geom:
                if multi_geom.split('MULTI')[1] == simple_geom:
                    self._check_for_conversion = True
                    geometry_name = multi_geom
                    return geometry_name
            else:
                return 'GEOMETRY'
        elif len(geometry_names) > 2:
            return 'GEOMETRY'
        log.debug('No geometry name found, falling back to GEOMETRY')
        return 'GEOMETRY'

    def get_sample_data(self, layer):
        feat_data = {}
        layer.ResetReading()
        for feat in layer:
            if not feat:
                continue

            for y in range(feat.GetFieldCount()):
                layerDefinition = layer.GetLayerDefn()
                field_name = layerDefinition.GetFieldDefn(y).GetName()
                feat_data[field_name] = feat.GetField(y)

            break
        return feat_data

    def write_to_db(self, table_name, layer, srs, layer_geom_name):
        i = 0
        for feat in layer:
            feature_fields = '%s,' % i
            i = i + 1
            if not feat:
                continue
            for y in range(feat.GetFieldCount()):
                if not feat.GetField(y) == None:
                    if layer.GetLayerDefn().GetFieldDefn(y).GetType() in (4, 9, 10, 11):
                        field_value = feat.GetField(y).decode(self.encoding, 'ignore').encode(db_encoding)
                        feature_fields += adapt(field_value).getquoted().decode(db_encoding) + ','
                    else:
                        feature_fields += str(feat.GetField(y)) + ','
                else:
                    feature_fields += 'NULL,'

            convert_to_multi = False
            if self._check_for_conversion:
                convert_to_multi = self.needs_conversion_to_multi(feat, layer_geom_name)
            self._db.insert_to_table(table_name, feature_fields, feat.GetGeometryRef(), convert_to_multi, srs)

        self._db.create_spatial_index(table_name)
        self._db.commit_and_close()

    def needs_conversion_to_multi(self, feat, layer_geom_name):
        try:
            if not feat.GetGeometryRef().GetGeometryName() == layer_geom_name:
                return True
            else:
                return False
        except AttributeError:
            log.debug("Feature does not have a Geometry")
            return True

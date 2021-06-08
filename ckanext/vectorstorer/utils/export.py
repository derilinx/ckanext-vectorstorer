import os
import zipfile
import uuid
from ckan.common import config
from ckan.lib.base import c, request, \
    response, session, render, config, abort
from ckan.logic import get_action ,check_access, model
import urlparse
import shutil
from ckanext.vectorstorer.settings import ogr, osr
from ckanext.vectorstorer import settings
from ckanext.vectorstorer.resources import DBTableResource
import json
import urllib2
from ckan.common import _

_check_access = check_access


class NotVectorStorerDB(Exception):
    pass


def export(id, resource_id, operation):
    if operation=="index":
        _get_context(id, resource_id)
        c.gdal_drv_list=_get_gdal_driver_list()
        return render('export/export.html')
    elif operation=="export_file":
        return _export_file(id,resource_id)
    else:
        abort(404, _('Not found'))


def _export_file(id, resource_id):
    export_format = request.params.get('format')
    export_projection_param = request.params.get('projection', u'')

    if export_format:
        export_format=export_format.lower()
        export_projection = int(export_projection_param)

        return _init_export(resource_id, export_format, export_projection )
    else:
        abort(400, _('No Export Format was defined !'))


def search_epsg():
    query = request.params.get('term')
    prj2epsg_api = "http://www.prj2epsg.org/search.json?terms=%s" % query

    response = urllib2.urlopen(prj2epsg_api).read()

    autocomplete_json = []

    results = json.loads(response)['codes']

    for idx, val in enumerate(results):
        autocomplete_dict = dict()
        autocomplete_dict['label'] = val['name'] + " - " + val['code']
        autocomplete_dict['value'] = val['code']

        autocomplete_json.append(autocomplete_dict)

    return json.dumps(autocomplete_json)


def _init_export(resource_id, export_format, export_projection):

    postgis_layer, connection = _get_layer(resource_id)

    if postgis_layer is None:
        return None

    #Set up the export GDAL driver

    tmp_folder = _create_temp_export_folder()

    resource_name = _get_resource_name(resource_id)
    if DBTableResource.name_extention in resource_name:
        resource_name=resource_name.replace(DBTableResource.name_extention,'')

    export_datasource, export_layer = _create_export_datasource(tmp_folder, resource_name, export_format,
                                                                export_projection, postgis_layer.GetGeomType())

    postgis_srs = _get_SRS(postgis_layer)

    coordTrans=None

    if not export_projection == postgis_srs:
        # input SpatialReference
        inSpatialRef = osr.SpatialReference()
        inSpatialRef.ImportFromEPSG(postgis_srs)

        # output SpatialReference
        outSpatialRef = osr.SpatialReference()
        outSpatialRef.ImportFromEPSG(export_projection)

        # create the CoordinateTransformation
        coordTrans = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

    postgislyrDefn = postgis_layer.GetLayerDefn()

    for i in range(postgislyrDefn.GetFieldCount()):
        fieldName = postgislyrDefn.GetFieldDefn(i).GetName()
        fieldTypeCode = postgislyrDefn.GetFieldDefn(i).GetType()
        fieldType = postgislyrDefn.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)
        fieldWidth = postgislyrDefn.GetFieldDefn(i).GetWidth()
        fieldPrecision = postgislyrDefn.GetFieldDefn(i).GetPrecision()

        field_region = ogr.FieldDefn(fieldName, fieldTypeCode)
        field_region.SetWidth(fieldWidth)
        field_region.SetPrecision(fieldPrecision)
        export_layer.CreateField(field_region)
        field_region.Destroy()

    for feat in postgis_layer:
        # create the feature
        feature = ogr.Feature(export_layer.GetLayerDefn())

        # Set the attributes using the values from the delimited text file
        for i in range(postgislyrDefn.GetFieldCount()):
            fieldName = postgislyrDefn.GetFieldDefn(i).GetName()
            feature.SetField(fieldName, feat[fieldName])

        geom = feat.GetGeometryRef()

        if coordTrans:
            geom.Transform(coordTrans)

        feature.SetGeometry(geom)
        export_layer.CreateFeature(feature)

        feature.Destroy()

    export_datasource.Destroy()

    file_path = _create_zip(tmp_folder)

    connection.Destroy()

    shutil.rmtree(tmp_folder)

    resp = _send_file_response(file_path, resource_name)
    os.remove(file_path)

    return resp


def _get_SRS(layer):
    default_epsg=4326
    if layer.GetSpatialRef():

        prj = layer.GetSpatialRef().ExportToWkt()
        srs_osr = osr.SpatialReference()
        srs_osr.ImportFromESRI([prj])

        epsg = srs_osr.GetAuthorityCode(None)

        if epsg is None or epsg == 0:
            epsg = default_epsg
        return int(epsg)
    else:
        return default_epsg


def _get_layer(resource_id):

    datastore_config = urlparse.urlparse(config.get('ckan.datastore.read_url'))
    databaseServer = datastore_config.hostname
    databaseName = datastore_config.path[1:]
    databaseUser = datastore_config.username
    databasePW = datastore_config.password
    connString = "PG: host=%s dbname=%s user=%s password=%s" % (
    databaseServer, databaseName, databaseUser, databasePW)

    layer_name = str(resource_id.lower())

    conn = ogr.Open(connString)

    layer = conn.GetLayer(layer_name)

    return layer, conn


def _create_temp_export_folder():
    random_folder_name = str(uuid.uuid4())
    random_folder_name = os.path.join('/tmp', random_folder_name)
    os.makedirs(random_folder_name)
    return random_folder_name


def _create_export_datasource(tmp_folder, layer_name, export_format, export_projection, layer_geom_type):

    GDAL_Driver = _get_GDAL_Driver_by_export_format(export_format)

    driver = ogr.GetDriverByName(GDAL_Driver)

    #Create the data source
    abs_filepath = os.path.join(tmp_folder, layer_name) + "." + export_format

    datasource_options=_get_datasource_options(GDAL_Driver)
    data_source = driver.CreateDataSource(abs_filepath,options = datasource_options)

    #Create the spatial reference
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(export_projection)

    #Create the layer
    layer_options=_get_layer_options(GDAL_Driver)
    layer = data_source.CreateLayer(str(layer_name), srs, layer_geom_type,options = layer_options)
    return data_source, layer


def _create_zip(src):
    zip_path = "%s.zip" % (src)
    zf = zipfile.ZipFile(zip_path, "w")

    abs_src = os.path.abspath(src)

    for files in os.listdir(src):
        absname = os.path.abspath(os.path.join(abs_src, files))
        arcname = absname[len(abs_src) + 1:]

        zf.write(absname, arcname)
    zf.close()
    return zip_path


def _get_GDAL_Driver_by_export_format(export_format):
    if export_format == "shp":
        return "ESRI Shapefile"

    elif export_format == "geopackage":
        return "GPKG"
    else:
        return str(export_format.upper())


def _get_datasource_options(gdal_driver):
    if gdal_driver =='GML':
        export_gml_format = request.params.get('gml_version',u'')
        if export_gml_format.upper() in ('GML3','GML3Deegree','GML3.2'):
            datasource_options="FORMAT=%s"%(export_gml_format.upper())
            return [datasource_options]
        else:
            return []
    else:
        return []


def _get_layer_options(gdal_driver):
    if gdal_driver =='CSV':
        export_csv_geom = request.params.get('csv_geom',u'')
        if export_csv_geom.upper() in ('XY','YX','WKT','XYZ'):
            layer_options="GEOMETRY=AS_%s"%(export_csv_geom.upper())
            return [layer_options]
        else:
            return []
    else:
        return []


def _send_file_response(filepath, resource_name):
    user_filename = '_'.join(filepath.split('/')[-2:])
    file_size = os.path.getsize(filepath)

    headers = [('Content-Disposition', 'attachment; filename=\"exported_geometry.zip\"'),#Avoid unicode issues: + resource_name + '.zip\"'),
               ('Content-Type', 'text/plain'),
               ('Content-Length', str(file_size))]

    from paste.fileapp import FileApp

    fapp = FileApp(filepath, headers=headers)
    # ?? What is start_response???
    return fapp(request.environ, self.start_response)


def _get_gdal_driver_list():
    gdal_drvs=[]
    drv_count=ogr.GetDriverCount()
    for drv_idx in range(drv_count):
        gdal_drv=ogr.GetDriver(drv_idx)
        gdal_drvs.append(gdal_drv.GetName().upper())
    return gdal_drvs


def _get_resource_name(resource_id):

    context = {'model': model, 'session': model.Session,
               'user': c.user}

    try:

        resource = get_action('resource_show')(context,
                                               {'id': resource_id})
        # return resource['name']
        return resource['name']
    except NotFound:
        abort(404, _('Resource not found'))


def _get_context(id, resource_id):
    context = {'model': model, 'session': model.Session,
               'user': c.user}

    try:
        c.resource = get_action('resource_show')(context,
                                                 {'id': resource_id})
        c.package = get_action('package_show')(context, {'id': id})
        c.pkg = context['package']
        c.pkg_dict = c.package
        if not (c.resource.has_key('vectorstorer_resource') and c.resource['format'] == settings.DB_TABLE_FORMAT):
            raise NotVectorStorerDB

    except NotVectorStorerDB:
        abort(404, _('Not VectorStorerDB Resource.'))
    except NotFound:
        abort(404, _('Resource not found'))
    except NotAuthorized:
        abort(401, _('Unauthorized to read resource %s') % id)

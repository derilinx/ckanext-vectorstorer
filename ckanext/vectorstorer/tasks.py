import zipfile
import os
import urllib2
import urllib
import requests
import json
import vector
import shutil
import random
from db_helpers import DB
from pyunpack import Archive
from geoserver.catalog import Catalog
from resources import *
import ckan.plugins.toolkit as toolkit
from ckanext.vectorstorer import settings
import cgi

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

RESOURCE_CREATE_DEFAULT_VIEWS_ACTION = 'resource_create_default_resource_views'
RESOURCE_CREATE_ACTION = 'resource_create'
RESOURCE_UPDATE_ACTION = 'resource_update'
RESOURCE_DELETE_ACTION = 'resource_delete'

def identify_resource(data,user_api_key):
    resource = json.loads(data)
    json_result = _identify(resource,user_api_key)
    return json.dumps(json_result)


def _identify(resource,user_api_key):
    json_result = {}
    
    resource_tmp_folder, _file_path = _download_resource(resource,user_api_key)

    gdal_driver, file_path,prj_exists = _get_gdalDRV_filepath(resource, resource_tmp_folder ,_file_path)
    
    if gdal_driver:
        json_result['gdal_driver'] = gdal_driver
        _vector = vector.Vector(gdal_driver, file_path, None, None)
        layer_count = _vector.get_layer_count()
        layers = {}
        for layer_idx in range(0, layer_count):
            layer_dict = {}
            layer = _vector.get_layer(layer_idx)
            layer_dict['layer_name'] = layer.GetName()
            layer_dict['layer_srs'] = _vector.get_SRS(layer)
            layer_dict['layer_geometry'] = _vector.get_geometry_name(layer)
            sample_data = _vector.get_sample_data(layer)
            layer_dict['sample_data'] = sample_data
            layers[layer_idx] = layer_dict

        json_result['layers'] = layers
        return json_result
    else:
        return u'invalid'
    _delete_temp(resource_tmp_folder)


def vectorstorer_upload(geoserver_cont, cont, data):
    log.debug("task: vectorstorer_upload")
    resource = json.loads(data)
    context = json.loads(cont)
    geoserver_context = json.loads(geoserver_cont)
    db_conn_params = context['db_params']
    _handle_resource(resource, db_conn_params, context, geoserver_context)


def _handle_resource(resource, db_conn_params, context, geoserver_context):
    log.debug("task: _handle_resource")
    user_api_key = context['apikey'].encode('utf8')
    resource_tmp_folder,_file_path = _download_resource(resource,user_api_key)
    gdal_driver, file_path ,prj_exists = _get_gdalDRV_filepath(resource, resource_tmp_folder,_file_path)
    
    if context.has_key('encoding'):
        _encoding = context['encoding']
    else:
        _encoding = 'utf-8'
    _selected_layers = None
    if context.has_key('selected_layers'):
        if len(context['selected_layers']) > 0:
            _selected_layers = context['selected_layers']
    if gdal_driver:
        _vector = vector.Vector(gdal_driver, file_path, _encoding, db_conn_params)
        layer_count = _vector.get_layer_count()
        for layer_idx in range(0, layer_count):
            if _selected_layers:
                if str(layer_idx) in _selected_layers:
                    _handle_vector(_vector, layer_idx, resource, context, geoserver_context)
            else:
                _handle_vector(_vector, layer_idx, resource, context, geoserver_context)

    _delete_temp(resource_tmp_folder)


def _get_gdalDRV_filepath(resource, resource_tmp_folder,file_path):

    resource_format = resource['format'].lower()
    _gdal_driver = None
    _file_path = os.path.join(resource_tmp_folder,file_path)
    prj_exists = None
    
    if resource_format.lower() in settings.ARCHIVE_FORMATS:
        Archive(_file_path).extractall(resource_tmp_folder)
        is_shp, _file_path, prj_exists = _is_shapefile(resource_tmp_folder)
        if is_shp:
            _gdal_driver = vector.SHAPEFILE
    elif resource_format == 'kml':
        _gdal_driver = vector.KML
    elif resource_format == 'gml':
        _gdal_driver = vector.GML
    elif resource_format == 'gpx':
        _gdal_driver = vector.GPX
    elif resource_format == 'geojson' or resource_format == 'json':
        _gdal_driver = vector.GEOJSON
    elif resource_format == 'sqlite':
        _gdal_driver = vector.SQLITE
    elif resource_format == 'geopackage' or resource_format == 'gpkg':
        _gdal_driver = vector.GEOPACKAGE
    elif resource_format == 'csv':
        _gdal_driver = vector.CSV
    elif resource_format == 'xls' or resource_format == 'xlsx':
        _gdal_driver = vector.XLS
   
    return _gdal_driver, _file_path ,prj_exists


def _download_resource(resource,user_api_key):
    resource_tmp_folder = settings.TMP_FOLDER + str(random.randint(1, 1000)) + resource['id'] + '/'
    file_name= None
    
    os.makedirs(resource_tmp_folder)
    resource_url = urllib2.unquote(resource['url'])
    
    if resource['url_type']:
	#Handle file uploads here
	file_name= _get_tmp_file_path(resource_tmp_folder,resource)
	request = urllib2.Request(resource_url)
	request.add_header('Authorization', user_api_key)
        resource_download_request = urllib2.urlopen(request)
	downloaded_resource = open( file_name, 'wb')
	downloaded_resource.write(resource_download_request.read())
	downloaded_resource.close()
	
    else :
	#Handle urls here
	resource_download_request = urllib2.urlopen(resource_url)
	_, params = cgi.parse_header(resource_download_request.headers.get('Content-Disposition', ''))
	filename = params['filename']
	file_name = filename
	downloaded_resource = open(resource_tmp_folder + filename, 'wb')
	downloaded_resource.write(resource_download_request.read())
	downloaded_resource.close()
 
    return resource_tmp_folder, file_name


def _get_tmp_file_path(resource_tmp_folder, resource):
    resource_url = urllib2.unquote(resource['url']).decode('utf8')
    url_parts = resource_url.split('/')
    resource_file_name = url_parts[len(url_parts) - 1]
    file_path = resource_tmp_folder + resource_file_name
    return file_path


def _handle_vector(_vector, layer_idx, resource, context, geoserver_context):
    log.debug('handle_vector')
    layer = _vector.get_layer(layer_idx)
    if layer and layer.GetFeatureCount() > 0:
        layer_name = layer.GetName()
        if 'OGR' in layer_name:
            layer_name = _vector.gdal_driver
        layer_name = resource['name'] + ' - ' + layer_name
        geom_name = _vector.get_geometry_name(layer)
        srs_epsg = int(_vector.get_SRS(layer))
        spatial_ref = settings.osr.SpatialReference()
        spatial_ref.ImportFromEPSG(srs_epsg)
        srs_wkt = spatial_ref.ExportToWkt()
        created_db_table_resource = _add_db_table_resource(context, resource, geom_name, layer_name)
        layer = _vector.get_layer(layer_idx)
        _vector.handle_layer(layer, geom_name, created_db_table_resource['id'].lower())
        wms_server, wms_layer = _publish_layer(geoserver_context, created_db_table_resource, srs_wkt)
        _add_wms_resource(context, layer_name, created_db_table_resource, wms_server, wms_layer)
        try:
            _add_db_table_resource_view(context, created_db_table_resource)
        except:
            pass #This currently fails because of https://github.com/ckan/ckan/pull/3444#issuecomment-312216983

def _add_db_table_resource(context, resource, geom_name, layer_name):
    log.debug('adding db table resource')
    db_table_resource = DBTableResource(context['package_id'], layer_name, "Datastore resource derived from \"" + layer_name + "\" in [this resource](" + resource['id'] + "), available in CKAN and GeoServer Store", resource['id'], 'http://_datastore_only_resource', geom_name)
    db_res_as_dict = db_table_resource.get_as_dict()
    created_db_table_resource = _api_resource_action(context, db_res_as_dict, RESOURCE_CREATE_ACTION)
    return created_db_table_resource

def _add_db_table_resource_view(context, resource):
    log.debug('adding db table resource view')
    payload = {"resource": resource, "create_datastore_views": True}
    _api_resource_action(context, payload, RESOURCE_CREATE_DEFAULT_VIEWS_ACTION)

def _add_wms_resource(context, layer_name, parent_resource, wms_server, wms_layer):
    log.debug('adding wms resource')
    wms_resource = WMSResource(context['package_id'], layer_name, "WMS publishing of the GeoServer layer \"" + layer_name + "\" stored in [this resource](" + parent_resource['id']  + ")", parent_resource['id'], wms_server, wms_layer)
    wms_res_as_dict = wms_resource.get_as_dict()
    created_wms_resource = _api_resource_action(context, wms_res_as_dict, RESOURCE_CREATE_ACTION)
    return created_wms_resource


def _delete_temp(res_tmp_folder):
    shutil.rmtree(res_tmp_folder)


def _is_shapefile(res_folder_path):
    shp_exists = False
    shx_exists = False
    dbf_exists = False
    prj_exists = False
    for file in os.listdir(res_folder_path):
        if file.lower().endswith('.shp'):
            shapefile_path = res_folder_path + file
            shp_exists = True
        elif file.lower().endswith('.shx'):
            shx_exists = True
        elif file.lower().endswith('.dbf'):
            dbf_exists = True

    if shp_exists and shx_exists and dbf_exists:
        return (True, shapefile_path, prj_exists)
    else:
        return (False, None, False)


def _publish_layer(geoserver_context, resource, srs_wkt):
    log.debug('publishing layer for %s' % resource['name'])
    geoserver_url = geoserver_context['geoserver_url']
    geoserver_workspace = geoserver_context['geoserver_workspace']
    geoserver_admin = geoserver_context['geoserver_admin']
    geoserver_password = geoserver_context['geoserver_password']
    geoserver_ckan_datastore = geoserver_context['geoserver_ckan_datastore']
    resource_id = resource['id'].lower()
    resource_name = resource['name']
    if DBTableResource.name_extention in resource_name:
        resource_name = resource_name.replace(DBTableResource.name_extention, '')
    resource_description = resource['description']
    url = geoserver_url + '/rest/workspaces/' + geoserver_workspace + '/datastores/' + geoserver_ckan_datastore + '/featuretypes'
    data = '<featureType><name>%s</name><title>%s</title><abstract>%s</abstract><nativeCRS>%s</nativeCRS></featureType>' % (
        resource_id,
        resource_name,
        resource_description,
        srs_wkt)
    log.debug("sending layer to geoserver: %s "% url)
    try:
        res = requests.post(url,
                            headers={'Content-type': 'text/xml'},
                            auth=(geoserver_admin, geoserver_password),
                            data=data,
                            timeout=10)
        res.raise_for_status()
    except requests.HTTPError as msg:
        log.debug("Exception posting to geoserver: %s" %str(msg))
        raise
    except Exception as msg:
        log.debug("Other Exception posting to geoserver: %s" %str(msg))
        raise        
    log.debug("sent layer to geoserver")
    wms_server = geoserver_url + '/wms'
    wms_layer = geoserver_workspace + ':' + resource_id
    log.debug('published layer %s' % wms_layer)
    return (wms_server, wms_layer)


def _api_resource_action(context, resource, action):
    api_key = context['apikey'].encode('utf8')
    site_url = context['site_url']
    data_string = urllib.quote(json.dumps(resource))
    request = urllib2.Request(site_url + 'api/action/' + action)
    request.add_header('Authorization', api_key)
    response = urllib2.urlopen(request, data_string)
    created_resource = json.loads(response.read())['result']
    return created_resource


def _update_resource_metadata(context, resource):
    api_key = context['apikey'].encode('utf8')
    site_url = context['site_url']
    resource['vectorstorer_resource'] = True
    data_string = urllib.quote(json.dumps(resource))
    request = urllib2.Request(site_url + 'api/action/resource_update')
    request.add_header('Authorization', api_key)
    urllib2.urlopen(request, data_string)


def vectorstorer_update(geoserver_cont, cont, data):
    log.debug('resource update')
    resource = json.loads(data)
    context = json.loads(cont)
    geoserver_context = json.loads(geoserver_cont)
    db_conn_params = context['db_params']
    resource_ids = context['resource_list_to_delete']
    if len(resource_ids) > 0:
        for res_id in resource_ids:
            res = {'id': res_id}
            try:
                _api_resource_action(context, res, RESOURCE_DELETE_ACTION)
            except urllib2.HTTPError as e:
                print e.reason

    _handle_resource(resource, db_conn_params, context, geoserver_context)


def vectorstorer_delete(geoserver_cont, cont, data):
    log.debug('resource delete')
    resource = json.loads(data)
    context = json.loads(cont)
    geoserver_context = json.loads(geoserver_cont)
    db_conn_params = context['db_params']
    if resource.has_key('format'):
        if resource['format'] == settings.DB_TABLE_FORMAT:
            _delete_from_datastore(resource['id'], db_conn_params, context)
        elif resource['format'] == settings.WMS_FORMAT:
            _unpublish_from_geoserver(resource['parent_resource_id'], geoserver_context)
    resource_ids = context['resource_list_to_delete']
    if resource_ids:
        resource_ids = context['resource_list_to_delete']
        for res_id in resource_ids:
            res = {'id': res_id}
            _api_resource_action(context, res, RESOURCE_DELETE_ACTION)


def _delete_from_datastore(resource_id, db_conn_params, context):
    _db = DB(db_conn_params)
    _db.drop_table(resource_id)
    _db.commit_and_close()


def _unpublish_from_geoserver(resource_id, geoserver_context):
    geoserver_url = geoserver_context['geoserver_url']
    geoserver_admin = geoserver_context['geoserver_admin']
    geoserver_password = geoserver_context['geoserver_password']
    cat = Catalog(geoserver_url + '/rest', username=geoserver_admin, password=geoserver_password)
    layer = cat.get_layer(resource_id.lower())
    cat.delete(layer)
    cat.reload()


def _delete_vectorstorer_resources(resource, context):
    resources_ids_to_delete = context['vector_storer_resources_ids']
    api_key = context['apikey'].encode('utf8')
    site_url = context['site_url']
    for res_id in resources_ids_to_delete:
        resource = {'id': res_id}
        data_string = urllib.quote(json.dumps(resource))
        request = urllib2.Request(site_url + 'api/action/resource_delete')
        request.add_header('Authorization', api_key)
        urllib2.urlopen(request, data_string)

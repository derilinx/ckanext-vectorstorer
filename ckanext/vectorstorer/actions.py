from ckan.plugins import toolkit
from .tasks import identify_resource, add_wms_resource
from ckan.common import config
from ckanext.vectorstorer import helper as v_hlp
from . import wms
from geoserver.catalog import Catalog
import ckan.logic as logic
import urlparse
import requests

import logging
log = logging.getLogger(__name__)

ValidationError = logic.ValidationError


def create_resource_given_wms_layer(resource):
    """
    Check of layer_name giben in resource dict

    Condition:

    - Resource should not have url selected
    - Resource format should be wms
    - Resource should not have vectorstorer_resource (means no db table should be associated with it)
    - Check for a valid layer_name in geoserver for a given workspace.

    :param resource: dict
    :return: dict
    """

    _resource_format = resource.get('format', '').lower().strip()
    _workspace = config['ckanext-vectorstorer.geoserver_workspace']
    layer_name = resource.get('odm_geoserver_layer_name', '')
    external_geoserver_wms_service = resource.get('odm_external_geoserver_url', '')

    if _resource_format == "wms" and resource.get('odm_geoserver_layer_name', ''):
        if resource.get('vectorstorer_resource', ''):
            # This means wms is updated and there is already a db table associated with it.
            raise ValidationError("WMS resources is associated with DB table cannot change layer name. "
                                  "Please create a new resource")
        else:
            resource = v_hlp.generate_wms_metadata(resource, layer_name, external_geoserver_wms_service, _workspace)

    if _resource_format != "wms" and resource.get('odm_geoserver_layer_name', ''):
        raise ValidationError(["You have given Layer Name but file format is not wms."
                               "Please select format as wms if you are trying to add wms layer."])

    return resource


def update_resource_given_wms_layer(current, resource):
    """
    Update wms resource
    :param current: dict existing resource (previous one)
    :param resource: new updated resource dict
    :return: dict
    """

    _del_keys = (
        "wms_layer",
        "vectorstorer_resource",
        "wms_server",
        "layer_url"
    )

    # If layer name given then update the resource
    if resource.get('odm_geoserver_layer_name', ''):
        return create_resource_given_wms_layer(resource)

    # Edge case
    # If resource format is WMS and the URL is changed from the previous resource and doesnt contain layer_name
    # This means its external WMS url. Rarely occurs but possible
    if current.get('url') != resource.get('url') and resource.get('format', '').strip().lower() == "wms":
        for _key in _del_keys:
            try:
                del resource[_key]
            except KeyError:
                pass
    return resource


def add_wms(context, data_dict):
    """
    creates a WMS resource for the given KML/SHP resource that's in the geoserver

    data dict params:
    :param id: resource id to create a WMS for
    :returns: wms resource dict
    """
    
    geoserver_url = '/geoserver'
    
    res = toolkit.get_action('resource_show')(context, {'id': data_dict['id']})
    if not res['format'] in ('KML', 'SHP') or not 'geoserver' in res['url']:
        log.warning('Issue: resource %s not in kml or shp format: %s' % (
            res['name'], res['format'] ))
        return {}
    if not geoserver_url in res['url']:
        log.warning('Issue: resource %s geoserver_url (%s) not in url: %s' % (
            res['name'],
            geoserver_url,
            res['url'] ))
        return {}


    res_url = urlparse.urlparse(res['url'])
    params = urlparse.parse_qs(res_url.query)

    wms_layer = params.get('layers', params.get('typeName',[]))[0]
    if not wms_layer: return {}

    user = toolkit.get_action('get_site_user')({'ignore_auth': True,
                                                'defer_commit': True}, {})

    geo_context = { 'package_id': res['package_id'],
                    'apikey': user['apikey'],
                    'site_url': toolkit.config['ckan.site_url'] + '/'
    }


    name = res['name']

    wms_server = toolkit.config['ckanext-vectorstorer.geoserver_url'] + "/wms"

    try:
        return add_wms_resource(geo_context, name, res, wms_server, wms_layer)
    except Exception as msg:
        log.error("Exception creating resource %s: %s, continuing"% (name, msg))


def add_wms_for_layer(context, data_dict):
    """
    creates a WMS resource for the given layer  in the geoserver

    data dict params:
    :param package_id: the package destination for the resource
    :param layer: the qualified layer name in the geoserver
    :returns: wms resource dict
    """

    pkg = toolkit.get_action('package_show')(context, {'id': data_dict['package_id']})

    wms_layer = data_dict['layer']
    if not wms_layer: return {}

    name = wms_layer

    geo_context = { 'package_id': data_dict['package_id'],
                    'apikey': context['userobj']['apikey'],
                    'site_url': toolkit.config['ckan.site_url'] + '/'
    }

    wms_server = toolkit.config['ckanext-vectorstorer.geoserver_url'] + "/wms"

    try:
        return add_wms_resource(geo_context, name, {'id':data_dict['package_id']},
                                wms_server, wms_layer)
    except Exception as msg:
        print "Exception creating resource: %s, continuing"% msg



def spatial_metadata_for_resource(context, data_dict):
    """
    Gets the WMS spatial metadata for the given WMS layer in in the geoserver
    
    data dict params:
    :param resource_id: The wms resource id -- must have a wms_layer field
    """

    resource = toolkit.get_action('resource_show')(context, {'id': data_dict['resource_id']})

    layer_url = resource.get('layer_url', None)
    if not layer_url:
        raise Exception("Could not find layer url")

    try:
        return wms.geo_metadata(wms.wms_from_url(layer_url))
    except Exception as msg:
        log.error("Exception getting layer metadata: %s" % msg)
        raise



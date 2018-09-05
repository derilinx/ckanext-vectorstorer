from ckan.plugins import toolkit

from .tasks import identify_resource, add_wms_resource

import urlparse


# update resource set url= replace(url, 'https://data2.odc.staging.derilinx.com', 'http://odt.localhost') where url like 'https://data2.odc.staging.derilinx.com/geoserver%'
# update resource set url= replace(url, 'https://data2.odc.staging.derilinx.com', 'https://data2.odt.staging.derilinx.com') where url like 'https://data2.odc.staging.derilinx.com/geoserver%'

def add_wms(context, data_dict):
    """
    creates a WMS resource for the given KML/SHP resource that's in the geoserver

    data dict params:
    :param id: resource id to create a WMS for
    :returns: wms resource dict
    """

    res = toolkit.get_action('resource_show')(context, {'id': data_dict['id']})

    if not res['format'] in ('KML', 'SHP') or not 'geoserver' in res['url']: return {}
    if not toolkit.config['ckanext-vectorstorer.geoserver_url'] in res['url']: return {}


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


    name = "Web Map Service in GeoServer"

    wms_server = toolkit.config['ckanext-vectorstorer.geoserver_url'] + "/wms"

    try:
        return add_wms_resource(geo_context, name, res, wms_server, wms_layer)
    except Exception as msg:
        print "Exception creating resource: %s, continuing"% msg


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

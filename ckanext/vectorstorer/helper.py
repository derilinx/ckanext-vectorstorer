from ckan.common import config
import requests


def vectorstore_show_resource_add_wms_forms(data):
    """
    Show wms for for the UI. Check odm_dataset_schmea.json.

    Conditions:

        -  If the resource format is wms and should not have vectorstorer_resource
        - If new resource show forms


    :param data: dict
    :return: Boolean
    """
    if not data:
        return True

    if data.get("format", '').lower().strip() == "wms" and not data.get('vectorstorer_resource'):
        return True

    return False


def vectorstore_get_workspace():
    """
    Get workspace for the given site.
    :return: str
    """

    return config['ckanext-vectorstorer.geoserver_workspace'] or ''


def generate_wms_metadata(resource, layer_name, external_geoserver_wms_service, workspace):
    """
    Generate wms layer given layer name, geoserver and work space
    :param resource:
    :param layer_name:
    :param external_geoserver_wms_service:
    :param workspace:
    :return:
    """

    if not external_geoserver_wms_service:
        layer_url = "{url}/{workspace}/{layer}/wms?service=WMS&request=GetCapabilities&layers={workspace}:{layer}".format(
            url=config['ckanext-vectorstorer.geoserver_url'],
            workspace=workspace,
            layer=layer_name
        )
        resource['wms_server'] = config['ckanext-vectorstorer.geoserver_url'] + "/wms"
        resource['wms_layer'] = "{}:{}".format(workspace, layer_name)
    else:
        layer_url = "{external_geoserver_wms_service}?service=WMS&request=GetCapabilities&layers={layer}".format(
            external_geoserver_wms_service=external_geoserver_wms_service,
            layer=layer_name
        )
        resource['wms_server'] = external_geoserver_wms_service
        resource['wms_layer'] = "{}".format(layer_name)

    resource['vectorstorer_resource'] = ""
    resource['layer_url'] = layer_url
    resource['url'] = layer_url

    return resource


def is_layer_exists(layer_name, workspace):
    """
    Check if the layer exists
    :param layer_name: str
    :param workspace: workspace
    :return: boolean
    """
    _url = generate_wms_url(layer_name, workspace)
    resp = requests.head(_url)

    if resp.status_code == 200:
        return True
    else:
        return False


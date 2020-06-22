import logging
from ckan.plugins import toolkit
from ckan.common import config
from geoserver.catalog import Catalog
from ckanext.vectorstorer import helper as v_hlp
from ckan.common import _

log = logging.getLogger(__name__)


def check_if_layer_is_valid(value):
    """
    Check if the given layer name is valid on not for a given workspace
    :param value: str
    :return: None
    """
    if value:
        log.info("Validating the layer name: ")
        log.info(value)
        _workspace = config['ckanext-vectorstorer.geoserver_workspace']
        try:

            if not v_hlp.is_layer_exists(value, _workspace):
                raise ValueError
        except Exception as e:
            log.error(e)
            raise toolkit.Invalid(_('Not a valid layer name for the workspace - {}. '
                                    'The workspace is default for the individual site. Please select'
                                    ' appropriate site to change geoserver workspace.'.format(_workspace)))
    return

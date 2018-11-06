from ckan import plugins
from ckan.plugins import toolkit
from ckan import model, logic
from ckan.lib.base import abort
from ckan.common import _
import ckan
from ckanext.vectorstorer import settings
from ckanext.vectorstorer import resource_actions

from pylons import config

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

def isInVectorStore(package_id, resource_id):
    parent_resource = {}
    parent_resource['package_id'] = package_id
    parent_resource['id'] = resource_id

    child_resources = resource_actions._get_child_resources(parent_resource)

    if len(child_resources) > 0:
        return True
    else:
        return False

def supportedFormat(format):
    return format.lower() in settings.SUPPORTED_DATA_FORMATS

def _drop_view_and_table(context, data_dict):
    log.debug("vectorstorer drop view and table")
    try:
        trans = context['connection'].begin()

        if 'filters' not in data_dict:
            log.debug("dropping view")
            context['connection'].execute(
                u'DROP view "{0}" CASCADE'.format(
                    data_dict['resource_id'])
            )
            log.debug("dropping table")
            context['connection'].execute(
                u'DROP table "{0}_tbl" CASCADE'.format(
                    data_dict['resource_id'])
            )
    except Exception as msg:
        log.debug("error dropping table and view: %s"%msg)
        trans.rollback()
        raise

def _resource_exists(self, id):
    import sqlalchemy
    ## the _tbl aliases mess up the datastore backend queries
    log.debug("vectorstorer resource exists")
    resources_sql = sqlalchemy.text(
        u'''SELECT 1 FROM "_table_metadata"
        WHERE name = :id''')
    results = self._get_read_engine().execute(resources_sql, id=id)
    res_exists = results.rowcount > 0
    return res_exists

def _wrap_backend_delete(self):
    from ckanext.datastore.backend import postgres as backend_postgres
    def _datastore_backend_delete(context, data_dict):
        log.debug("vectorstorer datastorebackend_delete")
        log.debug(data_dict)
        engine = self._get_write_engine()
        context['connection'] = engine.connect()
        backend_postgres._cache_types(context)

        trans = context['connection'].begin()
        try:
            # check if table exists
            if 'filters' not in data_dict:
                log.debug("dropping resource table")
                context['connection'].execute(
                    u'DROP TABLE "{0}" CASCADE'.format(
                        data_dict['resource_id'])
                )
            else:
                backend_postgres.delete_data(context, data_dict)

            trans.commit()
            return backend_postgres._unrename_json_field(data_dict)
        except Exception as msg:
            log.debug("error dropping table: %s"%msg)
            trans.rollback()
            _drop_view_and_table(context, data_dict)
        finally:
            context['connection'].close()
    return _datastore_backend_delete

def horrific_monkey_patch():
    log.debug('monkeypatching')
    from ckanext.datastore.backend import DatastoreBackend
    backend = DatastoreBackend.get_active_backend()
    backend.delete = _wrap_backend_delete(backend)
    backend.resource_exists = _resource_exists
    log.debug('backend: %s' % backend.delete)

class VectorStorer(plugins.SingletonPlugin):
    STATE_DELETED='deleted'

    resource_delete_action= None
    resource_update_action=None

    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IConfigurer, inherit=True)
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.IResourceUrlChange)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.IResourceController, inherit=True)

    def get_helpers(self):
        return {
            'vectorstore_is_in_vectorstore': isInVectorStore,
            'vectorstore_supported_format': supportedFormat
        }

    def configure(self, config):
        ''' Extend the resource_delete action in order to get notification of deleted resources'''
        horrific_monkey_patch()

        if self.resource_delete_action is None:

            resource_delete = toolkit.get_action('resource_delete')

            @logic.side_effect_free
            def new_resource_delete(context, data_dict):
                resource=ckan.model.Session.query(model.Resource).get(data_dict['id'])
                self.notify(resource,model.domain_object.DomainObjectOperation.deleted)
                res_delete = resource_delete(context, data_dict)

                return res_delete
            logic._actions['resource_delete'] = new_resource_delete
            self.resource_delete_action=new_resource_delete

        ''' Extend the resource_update action in order to pass the extra keys to vectorstorer resources
        when they are being updated'''
        if self.resource_update_action is None:

            resource_update = toolkit.get_action('resource_update')

            @logic.side_effect_free
            def new_resource_update(context, data_dict):
                log.debug("new_resource_update: vectorstorer %s" % (data_dict['id']))
                resource=ckan.model.Session.query(model.Resource).get(data_dict['id']).as_dict()
                if resource.has_key('vectorstorer_resource'):
                    if resource['format'].lower()==settings.WMS_FORMAT:
                        data_dict['parent_resource_id']=resource['parent_resource_id']
                        data_dict['vectorstorer_resource']=resource['vectorstorer_resource']
                        data_dict['wms_server']=resource['wms_server']
                        data_dict['wms_layer']=resource['wms_layer']
                    if resource['format'].lower()==settings.DB_TABLE_FORMAT:
                        data_dict['vectorstorer_resource']=resource['vectorstorer_resource']
                        data_dict['parent_resource_id']=resource['parent_resource_id']
                        data_dict['geometry']=resource['geometry']

                    if  not data_dict['url']==resource['url']:
                        abort(400 , _('You cant upload a file to a '+resource['format']+' resource.'))
                res_update = resource_update(context, data_dict)

                return res_update
            logic._actions['resource_update'] = new_resource_update
            self.resource_update_action=new_resource_update

    def before_map(self, map):
        map.connect('style', '/dataset/{id}/resource/{resource_id}/style/{operation}',
            controller='ckanext.vectorstorer.controllers.style:StyleController',
            action='style', operation='operation')
        map.connect('export', '/dataset/{id}/resource/{resource_id}/export/{operation}',
            controller='ckanext.vectorstorer.controllers.export:ExportController',
            action='export',operation='{operation}')
        map.connect('search_epsg', '/api/search_epsg',
            controller='ckanext.vectorstorer.controllers.export:ExportController',
            action='search_epsg')
        map.connect('publish', '/api/vector/publish',
            controller='ckanext.vectorstorer.controllers.vector:VectorController',
            action='publish')

        return map

    def update_config(self, config):

        toolkit.add_public_directory(config, 'public')
        toolkit.add_template_directory(config, 'templates')
        toolkit.add_resource('public', 'ckanext-vectorstorer')

    def notify(self, entity, operation=None):

        if isinstance(entity, model.resource.Resource):
            if operation==model.domain_object.DomainObjectOperation.new and entity.format.lower() in settings.SUPPORTED_DATA_FORMATS:
                #A new vector resource has been created
                #resource_actions.create_vector_storer_task(entity)
                resource_actions.identify_resource(entity)
            #elif operation==model.domain_object.DomainObjectOperation.deleted:
                ##A vectorstorer resource has been deleted
                #resource_actions.delete_vector_storer_task(entity.as_dict())

            #elif operation is None:
                ##Resource Url has changed

                #if entity.format.lower() in settings.SUPPORTED_DATA_FORMATS:
                    ##Vector file was updated

                    #resource_actions.update_vector_storer_task(entity)

                #else :
                    ##Resource File updated but not in supported formats

                    #resource_actions.delete_vector_storer_task(entity.as_dict())

        elif isinstance(entity, model.Package):
            if entity.state==self.STATE_DELETED:
                resource_actions.pkg_delete_vector_storer_task(entity.as_dict())

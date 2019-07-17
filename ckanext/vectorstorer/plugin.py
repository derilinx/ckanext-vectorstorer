from ckan import plugins
from ckan.plugins import SingletonPlugin, implements, toolkit
from ckan import model,logic
from ckan.lib.base import abort
from ckan.common import _
import ckan
from . import settings, resource_actions, actions


from ckan.common import config

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

class VectorStorer(SingletonPlugin):
    STATE_DELETED='deleted'

    resource_delete_action= None
    resource_update_action=None

    implements(plugins.IRoutes, inherit=True)
    implements(plugins.IConfigurer, inherit=True)
    implements(plugins.IConfigurable, inherit=True)
    implements(plugins.IResourceUrlChange)
    implements(plugins.ITemplateHelpers)
    implements(plugins.IDomainObjectModification, inherit=True)
    implements(plugins.IActions)

    def get_helpers(self):
        return {
            'vectorstore_is_in_vectorstore': isInVectorStore,
            'vectorstore_supported_format': supportedFormat
        }

    def configure(self, config):
        ''' Extend the resource_delete action in order to get notification of deleted resources'''
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
        map.connect('vectorstorer_style', '/dataset/{id}/resource/{resource_id}/style/{action}',
                    controller='ckanext.vectorstorer.controllers.style:StyleController')
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

            elif operation is None:
                ##Resource Url has changed

                if entity.format.lower() in settings.SUPPORTED_DATA_FORMATS:
                    #Vector file was potentially updated
                    # is there an existing DB_TABLE or WMS layer?

                    resource_actions.update_vector_storer_task(entity)

                #else :
                    ##Resource File updated but not in supported formats

                    #resource_actions.delete_vector_storer_task(entity.as_dict())

        elif isinstance(entity, model.Package):

            if entity.state==self.STATE_DELETED:

                resource_actions.pkg_delete_vector_storer_task(entity.as_dict())

    #IActions
    def get_actions(self):

        return {
            'vectorstorer_add_wms': actions.add_wms,
            'vectorstorer_add_wms_for_layer': actions.add_wms_for_layer,
            'vectorstorer_spatial_metadata_for_resource': actions.spatial_metadata_for_resource,
        }

import ckan.lib.helpers as h
from ckan.lib.dictization.model_dictize import resource_dictize
from ckan.logic import get_action
import ckan.lib.jobs as jobs
from ckan import model, logic
from ckan.lib.base import abort
from ckan.common import _
import json
import ckan
from ckan.common import config
from ckanext.vectorstorer import settings
from ckanext.vectorstorer import tasks
from ckanext.publicamundi.model.resource_identify import ResourceIdentify
import itertools

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

logging.getLogger('ckan.lib.jobs').setLevel(logging.DEBUG)
logging.getLogger('pysolr').setLevel(logging.ERROR)
logging.getLogger('repoze.who').setLevel(logging.ERROR)

def _get_site_url():
    try:
        return h.url_for_static('/', qualified=True)
    except AttributeError:
        return config.get('ckan.site_url', '')


def _get_site_user():
    user = get_action('get_site_user')({'model': model,
     'ignore_auth': True,
     'defer_commit': True}, {})
    return user


def identify_resource(resource_obj):
    user_api_key =  _get_site_user()['apikey']
    res_dict = resource_dictize(resource_obj, {'model': model})
    resource=resource_obj.as_dict()

    '''With resource_dictize we get the correct resource url even if dataset is in draft state   '''

    resource['url']=res_dict['url']

    data = json.dumps(resource)
    job = jobs.enqueue(tasks.identify_resource, [data,user_api_key])

    #res_identify = ResourceIdentify(job.id, resource['id'], None)
    #ckan.model.Session.add(res_identify)


def get_geoserver_context():
    geoserver_context = json.dumps({'geoserver_url': config['ckanext-vectorstorer.geoserver_url'],
     'geoserver_workspace': config['ckanext-vectorstorer.geoserver_workspace'],
     'geoserver_admin': config['ckanext-vectorstorer.geoserver_admin'],
     'geoserver_password': config['ckanext-vectorstorer.geoserver_password'],
     'geoserver_ckan_datastore': config['ckanext-vectorstorer.geoserver_ckan_datastore']})
    return geoserver_context

def get_context(additional=None):
    user = _get_site_user()
    context = {
     'site_url': _get_site_url(),
     'apikey': user.get('apikey'),
     'site_user_apikey': user.get('apikey'),
     'user': user.get('name'),
     'db_params': config['ckan.datastore.write_url']}
    if additional:
        context.update(additional)
    return json.dumps(context)

def create_vector_storer_task(resource, extra_params = None):
    res_dict = resource.as_dict()
    resource_package_id = res_dict['package_id']
    extra_items = {'package_id': resource_package_id}
    if extra_params:
        extra_items.update(extra_params)
    context = get_context(extra_items)
    geoserver_context = get_geoserver_context()
    data = json.dumps(resource_dictize(resource, {'model': model}))
    log.debug('create vectorstore task, pkg: %s, resource: %s, format: %s',
              resource_package_id, res_dict['id'], res_dict['format'])
    try:
        child_resources = get_child_resources(res_dict)
        if len(child_resources):
            log.error("create vectorstore task: child resources found, bailing.")
            return
    except Exception as msg:
        log.error("create vectorstore task: exception checking for children", msg)

    jobs.enqueue(tasks.vectorstorer_upload, [geoserver_context, context, data])


def update_vector_storer_task(resource):
    res_dict = resource.as_dict()
    resource_package_id = res_dict['package_id']
    resource_list_to_update = get_child_resources(res_dict)
    log.debug('update vectorstore task: pkg: %s, resource: %s, format: %s child_resources: %s',
              resource_package_id, res_dict['id'], res_dict['format'], resource_list_to_update)
    if len(resource_list_to_update) < 2:
        log.debug('not updating, not enough resources to update')
        return
    elif len(resource_list_to_update) > 2:
        log.error("Duplicated WMS/DBTable, too many resources, continuing")

    context = get_context({'resource_list_to_update': resource_list_to_update,
                           'package_id': resource_package_id})
    geoserver_context =  get_geoserver_context()
    data = json.dumps(resource_dictize(resource, {'model': model}))
    jobs.enqueue(tasks.vectorstorer_update, [geoserver_context, context, data])


def delete_vector_storer_task(resource, pkg_delete = False):
    data = None
    resource_list_to_delete = None
    if resource['format'] in (settings.WMS_FORMAT, settings.DB_TABLE_FORMAT) and resource.has_key('vectorstorer_resource'):
        data = json.dumps(resource)
        if pkg_delete:
            resource_list_to_delete = get_child_resources(resource)
    else:
        data = json.dumps(resource)
        resource_list_to_delete = get_child_resources(resource)
    context = get_context({'resource_list_to_delete': resource_list_to_delete})
    geoserver_context = get_geoserver_context()
    jobs.enqueue(tasks.vectorstorer_delete, [geoserver_context, context, data])
    if resource.has_key('vectorstorer_resource') and not pkg_delete:
        _delete_child_resources(resource)


def _temp_context():
    return {'model': ckan.model,
            'user': _get_site_user().get('name')}

def _delete_child_resources(parent_resource):
    for child_resource in get_child_resources(parent_resource):
        action_result = logic._actions['resource_delete'](_temp_context(), {'id': child_resource })

def get_child_resources(parent_resource):
    package = get_action('package_show')(_temp_context(), {'id': parent_resource['package_id']})
    # Parent resource ids are actually a tree. Orig->DbTable->WMS
    # These might be in a different order, so shake the tree twice to find
    # all the children of this resource
    ids = {parent_resource['id']}
    for r in itertools.chain(package['resources'], package['resources']):
        if r.get('parent_resource_id','') in ids:
            ids.add(r['id'])
    ids.remove(parent_resource['id'])
    return list(ids)


def pkg_delete_vector_storer_task(package):
    user = _get_site_user()
    context = {'model': ckan.model,
     'session': ckan.model.Session,
     'user': user.get('name')}
    resources = package['resources']
    for res in resources:
        if res.has_key('vectorstorer_resource') and res['format'] == settings.DB_TABLE_FORMAT:
            res['package_id'] = package['id']
            delete_vector_storer_task(res, True)

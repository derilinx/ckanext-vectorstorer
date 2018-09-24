import os
from xml.dom import minidom
from pylons import config
from geoserver.catalog import Catalog
from ckan.lib.base import BaseController, c, request, \
                          response, session, render, config, abort
from ckan.plugins import toolkit

from geoserver.catalog import UploadError
from ckan.logic import *
from ckan.common import _
_check_access = check_access

NoFileSelected='No XML file was selected.'

redirect = toolkit.redirect_to

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class NotVectorStorerWMS(Exception):
    pass

class StyleController(BaseController):

    def upload_sld(self,id,resource_id,operation):
        if operation:
            self._get_context(id,resource_id)
            if operation.lower()=='show':
                pass
            elif operation.lower()=='upload':
                sld_file_param = request.POST['sld_file']
                try:
                    fileExtension = os.path.splitext(sld_file_param.filename)[1]
                    if fileExtension.lower()==".xml":
                        sld_file=sld_file_param.file
                        c.sld_body=sld_file.read()
                    else:
                        raise AttributeError
                except AttributeError:
                   c.error=NoFileSelected
            elif operation.lower()=='submit':
                sld_body = request.POST['sld_body']
                self._submit_sld(sld_body)
            return render('style/upload_sld_form.html')
        else:
            abort(404, _('Resource not found'))

    def show(self, id, resource_id):
        self._get_context(id, resource_id)
        c.sld_body=self._get_layer_style(resource_id)
        log.debug(c.sld_body)
        return render('style/edit_sld_form.html')

    def edit(self, id, resource_id):
        self._get_context(id, resource_id)
        sld_body = request.POST.get('sld_body', '')
        if not sld_body:
            abort(409, 'ValidationError, sld_body required in POST')
        self._submit_sld(sld_body)
        return render('style/edit_sld_form.html')

    def _get_catalog(self):
        geoserver_url=config['ckanext-vectorstorer.geoserver_url']
        geoserver_admin = config['ckanext-vectorstorer.geoserver_admin']
        geoserver_password = config['ckanext-vectorstorer.geoserver_password']
        return Catalog(geoserver_url + '/rest', username=geoserver_admin, password=geoserver_password)

    def _get_layer_style(self,resource_id):
        cat = self._get_catalog()
        layer = cat.get_layer(c.layer_id)
        if not layer:
            abort(404, "Could not retrieve layer")
        default_style=layer._get_default_style()
        xml =  minidom.parseString(default_style.sld_body)
        return xml.toprettyxml()

    def _get_context(self,id,resource_id):
        context = {'model': model, 'session': model.Session,
                   'user': c.user}

        try:
            _check_access('package_update',context, {'id':id })
            c.resource = get_action('resource_show')(context,
                                                     {'id': resource_id})
            c.package = get_action('package_show')(context, {'id': id})
            c.pkg = context['package']
            c.pkg_dict = c.package
            if c.resource.has_key('vectorstorer_resource') and c.resource['format'].lower()=='wms':
                   c.layer_id=c.resource['wms_layer']
            else:
                raise NotVectorStorerWMS
        except NotVectorStorerWMS:
            abort(400, _('Resource is not WMS VectorStorer resource'))
        except NotFound:
            abort(404, _('Resource not found'))
        except NotAuthorized:
            abort(401, _('Unauthorized to read resource %s') % id)

    def _submit_sld(self,sld_body):
        try:
            geoserver_url=config['ckanext-vectorstorer.geoserver_url']
            cat = self._get_catalog()
            layer = cat.get_layer(c.layer_id)
            default_style=layer._get_default_style()
            if default_style.name ==c.layer_id:
                cat.create_style(default_style.name, sld_body, overwrite=True)
            else:
                cat.create_style(c.layer_id, sld_body, overwrite=True)
                layer._set_default_style(c.layer_id)
                cat.save(layer)

            c.success=True

        except UploadError, e:
            c.sld_body=sld_body
            c.error=e

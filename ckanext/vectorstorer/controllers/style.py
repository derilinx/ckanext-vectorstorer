import os
from lxml import etree
from ckan.common import config
from geoserver.catalog import Catalog
from ckan.lib.base import BaseController, c, request, \
                          response, session, render, config, abort
from ckan.plugins import toolkit
from ckan import logic
from .. import settings

from geoserver.catalog import UploadError
from ckan.logic import *
from ckan.common import _
_check_access = check_access

NoFileSelected='No XML file was selected.'

redirect = toolkit.redirect_to

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized

class NotVectorStorerWMS(Exception):
    pass

class StyleController(BaseController):
    def create_form(self, id, resource_id):
        self._get_context(id, resource_id)
        return render('style/upload_sld_form.html')

    def create(self, id, resource_id):
        self._get_context(id, resource_id)
        sld_file_param = request.POST['sld_file']
        try:
            fileExtension = os.path.splitext(sld_file_param.filename)[1]
            if fileExtension.lower() in (".xml", ".sld"):
                sld_file=sld_file_param.file
                c.sld_body=sld_file.read().decode('utf-8')
            else:
                raise AttributeError
        except AttributeError:
            c.error=NoFileSelected
        return render('style/upload_sld_form.html')

    def show(self, id, resource_id):
        self._get_context(id, resource_id)
        # remove lines of whitespace
        body = '\n'.join([s for s in self._get_layer_style(resource_id).split('\n') if s.strip()])
        # log.debug(body)
        c.sld_body = body
        return render('style/edit_sld_form.html')

    def edit(self, id, resource_id):
        self._get_context(id, resource_id)
        sld_body = request.POST.get('sld_body', '')
        if not sld_body:
            c.error = _('Validation Error:')+ " " +_('SLD Style required')
            return render('style/edit_sld_form.html')
        try:
            sld_body = self._xml_pp(sld_body)
        except:
            c.error = _('Validation Error:') + " " +_('XML style is not valid')
            c.sld_body = sld_body
        else:
            self._submit_sld(sld_body)
        finally:
            return render('style/edit_sld_form.html')

    def _xml_pp(self, style):
        parser = etree.XMLParser(remove_blank_text=True)
        xml =  etree.fromstring(style, parser)
        return etree.tostring(xml, pretty_print=True)

    def _get_layer_style(self,resource_id):
        cat = self._get_catalog()
        layer = cat.get_layer(c.layer_id)
        log.debug(layer)
        if not layer:
            abort(404, "Could not retrieve layer")
        default_style=layer._get_default_style()
        # Need to encode/decode the xml, as xml is actually a bytestream,
        # and at the submit end, we need to convert to a utf-8 bytestream.
        # pretty printing with a better parser
        try:
            return self._xml_pp(default_style.sld_body)
        except Exception as msg:
            # if it's not really valid XML
            log.error("Exception parsing style xml: %s", msg)
            try:
                return default_style.sld_body.decode('utf-8')
            except Exception as msg:
                # If it's not really UTF-8
                log.error("Exception decoding UTF8: %s", msg)
                return "<!-- Style is corrypt, please replace --!>"
 
    def _get_catalog(self):
        geoserver_url=config['ckanext-vectorstorer.geoserver_url']
        geoserver_admin = config['ckanext-vectorstorer.geoserver_admin']
        geoserver_password = config['ckanext-vectorstorer.geoserver_password']
        return Catalog(geoserver_url + '/rest', username=geoserver_admin, password=geoserver_password)
    
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

    def _submit_sld(self, sld_body):
        log.debug('_submit_sld: submitting sld for %s' % c.layer_id)
        try:
            geoserver_url=config['ckanext-vectorstorer.geoserver_url']
            cat = self._get_catalog()
            layer = cat.get_layer(c.layer_id)
            try:
                workspace, layer_name = c.layer_id.split(':')
            except:
                layer_name = c.layer_id
                workspace = None
            log.debug('checking default style for: %s' % layer_name)
            default_style=layer._get_default_style()
            log.debug('default style: %s' % default_style)
            log.debug('default style name: %s' % default_style.name)
            if default_style.name == layer_name:
                log.debug('is default style, updating default style')
                # need to be encoding here.
                cat.create_style(default_style.name, sld_body.encode('utf-8'), overwrite=True, workspace=workspace, raw=True)
            else:
                log.debug('creating a style for layer')
                # need to be encoding here.
                cat.create_style(layer_name, sld_body.encode('utf-8'), overwrite=True, workspace=workspace, raw=True)
                log.debug('setting the default style')
                layer._set_default_style(layer_name)
                log.debug('saving layer')
                cat.save(layer)

            c.success=True

        except UploadError, e:
            c.sld_body=sld_body
            c.error=e


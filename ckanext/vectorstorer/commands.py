from ckan.lib.cli import CkanCommand
from ckan.plugins import toolkit

import json
import sys
import csv

import tasks
import wms
from . import settings, resource_actions

class VectorStorer(CkanCommand):

    """
    Usage::
        paster vectorstorer add_wms [package_id]
           - Creates wms resources for existing geoserver shp/kml resources
        paster vectorstorer add_wms_for_layer package_id layer
           - Creates wms resources for existing geoserver layer
        paster vectorstorer add_wms_from_csv /path/to/csv
           - Creates wms resources for the csv, each line is package_id, layer
        paster vectorstorer add_datasets_from_json path/to/json
           - Creates datasets for existing geoserver layers.
        paster vectorstorer add_gwc_layers
           - Creates gwc caching layers for all of the existing geoserver layers.
    """

    """
    paster --plugin=ckanext-vectorstorer vectorstorer add_wms
    1  paster --plugin=ckanext-vectorstorer vectorstorer add_wms_for_layer post-office-2018 "ODCambodia:Cambodia_post_office"
    2  paster --plugin=ckanext-vectorstorer vectorstorer add_wms_for_layer post-office-2018 "ODCambodia:Cambodia_post_office_kh"
"""

    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 0

    def command(self):
        self._load_config()

        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print self.__doc__
            return

        cmd = self.args[0]
        if cmd == "add_wms":
            self.add_wms(*self.args[1:])
        elif cmd == "add_wms_for_datastore":
            self.add_wms_for_datastore(*self.args[1:])            
        elif cmd == "add_wms_for_layer":
            self.add_wms_for_layer(*self.args[1:])
        elif cmd == "add_wms_from_csv":
            self.add_wms_from_csv(*self.args[1:])
        elif cmd == "add_datasets_from_json":
            self.add_datasets_from_json(*self.args[1:])
        elif cmd == "add_gwc_layers":
            self.add_gwc_layers(*self.args[1:])

        else:
            print self.__doc__
            return


    def add_wms(self, *args):
        #geoserver_url = toolkit.config['ckanext-vectorstorer.geoserver_url']
        geoserver_url = '/geoserver'
        user = toolkit.get_action('get_site_user')({'ignore_auth': True,
                                                    'defer_commit': True}, {})

        context = {'userobj': user, 'user': user['name']}
        if len(args):
            packages = [toolkit.get_action('package_show')(context, {'id':args[0]})]
        else:
            packages = toolkit.get_action('package_search')(context, {'q':'kml|shp|GeoJSON', 'rows':1000})['results']
            pkg_dict = dict([(p['id'], p) for p in packages])

        for pkg in pkg_dict.values():
            print("%s, %s resources" %(pkg['name'], len(pkg['resources'])))

            if any(res['format'] == settings.WMS_FORMAT for res in pkg['resources']):
                print("found wms, continuing")
                continue

            for res in pkg['resources']:
                #print res['format'], res['url']
                if (res['format'] in ('KML', 'SHP') and geoserver_url in res['url']):
                    print("Adding WMS for %s: %s %s" % (res['format'], pkg['name'], res['id']))
                    new_resource = toolkit.get_action('vectorstorer_add_wms')(context,{'id':res['id']})
                    if new_resource:
                        print("Added new resource: %s" % new_resource['name'])                        
                        break
                    else:
                        print("Didn't save a new resource, continuing")
                if res['format'] in ('GeoJSON', 'KML'):
                    print("Adding new Remote GeoJSON/KML resource")
                    tasks.vectorstorer_upload(resource_actions.get_geoserver_context(),
                                              resource_actions.get_context({'package_id':res['package_id']}),
                                              json.dumps(res))
                    

    def add_wms_for_datastore(self, *args):
        pkg_id = args[0]
        res_id = args[1]

        context = json.loads(resource_actions.get_context())
        
        pkg = toolkit.get_action('package_show')(context, {'name_or_id': pkg_id})
        res = toolkit.get_action('resource_show')(context, {'id': res_id})
        context['package_id'] = pkg['id']
        
        geoserver_context = json.loads(resource_actions.get_geoserver_context())

        tasks.add_wms(context, geoserver_context, res, 4326, pkg['name'])
                    
    def add_wms_for_layer(self, *args):
        user = toolkit.get_action('get_site_user')({'ignore_auth': True,
                                                    'defer_commit': True}, {})

        context = {'userobj': user, 'user': user['name']}
        try:
            package = toolkit.get_action('package_show')(context, {'id':args[0]})
        except toolkit.ObjectNotFound:
            print "Package %s not found" % args[0]
            raise


        if not package:
            print("Package not found")
            return 2

        new_resource = toolkit.get_action('vectorstorer_add_wms_for_layer')(context,
                                                                   {'package_id': args[0],
                                                                    'layer': args[1]})
        print(new_resource)


    def add_wms_from_csv(self, *args):
        try:
            path = args[0]
            with open(path, 'r') as f:
                reader = csv.reader(f)

                for row in reader:
                    try:
                        print("Adding wms:  %s -- %s" % (row[0], row[1]))
                        #import pdb; pdb.set_trace()
                        self.add_wms_for_layer(row[0].strip(), row[1].strip())
                    except Exception as msg:
                        print("Error adding layer: %s" % msg)

        except Exception as msg:
            print("Couldn't read input file: %s" % msg)
            sys.exit(4)


    def add_datasets_from_json(self, *args):
        from ckan import model

        with open(self.args[1], 'r') as f:
            datasets = json.load(f)

        package_create = toolkit.get_action('package_create')
        package_show = toolkit.get_action('package_show')

        context = toolkit.get_action('get_site_user')({},{})
        orgs = dict([(d['display_name'].lower(), d)
                     for d in toolkit.get_action('organization_list')(context, {'all_fields': True})])

        for dataset in datasets:
            # need to get the site user each time, otherwise the session is borky and only one dataset is
            # created.
            context = toolkit.get_action('get_site_user')({},{})

            org_name = dataset['org_name'].lower()
            if org_name == 'none':
                org_name = "Open Development Mekong".lower()
            if not org_name in orgs:
                print "Exception finding org: %s for dataset %s, continuing." %(org_name, dataset['title'])
                print "\n".join(orgs.keys())
                continue

            resources = dataset['resources']
            del(dataset['resources'])
            dataset['owner_org'] = orgs[org_name]['id']

            try:
                pkg = package_show(context, {'id': dataset['name']})
            except:
                try:
                    pkg = package_create(context, dataset)
                    model.Session.commit()
                    print "Created package id: %s for %s" % (pkg['id'], pkg['title'])
                except Exception as msg:
                    print "Exception creating package %s: %s" % (dataset['name'], msg)
                    continue

            if not pkg:
                continue

            existing_layers = [r['wms_layer'] for r in pkg['resources'] if r.get('wms_layer')]

            # clear existing layers ...  Dangerous, but useful
            #if existing_layers:
            #    context = toolkit.get_action('get_site_user')({},{})
            #    toolkit.get_action('package_patch')(context, {'id': pkg['id'],
            #                                                  'resources': []})

            for r in resources:
                pkg_name = r[0].strip()
                layer = r[1].strip()
                #print layer, existing_layers, layer in existing_layers
                #raise Exception()
                if (layer in existing_layers):
                    continue
                print("Adding wms:  %s -- %s" % (pkg_name, layer))
                try:
                    self.add_wms_for_layer(pkg_name, layer)
                except Exception as msg:
                    print "Exception adding resources to package %s: %s" % (dataset['name'], msg)
                    import inspect; print inspect.trace()
                    continue

    def add_gwc_layers(self, *args):
        # https://data.odm-eu.staging.derilinx.com/geoserver/wms?service=WMS&version=1.1.0&request=GetCapabilities

        print("Getting wms doc")
        wms_doc = tasks.get_wms()
        for layer in wms.layers(wms_doc):
            try:
                print("Adding GWC for %s" % wms.name_for_layer(layer))
                tasks.add_geowebcache_layer(wms.name_for_layer(layer))
            except Exception as msg:
                print msg

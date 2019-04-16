from ckan.lib.cli import CkanCommand
from ckan.plugins import toolkit

import sys
import csv

class VectorStorer(CkanCommand):

    """
    Usage::
        paster vectorstorer add_wms [package_id]
           - Creates wms resources for existing geoserver shp/kml resources
        paster vectorstorer add_wms_for_layer package_id layer
           - Creates wms resources for existing geoserver layer
        paster vectorstorer add_wms_for_csv /path/to/csv
           - Creates wms resources for the csv, each line is package_id, layer
 
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
        elif cmd == "add_wms_for_layer":
            self.add_wms_for_layer(*self.args[1:])
        elif cmd == "add_wms_from_csv":
            self.add_wms_from_csv(*self.args[1:])
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
            packages = toolkit.get_action('package_search')(context, {'q':'kml|shp', 'rows':1000})['results']
            pkg_dict = dict([(p['id'], p) for p in packages])

        for pkg in pkg_dict.values():
            print("%s, %s resources" %(pkg['name'], len(pkg['resources'])))

            if any(res['format'] =='WMS' for res in pkg['resources']):
                print("found wms, continuing")
                continue

            for res in pkg['resources']:
                #print res['format'], res['url']
                if res['format'] in ('KML', 'SHP') and geoserver_url in res['url']:
                    print("Adding WMS for %s %s" % (pkg['name'], res['id']))
                    new_resource = toolkit.get_action('vectorstorer_add_wms')(context,{'id':res['id']})
                    print("Added new resource: %s" % new_resource['name'])
                    break

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

                

        
                
        
    

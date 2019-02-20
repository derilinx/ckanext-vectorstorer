from ckan.lib.cli import CkanCommand
from ckan.plugins import toolkit

class VectorStorer(CkanCommand):

    """
    Usage::
        paster vectorstorer add_wms [package_id]
           - Creates wms resources for existing geoserver shp/kml resources
        paster vectorstorer add_wms_for_layer package_id layer
           - Creates wms resources for existing geoserver layer
    """

    """
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



    def add_wms(self, *args):

        user = toolkit.get_action('get_site_user')({'ignore_auth': True,
                                                    'defer_commit': True}, {})

        context = {'userobj': user, 'user': user['name']}
        if len(args):
            packages = [toolkit.get_action('package_show')(context, {'id':args[0]})]
        else:
            packages = toolkit.get_action('package_search')(context, {'q':'kml|shp', 'rows':1000})['results']
            pkg_dict = dict([(p['id'], p) for p in packages])

        for pkg in pkg_dict.values():
            print "%s, %s resources" %(pkg['name'], len(pkg['resources']))

            if any(res['format'] =='WMS' for res in pkg['resources']):
                print("found wms, continuing")
                continue

            for res in pkg['resources']:
                print res['format'], res['url']
                if res['format'] in ('KML', 'SHP') and 'geoserver' in res['url']:
                    print "Adding WMS for %s" % res['id']
                    print toolkit.get_action('vectorstorer_add_wms')(context,{'id':res['id']})
                    break

    def add_wms_for_layer(self, *args):
        user = toolkit.get_action('get_site_user')({'ignore_auth': True,
                                                    'defer_commit': True}, {})

        context = {'userobj': user, 'user': user['name']}
        package = toolkit.get_action('package_show')(context, {'id':args[0]})

        if not package:
            print "Package not found"
            return 2

        print toolkit.get_action('vectorstorer_add_wms_for_layer')(context,
                                                                   {'package_id': args[0],
                                                                    'layer': args[1]})
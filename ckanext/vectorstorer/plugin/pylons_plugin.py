from ckan.plugins import SingletonPlugin, implements, toolkit


class VectorStorerMixinPlugin(SingletonPlugin):
    implements(plugins.IRoutes, inherit=True)

    def before_map(self, map):
        map.connect('vectorstorer_style', '/dataset/{id}/resource/{resource_id}/style/{action}',
                    controller='ckanext.vectorstorer.controller:StyleController')
        map.connect('export', '/dataset/{id}/resource/{resource_id}/export/{operation}',
                    controller='ckanext.vectorstorer.controller:ExportController',
                    action='export',operation='{operation}')
        map.connect('search_epsg', '/api/search_epsg',
                    controller='ckanext.vectorstorer.controller:ExportController',
                    action='search_epsg')
        map.connect('publish', '/api/vector/publish',
                    controller='ckanext.vectorstorer.controller:VectorController',
                    action='publish')

        return map

from ckan.plugins import SingletonPlugin, implements, toolkit
from ckanext.vectorstorer.views import vecorstorer_views


class VectorStorerMixinPlugin(SingletonPlugin):
    implements(plugins.IBlueprint)

    def get_blueprint(self):
        return [vecorstorer_views]

from ckan.plugins import SingletonPlugin, implements, toolkit, IBlueprint
from ckanext.vectorstorer.views import vecorstorer_views


class VectorStorerMixinPlugin(SingletonPlugin):
    implements(IBlueprint)

    def get_blueprint(self):
        return [vecorstorer_views]

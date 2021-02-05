from ckan.lib.base import BaseController
from ckanext.vectorstorer import utils


class StyleController(BaseController):

    def create_form(self, id, resource_id):
        return utils.style.create_form(id, resource_id)

    def create(self, id, resource_id):
        return utils.style.create(id, resource_id)

    def show(self, id, resource_id):
        return utils.style.show(id, resource_id)

    def edit(self, id, resource_id):
        return utils.style.edit(id, resource_id)


class ExportController(BaseController):

    def export(self, id, resource_id, operation):
        return utils.export.export(id, resource_id, operation)

    def search_epsg(self):
        return utils.export.search_epsg()


class VectorController(BaseController):

    def publish(self):
        return utils.vector.publish()

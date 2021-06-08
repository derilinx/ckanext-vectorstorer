from flask import Blueprint
from ckanext.vectorstorer.utils import style, export, vector
import logging

log = logging.getLogger(__name__)

vecorstorer_views = Blueprint("vectorstorer", __name__)


def vectorstorer_style_action(id, resource_id, action):

    _allowed_action = (
        "create_form",
        "create",
        "show",
        "edit"
    )
    if action not in _allowed_action:
        toolkit.abort(404, "Given url not found")

    return getattr(style, action)(id, resource_id)


vecorstorer_views.add_url_rule(
    "/dataset/<id>/resource/<resource_id>/style/<action>", methods=["GET", "POST"], view_func=vectorstorer_style_action,
)
vecorstorer_views.add_url_rule(
    "/dataset/<id>/resource/<resource_id>/export/<operation>", methods=["GET", "POST"], view_func=export.export,
)
vecorstorer_views.add_url_rule(
    "/api/search_epsg", methods=["GET"], view_func=export.search_epsg,
)
vecorstorer_views.add_url_rule(
    "/api/vector/publish", methods=["GET"], view_func=vector.publish,
)

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from teelo.web.services import legacy_main_handlers as legacy
from teelo.web.services import prediction_handlers as pred

router = APIRouter()
router.add_api_route('/admin/login', legacy.admin_login_page, methods=['GET'], response_class=legacy.HTMLResponse)
router.add_api_route('/admin/login', legacy.admin_login_submit, methods=['POST'], response_class=legacy.HTMLResponse)
router.add_api_route('/admin/logout', legacy.admin_logout, methods=['POST'])
router.add_api_route('/admin', legacy.admin_home, methods=['GET'], response_class=legacy.HTMLResponse)
router.add_api_route('/admin/invalid-matches', legacy.admin_invalid_matches, methods=['GET'], response_class=legacy.HTMLResponse)
router.add_api_route('/admin/invalid-matches/{match_id}/update', legacy.admin_invalid_match_update, methods=['POST'])
router.add_api_route('/admin/invalid-matches/{match_id}/delete', legacy.admin_invalid_match_delete, methods=['POST'])
router.add_api_route('/admin/duplicates', legacy.admin_duplicates_queue, methods=['GET'], response_class=legacy.HTMLResponse)
router.add_api_route('/admin/duplicates/{review_id}/match', legacy.admin_duplicate_match, methods=['POST'])
router.add_api_route('/admin/duplicates/{review_id}/create', legacy.admin_duplicate_create, methods=['POST'])
router.add_api_route('/admin/duplicates/{review_id}/ignore', legacy.admin_duplicate_ignore, methods=['POST'])
router.add_api_route('/admin/sql/execute', legacy.admin_sql_execute, methods=['POST'])
router.add_api_route('/admin/sql/schema', legacy.admin_sql_schema, methods=['GET'])
router.add_api_route('/admin/sql/recent', legacy.admin_sql_recent, methods=['GET'])
router.add_api_route('/admin/sql', legacy.admin_sql_editor, methods=['GET'], response_class=legacy.HTMLResponse)
router.add_api_route('/admin/predictions', pred.admin_predictions_page, methods=['GET'], response_class=HTMLResponse)
router.add_api_route('/admin/api/predictions/summary', pred.admin_predictions_summary, methods=['GET'])
router.add_api_route('/admin/api/predictions/breakdown', pred.admin_predictions_breakdown, methods=['GET'])
router.add_api_route('/admin/api/predictions/charts/accuracy', pred.admin_predictions_charts_accuracy, methods=['GET'])
router.add_api_route('/admin/api/predictions/charts/calibration', pred.admin_predictions_charts_calibration, methods=['GET'])
router.add_api_route('/admin/api/predictions/charts/distribution', pred.admin_predictions_charts_distribution, methods=['GET'])
router.add_api_route('/admin/activity-log', legacy.admin_activity_log, methods=['GET'], response_class=legacy.HTMLResponse)

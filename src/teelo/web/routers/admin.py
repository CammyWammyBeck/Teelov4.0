from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from teelo.web.services import main_handlers as handlers
from teelo.web.services import prediction_handlers as pred
from teelo.web.services import tweet_activity_handlers as tweet_activity

router = APIRouter()
router.add_api_route('/admin/login', handlers.admin_login_page, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route('/admin/login', handlers.admin_login_submit, methods=['POST'], response_class=handlers.HTMLResponse)
router.add_api_route('/admin/logout', handlers.admin_logout, methods=['POST'])
router.add_api_route('/admin', handlers.admin_home, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route('/admin/invalid-matches', handlers.admin_invalid_matches, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route('/admin/invalid-matches/{match_id}/update', handlers.admin_invalid_match_update, methods=['POST'])
router.add_api_route('/admin/invalid-matches/{match_id}/delete', handlers.admin_invalid_match_delete, methods=['POST'])
router.add_api_route('/admin/duplicates', handlers.admin_duplicates_queue, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route('/admin/duplicates/{review_id}/match', handlers.admin_duplicate_match, methods=['POST'])
router.add_api_route('/admin/duplicates/{review_id}/create', handlers.admin_duplicate_create, methods=['POST'])
router.add_api_route('/admin/duplicates/{review_id}/ignore', handlers.admin_duplicate_ignore, methods=['POST'])
router.add_api_route('/admin/sql/execute', handlers.admin_sql_execute, methods=['POST'])
router.add_api_route('/admin/sql/schema', handlers.admin_sql_schema, methods=['GET'])
router.add_api_route('/admin/sql/recent', handlers.admin_sql_recent, methods=['GET'])
router.add_api_route('/admin/sql', handlers.admin_sql_editor, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route('/admin/predictions', pred.admin_predictions_page, methods=['GET'], response_class=HTMLResponse)
router.add_api_route('/admin/api/predictions/summary', pred.admin_predictions_summary, methods=['GET'])
router.add_api_route('/admin/api/predictions/breakdown', pred.admin_predictions_breakdown, methods=['GET'])
router.add_api_route('/admin/api/predictions/charts/accuracy', pred.admin_predictions_charts_accuracy, methods=['GET'])
router.add_api_route('/admin/api/predictions/charts/calibration', pred.admin_predictions_charts_calibration, methods=['GET'])
router.add_api_route('/admin/api/predictions/charts/distribution', pred.admin_predictions_charts_distribution, methods=['GET'])
router.add_api_route('/admin/activity-log', handlers.admin_activity_log, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route(
    '/admin/tweet-activity',
    tweet_activity.admin_tweet_activity,
    methods=['GET'],
    response_class=handlers.HTMLResponse,
)
router.add_api_route(
    '/admin/tweet-activity/{content_key}',
    tweet_activity.admin_tweet_activity_detail,
    methods=['GET'],
    response_class=handlers.HTMLResponse,
)

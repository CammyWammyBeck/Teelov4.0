from fastapi import APIRouter

from teelo.web.services import legacy_main_handlers as legacy

router = APIRouter()
router.add_api_route('/rankings', legacy.rankings_page, methods=['GET'], response_class=legacy.HTMLResponse)
router.add_api_route('/api/rankings', legacy.api_rankings, methods=['GET'])

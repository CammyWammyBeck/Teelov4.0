from fastapi import APIRouter

from teelo.web.services import main_handlers as handlers

router = APIRouter()
router.add_api_route('/rankings', handlers.rankings_page, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route('/api/rankings', handlers.api_rankings, methods=['GET'])

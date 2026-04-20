from teelo.web.services import main_handlers as handlers
from fastapi import APIRouter

router = APIRouter()

router.add_api_route('/players/{player_id}', handlers.player_page, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route('/players/{player_id}/{slug}', handlers.player_page, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route('/api/players/{player_id}/elo-history', handlers.api_player_elo_history, methods=['GET'])
router.add_api_route('/api/players/{player_id}/overview', handlers.api_player_overview, methods=['GET'])
router.add_api_route('/api/players/{player_id}/surface-elo', handlers.api_player_surface_elo, methods=['GET'])
router.add_api_route('/api/players/{player_id}/profile-overview', handlers.api_player_profile_overview, methods=['GET'])
router.add_api_route('/api/players/{player_id}/records', handlers.api_player_records, methods=['GET'])
router.add_api_route('/api/players/{player_id}/matches', handlers.api_player_matches, methods=['GET'])
router.add_api_route('/api/players/{player_id}/tournaments', handlers.api_player_tournaments, methods=['GET'])

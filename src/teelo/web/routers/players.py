from teelo.web.services import legacy_main_handlers as legacy
from fastapi import APIRouter

router = APIRouter()

router.add_api_route('/players/{player_id}', legacy.player_page, methods=['GET'], response_class=legacy.HTMLResponse)
router.add_api_route('/api/players/{player_id}/elo-history', legacy.api_player_elo_history, methods=['GET'])
router.add_api_route('/api/players/{player_id}/overview', legacy.api_player_overview, methods=['GET'])
router.add_api_route('/api/players/{player_id}/surface-elo', legacy.api_player_surface_elo, methods=['GET'])
router.add_api_route('/api/players/{player_id}/profile-overview', legacy.api_player_profile_overview, methods=['GET'])
router.add_api_route('/api/players/{player_id}/records', legacy.api_player_records, methods=['GET'])
router.add_api_route('/api/players/{player_id}/matches', legacy.api_player_matches, methods=['GET'])
router.add_api_route('/api/players/{player_id}/tournaments', legacy.api_player_tournaments, methods=['GET'])

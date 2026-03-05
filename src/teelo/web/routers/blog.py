from fastapi import APIRouter

from teelo.web.services import legacy_main_handlers as legacy

router = APIRouter()
router.add_api_route('/blog', legacy.blog_list, methods=['GET'], response_class=legacy.HTMLResponse)
router.add_api_route('/blog/{slug}', legacy.blog_detail, methods=['GET'], response_class=legacy.HTMLResponse)

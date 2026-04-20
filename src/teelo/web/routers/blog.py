from fastapi import APIRouter

from teelo.web.services import main_handlers as handlers

router = APIRouter()
router.add_api_route('/blog', handlers.blog_list, methods=['GET'], response_class=handlers.HTMLResponse)
router.add_api_route('/blog/{slug}', handlers.blog_detail, methods=['GET'], response_class=handlers.HTMLResponse)

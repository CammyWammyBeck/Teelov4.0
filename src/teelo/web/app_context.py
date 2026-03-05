from pathlib import Path

from fastapi.templating import Jinja2Templates

from teelo.config import settings
from teelo.match_statuses import get_status_group

BASE_PATH = Path(__file__).parent
STATIC_PATH = BASE_PATH / "static"
TEMPLATES_PATH = BASE_PATH / "templates"
CONTENT_PATH = BASE_PATH / "content"

templates = Jinja2Templates(directory=TEMPLATES_PATH)
templates.env.globals["features"] = settings

MATCHES_PAGE_STATUS_FILTERS = get_status_group("all")
ADMIN_SESSION_KEY = "admin_user_id"

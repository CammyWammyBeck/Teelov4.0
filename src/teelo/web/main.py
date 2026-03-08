from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.exceptions import StarletteHTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from teelo.config import settings
from teelo.web.app_context import STATIC_PATH, templates
from teelo.web.routers.admin import router as admin_router
from teelo.web.routers.blog import router as blog_router
from teelo.web.routers.matches import router as matches_router
from teelo.web.routers.players import router as players_router
from teelo.web.routers.public import router as public_router
from teelo.web.routers.rankings import router as rankings_router
from teelo.web.routers.tournaments import router as tournaments_router

app = FastAPI(title="Teelo Ratings")
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.admin_session_secret,
    max_age=settings.admin_session_max_age_seconds,
)
app.mount("/static", StaticFiles(directory=STATIC_PATH), name="static")


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return templates.TemplateResponse(
            "404.html",
            {"request": request, "now": datetime.utcnow(), "current_path": request.url.path},
            status_code=404,
        )
    return HTMLResponse(content=str(exc.detail), status_code=exc.status_code)


app.include_router(public_router)
app.include_router(matches_router)
app.include_router(rankings_router)
app.include_router(players_router)
app.include_router(tournaments_router)
app.include_router(blog_router)
app.include_router(admin_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("teelo.web.main:app", host="0.0.0.0", port=8000, reload=True)

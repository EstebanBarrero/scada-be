"""
FastAPI application factory.

Uses lifespan context manager (FastAPI >= 0.93) instead of deprecated
on_event decorators, ensuring startup/shutdown logic is type-safe.

Routers are registered incrementally as features are added:
  - alarms  → feat/api-alarms
  - metrics → feat/api-metrics
  - etl     → feat/api-etl-trigger
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import init_db
from app.exceptions.handlers import register_exception_handlers


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB schema on startup."""
    init_db()
    yield
    # Cleanup: SQLAlchemy disposes connection pool automatically on process exit


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version=settings.VERSION,
        description=(
            "REST API for SCADA industrial alarm data. "
            "Supports filtering, pagination, and aggregated metrics."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # CORS: allow frontend dev server and Docker nginx
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",  # Vite dev server
            "http://localhost:3000",
            "http://frontend",
            "http://frontend:80",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    register_exception_handlers(app)

    @app.get("/health", tags=["Health"])
    def health_check():
        return {"status": "ok", "version": settings.VERSION}

    return app


app = create_app()

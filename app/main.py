"""
FastAPI application factory.

Uses lifespan context manager (FastAPI >= 0.93) instead of deprecated
on_event decorators, ensuring startup/shutdown logic is type-safe.

Routers are registered incrementally as features are added:
  - alarms  ✓ feat/api-alarms
  - metrics ✓ feat/api-metrics
  - etl     ✓ feat/api-etl-trigger
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.config import settings
from app.database import init_db
from app.exceptions.handlers import register_exception_handlers
from app.routers import alarms, etl, metrics
from app.utils.rate_limit import limiter


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

    # Rate limiting: keyed by remote IP, enforced per-route via @limiter.limit()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)

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

    app.include_router(alarms.router, prefix=settings.API_PREFIX)
    app.include_router(metrics.router, prefix=settings.API_PREFIX)
    app.include_router(etl.router, prefix=settings.API_PREFIX)

    @app.get("/health", tags=["Health"])
    def health_check():
        return {"status": "ok", "version": settings.VERSION}

    return app


app = create_app()

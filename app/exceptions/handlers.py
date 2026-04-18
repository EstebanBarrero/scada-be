"""
Global exception handlers.

Centralizing error handling here ensures:
- Consistent response structure for all error types
- No sensitive internal details leaked to clients
- HTTP semantics respected (400 for client errors, 500 for server errors)
"""

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError


def _error_response(status_code: int, message: str, detail=None) -> JSONResponse:
    body = {"error": {"message": message}}
    if detail:
        body["error"]["detail"] = detail
    return JSONResponse(status_code=status_code, content=body)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """
        Pydantic v2 validation errors → 422 with structured field-level messages.
        Clients get actionable feedback without server internals.
        """
        errors = [
            {
                "field": " → ".join(str(loc) for loc in err["loc"]),
                "message": err["msg"],
                "type": err["type"],
            }
            for err in exc.errors()
        ]
        return _error_response(422, "Request validation failed", detail=errors)

    @app.exception_handler(ValueError)
    async def value_error_handler(
        request: Request, exc: ValueError
    ) -> JSONResponse:
        return _error_response(400, str(exc))

    @app.exception_handler(FileNotFoundError)
    async def not_found_handler(
        request: Request, exc: FileNotFoundError
    ) -> JSONResponse:
        return _error_response(404, str(exc))

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(
        request: Request, exc: RuntimeError
    ) -> JSONResponse:
        # ETL lock conflict → 409
        if "already running" in str(exc).lower():
            return _error_response(409, str(exc))
        return _error_response(500, "Internal server error")

    @app.exception_handler(SQLAlchemyError)
    async def db_error_handler(
        request: Request, exc: SQLAlchemyError
    ) -> JSONResponse:
        # Never expose raw SQL errors to clients
        return _error_response(500, "Database error occurred")

    @app.exception_handler(Exception)
    async def generic_error_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        return _error_response(500, "An unexpected error occurred")

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.alarm import AlarmFilters, AlarmRead, CriticalityEnum
from app.schemas.common import Meta, PaginatedResponse
from app.services.alarm_service import AlarmService

router = APIRouter(prefix="/alarms", tags=["Alarms"])


@router.get("", response_model=PaginatedResponse[AlarmRead])
def list_alarms(
    start_date: Annotated[str | None, Query(description="ISO 8601 start datetime")] = None,
    end_date: Annotated[str | None, Query(description="ISO 8601 end datetime")] = None,
    criticality: Annotated[list[CriticalityEnum] | None, Query(description="Filter by criticality")] = None,
    tag: Annotated[str | None, Query(description="Tag name partial match")] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    size: Annotated[int, Query(ge=1, le=500)] = 50,
    db: Session = Depends(get_db),
):
    """
    List alarms with optional filters.

    - **start_date / end_date**: ISO 8601 datetime strings
    - **criticality**: repeatable, e.g. `?criticality=CRITICAL&criticality=HIGH`
    - **tag**: partial match on tag name
    - **page / size**: pagination
    """
    parsed_start = None
    parsed_end = None
    try:
        if start_date:
            parsed_start = datetime.fromisoformat(start_date)
        if end_date:
            parsed_end = datetime.fromisoformat(end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {exc}")

    filters = AlarmFilters(
        start_date=parsed_start,
        end_date=parsed_end,
        criticality=criticality,
        tag=tag,
        page=page,
        size=size,
    )

    service = AlarmService(db)
    items, total = service.get_alarms(filters)

    return PaginatedResponse(
        data=[AlarmRead.model_validate(item) for item in items],
        meta=Meta(page=page, size=size, total=total),
    )


@router.get("/{alarm_id}", response_model=AlarmRead)
def get_alarm(alarm_id: int, db: Session = Depends(get_db)):
    """Retrieve a single alarm by ID."""
    service = AlarmService(db)
    alarm = service.get_alarm_by_id(alarm_id)
    if not alarm:
        raise HTTPException(status_code=404, detail=f"Alarm {alarm_id} not found")
    return AlarmRead.model_validate(alarm)

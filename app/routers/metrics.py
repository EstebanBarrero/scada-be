from datetime import datetime
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.metrics import (
    CriticalityResponse,
    IntervalEnum,
    TimelineResponse,
    TopTagsResponse,
)
from app.services.metrics_service import MetricsService

router = APIRouter(prefix="/metrics", tags=["Metrics"])


@router.get("/top-tags", response_model=TopTagsResponse)
def get_top_tags(
    limit: Annotated[int, Query(ge=1, le=100, description="Number of tags to return")] = 10,
    db: Session = Depends(get_db),
):
    """Top N tags ranked by total alarm event count."""
    return MetricsService(db).get_top_tags(limit=limit)


@router.get("/by-criticality", response_model=CriticalityResponse)
def get_by_criticality(db: Session = Depends(get_db)):
    """Alarm count and percentage breakdown by criticality level."""
    return MetricsService(db).get_by_criticality()


@router.get("/timeline", response_model=TimelineResponse)
def get_timeline(
    interval: Annotated[IntervalEnum, Query(description="Bucketing interval")] = IntervalEnum.day,
    start_date: Annotated[Optional[str], Query(description="ISO 8601 start")] = None,
    end_date: Annotated[Optional[str], Query(description="ISO 8601 end")] = None,
    db: Session = Depends(get_db),
):
    """
    Alarm event counts over time, bucketed by hour or day.
    Includes breakdown of CRITICAL and HIGH counts per bucket.
    """
    parsed_start = datetime.fromisoformat(start_date) if start_date else None
    parsed_end = datetime.fromisoformat(end_date) if end_date else None

    return MetricsService(db).get_timeline(
        interval=interval,
        start_date=parsed_start,
        end_date=parsed_end,
    )

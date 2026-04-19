"""
Metrics aggregation service.

All aggregations run as single SQL queries — no Python-level grouping.
Timeline bucketing uses DB-level strftime (SQLite) abstracted behind
a helper so it can be swapped for DATEADD/DATEDIFF on SQL Server.
"""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.models.alarm import Alarm
from app.models.tag import Tag
from app.schemas.metrics import (
    CriticalityCount,
    CriticalityResponse,
    IntervalEnum,
    TimelinePoint,
    TimelineResponse,
    TopTagItem,
    TopTagsResponse,
)


class MetricsService:
    def __init__(self, db: Session):
        self.db = db

    def get_top_tags(self, limit: int = 10) -> TopTagsResponse:
        """
        Top N tags by total alarm count.
        Uses GROUP BY on tag_id (integer FK) — faster than grouping by name string.
        LEFT JOIN to tags so alarms with null tag_id still appear.
        """
        stmt = (
            select(
                func.coalesce(Tag.name, Alarm.raw_tag).label("tag_name"),
                Alarm.tag_id,
                func.count(Alarm.id).label("alarm_count"),
                Tag.area,
            )
            .outerjoin(Tag, Alarm.tag_id == Tag.id)
            .group_by(Alarm.tag_id, Tag.name, Tag.area, Alarm.raw_tag)
            .order_by(func.count(Alarm.id).desc())
            .limit(limit)
        )

        rows = self.db.execute(stmt).all()
        items = [
            TopTagItem(
                tag_name=row.tag_name,
                tag_id=row.tag_id,
                alarm_count=row.alarm_count,
                area=row.area,
            )
            for row in rows
        ]
        return TopTagsResponse(data=items, limit=limit)

    def get_by_criticality(self) -> CriticalityResponse:
        """
        Count and percentage of alarms per criticality level.
        Single aggregation query — no Python math on result sets.
        """
        total_stmt = select(func.count(Alarm.id))
        total = self.db.execute(total_stmt).scalar_one() or 1  # avoid division by zero

        stmt = (
            select(
                Alarm.criticality,
                func.count(Alarm.id).label("count"),
            )
            .group_by(Alarm.criticality)
            .order_by(func.count(Alarm.id).desc())
        )
        rows = self.db.execute(stmt).all()
        items = [
            CriticalityCount(
                criticality=row.criticality,
                count=row.count,
                percentage=round((row.count / total) * 100, 2),
            )
            for row in rows
        ]
        return CriticalityResponse(data=items, total=total)

    def get_timeline(
        self,
        interval: IntervalEnum = IntervalEnum.day,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> TimelineResponse:
        """
        Alarm counts bucketed by hour or day.

        SQLite: uses strftime() for bucketing.
        SQL Server migration: replace strftime expression with
          DATEADD(hour, DATEDIFF(hour, 0, timestamp), 0)  -- for hour
          CAST(timestamp AS DATE)                          -- for day
        """
        fmt = "%Y-%m-%d %H:00:00" if interval == IntervalEnum.hour else "%Y-%m-%d"
        bucket_expr = func.strftime(fmt, Alarm.timestamp).label("bucket")

        stmt = (
            select(
                bucket_expr,
                func.count(Alarm.id).label("count"),
                func.sum(
                    case((Alarm.criticality == "CRITICAL", 1), else_=0)
                ).label("critical_count"),
                func.sum(
                    case((Alarm.criticality == "HIGH", 1), else_=0)
                ).label("high_count"),
            )
            .group_by("bucket")
            .order_by("bucket")
        )

        if start_date:
            stmt = stmt.where(Alarm.timestamp >= start_date)
        if end_date:
            stmt = stmt.where(Alarm.timestamp <= end_date)

        rows = self.db.execute(stmt).all()
        items = [
            TimelinePoint(
                bucket=row.bucket,
                count=row.count,
                critical_count=row.critical_count or 0,
                high_count=row.high_count or 0,
            )
            for row in rows
        ]
        return TimelineResponse(
            data=items,
            interval=interval.value,
            start_date=start_date,
            end_date=end_date,
        )

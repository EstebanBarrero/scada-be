"""
Alarm query service.

All filtering and pagination happens at the DB level via SQLAlchemy ORM queries.
NO Python-level filtering of query results — this ensures indexes are used
and memory usage scales with page size, not total dataset size.
"""

from typing import List, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.alarm import Alarm
from app.models.tag import Tag
from app.schemas.alarm import AlarmFilters


class AlarmService:
    def __init__(self, db: Session):
        self.db = db

    def get_alarms(self, filters: AlarmFilters) -> tuple[List[Alarm], int]:
        """
        Return a paginated, filtered list of alarms and the total count.
        Returns (items, total) so the router can build the meta object.
        """
        query = self._build_query(filters)

        # Count total matching rows BEFORE pagination
        # Uses a subquery so we avoid re-specifying all WHERE conditions
        count_query = select(func.count()).select_from(query.subquery())
        total = self.db.execute(count_query).scalar_one()

        # Apply pagination
        offset = (filters.page - 1) * filters.size
        items = (
            self.db.execute(
                query.order_by(Alarm.timestamp.desc())
                .offset(offset)
                .limit(filters.size)
            )
            .scalars()
            .all()
        )

        return list(items), total

    def get_alarm_by_id(self, alarm_id: int) -> Optional[Alarm]:
        return self.db.get(Alarm, alarm_id)

    def _build_query(self, filters: AlarmFilters):
        """
        Build a SQLAlchemy SELECT with only the WHERE clauses needed.
        Conditional filter building avoids passing None to BETWEEN operators.
        """
        query = select(Alarm)

        if filters.start_date:
            query = query.where(Alarm.timestamp >= filters.start_date)

        if filters.end_date:
            query = query.where(Alarm.timestamp <= filters.end_date)

        if filters.criticality:
            # Multiple criticality values → IN clause
            crit_values = [c.value for c in filters.criticality]
            query = query.where(Alarm.criticality.in_(crit_values))

        if filters.tag:
            # Partial match on normalized tag name via tags join
            query = query.join(Tag, Alarm.tag_id == Tag.id, isouter=True).where(
                func.upper(Tag.name).contains(filters.tag.upper())
            )

        return query

from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Numeric,
    ForeignKey,
    Index,
    func,
)
from sqlalchemy.orm import relationship
from app.database import Base


class Alarm(Base):
    """
    Central fact table for SCADA alarm events.

    Column design rationale:
    - raw_tag: preserved for audit/debugging; tag_id is the normalized FK
    - Numeric(10,4): SQL Server-compatible precision type (avoids REAL/FLOAT drift)
    - quality_notes: tracks which cleaning steps were applied per row (observability)
    - ingested_at: server-set timestamp, not from source data (reliable for ETL auditing)

    Index rationale (see README for full justification):
    - ix_alarms_timestamp: primary filter for all time-range queries
    - ix_alarms_criticality: low-cardinality filter, high selectivity on CRITICAL/HIGH
    - ix_alarms_tag_id: FK index — prevents full scan on every join to tags table
    - ix_alarms_ts_crit: composite covering index for the dominant query pattern
    - ix_alarms_ingested_at: ETL idempotency check (MAX(ingested_at) per run)
    """

    __tablename__ = "alarms"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(
        Integer,
        ForeignKey("tags.id", ondelete="SET NULL"),
        nullable=True,
    )
    raw_tag = Column(String(100), nullable=False)
    description = Column(String(500), nullable=True)
    criticality = Column(String(20), nullable=False, default="UNKNOWN")
    timestamp = Column(DateTime, nullable=False)
    value = Column(Numeric(10, 4), nullable=True)
    unit = Column(String(20), nullable=True)
    status = Column(String(20), nullable=False, default="ACTIVE")
    source = Column(String(100), nullable=True)
    quality_notes = Column(String(500), nullable=True)
    ingested_at = Column(
        DateTime,
        nullable=False,
        # Python-side default uses timezone-aware UTC (datetime.utcnow is deprecated in 3.12+)
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
    )

    tag = relationship("Tag", backref="alarms", lazy="joined")

    __table_args__ = (
        Index("ix_alarms_timestamp", "timestamp"),
        Index("ix_alarms_criticality", "criticality"),
        Index("ix_alarms_tag_id", "tag_id"),
        Index("ix_alarms_ts_crit", "timestamp", "criticality"),
        Index("ix_alarms_ingested_at", "ingested_at"),
    )

    def __repr__(self) -> str:
        return f"<Alarm id={self.id} tag={self.raw_tag!r} ts={self.timestamp}>"

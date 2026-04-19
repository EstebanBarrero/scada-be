from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CriticalityEnum(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNKNOWN = "UNKNOWN"


class StatusEnum(str, Enum):
    ACTIVE = "ACTIVE"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    CLEARED = "CLEARED"


class TagRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    area: Optional[str] = None
    system: Optional[str] = None


class AlarmRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    tag_id: Optional[int] = None
    raw_tag: str
    description: Optional[str] = None
    criticality: str
    timestamp: datetime
    value: Optional[float] = None
    unit: Optional[str] = None
    status: str
    source: Optional[str] = None
    quality_notes: Optional[str] = None
    ingested_at: datetime
    tag: Optional[TagRead] = None


class AlarmFilters(BaseModel):
    """
    Query parameters for alarm listing.
    Validated here so invalid input returns 422 before hitting the DB.
    """

    start_date: Optional[datetime] = Field(None, description="ISO 8601 start datetime")
    end_date: Optional[datetime] = Field(None, description="ISO 8601 end datetime")
    criticality: Optional[List[CriticalityEnum]] = Field(
        None, description="One or more criticality levels"
    )
    tag: Optional[str] = Field(None, description="Tag name partial match")
    page: int = Field(1, ge=1, description="Page number (1-indexed)")
    size: int = Field(50, ge=1, le=500, description="Items per page")

    @model_validator(mode="after")
    def validate_date_range(self) -> "AlarmFilters":
        if self.start_date and self.end_date:
            if self.start_date >= self.end_date:
                raise ValueError("start_date must be strictly before end_date")
        return self

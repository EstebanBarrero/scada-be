from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class IntervalEnum(str, Enum):
    hour = "hour"
    day = "day"


class TopTagItem(BaseModel):
    tag_name: str
    tag_id: Optional[int] = None
    alarm_count: int
    area: Optional[str] = None


class TopTagsResponse(BaseModel):
    data: List[TopTagItem]
    limit: int


class CriticalityCount(BaseModel):
    criticality: str
    count: int
    percentage: float


class CriticalityResponse(BaseModel):
    data: List[CriticalityCount]
    total: int


class TimelinePoint(BaseModel):
    bucket: str
    count: int
    critical_count: int
    high_count: int


class TimelineResponse(BaseModel):
    data: List[TimelinePoint]
    interval: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

from datetime import datetime
from enum import Enum
from typing import List

from pydantic import BaseModel


class ETLStatus(str, Enum):
    success = "success"
    partial = "partial"
    failed = "failed"


class ETLRunResponse(BaseModel):
    status: ETLStatus
    raw_count: int
    rejected_count: int
    duplicate_count: int
    null_imputed_count: int
    type_coerced_count: int
    loaded_count: int
    tags_created: int
    tags_reused: int
    duration_seconds: float
    errors: List[str]
    started_at: datetime
    completed_at: datetime

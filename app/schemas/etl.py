from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class ETLStatus(str, Enum):
    success = "success"
    partial = "partial"
    failed = "failed"


class DatasetInfoResponse(BaseModel):
    exists: bool
    total_rows: int
    file_size_kb: float
    generated_at: Optional[datetime]
    # Quality issues detected in the raw CSV
    null_criticality: int
    null_tag: int
    null_value: int
    null_description: int
    exact_duplicates: int
    mixed_timestamp_formats: bool


class GenerateDatasetResponse(BaseModel):
    total_rows: int
    file_size_kb: float
    output_path: str


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

"""
ETL Pipeline orchestrator.

Chains: ingest → clean → normalize → load
Returns a structured ETLRunResponse with full stats and timing.

File-based lock prevents concurrent pipeline runs without needing a task queue.
For production scale: replace with Celery + Redis and return job_id immediately.
"""

import os
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from app.config import settings
from app.schemas.etl import ETLRunResponse, ETLStatus
from etl.clean import clean
from etl.ingest import ingest
from etl.load import load
from etl.normalize import normalize

LOCK_FILE = Path(settings.ETL_DATA_PATH).parent / "etl.lock"


def run_pipeline(
    db: Session,
    data_path: str | None = None,
) -> ETLRunResponse:
    """
    Execute the full ETL pipeline.

    Args:
        db: Active SQLAlchemy session — pipeline shares the session so tag upserts
            and alarm inserts participate in the same transaction scope.
        data_path: Override for the data file path (useful in tests).

    Returns:
        ETLRunResponse with full statistics.
    """
    started_at = _utc_now()
    errors: list[str] = []

    _acquire_lock()
    try:
        result = _execute_pipeline(db, data_path, errors, started_at)
    except Exception as exc:
        errors.append(str(exc))
        result = ETLRunResponse(
            status=ETLStatus.failed,
            raw_count=0,
            rejected_count=0,
            duplicate_count=0,
            null_imputed_count=0,
            type_coerced_count=0,
            loaded_count=0,
            tags_created=0,
            tags_reused=0,
            duration_seconds=round(time.time() - started_at.timestamp(), 3),
            errors=errors,
            started_at=started_at,
            completed_at=_utc_now(),
        )
    finally:
        _release_lock()

    return result


def _execute_pipeline(
    db: Session,
    data_path: str | None,
    errors: list[str],
    started_at: datetime,
) -> ETLRunResponse:
    t0 = time.time()
    source = data_path or settings.ETL_DATA_PATH

    # 1. Ingest
    raw_df = ingest(source)
    raw_count = len(raw_df)

    # Preserve raw_tag before normalization mutates tag column
    raw_df["raw_tag"] = raw_df["tag"].fillna("UNKNOWN")

    # 2. Clean
    clean_df, clean_stats = clean(raw_df)
    duplicate_count = (
        clean_stats.exact_duplicates_removed + clean_stats.near_duplicates_removed
    )
    rejected_count = clean_stats.total_rejected
    null_imputed = (
        clean_stats.null_criticality_imputed + clean_stats.null_description_imputed
    )
    errors.extend(clean_stats.warnings)

    # 3. Normalize
    norm_df, norm_stats = normalize(clean_df, db)
    rejected_count += norm_stats.timestamps_rejected
    errors.extend(norm_stats.warnings)

    # 4. Load
    load_stats = load(norm_df, db, chunk_size=settings.ETL_CHUNK_SIZE)

    duration = round(time.time() - t0, 3)
    status = (
        ETLStatus.success
        if not errors
        else (ETLStatus.partial if load_stats.loaded_count > 0 else ETLStatus.failed)
    )

    return ETLRunResponse(
        status=status,
        raw_count=raw_count,
        rejected_count=rejected_count,
        duplicate_count=duplicate_count,
        null_imputed_count=null_imputed,
        type_coerced_count=norm_stats.type_coerced_count,
        loaded_count=load_stats.loaded_count,
        tags_created=norm_stats.tags_created,
        tags_reused=norm_stats.tags_reused,
        duration_seconds=duration,
        errors=errors,
        started_at=started_at,
        completed_at=_utc_now(),
    )


def is_running() -> bool:
    return LOCK_FILE.exists()


def _acquire_lock() -> None:
    if LOCK_FILE.exists():
        raise RuntimeError("ETL pipeline is already running")
    LOCK_FILE.write_text(str(os.getpid()))


def _release_lock() -> None:
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()


def _utc_now() -> datetime:
    """Return current UTC time as a naive datetime (DB-safe)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

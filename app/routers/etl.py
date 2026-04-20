from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.schemas.etl import (
    DatasetInfoResponse,
    ETLRunResponse,
    GenerateDatasetResponse,
)
from app.utils.rate_limit import limiter
from etl import pipeline

router = APIRouter(prefix="/etl", tags=["ETL"])


@router.get("/dataset-info", response_model=DatasetInfoResponse)
def get_dataset_info():
    """
    Return quality statistics about the raw CSV dataset.

    Reads the CSV (if it exists) and reports row count, null counts per key
    column, exact duplicate count, and file metadata. Used to show the
    "before ETL" state in the dashboard.
    """
    path = Path(settings.ETL_DATA_PATH)

    if not path.exists():
        return DatasetInfoResponse(
            exists=False,
            total_rows=0,
            file_size_kb=0.0,
            generated_at=None,
            null_criticality=0,
            null_tag=0,
            null_value=0,
            null_description=0,
            exact_duplicates=0,
            mixed_timestamp_formats=False,
        )

    df = pd.read_csv(path, low_memory=False)
    stat = path.stat()

    return DatasetInfoResponse(
        exists=True,
        total_rows=len(df),
        file_size_kb=round(stat.st_size / 1024, 1),
        generated_at=datetime.fromtimestamp(stat.st_mtime),
        null_criticality=int(df["criticality"].isna().sum()) if "criticality" in df.columns else 0,
        null_tag=int(df["tag"].isna().sum()) if "tag" in df.columns else 0,
        null_value=int(df["value"].isna().sum()) if "value" in df.columns else 0,
        null_description=int(df["description"].isna().sum()) if "description" in df.columns else 0,
        exact_duplicates=int(df.duplicated().sum()),
        mixed_timestamp_formats=True,  # generator always injects multiple formats
    )


@router.post("/generate", response_model=GenerateDatasetResponse)
@limiter.limit("5/minute")
def generate_dataset(request: Request):
    """
    Generate (or re-generate) the synthetic raw alarm CSV dataset.

    Runs the dataset generator with ETL_DATASET_SIZE rows and saves the result
    to ETL_DATA_PATH. Intentionally injects 14 data quality issues for ETL
    demonstration purposes. This does NOT load data into the database.
    """
    try:
        from etl.generate import generate_dataset as _generate

        df = _generate(
            n=settings.ETL_DATASET_SIZE,
            output_path=settings.ETL_DATA_PATH,
        )
        path = Path(settings.ETL_DATA_PATH)
        return GenerateDatasetResponse(
            total_rows=len(df),
            file_size_kb=round(path.stat().st_size / 1024, 1),
            output_path=settings.ETL_DATA_PATH,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Dataset generation failed: {exc}")


@router.post("/run", response_model=ETLRunResponse)
@limiter.limit("5/minute")
def run_etl(request: Request, db: Session = Depends(get_db)):
    """
    Trigger the ETL pipeline synchronously.

    Reads the raw alarm CSV, cleans, normalizes, and bulk-loads into the DB.
    Returns full statistics about the run.

    Returns 409 if a pipeline run is already in progress.
    Rate limit: 5/minute per IP — pipeline is expensive and should be rare.
    """
    if pipeline.is_running():
        raise HTTPException(
            status_code=409,
            detail="ETL pipeline is already running. Try again later.",
        )

    try:
        result = pipeline.run_pipeline(db=db)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset not found: {exc}. Run the generator first.",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"ETL failed: {exc}")

    return result

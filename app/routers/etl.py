from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.etl import ETLRunResponse
from etl import pipeline

router = APIRouter(prefix="/etl", tags=["ETL"])


@router.post("/run", response_model=ETLRunResponse)
def run_etl(db: Session = Depends(get_db)):
    """
    Trigger the ETL pipeline synchronously.

    Reads the raw alarm CSV, cleans, normalizes, and bulk-loads into the DB.
    Returns full statistics about the run.

    Returns 409 if a pipeline run is already in progress.
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

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.etl import ETLRunResponse
from app.utils.rate_limit import limiter
from etl import pipeline

router = APIRouter(prefix="/etl", tags=["ETL"])


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

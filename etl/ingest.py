"""
Ingest layer: reads the raw CSV/JSON into a DataFrame.

Design decisions:
- dtype=str: all columns read as strings to preserve raw values exactly as they
  appear in the source. Type coercion happens in the normalize step, not here.
  This prevents pandas from silently converting "1" to 1 or "N/A" to NaN.
- keep_default_na=False: prevents pandas from auto-converting common strings
  like "NULL", "N/A", "none", "" to NaN before we can log them explicitly.
- low_memory=False: avoids mixed-type column warnings on large files.
"""

from pathlib import Path
from typing import Union

import pandas as pd


EXPECTED_COLUMNS = {
    "id", "timestamp", "tag", "criticality",
    "value", "unit", "status", "description", "source", "area",
}


def ingest_csv(path: Union[str, Path]) -> pd.DataFrame:
    """
    Read raw alarm CSV. All values preserved as strings.
    Raises FileNotFoundError or ValueError on structural issues.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    df = pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        low_memory=False,
    )

    _validate_structure(df, path)
    return df


def ingest_json(path: Union[str, Path]) -> pd.DataFrame:
    """Read raw alarm JSON (array of objects)."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    df = pd.read_json(path, dtype=str)
    _validate_structure(df, path)
    return df


def ingest(path: Union[str, Path]) -> pd.DataFrame:
    """Dispatch to the correct reader based on file extension."""
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return ingest_csv(path)
    elif suffix == ".json":
        return ingest_json(path)
    else:
        raise ValueError(f"Unsupported file format: {suffix!r}. Use .csv or .json")


def _validate_structure(df: pd.DataFrame, path: Path) -> None:
    """Warn on missing expected columns — do not fail; ETL is defensive."""
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        # Non-fatal: log missing columns, proceed with what we have
        print(f"[ingest] WARNING: missing expected columns in {path.name}: {missing}")

    if len(df) == 0:
        raise ValueError(f"File {path.name} is empty — nothing to process.")

    print(f"[ingest] Read {len(df):,} rows from {path.name}")

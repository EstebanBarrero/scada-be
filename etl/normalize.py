"""
Normalization layer: converts raw string values into typed, canonical forms.

Handles:
- Datetime standardization across 4+ formats (Issues 05, 11, 12)
- Criticality mapping from variants to canonical enum (Issue 06)
- Status boolean/string normalization (Issue 07)
- Numeric value extraction from strings with embedded units (Issue 10)
- Tag name standardization + unit extraction (Issue 13)
- Tag resolution / upsert against the tags table

Design: all transformations track counts in NormalizeStats for reporting.
"""

import re
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from app.models.tag import Tag

# ---------------------------------------------------------------------------
# Canonical mappings
# ---------------------------------------------------------------------------

CRITICALITY_MAP: dict[str, str] = {
    "critical": "CRITICAL",
    "crit": "CRITICAL",
    "critica": "CRITICAL",
    "high": "HIGH",
    "hi": "HIGH",
    "medium": "MEDIUM",
    "med": "MEDIUM",
    "moderate": "MEDIUM",
    "low": "LOW",
    "lo": "LOW",
    "unknown": "UNKNOWN",
    "n/a": "UNKNOWN",
}

STATUS_MAP: dict[str, str] = {
    "active": "ACTIVE",
    "true": "ACTIVE",
    "1": "ACTIVE",
    "yes": "ACTIVE",
    "on": "ACTIVE",
    "acknowledged": "ACKNOWLEDGED",
    "ack": "ACKNOWLEDGED",
    "cleared": "CLEARED",
    "false": "CLEARED",
    "0": "CLEARED",
    "no": "CLEARED",
    "off": "CLEARED",
    "inactive": "CLEARED",
    "resolved": "CLEARED",
}

# Multiple date format strings for pd.to_datetime fallback chain
DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%d-%b-%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d",
]

# Regex to extract a leading numeric value (handles "45.2 psi", "-3.1bar", "300 rpm")
VALUE_RE = re.compile(r"^\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)")

# Tag normalization: strip, upper, remove separators, collapse whitespace
TAG_NORM_RE = re.compile(r"[\s_]+")


@dataclass
class NormalizeStats:
    timestamps_parsed: int = 0
    timestamps_rejected: int = 0
    future_timestamps_clamped: int = 0
    criticality_mapped: int = 0
    status_mapped: int = 0
    values_extracted: int = 0
    values_unparseable: int = 0
    tags_normalized: int = 0
    tags_created: int = 0
    tags_reused: int = 0
    type_coerced_count: int = 0
    warnings: list = field(default_factory=list)


def normalize(df: pd.DataFrame, db: Session) -> tuple[pd.DataFrame, NormalizeStats]:
    """
    Apply all normalization steps in order. Returns normalized DataFrame + stats.
    """
    stats = NormalizeStats()
    df = df.copy()

    df, stats = _normalize_timestamps(df, stats)
    df, stats = _normalize_criticality(df, stats)
    df, stats = _normalize_status(df, stats)
    df, stats = _normalize_values(df, stats)
    df, stats = _normalize_tags(df, stats)
    df, stats = _resolve_tag_ids(df, db, stats)

    # Final reject: rows where timestamp parsing failed
    if "timestamp" in df.columns:
        null_ts_mask = df["timestamp"].isna()
        n_rejected = int(null_ts_mask.sum())
        if n_rejected > 0:
            stats.timestamps_rejected += n_rejected
            stats.warnings.append(
                f"{n_rejected} rows rejected due to unparseable timestamp"
            )
            df = df[~null_ts_mask]

    print(
        f"[normalize] ts_parsed={stats.timestamps_parsed:,} "
        f"ts_rejected={stats.timestamps_rejected:,} "
        f"tags_created={stats.tags_created} "
        f"tags_reused={stats.tags_reused}"
    )
    return df, stats


# ---------------------------------------------------------------------------
# Individual normalizers
# ---------------------------------------------------------------------------

def _normalize_timestamps(
    df: pd.DataFrame, stats: NormalizeStats
) -> tuple[pd.DataFrame, NormalizeStats]:
    """
    Handles Issues 05, 11, 12:
    - Parse multiple date formats
    - Reject unparseable values (set to NaT → rejected in final step)
    - Clamp future timestamps to now()

    Strategy: try pd.to_datetime with infer_datetime_format first (handles most cases).
    For Unix epoch strings, detect with regex and convert separately.
    """
    if "timestamp" not in df.columns:
        return df, stats

    raw = df["timestamp"].copy()
    # Use timezone-aware now, then strip tzinfo — DB stores naive UTC datetimes
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)

    # Step 1: detect Unix epoch strings (all digits)
    epoch_mask = raw.str.fullmatch(r"\d{9,11}", na=False)
    parsed = pd.Series(pd.NaT, index=df.index)
    if epoch_mask.any():
        parsed.loc[epoch_mask] = pd.to_datetime(
            raw.loc[epoch_mask].astype(float), unit="s", errors="coerce"
        )

    # Step 2: try explicit format list for the rest
    # Explicit formats are critical in pandas 2.x where auto-parsing is stricter.
    # We try each known format in order, filling in rows that failed previous attempts.
    rest_mask = ~epoch_mask & raw.notna()
    if rest_mask.any():
        unparsed = rest_mask.copy()
        for fmt in DATETIME_FORMATS:
            remaining = unparsed & raw.notna()
            if not remaining.any():
                break
            attempt = pd.to_datetime(
                raw.loc[remaining], format=fmt, errors="coerce"
            )
            success = attempt.notna()
            parsed.loc[remaining[remaining].index[success]] = attempt[success].values
            success_idx = remaining[remaining].index[success]
            unparsed.loc[success_idx] = False

        # Fallback: any still-unparsed rows get one last generic attempt
        still_unparsed = unparsed & raw.notna()
        if still_unparsed.any():
            parsed.loc[still_unparsed] = pd.to_datetime(
                raw.loc[still_unparsed], errors="coerce", dayfirst=False
            )

    # Step 3: clamp future timestamps (Issue 12)
    future_mask = parsed.notna() & (parsed > now)
    n_future = int(future_mask.sum())
    if n_future > 0:
        parsed.loc[future_mask] = now
        stats.future_timestamps_clamped = n_future
        stats.warnings.append(f"{n_future} future timestamps clamped to now()")

    stats.timestamps_parsed = int(parsed.notna().sum())
    stats.type_coerced_count += stats.timestamps_parsed

    df["timestamp"] = parsed
    return df, stats


def _normalize_criticality(
    df: pd.DataFrame, stats: NormalizeStats
) -> tuple[pd.DataFrame, NormalizeStats]:
    """
    Handles Issue 06: maps all criticality variants to canonical uppercase set.
    Unknown variants default to UNKNOWN.
    """
    if "criticality" not in df.columns:
        return df, stats

    original = df["criticality"].copy()

    def map_criticality(val: str) -> str:
        if pd.isna(val) or val == "UNKNOWN":
            return "UNKNOWN"
        canonical = CRITICALITY_MAP.get(val.strip().lower())
        if canonical:
            return canonical
        upper = val.strip().upper()
        if upper in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
            return upper
        return "UNKNOWN"

    df["criticality"] = df["criticality"].apply(map_criticality)
    stats.criticality_mapped = int((df["criticality"] != original.fillna("UNKNOWN")).sum())
    return df, stats


def _normalize_status(
    df: pd.DataFrame, stats: NormalizeStats
) -> tuple[pd.DataFrame, NormalizeStats]:
    """
    Handles Issue 07: maps boolean/string status variants to canonical set.
    """
    if "status" not in df.columns:
        return df, stats

    original = df["status"].copy()

    def map_status(val: str) -> str:
        if pd.isna(val):
            return "ACTIVE"  # safe default: treat unknown status as active
        canonical = STATUS_MAP.get(val.strip().lower())
        if canonical:
            return canonical
        upper = val.strip().upper()
        if upper in ("ACTIVE", "ACKNOWLEDGED", "CLEARED"):
            return upper
        return "ACTIVE"

    df["status"] = df["status"].apply(map_status)
    stats.status_mapped = int((df["status"] != original.fillna("ACTIVE")).sum())
    return df, stats


def _normalize_values(
    df: pd.DataFrame, stats: NormalizeStats
) -> tuple[pd.DataFrame, NormalizeStats]:
    """
    Handles Issue 10: extract numeric value from strings like "45.2 psi", "300 rpm".
    Also handles Issue 02: null values remain null (not imputed with 0).
    """
    if "value" not in df.columns:
        return df, stats

    def extract_numeric(val) -> Optional[float]:
        if pd.isna(val) or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            pass
        match = VALUE_RE.match(str(val))
        if match:
            return float(match.group(1))
        return None

    original_nulls = df["value"].isna().sum()
    df["value"] = df["value"].apply(extract_numeric)
    new_nulls = df["value"].isna().sum()

    stats.values_extracted = int(
        df["value"].notna().sum() - (original_nulls < new_nulls and 0)
    )
    stats.values_unparseable = int(new_nulls - original_nulls)
    if stats.values_unparseable > 0:
        stats.warnings.append(
            f"{stats.values_unparseable} values could not be parsed as numeric"
        )
    return df, stats


def _normalize_tags(
    df: pd.DataFrame, stats: NormalizeStats
) -> tuple[pd.DataFrame, NormalizeStats]:
    """
    Handles Issue 13: normalize tag names to canonical uppercase form.
    Rules: strip whitespace → uppercase → replace _ with - → collapse spaces.
    Example: " fic_101 " → "FIC-101"
    """
    if "tag" not in df.columns:
        return df, stats

    def normalize_tag(tag) -> Optional[str]:
        if pd.isna(tag):
            return None
        s = str(tag).strip().upper()
        s = s.replace("_", "-")
        s = TAG_NORM_RE.sub("", s)  # remove all remaining whitespace
        return s if s else None

    original = df["tag"].copy()
    df["tag"] = df["tag"].apply(normalize_tag)
    stats.tags_normalized = int((df["tag"] != original).sum())
    return df, stats


def _resolve_tag_ids(
    df: pd.DataFrame, db: Session, stats: NormalizeStats
) -> tuple[pd.DataFrame, NormalizeStats]:
    """
    Upsert tags into the tags table; add tag_id column to DataFrame.

    Uses a name→id cache to avoid N+1 queries during tag resolution.
    Each unique normalized tag name results in at most one SELECT + one INSERT.
    """
    if "tag" not in df.columns:
        df["tag_id"] = None
        return df, stats

    unique_tags = df["tag"].dropna().unique()
    tag_id_map: dict[str, int] = {}

    for tag_name in unique_tags:
        existing = db.query(Tag).filter(Tag.name == tag_name).first()
        if existing:
            tag_id_map[tag_name] = existing.id
            stats.tags_reused += 1
        else:
            new_tag = Tag(name=tag_name)
            db.add(new_tag)
            db.flush()  # get the ID without committing
            tag_id_map[tag_name] = new_tag.id
            stats.tags_created += 1

    df["tag_id"] = df["tag"].map(tag_id_map)
    return df, stats

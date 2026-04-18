"""
Cleaning layer: handles data quality issues WITHOUT changing semantics.

Responsibilities:
- Remove exact duplicates
- Mark null/empty fields as proper NaN
- Detect structurally invalid records (unparseable timestamp, null tag)
- Track statistics for the ETL run report

This layer does NOT normalize values (that is normalize.py's job).
Separation of concerns: clean = structural validity; normalize = semantic validity.
"""

from dataclasses import dataclass, field
from typing import List

import numpy as np
import pandas as pd

# Strings that should be treated as null regardless of how they appear
NULL_SENTINELS = {
    "", "null", "none", "n/a", "na", "nan", "undefined",
    "nil", "missing", "####", "0000-00-00",
}


@dataclass
class CleanStats:
    exact_duplicates_removed: int = 0
    near_duplicates_removed: int = 0
    null_tag_rejected: int = 0
    null_timestamp_rejected: int = 0
    null_criticality_imputed: int = 0
    null_description_imputed: int = 0
    null_value_count: int = 0
    whitespace_stripped: int = 0
    total_rejected: int = 0
    warnings: List[str] = field(default_factory=list)


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, CleanStats]:
    """
    Apply all cleaning steps. Returns cleaned DataFrame + stats.
    Order matters: normalize sentinels first, then remove duplicates,
    then reject invalid rows, then impute soft nulls.
    """
    stats = CleanStats()
    df = df.copy()

    df = _strip_whitespace(df, stats)
    df = _normalize_null_sentinels(df)
    df = _remove_exact_duplicates(df, stats)
    df = _remove_near_duplicates(df, stats)
    df = _reject_null_tags(df, stats)
    df = _impute_soft_nulls(df, stats)

    stats.total_rejected = stats.null_tag_rejected + stats.null_timestamp_rejected
    print(
        f"[clean] Removed {stats.exact_duplicates_removed:,} exact dupes, "
        f"{stats.near_duplicates_removed:,} near-dupes, "
        f"{stats.total_rejected:,} rejected rows. "
        f"Remaining: {len(df):,}"
    )
    return df, stats


def _strip_whitespace(df: pd.DataFrame, stats: CleanStats) -> pd.DataFrame:
    """
    Strip leading/trailing whitespace from all string columns.
    Handles Issue 14.
    """
    str_cols = df.select_dtypes(include="object").columns
    count = 0
    for col in str_cols:
        original = df[col].copy()
        df[col] = df[col].str.strip()
        count += (df[col] != original).sum()
    stats.whitespace_stripped = int(count)
    return df


def _normalize_null_sentinels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NULL_SENTINELS with actual NaN so downstream steps treat them uniformly.
    Case-insensitive comparison.
    """
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        mask = df[col].str.lower().isin(NULL_SENTINELS)
        df.loc[mask, col] = np.nan
    return df


def _remove_exact_duplicates(df: pd.DataFrame, stats: CleanStats) -> pd.DataFrame:
    """
    Remove rows that are byte-for-byte identical across the key fields.
    Handles Issue 08.

    Dedup key: (tag, timestamp, criticality, value) — intentionally excludes
    `source` so that the same event logged by two systems is caught here
    only if all meaningful fields match.
    """
    key_cols = [c for c in ["tag", "timestamp", "criticality", "value"] if c in df.columns]
    before = len(df)
    df = df.drop_duplicates(subset=key_cols, keep="first")
    stats.exact_duplicates_removed = before - len(df)
    return df


def _remove_near_duplicates(df: pd.DataFrame, stats: CleanStats) -> pd.DataFrame:
    """
    Remove near-duplicates: same (tag, timestamp, value), different source.
    Keeps the row with a non-null source if one exists.
    Handles Issue 09.
    """
    if not all(c in df.columns for c in ["tag", "timestamp", "value"]):
        return df

    before = len(df)
    # Sort: non-null source first, then keep first occurrence
    df = df.sort_values(
        "source",
        na_position="last",
        key=lambda s: s.isna().astype(int),
    )
    df = df.drop_duplicates(subset=["tag", "timestamp", "value"], keep="first")
    stats.near_duplicates_removed = before - len(df)
    return df


def _reject_null_tags(df: pd.DataFrame, stats: CleanStats) -> pd.DataFrame:
    """
    Rows with no tag name cannot be resolved to a tag_id — reject them.
    A row with no timestamp is also structurally invalid for a time-series store.
    Handles Issues 03, 11 (we mark timestamp as null in normalize; reject happens here).
    """
    if "tag" in df.columns:
        null_tag_mask = df["tag"].isna()
        stats.null_tag_rejected = int(null_tag_mask.sum())
        df = df[~null_tag_mask]

    return df


def _impute_soft_nulls(df: pd.DataFrame, stats: CleanStats) -> pd.DataFrame:
    """
    For non-critical nulls, impute with safe defaults rather than rejecting rows.
    This preserves the event record while flagging it.
    Handles Issues 01, 04.
    """
    if "criticality" in df.columns:
        mask = df["criticality"].isna()
        stats.null_criticality_imputed = int(mask.sum())
        df.loc[mask, "criticality"] = "UNKNOWN"

    if "description" in df.columns:
        mask = df["description"].isna()
        stats.null_description_imputed = int(mask.sum())
        df.loc[mask, "description"] = "[NO DESCRIPTION]"

    if "value" in df.columns:
        stats.null_value_count = int(df["value"].isna().sum())

    return df

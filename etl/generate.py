"""
Dataset generator for SCADA alarm system.

Produces a realistic industrial alarm dataset with intentional data quality issues
that simulate real-world SCADA export conditions. All issues are documented below
and in README.md.

INTENTIONAL QUALITY ISSUES INTRODUCED:
  Issue 01 — NULL criticality         
  Issue 02 — NULL value               
  Issue 03 — NULL tag                 
  Issue 04 — NULL description         
  Issue 05 — Mixed datetime formats   
  Issue 06 — Mixed criticality casing (critical, CRITICAL, Critical, Crit, crit)
  Issue 07 — Boolean status variants  (true, TRUE, 1, yes, active, ACTIVE, 0, false)
  Issue 08 — Exact duplicate rows     
  Issue 09 — Near-duplicate rows      
  Issue 10 — Value with embedded unit 
  Issue 11 — Unparseable timestamps   (random garbage strings as timestamp)
  Issue 12 — Future timestamps        
  Issue 13 — Tag name inconsistencies (FIC-101, FIC101, fic-101, FIC_101, " FIC-101")
  Issue 14 — Leading/trailing whitespace in string fields
"""

import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from faker import Faker
    fake = Faker()
except ImportError:
    fake = None

rng = np.random.default_rng(42)  # reproducible seed

TAGS = [
    # (name, area, system)
    ("FIC-101", "Reactor", "DCS"),
    ("FIC-102", "Reactor", "DCS"),
    ("TIC-201", "Distillation", "DCS"),
    ("TIC-202", "Distillation", "DCS"),
    ("PIC-301", "Compression", "SCADA"),
    ("PIC-302", "Compression", "SCADA"),
    ("LIC-401", "Storage", "DCS"),
    ("LIC-402", "Storage", "DCS"),
    ("FCV-501", "Utilities", "PLC"),
    ("FCV-502", "Utilities", "PLC"),
    ("TT-601", "Reactor", "HMI"),
    ("TT-602", "Reactor", "HMI"),
    ("PT-701", "Compression", "SCADA"),
    ("PT-702", "Compression", "SCADA"),
    ("FT-801", "Distillation", "DCS"),
    ("FT-802", "Distillation", "DCS"),
    ("AI-901", "Utilities", "DCS"),
    ("AI-902", "Utilities", "DCS"),
    ("XV-101", "Reactor", "PLC"),
    ("XV-102", "Reactor", "PLC"),
    ("HIC-201", "Distillation", "DCS"),
    ("HIC-202", "Distillation", "DCS"),
    ("SIC-301", "Compression", "SCADA"),
    ("SIC-302", "Compression", "SCADA"),
    ("TIC-501", "Storage", "DCS"),
]

CRITICALITY_CLEAN = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
CRITICALITY_WEIGHTS = [0.15, 0.25, 0.35, 0.25]

STATUS_CLEAN = ["ACTIVE", "ACKNOWLEDGED", "CLEARED"]
STATUS_WEIGHTS = [0.5, 0.3, 0.2]

UNITS = {
    "FIC": "m3/h",
    "TIC": "°C",
    "TT": "°C",
    "PIC": "bar",
    "PT": "bar",
    "LIC": "%",
    "FCV": "%",
    "FT": "m3/h",
    "AI": "ppm",
    "XV": "",
    "HIC": "%",
    "SIC": "rpm",
}

SOURCES = ["DCS-EXPORT", "SCADA-OPC", "HISTORIAN", "PLC-LOG", "HMI-BACKUP"]

ALARM_DESCRIPTIONS = [
    "High temperature alarm",
    "Low pressure warning",
    "Flow rate deviation",
    "Level out of range",
    "Equipment fault detected",
    "Sensor communication failure",
    "Process setpoint deviation",
    "Emergency shutdown triggered",
    "Valve position feedback error",
    "Motor overload protection",
    "Pump cavitation detected",
    "Heat exchanger fouling alarm",
    "Compressor surge warning",
    "Tank overflow protection",
    "Feed rate low alarm",
    "Product quality deviation",
    "Utility failure: steam pressure low",
    "Catalyst temperature runaway",
    "Instrument air failure",
    "Safety interlock activated",
]

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 6, 30)


def _random_timestamp() -> datetime:
    delta = END_DATE - START_DATE
    return START_DATE + timedelta(seconds=int(rng.integers(0, int(delta.total_seconds()))))


def _format_timestamp(dt: datetime, fmt_index: int) -> str:
    formats = [
        dt.strftime("%Y-%m-%d %H:%M:%S"),       # ISO-ish
        dt.strftime("%m/%d/%Y %H:%M"),            # US format
        dt.strftime("%d-%b-%Y %H:%M:%S"),         # Day-Mon-Year
        str(int(dt.timestamp())),                  # Unix epoch string
    ]
    return formats[fmt_index % 4]

def generate_dataset(n: int = 10_000, output_path: str | None = None) -> pd.DataFrame:
    """
    Generate a synthetic SCADA alarm dataset with realistic domain values
    and intentional data quality issues for ETL testing purposes.

    Args:
        n: Number of base rows before duplicate injection.
        output_path: If provided, saves the CSV to this path.

    Returns:
        DataFrame with all issues applied.
    """
    rows = []

    for i in range(n):
        tag_info = random.choice(TAGS)
        tag_name = tag_info[0]
        area = tag_info[1]
        source = random.choice(SOURCES)

        ts = _random_timestamp()
        fmt_index = i % 4  # rotate through 4 formats

        prefix = tag_name.split("-")[0]
        base_value = _generate_value(prefix)
        unit = UNITS.get(prefix, "")
        criticality = str(rng.choice(CRITICALITY_CLEAN, p=CRITICALITY_WEIGHTS))
        status = str(rng.choice(STATUS_CLEAN, p=STATUS_WEIGHTS))
        description = random.choice(ALARM_DESCRIPTIONS)

        rows.append({
            "id": i + 1,
            "timestamp": _format_timestamp(ts, fmt_index),
            "tag": tag_name,
            "criticality": criticality,
            "value": base_value,
            "unit": unit,
            "status": status,
            "description": description,
            "source": source,
            "area": area,
        })

    df = pd.DataFrame(rows)

    df = _inject_null_criticality(df)       # Issue 01
    df = _inject_null_value(df)             # Issue 02
    df = _inject_null_tag(df)               # Issue 03
    df = _inject_null_description(df)       # Issue 04
    # Issue 05: already applied via _format_timestamp rotation
    df = _inject_casing_criticality(df)     # Issue 06
    df = _inject_boolean_status(df)         # Issue 07
    df = _inject_duplicates(df)             # Issue 08
    df = _inject_near_duplicates(df)        # Issue 09
    df = _inject_value_with_units(df)       # Issue 10
    df = _inject_invalid_timestamps(df)     # Issue 11
    df = _inject_future_timestamps(df)      # Issue 12
    df = _inject_tag_inconsistencies(df)    # Issue 13
    df = _inject_whitespace(df)             # Issue 14

    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"[generate] Dataset saved: {output_path} ({len(df):,} rows)")

    return df


def _generate_value(prefix: str) -> float:
    ranges = {
        "FIC": (0.0, 1000.0),
        "TIC": (20.0, 450.0),
        "TT": (20.0, 450.0),
        "PIC": (0.1, 150.0),
        "PT": (0.1, 150.0),
        "LIC": (0.0, 100.0),
        "FCV": (0.0, 100.0),
        "FT": (0.0, 1000.0),
        "AI": (0.0, 500.0),
        "XV": (0.0, 1.0),
        "HIC": (0.0, 100.0),
        "SIC": (0.0, 3600.0),
    }
    lo, hi = ranges.get(prefix, (0.0, 100.0))
    return round(float(rng.uniform(lo, hi)), 4)

def _inject_null_criticality(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 01: ~8% of criticality values set to null."""
    mask = rng.random(len(df)) < 0.08
    df = df.copy()
    df.loc[mask, "criticality"] = np.nan
    return df


def _inject_null_value(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 02: ~12% of numeric values set to null (sensor offline)."""
    mask = rng.random(len(df)) < 0.12
    df = df.copy()
    df.loc[mask, "value"] = np.nan
    return df


def _inject_null_tag(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 03: ~3% of tags set to null (source system export bug)."""
    mask = rng.random(len(df)) < 0.03
    df = df.copy()
    df.loc[mask, "tag"] = np.nan
    return df


def _inject_null_description(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 04: ~15% of descriptions set to null."""
    mask = rng.random(len(df)) < 0.15
    df = df.copy()
    df.loc[mask, "description"] = np.nan
    return df


def _inject_casing_criticality(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 06: criticality variants — mixed case, typos, abbreviations."""
    variants = {
        "CRITICAL": ["critical", "Critical", "CRITICAL", "Crit", "crit", "CRITICA"],
        "HIGH": ["high", "High", "HIGH", "Hi", "hi"],
        "MEDIUM": ["medium", "Medium", "MEDIUM", "Med", "med"],
        "LOW": ["low", "Low", "LOW", "Lo"],
    }
    df = df.copy()
    mask = rng.random(len(df)) < 0.30  # 30% of rows get a variant
    for idx in df[mask].index:
        orig = df.at[idx, "criticality"]
        if pd.notna(orig) and str(orig) in variants:
            df.at[idx, "criticality"] = random.choice(variants[str(orig)])
    return df


def _inject_boolean_status(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 07: status field with boolean/string variants."""
    status_variants = {
        "ACTIVE": ["active", "Active", "true", "TRUE", "1", "yes", "YES"],
        "ACKNOWLEDGED": ["acknowledged", "Acknowledged", "ack", "ACK"],
        "CLEARED": ["cleared", "Cleared", "false", "FALSE", "0", "no", "NO"],
    }
    df = df.copy()
    mask = rng.random(len(df)) < 0.25
    for idx in df[mask].index:
        orig = df.at[idx, "status"]
        if str(orig) in status_variants:
            df.at[idx, "status"] = random.choice(status_variants[str(orig)])
    return df


def _inject_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 08: ~5% of rows are exact duplicates."""
    n_dupes = max(1, int(len(df) * 0.05))
    dupe_rows = df.sample(n=n_dupes, random_state=1)
    return pd.concat([df, dupe_rows], ignore_index=True)


def _inject_near_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 09: ~3% near-duplicates — same alarm, different source."""
    n_near = max(1, int(len(df) * 0.03))
    near_rows = df.sample(n=n_near, random_state=2).copy()
    near_rows["source"] = near_rows["source"].apply(
        lambda s: random.choice(SOURCES) if pd.notna(s) else s
    )
    return pd.concat([df, near_rows], ignore_index=True)


def _inject_value_with_units(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 10: ~10% of values have embedded unit strings."""
    units_suffixes = ["psi", "rpm", "bar", "kPa", "m3/h", "gpm", "%"]
    df = df.copy()
    # Convert to object dtype to allow mixed numeric + string values (simulates raw export)
    df["value"] = df["value"].astype(object)
    mask = rng.random(len(df)) < 0.10
    for idx in df[mask].index:
        v = df.at[idx, "value"]
        if pd.notna(v):
            df.at[idx, "value"] = f"{v} {random.choice(units_suffixes)}"
    return df


def _inject_invalid_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 11: ~2% of timestamps replaced with unparseable strings."""
    garbage = [
        "N/A", "NULL", "undefined", "2023-13-45 99:99:99",
        "not-a-date", "####", "", "0000-00-00",
    ]
    df = df.copy()
    mask = rng.random(len(df)) < 0.02
    for idx in df[mask].index:
        df.at[idx, "timestamp"] = random.choice(garbage)
    return df


def _inject_future_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 12: ~2% of timestamps are in the future (clock drift simulation)."""
    future_start = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=1)
    df = df.copy()
    mask = rng.random(len(df)) < 0.02
    for idx in df[mask].index:
        future_ts = future_start + timedelta(days=int(rng.integers(1, 365)))
        df.at[idx, "timestamp"] = future_ts.strftime("%Y-%m-%d %H:%M:%S")
    return df


def _inject_tag_inconsistencies(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 13: tag name variants — case, separator, whitespace."""
    def mangle_tag(tag: str) -> str:
        if not isinstance(tag, str):
            return tag
        variant = int(rng.integers(0, 5))
        if variant == 0:
            return tag.lower()
        elif variant == 1:
            return tag.replace("-", "")
        elif variant == 2:
            return tag.replace("-", "_")
        elif variant == 3:
            return f" {tag}"  # leading space
        else:
            return tag  # keep clean

    df = df.copy()
    mask = rng.random(len(df)) < 0.20
    df.loc[mask, "tag"] = df.loc[mask, "tag"].apply(
        lambda t: mangle_tag(t) if pd.notna(t) else t
    )
    return df


def _inject_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Issue 14: leading/trailing whitespace in string fields."""
    df = df.copy()
    mask = rng.random(len(df)) < 0.05
    for col in ["description", "source", "area"]:
        col_mask = mask & df[col].notna()
        df.loc[col_mask, col] = df.loc[col_mask, col].apply(
            lambda s: f"  {s}  " if isinstance(s, str) else s
        )
    return df


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "data/raw_alarms.csv"
    generate_dataset(n=10_000, output_path=output)

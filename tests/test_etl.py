"""
ETL unit tests.

Tests each pipeline stage in isolation so failures are easy to pinpoint.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from etl.clean import clean
from etl.normalize import (
    NormalizeStats,
    _normalize_criticality,
    _normalize_status,
    _normalize_tags,
    _normalize_timestamps,
)

class TestClean:
    def _make_df(self, **overrides) -> pd.DataFrame:
        base = {
            "id": "1",
            "timestamp": "2023-06-15 10:00:00",
            "tag": "FIC-101",
            "criticality": "HIGH",
            "value": "42.5",
            "unit": "bar",
            "status": "ACTIVE",
            "description": "Test alarm",
            "source": "DCS",
            "area": "Reactor",
        }
        base.update(overrides)
        return pd.DataFrame([base])

    def test_removes_exact_duplicates(self):
        df = pd.DataFrame([
            {"id": "1", "timestamp": "2023-06-15 10:00:00", "tag": "FIC-101",
             "criticality": "HIGH", "value": "42.5", "unit": "bar",
             "status": "ACTIVE", "description": "Test", "source": "DCS", "area": "Reactor"},
            {"id": "1", "timestamp": "2023-06-15 10:00:00", "tag": "FIC-101",
             "criticality": "HIGH", "value": "42.5", "unit": "bar",
             "status": "ACTIVE", "description": "Test", "source": "DCS", "area": "Reactor"},
        ])
        cleaned, stats = clean(df)
        assert len(cleaned) == 1
        assert stats.exact_duplicates_removed == 1

    def test_rejects_null_tag_rows(self):
        df = pd.DataFrame([
            {"id": "1", "timestamp": "2023-01-01 00:00:00", "tag": "FIC-101",
             "criticality": "HIGH", "value": "1.0", "unit": "bar",
             "status": "ACTIVE", "description": "a", "source": "DCS", "area": "Reactor"},
            {"id": "2", "timestamp": "2023-01-01 01:00:00", "tag": None,
             "criticality": "HIGH", "value": "2.0", "unit": "bar",
             "status": "ACTIVE", "description": "b", "source": "DCS", "area": "Reactor"},
        ])
        cleaned, stats = clean(df)
        assert len(cleaned) == 1
        assert stats.null_tag_rejected == 1

    def test_imputes_unknown_criticality(self):
        df = self._make_df(criticality="")
        cleaned, stats = clean(df)
        assert cleaned.iloc[0]["criticality"] == "UNKNOWN"
        assert stats.null_criticality_imputed == 1

    def test_imputes_description(self):
        df = self._make_df(description="NULL")
        cleaned, stats = clean(df)
        assert cleaned.iloc[0]["description"] == "[NO DESCRIPTION]"
        assert stats.null_description_imputed == 1

    def test_strips_whitespace(self):
        df = self._make_df(description="  Test alarm  ")
        cleaned, stats = clean(df)
        assert cleaned.iloc[0]["description"] == "Test alarm"

    def test_null_sentinel_normalized(self):
        """'N/A', 'undefined', '' → NaN before further processing."""
        df = self._make_df(criticality="N/A")
        cleaned, stats = clean(df)
        assert cleaned.iloc[0]["criticality"] == "UNKNOWN"

    def test_removes_near_duplicates_keeps_non_null_source(self):
        df = pd.DataFrame([
            {"id": "1", "timestamp": "2023-01-01 00:00:00", "tag": "FIC-101",
             "criticality": "HIGH", "value": "1.0", "unit": "bar",
             "status": "ACTIVE", "description": "a", "source": "DCS", "area": "Reactor"},
            {"id": "2", "timestamp": "2023-01-01 00:00:00", "tag": "FIC-101",
             "criticality": "HIGH", "value": "1.0", "unit": "bar",
             "status": "ACTIVE", "description": "a", "source": None, "area": "Reactor"},
        ])
        cleaned, stats = clean(df)
        assert len(cleaned) == 1
        assert cleaned.iloc[0]["source"] == "DCS"

class TestNormalizeCriticality:
    def test_canonical_uppercase_passthrough(self):
        df = pd.DataFrame({"criticality": ["CRITICAL", "HIGH", "MEDIUM", "LOW"]})
        stats = NormalizeStats()
        result, _ = _normalize_criticality(df, stats)
        assert list(result["criticality"]) == ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

    def test_lowercase_mapped(self):
        df = pd.DataFrame({"criticality": ["critical", "high", "medium", "low"]})
        stats = NormalizeStats()
        result, _ = _normalize_criticality(df, stats)
        assert list(result["criticality"]) == ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

    def test_abbreviation_mapped(self):
        df = pd.DataFrame({"criticality": ["crit", "hi", "med", "lo"]})
        stats = NormalizeStats()
        result, _ = _normalize_criticality(df, stats)
        assert list(result["criticality"]) == ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

    def test_unknown_variant_mapped(self):
        df = pd.DataFrame({"criticality": ["garbage", "???", "UNKNOWN"]})
        stats = NormalizeStats()
        result, _ = _normalize_criticality(df, stats)
        assert all(v == "UNKNOWN" for v in result["criticality"])


class TestNormalizeStatus:
    def test_boolean_variants_mapped_to_active(self):
        df = pd.DataFrame({"status": ["true", "1", "yes", "TRUE"]})
        stats = NormalizeStats()
        result, _ = _normalize_status(df, stats)
        assert all(v == "ACTIVE" for v in result["status"])

    def test_false_variants_mapped_to_cleared(self):
        df = pd.DataFrame({"status": ["false", "0", "no", "FALSE"]})
        stats = NormalizeStats()
        result, _ = _normalize_status(df, stats)
        assert all(v == "CLEARED" for v in result["status"])

    def test_ack_variant_mapped(self):
        df = pd.DataFrame({"status": ["ack", "ACK", "acknowledged"]})
        stats = NormalizeStats()
        result, _ = _normalize_status(df, stats)
        assert all(v == "ACKNOWLEDGED" for v in result["status"])


class TestNormalizeTags:
    def test_lowercase_uppercased(self):
        df = pd.DataFrame({"tag": ["fic-101"]})
        stats = NormalizeStats()
        result, _ = _normalize_tags(df, stats)
        assert result.iloc[0]["tag"] == "FIC-101"

    def test_underscore_replaced_with_hyphen(self):
        df = pd.DataFrame({"tag": ["FIC_101"]})
        stats = NormalizeStats()
        result, _ = _normalize_tags(df, stats)
        assert result.iloc[0]["tag"] == "FIC-101"

    def test_leading_space_stripped(self):
        df = pd.DataFrame({"tag": [" FIC-101"]})
        stats = NormalizeStats()
        result, _ = _normalize_tags(df, stats)
        assert result.iloc[0]["tag"] == "FIC-101"


class TestNormalizeTimestamps:
    def test_iso_format_parsed(self):
        df = pd.DataFrame({"timestamp": ["2023-06-15 10:30:00"]})
        stats = NormalizeStats()
        result, s = _normalize_timestamps(df, stats)
        assert result.iloc[0]["timestamp"] is not pd.NaT
        assert s.timestamps_parsed == 1

    def test_us_format_parsed(self):
        df = pd.DataFrame({"timestamp": ["06/15/2023 10:30"]})
        stats = NormalizeStats()
        result, _ = _normalize_timestamps(df, stats)
        assert pd.notna(result.iloc[0]["timestamp"])

    def test_epoch_string_parsed(self):
        df = pd.DataFrame({"timestamp": ["1686825000"]})
        stats = NormalizeStats()
        result, _ = _normalize_timestamps(df, stats)
        assert pd.notna(result.iloc[0]["timestamp"])

    def test_garbage_becomes_nat(self):
        df = pd.DataFrame({"timestamp": ["not-a-date", "####"]})
        stats = NormalizeStats()
        result, _ = _normalize_timestamps(df, stats)
        assert result["timestamp"].isna().all()

    def test_future_timestamp_clamped(self):
        future = (
            datetime.now(timezone.utc) + timedelta(days=30)
        ).strftime("%Y-%m-%d %H:%M:%S")
        df = pd.DataFrame({"timestamp": [future]})
        stats = NormalizeStats()
        result, s = _normalize_timestamps(df, stats)
        assert s.future_timestamps_clamped == 1
        now = pd.Timestamp.now(tz="UTC").tz_localize(None)
        assert result.iloc[0]["timestamp"] <= now

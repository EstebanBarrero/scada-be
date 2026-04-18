"""
API integration tests.

Use TestClient with in-memory DB (see conftest.py).
Tests cover: filters, pagination, validation errors, 404 handling.
"""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import insert, text

from app.models.alarm import Alarm
from app.models.tag import Tag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def seed_tags(db) -> list[int]:
    """Insert test tags, return their IDs."""
    tags = [
        {"name": "FIC-101", "area": "Reactor", "system": "DCS"},
        {"name": "TIC-201", "area": "Distillation", "system": "DCS"},
    ]
    db.execute(insert(Tag), tags)
    db.flush()
    tag_ids = db.execute(text("SELECT id FROM tags ORDER BY id")).fetchall()
    return [row[0] for row in tag_ids]


def seed_alarms(db, tag_ids: list[int]) -> None:
    """Insert a mix of alarms for filtering tests."""
    # Naive UTC datetime — matches how the ETL pipeline stores timestamps
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    alarms = [
        {
            "tag_id": tag_ids[0],
            "raw_tag": "FIC-101",
            "criticality": "CRITICAL",
            "timestamp": now - timedelta(hours=2),
            "value": 55.0,
            "status": "ACTIVE",
            "ingested_at": now,
        },
        {
            "tag_id": tag_ids[0],
            "raw_tag": "FIC-101",
            "criticality": "HIGH",
            "timestamp": now - timedelta(hours=1),
            "value": 42.0,
            "status": "ACKNOWLEDGED",
            "ingested_at": now,
        },
        {
            "tag_id": tag_ids[1],
            "raw_tag": "TIC-201",
            "criticality": "LOW",
            "timestamp": now - timedelta(minutes=30),
            "value": 22.1,
            "status": "CLEARED",
            "ingested_at": now,
        },
    ]
    db.execute(insert(Alarm), alarms)
    db.flush()


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# GET /api/v1/alarms
# ---------------------------------------------------------------------------


class TestListAlarms:
    def test_returns_paginated_structure(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/alarms")
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        assert "meta" in body
        assert "page" in body["meta"]
        assert "size" in body["meta"]
        assert "total" in body["meta"]
        assert "pages" in body["meta"]

    def test_filter_by_criticality(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/alarms?criticality=CRITICAL")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert all(a["criticality"] == "CRITICAL" for a in data)

    def test_filter_by_multiple_criticality(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/alarms?criticality=CRITICAL&criticality=HIGH")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert all(a["criticality"] in ("CRITICAL", "HIGH") for a in data)

    def test_filter_by_tag(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/alarms?tag=FIC")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert all("FIC" in a["raw_tag"] for a in data)

    def test_pagination(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/alarms?page=1&size=2")
        assert resp.status_code == 200
        assert len(resp.json()["data"]) <= 2
        assert resp.json()["meta"]["size"] == 2

    def test_invalid_page_returns_422(self, client):
        resp = client.get("/api/v1/alarms?page=0")
        assert resp.status_code == 422

    def test_size_exceeds_max_returns_422(self, client):
        resp = client.get("/api/v1/alarms?size=9999")
        assert resp.status_code == 422

    def test_invalid_criticality_returns_422(self, client):
        resp = client.get("/api/v1/alarms?criticality=INVALID")
        assert resp.status_code == 422

    def test_invalid_date_returns_400(self, client):
        resp = client.get("/api/v1/alarms?start_date=not-a-date")
        assert resp.status_code == 400

    def test_start_after_end_returns_4xx(self, client):
        resp = client.get(
            "/api/v1/alarms?start_date=2024-01-10T00:00:00&end_date=2024-01-01T00:00:00"
        )
        # 400 or 422 both acceptable — client error for invalid date range
        assert resp.status_code in (400, 422)


# ---------------------------------------------------------------------------
# GET /api/v1/alarms/{id}
# ---------------------------------------------------------------------------


class TestGetAlarm:
    def test_get_existing_alarm(self, client, db):
        seed_alarms(db, seed_tags(db))
        list_resp = client.get("/api/v1/alarms?size=1")
        alarm_id = list_resp.json()["data"][0]["id"]

        resp = client.get(f"/api/v1/alarms/{alarm_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == alarm_id

    def test_get_nonexistent_alarm_returns_404(self, client):
        resp = client.get("/api/v1/alarms/999999")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/v1/metrics/*
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_top_tags_structure(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/metrics/top-tags")
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        assert "limit" in body
        assert len(body["data"]) > 0
        item = body["data"][0]
        assert "tag_name" in item
        assert "alarm_count" in item

    def test_by_criticality_structure(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/metrics/by-criticality")
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        assert "total" in body
        assert sum(i["count"] for i in body["data"]) == body["total"]

    def test_percentages_sum_to_100(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/metrics/by-criticality")
        total_pct = sum(i["percentage"] for i in resp.json()["data"])
        assert abs(total_pct - 100.0) < 1.0  # allow rounding error

    def test_timeline_day_interval(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/metrics/timeline?interval=day")
        assert resp.status_code == 200
        assert resp.json()["interval"] == "day"

    def test_timeline_hour_interval(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/metrics/timeline?interval=hour")
        assert resp.status_code == 200
        assert resp.json()["interval"] == "hour"

    def test_timeline_invalid_interval_returns_422(self, client):
        resp = client.get("/api/v1/metrics/timeline?interval=week")
        assert resp.status_code == 422

    def test_top_tags_limit_param(self, client, db):
        seed_alarms(db, seed_tags(db))
        resp = client.get("/api/v1/metrics/top-tags?limit=1")
        assert len(resp.json()["data"]) <= 1

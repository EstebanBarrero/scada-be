"""
Rate limiter singleton.

Using a module-level singleton so all routers share the same Limiter instance.
Keyed by remote IP address — appropriate for a plant-internal API where
clients are identifiable systems, not anonymous users.

Limits per endpoint (conservative for an industrial API):
  - GET /alarms, /alarms/{id}: 60/minute  — main query endpoints
  - GET /metrics/*:             30/minute  — aggregation queries are heavier
  - POST /etl/run:               5/minute  — pipeline is expensive; should be rare

For production: replace IP-based keying with API key keying once auth is added.
"""

from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])

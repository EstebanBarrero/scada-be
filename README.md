# SCADA Alarm System — Backend

REST API for processing and querying industrial SCADA alarm data.
Includes a full ETL pipeline for ingestion, cleaning, and normalization of legacy datasets.

## Stack

- **Python 3.13** + **FastAPI** — web framework and request validation
- **SQLAlchemy 2.0** — ORM and Core for bulk inserts
- **SQLite** — relational database (portable to SQL Server)
- **pandas** — data processing and cleaning
- **pytest** — automated testing

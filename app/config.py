from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Database
    DATABASE_URL: str = f"sqlite:///{BASE_DIR}/data/scada.db"

    # API
    API_PREFIX: str = "/api/v1"
    PROJECT_NAME: str = "SCADA Alarm System"
    VERSION: str = "1.0.0"

    # ETL
    ETL_DATA_PATH: str = str(BASE_DIR / "data" / "raw_alarms.csv")
    ETL_CHUNK_SIZE: int = 500
    ETL_DATASET_SIZE: int = 10_000

    # Pagination
    DEFAULT_PAGE_SIZE: int = 50
    MAX_PAGE_SIZE: int = 500


settings = Settings()

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    database_url: str = "sqlite:///./data/scada.db"

    app_env: str = "development"
    app_debug: bool = False
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    etl_data_dir: str = "./data"
    etl_dataset_file: str = "alarms_raw.csv"
    etl_batch_size: int = 500

    default_page_size: int = 20
    max_page_size: int = 100


settings = Settings()

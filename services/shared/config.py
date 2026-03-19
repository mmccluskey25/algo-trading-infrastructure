from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # container variables
    prefix: str | None = None
    host_data_path: str | None = None

    # oanda listener
    account_id: str | None = None
    api_token: str | None = None
    instruments: str | None = None

    # redis
    redis_port: int = 6379
    redis_ext_port: int = 6379
    redis_host: str | None = None
    host_redis_path: str | None = None

    # compactor-ingestor
    delete_after_compaction: bool = True
    compaction_interval_mins: int = 720
    bronze_dir: str = "/data/bronze"

    # stream writer
    landing_dir: str = "./data/landing"
    writer_batch_interval: int = 60
    queue_key: str = "tick_queue:oanda"
    broker_name: str = "oanda"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )


settings = Settings()

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # container variables
    prefix: str
    host_data_path: str

    # oanda listener
    account_id: str
    api_token: str
    instruments: str

    # redis
    redis_port: int
    redis_ext_port: int
    redis_host: str
    host_redis_path: str

    # compactor-ingestor
    delete_after_compaction: bool = True
    compaction_interval_mins: int = 720
    bronze_dir: str = "/data/bronze/ticks"

    # stream writer
    landing_dir: str = "./data/landing/ticks"
    writer_batch_interval: int = 60
    queue_key: str = "tick_queue:oanda"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )


settings = Settings()

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # container variables
    prefix: str | None = None
    host_data_path: str | None = None
    data_root: str = "/data"

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

    # stream writer
    writer_batch_interval: int = 60
    queue_key: str = "tick_queue:oanda"
    broker_name: str = "oanda"

    # candle builder
    candle_builder_queue_key: str = "tick_queue:candle_builder"
    candle_flush_interval: int = 900
    candle_redis_ttl: int = 172800

    @property
    def landing_dir(self) -> str:
        return f"{self.data_root}/landing"

    @property
    def bronze_dir(self) -> str:
        return f"{self.data_root}/bronze"

    @property
    def duckdb_path(self) -> str:
        return f"{self.data_root}/catalog.duckdb"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )


settings = Settings()

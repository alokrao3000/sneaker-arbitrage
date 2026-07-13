from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql://arbitrage:arbitrage_dev@localhost:5432/sneaker_arbitrage"
    host: str = "0.0.0.0"
    port: int = 8000

    roi_threshold: float = 20.0
    commission_rate: float = 0.12

    scrape_delay_min: float = 1.5
    scrape_delay_max: float = 4.0
    scrape_interval_minutes: int = 60

    kicks_dev_api_key: str = ""
    alias_api_key: str = ""


settings = Settings()

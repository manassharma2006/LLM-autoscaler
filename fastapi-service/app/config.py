from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_QUEUE: str = "inference_queue"

    class Config:
        env_file = ".env"


settings = Settings()

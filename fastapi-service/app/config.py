import os
from pathlib import Path
from pydantic_settings import BaseSettings


def _load_dotenv() -> None:
    base = Path(__file__).resolve().parent.parent
    for env_path in (base / ".env", base.parent / ".env"):
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()


class Settings(BaseSettings):
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_QUEUE: str = "inference_queue"

    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3.2:1b"

    INFERENCE_BACKEND: str = "ollama"
    CLOUD_API_KEY: str = ""
    CLOUD_MODEL: str = "llama-3.1-8b-instant"
    CLOUD_MAX_REQUESTS: int = 500
    WORKER_TIER: str = "fast"


settings = Settings()

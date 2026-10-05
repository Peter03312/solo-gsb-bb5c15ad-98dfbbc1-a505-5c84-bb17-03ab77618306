"""应用配置（环境变量可覆盖）。"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STOWAGE_", env_file=".env", extra="ignore")

    database_url: str = f"sqlite:///{BASE_DIR / 'data' / 'stowage.db'}"
    api_prefix: str = "/api"
    app_name: str = "集装箱船配载方案核算服务"
    # 启动升级时后到进程等待先到进程完成升级的最长时间（毫秒，仅 SQLite）
    startup_lock_timeout_ms: int = 300_000


@lru_cache
def get_settings() -> Settings:
    return Settings()

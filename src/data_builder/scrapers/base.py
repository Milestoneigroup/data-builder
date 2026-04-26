"""Shared HTTP / parsing helpers for scrapers."""

from abc import ABC, abstractmethod
from typing import Any

import httpx

from data_builder.config import Settings, get_settings


class BaseScraper(ABC):
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()

    def _client(self) -> httpx.Client:
        kwargs: dict = {
            "headers": {"User-Agent": self._settings.scraper_user_agent},
            "timeout": self._settings.request_timeout_seconds,
        }
        https = (self._settings.https_proxy or "").strip()
        http = (self._settings.http_proxy or "").strip()
        if https or http:
            kwargs["proxy"] = https or http
        return httpx.Client(**kwargs)

    @abstractmethod
    def fetch(self, *args: Any, **kwargs: Any) -> Any:
        """Subclasses implement fetch + parse."""

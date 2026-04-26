from datetime import datetime, timezone

from pydantic import BaseModel, Field


class ScrapedDocument(BaseModel):
    """Example normalized document shape for downstream augmentation."""

    source_url: str
    title: str | None = None
    body_text: str | None = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

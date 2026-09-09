"""Canonical marketing facts and deliberately safe KPI calculations."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from enum import StrEnum
import math
import re
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.onboarding.models import IDENTIFIER_PATTERN


class MarketingGrain(StrEnum):
    CAMPAIGN_DAILY = "campaign_daily"
    AD_GROUP_DAILY = "ad_group_daily"
    AD_DAILY = "ad_daily"


RAW_ADDITIVE_METRICS = (
    "spend", "impressions", "clicks", "platform_conversions",
    "platform_conversion_value", "link_clicks", "video_views", "landing_page_views",
)
NON_ADDITIVE_METRICS = ("reach", "frequency")
DERIVED_METRICS = ("ctr", "cpc", "cpm", "cpa", "platform_roas")


def safe_divide(numerator: float | int, denominator: float | int) -> float | None:
    """Return a finite ratio or null when its denominator is zero."""
    if denominator == 0:
        return None
    value = float(numerator) / float(denominator)
    return value if math.isfinite(value) else None


def kpis(*, spend: float, impressions: int, clicks: int,
         platform_conversions: float, platform_conversion_value: float) -> dict[str, float | None]:
    return {
        "ctr": safe_divide(clicks, impressions),
        "cpc": safe_divide(spend, clicks),
        "cpm": None if impressions == 0 else safe_divide(spend * 1000.0, impressions),
        "cpa": safe_divide(spend, platform_conversions),
        "platform_roas": safe_divide(platform_conversion_value, spend),
    }


@dataclass(frozen=True, slots=True, kw_only=True)
class MarketingRecord:
    business_id: str
    source_type: str
    source_id: str
    ingestion_id: str
    record_id: str
    extracted_at_utc: datetime
    source_updated_at_utc: datetime | None
    schema_version: str
    platform: str
    account_id: str
    campaign_id: str
    campaign_name: str | None
    ad_group_id: str | None
    ad_group_name: str | None
    ad_id: str | None
    ad_name: str | None
    creative_id: str | None
    report_date: date
    reporting_timezone: str
    currency: str
    spend: float
    impressions: int
    clicks: int
    platform_conversions: float
    platform_conversion_value: float
    reach: int | None = None
    frequency: float | None = None
    link_clicks: int | None = None
    video_views: int | None = None
    landing_page_views: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("business_id", "source_type", "source_id", "platform"):
            if not IDENTIFIER_PATTERN.fullmatch(getattr(self, name)):
                raise ValueError(f"{name} must be a stable lowercase identifier")
        for name in ("account_id", "campaign_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"{name} is required")
        if bool(self.ad_id) and not self.ad_group_id:
            raise ValueError("ad_id requires ad_group_id")
        if not re.fullmatch(r"[A-Z]{3}", self.currency):
            raise ValueError("currency must be an uppercase ISO-style code")
        try:
            ZoneInfo(self.reporting_timezone)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError("reporting_timezone must be an IANA timezone") from None
        for name in ("extracted_at_utc", "source_updated_at_utc"):
            value = getattr(self, name)
            if value is not None:
                if value.utcoffset() is None:
                    raise ValueError(f"{name} must be timezone-aware")
                object.__setattr__(self, name, value.astimezone(timezone.utc))
        required_metrics = ("spend", "impressions", "clicks", "platform_conversions",
                            "platform_conversion_value")
        optional_metrics = ("reach", "frequency", "link_clicks", "video_views",
                            "landing_page_views")
        for name in (*required_metrics, *optional_metrics):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))
                                      or not math.isfinite(value) or value < 0):
                raise ValueError(f"{name} must be finite and nonnegative when supplied")
        if self.clicks > self.impressions:
            raise ValueError("clicks cannot exceed impressions for canonical ad reporting")
        if not isinstance(self.details, dict):
            raise ValueError("details must be an object")

    @property
    def grain(self) -> MarketingGrain:
        if self.ad_id is not None:
            return MarketingGrain.AD_DAILY
        if self.ad_group_id is not None:
            return MarketingGrain.AD_GROUP_DAILY
        return MarketingGrain.CAMPAIGN_DAILY

    @property
    def logical_key(self) -> tuple[str, ...]:
        base = (self.business_id, self.source_type, self.source_id, self.schema_version,
                self.platform, self.account_id, self.campaign_id)
        if self.grain == MarketingGrain.AD_DAILY:
            return (*base, str(self.ad_group_id), str(self.ad_id), self.report_date.isoformat())
        if self.grain == MarketingGrain.AD_GROUP_DAILY:
            return (*base, str(self.ad_group_id), self.report_date.isoformat())
        return (*base, self.report_date.isoformat())

    def to_dict(self) -> dict[str, Any]:
        output = asdict(self)
        output["report_date"] = self.report_date.isoformat()
        output["extracted_at_utc"] = self.extracted_at_utc.isoformat().replace("+00:00", "Z")
        output["source_updated_at_utc"] = (
            self.source_updated_at_utc.isoformat().replace("+00:00", "Z")
            if self.source_updated_at_utc else None
        )
        output["grain"] = self.grain.value
        return output

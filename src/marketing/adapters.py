"""Offline marketing adapters layered on the Phase 5.9 SourceAdapter contract."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from src.marketing.models import MarketingRecord
from src.onboarding.adapters import AdapterHealth, MockMetaAdsAdapter, MockSourceAdapter
from src.onboarding.contracts import contract_for
from src.onboarding.models import IngestionEnvelope, SourceConfig


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = PROJECT_ROOT / "data" / "fixtures" / "marketing"
EXTRACTED_AT = datetime(2026, 1, 7, 12, tzinfo=timezone.utc)
DEFAULT_LOOKBACK_DAYS = 3


def reporting_start(watermark: date | None, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> date | None:
    """Return the inclusive re-read boundary used by eventual API connectors."""
    if not isinstance(lookback_days, int) or isinstance(lookback_days, bool) or lookback_days < 0:
        raise ValueError("lookback_days must be a nonnegative integer")
    return None if watermark is None else watermark - timedelta(days=lookback_days)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("source update timestamp must include timezone")
    return parsed.astimezone(timezone.utc)


class MarketingSourceAdapter(MockSourceAdapter):
    """Common deterministic extractor and canonical normalizer for ad platforms.

    The class intentionally subclasses the existing local SourceAdapter
    implementation. Real network transports can replace only ``raw_payloads``;
    envelope, identity, validation, and normalization semantics remain shared.
    """

    fixture_name = ""
    platform = ""

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self.lookback_days = config.metadata.get("lookback_days", DEFAULT_LOOKBACK_DAYS)

    def validate_config(self) -> tuple[str, ...]:
        errors = list(super().validate_config())
        if self.config.metadata.get("adapter", "mock") != "mock":
            errors.append("marketing adapters are offline mock-only in Phase 6.1")
        try:
            reporting_start(date(2026, 1, 1), self.lookback_days)
        except ValueError as error:
            errors.append(str(error))
        return tuple(errors)

    def raw_payloads(self) -> tuple[dict[str, Any], ...]:
        value = json.loads((FIXTURE_ROOT / self.fixture_name).read_text(encoding="utf-8"))
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ValueError(f"Marketing fixture {self.fixture_name} must contain an object array")
        return tuple(value)

    def _canonical(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def extract(self, *, watermark: date | None = None) -> tuple[IngestionEnvelope, ...]:
        errors = self.validate_config()
        if errors:
            raise ValueError("; ".join(errors))
        start = reporting_start(watermark, self.lookback_days)
        contract = contract_for(self.config.source_type, self.config.schema_version)
        ingestion_id = str(uuid5(
            NAMESPACE_URL,
            f"marketing-ingestion|{self.config.business_id}|{self.config.source_id}|{start or 'full'}",
        ))
        output = []
        for payload in self.raw_payloads():
            contract.validate(payload)
            canonical = self._canonical(payload)
            report_date = date.fromisoformat(canonical["report_date"])
            if start is not None and report_date < start:
                continue
            logical = "|".join((
                self.config.business_id, self.config.source_type, self.config.source_id,
                self.config.schema_version, canonical["platform"], canonical["account_id"],
                canonical["campaign_id"], canonical.get("ad_group_id") or "",
                canonical.get("ad_id") or "", canonical["report_date"],
            ))
            output.append(IngestionEnvelope(
                business_id=self.config.business_id,
                source_type=self.config.source_type,
                source_id=self.config.source_id,
                ingestion_id=ingestion_id,
                record_id=str(uuid5(NAMESPACE_URL, f"marketing-record|{logical}")),
                extracted_at_utc=EXTRACTED_AT,
                source_updated_at_utc=_utc(payload[contract.source_timestamp]),
                schema_version=self.config.schema_version,
                payload=dict(payload),
            ))
        return tuple(output)

    def normalize(self, record: IngestionEnvelope) -> dict[str, Any]:
        if (record.business_id, record.source_type, record.source_id) != (
            self.config.business_id, self.config.source_type, self.config.source_id
        ):
            raise ValueError("Envelope identity does not match adapter configuration")
        contract_for(record.source_type, record.schema_version).validate(record.payload)
        values = self._canonical(record.payload)
        normalized = MarketingRecord(
            business_id=record.business_id,
            source_type=record.source_type,
            source_id=record.source_id,
            ingestion_id=record.ingestion_id,
            record_id=record.record_id,
            extracted_at_utc=record.extracted_at_utc,
            source_updated_at_utc=record.source_updated_at_utc,
            schema_version=record.schema_version,
            report_date=date.fromisoformat(values.pop("report_date")),
            **values,
        )
        return normalized.to_dict()

    def healthcheck(self) -> AdapterHealth:
        errors = self.validate_config()
        return AdapterHealth(healthy=not errors, message=("; ".join(errors) if errors
                             else "offline marketing fixture; no network access"))


class MockMetaMarketingAdapter(MarketingSourceAdapter, MockMetaAdsAdapter):
    fixture_name = "meta_ads_daily_v1.json"
    platform = "meta_ads"

    def _canonical(self, p):
        return {
            "platform": self.platform, "account_id": p["account_id"],
            "campaign_id": p["campaign_id"], "campaign_name": p.get("campaign_name"),
            "ad_group_id": p["adset_id"], "ad_group_name": p.get("adset_name"),
            "ad_id": p["ad_id"], "ad_name": p.get("ad_name"), "creative_id": p.get("creative_id"),
            "report_date": p["date_start"], "reporting_timezone": p["account_timezone"],
            "currency": p["account_currency"], "spend": float(p["spend"]),
            "impressions": p["impressions"], "reach": p.get("reach"),
            "frequency": p.get("frequency"), "clicks": p["clicks"],
            "link_clicks": p.get("inline_link_clicks"),
            "platform_conversions": float(p.get("actions") or 0),
            "platform_conversion_value": float(p.get("action_values") or 0),
            "video_views": p.get("video_views"), "landing_page_views": p.get("landing_page_views"),
            "details": {"native_ad_group_term": "adset", **p.get("details", {})},
        }


class MockTikTokMarketingAdapter(MarketingSourceAdapter):
    fixture_name = "tiktok_ads_daily_v1.json"
    platform = "tiktok_ads"

    def _canonical(self, p):
        frequency = None if not p.get("reach") else p["show_cnt"] / p["reach"]
        return {
            "platform": self.platform, "account_id": p["advertiser_id"],
            "campaign_id": p["campaign_id"], "campaign_name": p.get("campaign_name"),
            "ad_group_id": p["adgroup_id"], "ad_group_name": p.get("adgroup_name"),
            "ad_id": p["ad_id"], "ad_name": p.get("ad_name"), "creative_id": p.get("creative_id"),
            "report_date": p["stat_time_day"], "reporting_timezone": p["timezone"],
            "currency": p["currency"], "spend": float(p["stat_cost"]),
            "impressions": p["show_cnt"], "reach": p.get("reach"), "frequency": frequency,
            "clicks": p["click_cnt"], "link_clicks": None,
            "platform_conversions": float(p.get("conversion") or 0),
            "platform_conversion_value": float(p.get("conversion_value") or 0),
            "video_views": p.get("video_play_actions"),
            "landing_page_views": p.get("landing_page_view"),
            "details": {"native_ad_group_term": "adgroup", **p.get("details", {})},
        }


class MockGoogleMarketingAdapter(MarketingSourceAdapter):
    fixture_name = "google_ads_daily_v1.json"
    platform = "google_ads"

    def _canonical(self, p):
        return {
            "platform": self.platform, "account_id": p["customer_id"],
            "campaign_id": p["campaign_id"], "campaign_name": p.get("campaign_name"),
            "ad_group_id": p["ad_group_id"], "ad_group_name": p.get("ad_group_name"),
            "ad_id": p["ad_id"], "ad_name": p.get("ad_name"), "creative_id": p.get("creative_id"),
            "report_date": p["segments_date"], "reporting_timezone": p["timezone"],
            "currency": p["currency"], "spend": p["cost_micros"] / 1_000_000.0,
            "impressions": p["impressions"], "reach": None, "frequency": None,
            "clicks": p["clicks"], "link_clicks": None,
            "platform_conversions": float(p.get("conversions") or 0),
            "platform_conversion_value": float(p.get("conversions_value") or 0),
            "video_views": p.get("video_views"), "landing_page_views": None,
            "details": {"native_ad_group_term": "ad_group", "source_cost_unit": "micros",
                        **p.get("details", {})},
        }


class MockGenericMarketingAdapter(MarketingSourceAdapter):
    fixture_name = "generic_ads_daily_v1.json"

    def _canonical(self, p):
        return {
            "platform": p["platform"].strip().lower(), "account_id": p["account_id"],
            "campaign_id": p["campaign_id"], "campaign_name": p.get("campaign_name"),
            "ad_group_id": p["ad_group_id"], "ad_group_name": p.get("ad_group_name"),
            "ad_id": p["ad_id"], "ad_name": p.get("ad_name"), "creative_id": p.get("creative_id"),
            "report_date": p["report_date"], "reporting_timezone": p["reporting_timezone"],
            "currency": p["currency"], "spend": float(p["spend"]),
            "impressions": p["impressions"], "reach": p.get("reach"),
            "frequency": p.get("frequency"), "clicks": p["clicks"],
            "link_clicks": p.get("link_clicks"),
            "platform_conversions": float(p.get("conversions") or 0),
            "platform_conversion_value": float(p.get("conversion_value") or 0),
            "video_views": p.get("video_views"), "landing_page_views": p.get("landing_page_views"),
            "details": {"native_ad_group_term": "ad_group", **p.get("details", {})},
        }


MARKETING_ADAPTERS = {
    "meta_ads": MockMetaMarketingAdapter,
    "tiktok_ads": MockTikTokMarketingAdapter,
    "google_ads": MockGoogleMarketingAdapter,
    "generic_ads": MockGenericMarketingAdapter,
}


def marketing_adapter_for(config: SourceConfig) -> MarketingSourceAdapter:
    try:
        return MARKETING_ADAPTERS[config.source_type](config)
    except KeyError:
        raise ValueError(f"Source type {config.source_type!r} is not a marketing source") from None

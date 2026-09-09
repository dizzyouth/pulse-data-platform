"""Read-only Shopify Admin GraphQL API adapter.

The adapter selects only analytics fields and deliberately never requests names,
email addresses, phone numbers, street addresses, IP addresses, or free-form
customer/order text.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
import socket
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, uuid4, uuid5

from src.onboarding.contracts import SourceContractError, contract_for
from src.onboarding.credentials import CredentialResolutionError, resolve_credential
from src.onboarding.models import IngestionEnvelope, SourceConfig


DEFAULT_SHOPIFY_API_VERSION = "2026-07"
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100
API_VERSION_PATTERN = re.compile(r"^20\d{2}-(?:01|04|07|10)$")
SHOP_DOMAIN_PATTERN = re.compile(
    r"^(?:[a-z0-9][a-z0-9-]*\.)*myshopify\.com$", re.IGNORECASE
)
ENV_REFERENCE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")


class ShopifyAdapterError(RuntimeError):
    """A sanitized connector failure suitable for logs and operator output."""

    def __init__(self, message: str, *, status: str = "unhealthy"):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True, slots=True, kw_only=True)
class ShopifyHealth:
    healthy: bool
    status: str
    message: str
    configuration_valid: bool
    credentials_present: bool
    authenticated: bool
    api_reachable: bool
    permission_granted: bool
    rate_limited: bool = False


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


HttpTransport = Callable[[str, Mapping[str, str], bytes, float], HttpResponse]
Sleep = Callable[[float], None]


ORDERS_QUERY = """
query PulseOrders($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
    nodes {
      id
      name
      createdAt
      updatedAt
      processedAt
      cancelledAt
      displayFinancialStatus
      displayFulfillmentStatus
      currencyCode
      customer { id }
      shippingAddress { countryCodeV2 }
      totalPriceSet { shopMoney { amount currencyCode } }
      subtotalPriceSet { shopMoney { amount currencyCode } }
      totalDiscountsSet { shopMoney { amount currencyCode } }
      totalTaxSet { shopMoney { amount currencyCode } }
      totalShippingPriceSet { shopMoney { amount currencyCode } }
      totalRefundedSet { shopMoney { amount currencyCode } }
      lineItems(first: 250) {
        nodes {
          id
          product { id }
          variant { id }
          sku
          quantity
          originalUnitPriceSet { shopMoney { amount currencyCode } }
          totalDiscountSet { shopMoney { amount currencyCode } }
        }
        pageInfo { hasNextPage endCursor }
      }
      refunds {
        id
        createdAt
        totalRefundedSet { shopMoney { amount currencyCode } }
        refundLineItems(first: 250) {
          nodes {
            quantity
            subtotalSet { shopMoney { amount currencyCode } }
            lineItem { id product { id } variant { id } sku }
          }
          pageInfo { hasNextPage endCursor }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

ORDER_LINE_ITEMS_QUERY = """
query PulseOrderLineItems($id: ID!, $after: String) {
  order(id: $id) {
    lineItems(first: 250, after: $after) {
      nodes {
        id product { id } variant { id } sku quantity
        originalUnitPriceSet { shopMoney { amount currencyCode } }
        totalDiscountSet { shopMoney { amount currencyCode } }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

REFUND_LINE_ITEMS_QUERY = """
query PulseRefundLineItems($id: ID!, $after: String) {
  refund(id: $id) {
    refundLineItems(first: 250, after: $after) {
      nodes {
        quantity subtotalSet { shopMoney { amount currencyCode } }
        lineItem { id product { id } variant { id } sku }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

HEALTH_QUERY = """
query PulseShopifyHealth {
  shop { id }
  currentAppInstallation { accessScopes { handle } }
}
"""


def _default_transport(
    url: str, headers: Mapping[str, str], body: bytes, timeout: float
) -> HttpResponse:
    request = Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urlopen(request, timeout=timeout) as response:
            return HttpResponse(
                status=int(response.status),
                headers=dict(response.headers.items()),
                body=response.read(),
            )
    except HTTPError as error:
        return HttpResponse(
            status=int(error.code),
            headers=dict(error.headers.items()) if error.headers else {},
            body=error.read(),
        )


def _parse_utc(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError):
        raise ShopifyAdapterError(f"Shopify returned an invalid {field}") from None
    if parsed.utcoffset() is None:
        raise ShopifyAdapterError(f"Shopify returned a timezone-free {field}")
    return parsed.astimezone(timezone.utc)


def _money(value: Any, field: str, *, required: bool = False) -> float | None:
    if value is None and not required:
        return None
    try:
        raw = value["shopMoney"]["amount"]
        return float(raw)
    except (KeyError, TypeError, ValueError, OverflowError):
        raise ShopifyAdapterError(f"Shopify returned malformed money for {field}") from None


def _optional_id(value: Any) -> str | None:
    if isinstance(value, dict) and value.get("id"):
        return str(value["id"])
    return None


def _selected_order(node: Mapping[str, Any]) -> dict[str, Any]:
    """Defense-in-depth allowlist for the Bronze payload."""

    scalar = (
        "id", "name", "createdAt", "updatedAt", "processedAt", "cancelledAt",
        "displayFinancialStatus", "displayFulfillmentStatus", "currencyCode",
    )
    money_fields = (
        "totalPriceSet", "subtotalPriceSet", "totalDiscountsSet", "totalTaxSet",
        "totalShippingPriceSet", "totalRefundedSet",
    )
    selected = {name: node.get(name) for name in scalar}
    selected.update({name: node.get(name) for name in money_fields})
    selected["customer"] = {"id": (node.get("customer") or {}).get("id")} if node.get("customer") else None
    selected["shippingAddress"] = ({"countryCodeV2": (node.get("shippingAddress") or {}).get("countryCodeV2")}
                                   if node.get("shippingAddress") else None)
    lines = []
    for item in (node.get("lineItems") or {}).get("nodes", []):
        lines.append({
            "id": item.get("id"),
            "product": {"id": (item.get("product") or {}).get("id")} if item.get("product") else None,
            "variant": {"id": (item.get("variant") or {}).get("id")} if item.get("variant") else None,
            "sku": item.get("sku"), "quantity": item.get("quantity"),
            "originalUnitPriceSet": item.get("originalUnitPriceSet"),
            "totalDiscountSet": item.get("totalDiscountSet"),
        })
    selected["lineItems"] = {
        "nodes": lines,
        "pageInfo": {"hasNextPage": bool((node.get("lineItems") or {}).get("pageInfo", {}).get("hasNextPage"))},
    }
    refunds = []
    for refund in node.get("refunds") or []:
        refund_lines = []
        for item in (refund.get("refundLineItems") or {}).get("nodes", []):
            line = item.get("lineItem") or {}
            refund_lines.append({
                "quantity": item.get("quantity"), "subtotalSet": item.get("subtotalSet"),
                "lineItem": {
                    "id": line.get("id"),
                    "product": {"id": (line.get("product") or {}).get("id")} if line.get("product") else None,
                    "variant": {"id": (line.get("variant") or {}).get("id")} if line.get("variant") else None,
                    "sku": line.get("sku"),
                },
            })
        refunds.append({
            "id": refund.get("id"), "createdAt": refund.get("createdAt"),
            "totalRefundedSet": refund.get("totalRefundedSet"),
            "refundLineItems": {
                "nodes": refund_lines,
                "pageInfo": {"hasNextPage": bool((refund.get("refundLineItems") or {}).get("pageInfo", {}).get("hasNextPage"))},
            },
        })
    selected["refunds"] = refunds
    return selected


class ShopifyAdminApiAdapter:
    """Extract privacy-minimized order snapshots from Shopify Admin GraphQL."""

    def __init__(
        self,
        config: SourceConfig,
        *,
        environ: Mapping[str, str] | None = None,
        transport: HttpTransport | None = None,
        sleep: Sleep = time.sleep,
        now: Callable[[], datetime] | None = None,
    ):
        self.config = config
        self.environ = environ
        self.transport = transport or _default_transport
        self.sleep = sleep
        self.now = now or (lambda: datetime.now(timezone.utc))
        metadata = config.metadata
        self.api_version = str(metadata.get("api_version", DEFAULT_SHOPIFY_API_VERSION))
        self.domain_ref = str(metadata.get("shop_domain_ref", ""))
        self.page_size = int(metadata.get("page_size", DEFAULT_PAGE_SIZE))
        self.timeout_seconds = float(metadata.get("timeout_seconds", 30))
        self.max_retries = int(metadata.get("max_retries", 3))
        self.backoff_seconds = float(metadata.get("backoff_seconds", 1))

    @property
    def _environment(self) -> Mapping[str, str]:
        if self.environ is None:
            import os

            return os.environ
        return self.environ

    def validate_config(self) -> tuple[str, ...]:
        errors = []
        if not self.config.enabled:
            errors.append("source is disabled")
        if self.config.source_type != "shopify":
            errors.append("source_type must be shopify")
        if not ENV_REFERENCE_PATTERN.fullmatch(self.config.credential_ref or ""):
            errors.append("credential_ref must name an environment variable")
        if not ENV_REFERENCE_PATTERN.fullmatch(self.domain_ref):
            errors.append("metadata.shop_domain_ref must name an environment variable")
        if not API_VERSION_PATTERN.fullmatch(self.api_version):
            errors.append("metadata.api_version must use Shopify YYYY-MM quarterly format")
        if not 1 <= self.page_size <= MAX_PAGE_SIZE:
            errors.append(f"metadata.page_size must be between 1 and {MAX_PAGE_SIZE}")
        if not 0 < self.timeout_seconds <= 120:
            errors.append("metadata.timeout_seconds must be greater than 0 and at most 120")
        if not 0 <= self.max_retries <= 8:
            errors.append("metadata.max_retries must be between 0 and 8")
        if not 0 <= self.backoff_seconds <= 60:
            errors.append("metadata.backoff_seconds must be between 0 and 60")
        try:
            contract_for(self.config.source_type, self.config.schema_version)
        except SourceContractError as error:
            errors.append(str(error))
        return tuple(errors)

    def _credentials(self) -> tuple[str, str]:
        token = resolve_credential(self.config, self._environment)
        domain = self._environment.get(self.domain_ref, "").strip().lower()
        if not domain:
            raise CredentialResolutionError(
                f"Shop domain reference {self.domain_ref!r} is not set for source {self.config.source_id!r}"
            )
        if not SHOP_DOMAIN_PATTERN.fullmatch(domain):
            raise ShopifyAdapterError(
                "Shop domain must be a myshopify.com hostname without a scheme or path",
                status="configuration_invalid",
            )
        return domain, token

    def _sanitized(self, message: str, token: str | None = None) -> str:
        value = str(message)
        if token:
            value = value.replace(token, "[REDACTED]")
        return value[:500]

    def _request(self, query: str, variables: Mapping[str, Any]) -> dict[str, Any]:
        domain, token = self._credentials()
        url = f"https://{domain}/admin/api/{self.api_version}/graphql.json"
        payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "pulse-data-platform-shopify/6.0",
            "X-Shopify-Access-Token": token,
        }
        for attempt in range(self.max_retries + 1):
            response = None
            try:
                response = self.transport(url, headers, payload, self.timeout_seconds)
            except (TimeoutError, socket.timeout) as error:
                failure = ShopifyAdapterError("Shopify request timed out", status="unhealthy")
            except (URLError, OSError) as error:
                failure = ShopifyAdapterError(
                    "Shopify API is unreachable due to a network or DNS error",
                    status="unreachable",
                )
            except Exception as error:
                failure = ShopifyAdapterError(
                    self._sanitized(f"Shopify transport failed: {type(error).__name__}", token),
                    status="unhealthy",
                )
            else:
                failure = self._response_failure(response)
                if failure is None:
                    try:
                        decoded = json.loads(response.body.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        raise ShopifyAdapterError("Shopify returned malformed JSON") from None
                    if not isinstance(decoded, dict):
                        raise ShopifyAdapterError("Shopify returned a non-object response")
                    graphql_errors = decoded.get("errors") or []
                    if graphql_errors:
                        codes = {
                            str(item.get("extensions", {}).get("code", "")).upper()
                            for item in graphql_errors
                            if isinstance(item, dict)
                        }
                        if "THROTTLED" in codes:
                            failure = ShopifyAdapterError(
                                "Shopify GraphQL rate limit was reached", status="rate_limited"
                            )
                        elif codes & {"ACCESS_DENIED", "FORBIDDEN"}:
                            raise ShopifyAdapterError(
                                "Shopify token lacks required order permissions",
                                status="permission_failure",
                            )
                        else:
                            raise ShopifyAdapterError(
                                "Shopify GraphQL returned an application error"
                            )
                    elif not isinstance(decoded.get("data"), dict):
                        raise ShopifyAdapterError("Shopify response is missing data")
                    else:
                        return decoded["data"]
            if attempt >= self.max_retries or failure.status not in {
                "rate_limited", "unreachable", "unhealthy", "server_error"
            }:
                raise failure
            retry_after = 0.0
            if response is not None:
                try:
                    retry_after = float(response.headers.get("Retry-After", 0))
                except (TypeError, ValueError):
                    retry_after = 0.0
            self.sleep(max(retry_after, self.backoff_seconds * (2**attempt)))
        raise ShopifyAdapterError("Shopify request failed")

    @staticmethod
    def _response_failure(response: HttpResponse) -> ShopifyAdapterError | None:
        if response.status == 200:
            return None
        if response.status in (401, 403):
            return ShopifyAdapterError(
                "Shopify authentication failed or order permission is missing",
                status="authentication_failed" if response.status == 401 else "permission_failure",
            )
        if response.status == 404:
            return ShopifyAdapterError(
                "Shopify shop or API endpoint was not found", status="not_found"
            )
        if response.status == 429:
            return ShopifyAdapterError("Shopify rate limit was reached", status="rate_limited")
        if 500 <= response.status <= 599:
            return ShopifyAdapterError(
                f"Shopify API returned server error {response.status}", status="server_error"
            )
        return ShopifyAdapterError(
            f"Shopify API returned HTTP {response.status}", status="unhealthy"
        )

    def _complete_connection(
        self, owner: dict[str, Any], *, owner_field: str, connection_field: str,
        query: str,
    ) -> None:
        connection = owner.get(connection_field)
        if not isinstance(connection, dict) or not isinstance(connection.get("nodes"), list):
            raise ShopifyAdapterError(f"Shopify returned malformed {connection_field}")
        while connection.get("pageInfo", {}).get("hasNextPage"):
            cursor = connection.get("pageInfo", {}).get("endCursor")
            owner_id = owner.get("id")
            if not cursor or not owner_id:
                raise ShopifyAdapterError(f"Shopify {connection_field} pagination cursor is missing")
            data = self._request(query, {"id": owner_id, "after": cursor})
            next_owner = data.get(owner_field)
            next_connection = next_owner.get(connection_field) if isinstance(next_owner, dict) else None
            if not isinstance(next_connection, dict) or not isinstance(next_connection.get("nodes"), list):
                raise ShopifyAdapterError(f"Shopify returned malformed paginated {connection_field}")
            connection["nodes"].extend(next_connection["nodes"])
            connection["pageInfo"] = next_connection.get("pageInfo") or {}

    def _complete_nested_pagination(self, order: dict[str, Any]) -> None:
        self._complete_connection(
            order, owner_field="order", connection_field="lineItems",
            query=ORDER_LINE_ITEMS_QUERY,
        )
        for refund in order.get("refunds") or []:
            if not isinstance(refund, dict):
                raise ShopifyAdapterError("Shopify returned a malformed refund")
            self._complete_connection(
                refund, owner_field="refund", connection_field="refundLineItems",
                query=REFUND_LINE_ITEMS_QUERY,
            )

    def healthcheck(self) -> ShopifyHealth:
        config_errors = self.validate_config()
        if config_errors:
            return ShopifyHealth(
                healthy=False,
                status="configuration_invalid",
                message="; ".join(config_errors),
                configuration_valid=False,
                credentials_present=False,
                authenticated=False,
                api_reachable=False,
                permission_granted=False,
            )
        try:
            self._credentials()
        except (CredentialResolutionError, ShopifyAdapterError) as error:
            return ShopifyHealth(
                healthy=False,
                status=getattr(error, "status", "credentials_missing"),
                message=str(error),
                configuration_valid=True,
                credentials_present=False,
                authenticated=False,
                api_reachable=False,
                permission_granted=False,
            )
        try:
            data = self._request(HEALTH_QUERY, {})
            scopes = {
                str(item.get("handle"))
                for item in data.get("currentAppInstallation", {}).get("accessScopes", [])
                if isinstance(item, dict)
            }
            permission = "read_orders" in scopes or "read_all_orders" in scopes
            return ShopifyHealth(
                healthy=bool(data.get("shop", {}).get("id")) and permission,
                status="healthy" if permission else "permission_failure",
                message=("Shopify API authenticated with order access"
                         if permission else "Shopify token is missing read_orders access"),
                configuration_valid=True,
                credentials_present=True,
                authenticated=True,
                api_reachable=True,
                permission_granted=permission,
            )
        except ShopifyAdapterError as error:
            return ShopifyHealth(
                healthy=False,
                status=error.status,
                message=str(error),
                configuration_valid=True,
                credentials_present=True,
                authenticated=error.status not in {"authentication_failed"},
                api_reachable=error.status not in {"unreachable"},
                permission_granted=False,
                rate_limited=error.status == "rate_limited",
            )

    def extract(
        self,
        *,
        watermark: datetime | None = None,
        backfill_start: datetime | None = None,
        limit: int | None = None,
    ) -> tuple[IngestionEnvelope, ...]:
        errors = self.validate_config()
        if errors:
            raise ShopifyAdapterError("Invalid Shopify source configuration: " + "; ".join(errors))
        if watermark is None and backfill_start is None:
            raise ShopifyAdapterError(
                "Initial Shopify extraction requires an explicit backfill start",
                status="configuration_invalid",
            )
        if limit is not None and limit < 1:
            raise ShopifyAdapterError("Extraction limit must be positive")
        boundary = watermark or backfill_start
        assert boundary is not None
        if boundary.utcoffset() is None:
            raise ShopifyAdapterError("Shopify extraction boundary must be timezone-aware")
        boundary_text = boundary.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        search = f"updated_at:>='{boundary_text}'"
        ingestion_id = str(uuid4())
        extracted_at = self.now().astimezone(timezone.utc)
        cursor: str | None = None
        output: list[IngestionEnvelope] = []
        while True:
            remaining = self.page_size if limit is None else min(self.page_size, limit - len(output))
            if remaining <= 0:
                break
            data = self._request(
                ORDERS_QUERY,
                {"first": remaining, "after": cursor, "query": search},
            )
            orders = data.get("orders")
            if not isinstance(orders, dict) or not isinstance(orders.get("nodes"), list):
                raise ShopifyAdapterError("Shopify returned a malformed orders page")
            for node in orders["nodes"]:
                if not isinstance(node, dict) or not node.get("id") or not node.get("updatedAt"):
                    raise ShopifyAdapterError("Shopify returned a malformed order")
                self._complete_nested_pagination(node)
                selected = _selected_order(node)
                updated_at = _parse_utc(str(selected["updatedAt"]), "updatedAt")
                order_id = str(selected["id"])
                record_id = str(uuid5(
                    NAMESPACE_URL,
                    f"shopify|{self.config.business_id}|{self.config.source_id}|{order_id}|{updated_at.isoformat()}",
                ))
                output.append(IngestionEnvelope(
                    business_id=self.config.business_id,
                    source_type=self.config.source_type,
                    source_id=self.config.source_id,
                    ingestion_id=ingestion_id,
                    record_id=record_id,
                    extracted_at_utc=extracted_at,
                    source_updated_at_utc=updated_at,
                    schema_version=self.config.schema_version,
                    payload=selected,
                ))
                if limit is not None and len(output) >= limit:
                    break
            if limit is not None and len(output) >= limit:
                break
            page_info = orders.get("pageInfo")
            if not isinstance(page_info, dict):
                raise ShopifyAdapterError("Shopify orders page is missing pagination metadata")
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not isinstance(cursor, str) or not cursor:
                raise ShopifyAdapterError("Shopify pagination cursor is missing")
        return tuple(output)

    def normalize(self, record: IngestionEnvelope) -> dict[str, Any]:
        if (record.business_id, record.source_type, record.source_id) != (
            self.config.business_id, self.config.source_type, self.config.source_id
        ):
            raise ShopifyAdapterError("Envelope identity does not match adapter configuration")
        node = record.payload
        try:
            normalized = {
                "order_id": str(node["id"]),
                "order_name": str(node["name"]) if node.get("name") is not None else None,
                "customer_id": _optional_id(node.get("customer")),
                "created_at_utc": _parse_utc(node["createdAt"], "createdAt").isoformat().replace("+00:00", "Z"),
                "updated_at_utc": _parse_utc(node["updatedAt"], "updatedAt").isoformat().replace("+00:00", "Z"),
                "processed_at_utc": (_parse_utc(node["processedAt"], "processedAt").isoformat().replace("+00:00", "Z")
                                     if node.get("processedAt") else None),
                "cancelled_at_utc": (_parse_utc(node["cancelledAt"], "cancelledAt").isoformat().replace("+00:00", "Z")
                                     if node.get("cancelledAt") else None),
                "financial_status": str(node.get("displayFinancialStatus") or "UNKNOWN"),
                "fulfillment_status": str(node.get("displayFulfillmentStatus") or "UNFULFILLED"),
                "currency": str(node["currencyCode"]).upper(),
                "total_amount": _money(node.get("totalPriceSet"), "totalPriceSet", required=True),
                "subtotal_amount": _money(node.get("subtotalPriceSet"), "subtotalPriceSet"),
                "discount_amount": _money(node.get("totalDiscountsSet"), "totalDiscountsSet"),
                "tax_amount": _money(node.get("totalTaxSet"), "totalTaxSet"),
                "shipping_amount": _money(node.get("totalShippingPriceSet"), "totalShippingPriceSet"),
                "refunded_amount": _money(node.get("totalRefundedSet"), "totalRefundedSet") or 0.0,
                "country": (node.get("shippingAddress") or {}).get("countryCodeV2"),
                "line_items": [self._normalize_line_item(item) for item in node.get("lineItems", {}).get("nodes", [])],
                "refunds": [self._normalize_refund(item) for item in node.get("refunds", [])],
            }
        except (KeyError, TypeError, ValueError) as error:
            if isinstance(error, ShopifyAdapterError):
                raise
            raise ShopifyAdapterError("Shopify order is missing a required analytics field") from None
        contract_for(record.source_type, record.schema_version).validate(normalized)
        return {
            "business_id": record.business_id,
            "source_type": record.source_type,
            "source_id": record.source_id,
            "record_id": record.record_id,
            "ingestion_id": record.ingestion_id,
            "source_updated_at_utc": record.source_updated_at_utc.isoformat().replace("+00:00", "Z") if record.source_updated_at_utc else None,
            "schema_version": record.schema_version,
            **normalized,
        }

    @staticmethod
    def _normalize_line_item(item: dict[str, Any]) -> dict[str, Any]:
        quantity = int(item["quantity"])
        unit_price = _money(item.get("originalUnitPriceSet"), "line item price", required=True)
        discount = _money(item.get("totalDiscountSet"), "line item discount") or 0.0
        return {
            "line_item_id": str(item["id"]),
            "product_id": _optional_id(item.get("product")),
            "variant_id": _optional_id(item.get("variant")),
            "sku": str(item["sku"]) if item.get("sku") else None,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_amount": discount,
            "line_amount": max(0.0, float(unit_price) * quantity - discount),
        }

    @staticmethod
    def _normalize_refund(refund: dict[str, Any]) -> dict[str, Any]:
        return {
            "refund_id": str(refund["id"]),
            "created_at_utc": (_parse_utc(refund["createdAt"], "refund createdAt").isoformat().replace("+00:00", "Z")
                               if refund.get("createdAt") else None),
            "amount": _money(refund.get("totalRefundedSet"), "refund total", required=True),
            "line_items": [
                {
                    "line_item_id": str((item.get("lineItem") or {}).get("id") or ""),
                    "product_id": _optional_id((item.get("lineItem") or {}).get("product")),
                    "variant_id": _optional_id((item.get("lineItem") or {}).get("variant")),
                    "sku": str((item.get("lineItem") or {}).get("sku")) if (item.get("lineItem") or {}).get("sku") else None,
                    "quantity": int(item["quantity"]),
                    "amount": _money(item.get("subtotalSet"), "refund line subtotal", required=True),
                }
                for item in refund.get("refundLineItems", {}).get("nodes", [])
            ],
        }

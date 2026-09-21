"""UCI Online Retail II flat-ledger portability benchmark.

The public workbook is read-only and parsed with the Python standard library.
Every source row is retained as a signed ledger fact.  Only ordinary,
positive-quantity merchandise lines are eligible for the canonical commerce
projection; reversals, charges, and adjustments remain native ledger facts.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from contextlib import ExitStack
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import time
from typing import Any, Iterable, Iterator, Mapping
from uuid import NAMESPACE_URL, uuid5
import xml.etree.ElementTree as ET
import zipfile
from zoneinfo import ZoneInfo

from src.onboarding.adapters import AdapterHealth
from src.onboarding.contracts import contract_for
from src.onboarding.models import IngestionEnvelope, SourceConfig
from src.operations.models import CommerceOrder, OrderLine, PaymentType
from src.quality.anomaly import MetricSeries, evaluate
from src.quality.anomaly_runner import policy_for


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = PROJECT_ROOT / "config" / "benchmarks" / "uci_online_retail_ii.json"
UCI_BUSINESS_ID = "public_uci_online_retail_ii"
UCI_SOURCE_ID = "uci_online_retail_ii"
PROVIDER = "uci_online_retail_ii"
EXPECTED_COLUMNS = (
    "Invoice", "StockCode", "Description", "Quantity", "InvoiceDate",
    "Price", "Customer ID", "Country",
)
XML_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


@dataclass(frozen=True, slots=True)
class SourceRow:
    worksheet: str
    source_row_number: int
    values: Mapping[str, str]


@dataclass(slots=True)
class InvoiceSummary:
    worksheet: str
    raw_invoice_id: str
    scoped_invoice_id: str
    native_invoice_type: str
    line_count: int = 0
    known_customers: set[str] = field(default_factory=set)
    has_missing_customer: bool = False
    countries: set[str] = field(default_factory=set)
    timestamps: set[datetime] = field(default_factory=set)
    positive_merchandise_value: Decimal = Decimal(0)
    cancellation_value: Decimal = Decimal(0)
    adjustment_value: Decimal = Decimal(0)
    non_merchandise_value: Decimal = Decimal(0)
    net_ledger_value: Decimal = Decimal(0)
    projected_order_value: Decimal = Decimal(0)
    projected_line_count: int = 0


class DqObservations:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.samples: dict[str, list[str]] = defaultdict(list)

    def add(self, code: str, record_id: str, count: int = 1) -> None:
        self.counts[code] += count
        if record_id and len(self.samples[code]) < 10:
            self.samples[code].append(record_id)

    def rows(self) -> list[dict[str, Any]]:
        definitions = {
            "malformed_invoice_identity": ("ERROR", "DATA_DEFECT"),
            "unparseable_quantity": ("ERROR", "DATA_DEFECT"),
            "unparseable_price": ("ERROR", "DATA_DEFECT"),
            "impossible_invoice_date": ("ERROR", "DATA_DEFECT"),
            "duplicate_source_row_identity": ("ERROR", "DATA_DEFECT"),
            "missing_customer_id": ("INFO", "SOURCE_COMPLETENESS"),
            "missing_description": ("INFO", "SOURCE_COMPLETENESS"),
            "zero_price_line": ("INFO", "BUSINESS_SOURCE_SEMANTICS"),
            "cancellation_invoice_line": ("INFO", "BUSINESS_SOURCE_SEMANTICS"),
            "negative_quantity_line": ("INFO", "BUSINESS_SOURCE_SEMANTICS"),
            "non_c_negative_quantity_line": ("INFO", "BUSINESS_SOURCE_SEMANTICS"),
            "special_stock_code_line": ("INFO", "BUSINESS_SOURCE_SEMANTICS"),
            "positive_quantity_on_c_invoice": ("WARNING", "UNUSUAL_SOURCE_BEHAVIOR"),
            "negative_price_line": ("WARNING", "UNUSUAL_SOURCE_BEHAVIOR"),
            "mixed_invoice_customer": ("WARNING", "UNUSUAL_SOURCE_BEHAVIOR"),
            "mixed_invoice_customer_presence": ("WARNING", "UNUSUAL_SOURCE_BEHAVIOR"),
            "mixed_invoice_country": ("WARNING", "UNUSUAL_SOURCE_BEHAVIOR"),
            "mixed_invoice_timestamp": ("WARNING", "UNUSUAL_SOURCE_BEHAVIOR"),
        }
        return [
            {"business_id": UCI_BUSINESS_ID, "code": code,
             "severity": definitions[code][0], "classification": definitions[code][1],
             "count": self.counts.get(code, 0), "sample_ids": self.samples.get(code, [])}
            for code in definitions
        ]


@dataclass(frozen=True, slots=True)
class UciManifest:
    raw: Mapping[str, Any]

    @classmethod
    def load(cls, path: Path = MANIFEST_PATH) -> "UciManifest":
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("UCI benchmark manifest must contain one object")
        return cls(value)

    @property
    def worksheets(self) -> Mapping[str, Mapping[str, Any]]:
        return self.raw["worksheets"]

    @property
    def required_files(self) -> Mapping[str, Mapping[str, Any]]:
        return self.worksheets

    def root(self, *, fixture: bool) -> Path:
        return (PROJECT_ROOT / self.raw["fixture_path"]).resolve() if fixture else (
            PROJECT_ROOT / self.raw["expected_local_path"]).resolve()

    def validate(self, root: Path, *, fixture: bool) -> tuple[str, ...]:
        errors: list[str] = []
        if fixture:
            if not root.is_dir():
                return (f"UCI fixture directory is missing: {root}",)
            for worksheet, spec in self.worksheets.items():
                path = root / spec["fixture_file"]
                if not path.is_file():
                    errors.append(f"Missing UCI fixture file: {path.name}")
                    continue
                with path.open("r", encoding="utf-8-sig", newline="") as handle:
                    header = tuple(next(csv.reader(handle), ()))
                if header != EXPECTED_COLUMNS:
                    errors.append(f"Unexpected schema for {path.name}: expected {','.join(EXPECTED_COLUMNS)}")
            return tuple(errors)
        if not root.is_file():
            return (f"UCI Online Retail II workbook is missing: {root}",)
        try:
            reader = XlsxReader(root)
            actual = tuple(reader.sheet_names)
            expected = tuple(self.worksheets)
            if actual != expected:
                errors.append(f"Unexpected worksheets: expected {expected!r}, found {actual!r}")
            for worksheet in expected:
                header = tuple(next(reader.iter_raw_rows(worksheet), (0, ())) [1])
                if header != EXPECTED_COLUMNS:
                    errors.append(f"Unexpected schema for worksheet {worksheet}: expected {','.join(EXPECTED_COLUMNS)}")
        except (OSError, KeyError, ValueError, zipfile.BadZipFile, ET.ParseError) as error:
            errors.append(f"Cannot read UCI workbook: {type(error).__name__}: {error}")
        return tuple(errors)


def _column_index(reference: str) -> int:
    letters = re.match(r"[A-Z]+", reference)
    if not letters:
        raise ValueError(f"Invalid XLSX cell reference: {reference}")
    value = 0
    for character in letters.group(0):
        value = value * 26 + ord(character) - 64
    return value - 1


class XlsxReader:
    """Small read-only XLSX iterator supporting shared and inline strings."""

    def __init__(self, path: Path):
        self.path = Path(path)
        with zipfile.ZipFile(self.path) as archive:
            workbook = ET.fromstring(archive.read("xl/workbook.xml"))
            relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
            targets = {
                item.attrib["Id"]: item.attrib["Target"]
                for item in relationships.findall(f"{{{PACKAGE_REL_NS}}}Relationship")
            }
            self._sheets: dict[str, str] = {}
            for sheet in workbook.findall(f".//{{{XML_NS}}}sheet"):
                target = targets[sheet.attrib[f"{{{REL_NS}}}id"]].lstrip("/")
                self._sheets[sheet.attrib["name"]] = target if target.startswith("xl/") else f"xl/{target}"
            self._shared_strings = self._read_shared_strings(archive)

    @property
    def sheet_names(self) -> tuple[str, ...]:
        return tuple(self._sheets)

    @staticmethod
    def _read_shared_strings(archive: zipfile.ZipFile) -> tuple[str, ...]:
        if "xl/sharedStrings.xml" not in archive.namelist():
            return ()
        output: list[str] = []
        with archive.open("xl/sharedStrings.xml") as handle:
            for _, element in ET.iterparse(handle, events=("end",)):
                if element.tag == f"{{{XML_NS}}}si":
                    output.append("".join(node.text or "" for node in element.iter(f"{{{XML_NS}}}t")))
                    element.clear()
        return tuple(output)

    def iter_raw_rows(self, worksheet: str) -> Iterator[tuple[int, tuple[str, ...]]]:
        target = self._sheets[worksheet]
        with zipfile.ZipFile(self.path) as archive, archive.open(target) as handle:
            for _, element in ET.iterparse(handle, events=("end",)):
                if element.tag != f"{{{XML_NS}}}row":
                    continue
                row_number = int(element.attrib.get("r", "0"))
                values = [""] * len(EXPECTED_COLUMNS)
                for cell in element.findall(f"{{{XML_NS}}}c"):
                    index = _column_index(cell.attrib.get("r", "A1"))
                    if index >= len(values):
                        continue
                    cell_type = cell.attrib.get("t")
                    raw = cell.findtext(f"{{{XML_NS}}}v", default="")
                    if cell_type == "s" and raw:
                        value = self._shared_strings[int(raw)]
                    elif cell_type == "inlineStr":
                        value = "".join(node.text or "" for node in cell.iter(f"{{{XML_NS}}}t"))
                    else:
                        value = raw
                    values[index] = value
                element.clear()
                if any(values):
                    yield row_number, tuple(values)


class UciOnlineRetailAdapter:
    """Offline adapter reusing the provider-neutral commerce dataset envelope."""

    def __init__(self, root: Path, manifest: UciManifest | None = None,
                 config: SourceConfig | None = None, *, fixture: bool | None = None):
        self.root = Path(root).resolve()
        self.manifest = manifest or UciManifest.load()
        self.fixture = self.root.is_dir() if fixture is None else fixture
        self.config = config or SourceConfig(
            source_type="commerce_dataset", source_id=UCI_SOURCE_ID, enabled=True,
            business_id=UCI_BUSINESS_ID, ingestion_mode="file", schedule="0 0 * * *",
            schema_version="commerce_dataset_v1",
            credential_ref="UCI_ONLINE_RETAIL_II_LOCAL_DATASET",
            metadata={"adapter": "uci_online_retail_ii", "root": str(self.root)},
        )
        self._xlsx: XlsxReader | None = None

    def validate_config(self) -> tuple[str, ...]:
        return self.manifest.validate(self.root, fixture=self.fixture)

    def healthcheck(self) -> AdapterHealth:
        errors = self.validate_config()
        return AdapterHealth(healthy=not errors,
                             message="ready; local files only" if not errors else "; ".join(errors))

    def rows(self, worksheet: str) -> Iterator[dict[str, str]]:
        if worksheet not in self.manifest.worksheets:
            raise ValueError(f"Unknown UCI worksheet: {worksheet}")
        if self.fixture:
            path = self.root / self.manifest.worksheets[worksheet]["fixture_file"]
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                yield from csv.DictReader(handle)
            return
        self._xlsx = self._xlsx or XlsxReader(self.root)
        rows = self._xlsx.iter_raw_rows(worksheet)
        _, header = next(rows)
        if tuple(header) != EXPECTED_COLUMNS:
            raise ValueError(f"Unexpected schema for worksheet {worksheet}")
        for _, values in rows:
            yield dict(zip(EXPECTED_COLUMNS, values))

    def iter_source_rows(self) -> Iterator[SourceRow]:
        if self.fixture:
            for worksheet, spec in self.manifest.worksheets.items():
                path = self.root / spec["fixture_file"]
                with path.open("r", encoding="utf-8-sig", newline="") as handle:
                    reader = csv.DictReader(handle)
                    if tuple(reader.fieldnames or ()) != EXPECTED_COLUMNS:
                        raise ValueError(f"Unexpected schema for {path.name}")
                    for row_number, row in enumerate(reader, start=2):
                        yield SourceRow(worksheet, row_number, dict(row))
            return
        self._xlsx = self._xlsx or XlsxReader(self.root)
        for worksheet in self.manifest.worksheets:
            rows = self._xlsx.iter_raw_rows(worksheet)
            _, header = next(rows)
            if tuple(header) != EXPECTED_COLUMNS:
                raise ValueError(f"Unexpected schema for worksheet {worksheet}")
            for row_number, values in rows:
                yield SourceRow(worksheet, row_number, dict(zip(EXPECTED_COLUMNS, values)))

    @staticmethod
    def record_key(row: SourceRow) -> str:
        return f"{row.worksheet}|{row.values.get('Invoice', '')}|{row.source_row_number}"

    def envelope(self, row: SourceRow, extracted_at: datetime) -> IngestionEnvelope:
        key = self.record_key(row)
        payload = {
            "source_file": self.root.name if not self.fixture else self.manifest.worksheets[row.worksheet]["fixture_file"],
            "table": row.worksheet,
            "record_key": key,
            "source_observed_at_utc": extracted_at.isoformat().replace("+00:00", "Z"),
            "native_record": dict(row.values),
        }
        contract_for(self.config.source_type, self.config.schema_version).validate(payload)
        ingestion_id = str(uuid5(NAMESPACE_URL,
                                 f"uci-ingestion|{self.config.business_id}|{extracted_at.isoformat()}"))
        return IngestionEnvelope(
            business_id=self.config.business_id, source_type=self.config.source_type,
            source_id=self.config.source_id, ingestion_id=ingestion_id,
            record_id=str(uuid5(NAMESPACE_URL,
                                f"{self.config.business_id}|{self.config.source_id}|{key}")),
            extracted_at_utc=extracted_at, source_updated_at_utc=None,
            schema_version=self.config.schema_version, payload=payload,
        )

    def extract(self) -> tuple[IngestionEnvelope, ...]:
        errors = self.validate_config()
        if errors:
            raise ValueError("; ".join(errors))
        extracted_at = datetime.now(timezone.utc)
        return tuple(self.envelope(row, extracted_at) for row in self.iter_source_rows())

    def normalize(self, record: IngestionEnvelope) -> dict[str, Any]:
        if (record.business_id, record.source_id) != (self.config.business_id, self.config.source_id):
            raise ValueError("Envelope identity does not match the UCI adapter")
        return dict(record.payload)


def _worksheet_slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")


def scoped_invoice_id(worksheet: str, raw_invoice_id: str) -> str:
    return f"{UCI_SOURCE_ID}:{_worksheet_slug(worksheet)}:{raw_invoice_id}"


def deterministic_line_id(worksheet: str, raw_invoice_id: str, source_row_number: int) -> str:
    return f"{scoped_invoice_id(worksheet, raw_invoice_id)}:{source_row_number}"


def classify_stock_code(stock_code: str) -> str:
    code = stock_code.strip().upper()
    if code in {"POST", "DOT"}:
        return "POSTAGE_OR_CHARGE"
    if code == "D":
        return "DISCOUNT"
    if code in {"M", "ADJUST", "ADJUST2", "B", "CRUK"}:
        return "MANUAL_ADJUSTMENT"
    if code == "BANK CHARGES":
        return "BANK_CHARGE"
    if code.startswith(("TEST", "GIFT_", "DCGSS")):
        return "TEST_OR_INTERNAL"
    if not code:
        return "UNKNOWN_SPECIAL"
    return "PRODUCT"


def classify_line(*, cancellation: bool, quantity: Decimal | None,
                  price: Decimal | None, stock_category: str) -> str:
    if cancellation:
        return "CANCELLATION_OR_REVERSAL"
    if quantity is None or price is None:
        return "UNKNOWN_SPECIAL_LINE"
    if quantity < 0:
        return "NON_C_NEGATIVE_ADJUSTMENT"
    if price == 0:
        return "ZERO_PRICE_LINE"
    if stock_category != "PRODUCT":
        return "NON_MERCHANDISE_CHARGE"
    return "MERCHANDISE_SALE"


def _decimal(value: str, field_name: str) -> Decimal:
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field_name} is not numeric") from None
    if not number.is_finite():
        raise ValueError(f"{field_name} is not finite")
    return number


def _invoice_timestamp(value: str, zone: ZoneInfo) -> datetime:
    if not value:
        raise ValueError("InvoiceDate is missing")
    try:
        if re.fullmatch(r"-?\d+(?:\.\d+)?", value):
            parsed = datetime(1899, 12, 30) + timedelta(days=float(Decimal(value)))
        else:
            parsed = datetime.fromisoformat(value)
    except (InvalidOperation, OverflowError, ValueError):
        raise ValueError("InvoiceDate is not a valid Excel or ISO timestamp") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=zone)
    return parsed.astimezone(timezone.utc)


def normalize_source_row(row: SourceRow, *, currency: str, zone: ZoneInfo) -> tuple[dict[str, Any], tuple[str, ...]]:
    raw = row.values
    invoice = raw.get("Invoice", "").strip()
    stock_code = raw.get("StockCode", "").strip()
    record_id = deterministic_line_id(row.worksheet, invoice or "MISSING", row.source_row_number)
    errors: list[str] = []
    if not invoice:
        errors.append("malformed_invoice_identity")
    try:
        quantity = _decimal(raw.get("Quantity", ""), "Quantity")
    except ValueError:
        quantity = None
        errors.append("unparseable_quantity")
    try:
        price = _decimal(raw.get("Price", ""), "Price")
    except ValueError:
        price = None
        errors.append("unparseable_price")
    try:
        invoice_at = _invoice_timestamp(raw.get("InvoiceDate", ""), zone)
    except ValueError:
        invoice_at = None
        errors.append("impossible_invoice_date")
    cancellation = invoice.upper().startswith("C")
    stock_category = classify_stock_code(stock_code)
    line_classification = classify_line(cancellation=cancellation, quantity=quantity,
                                        price=price, stock_category=stock_category)
    line_value = quantity * price if quantity is not None and price is not None else None
    projection_eligible = (
        not errors and line_classification == "MERCHANDISE_SALE" and
        quantity is not None and quantity > 0 and quantity == quantity.to_integral_value() and
        price is not None and price >= 0
    )
    return {
        "business_id": UCI_BUSINESS_ID,
        "source_id": UCI_SOURCE_ID,
        "worksheet": row.worksheet,
        "source_row_number": row.source_row_number,
        "line_id": record_id,
        "scoped_invoice_id": scoped_invoice_id(row.worksheet, invoice) if invoice else None,
        "raw_invoice_id": invoice or None,
        "stock_code": stock_code or None,
        "description": raw.get("Description", "").strip() or None,
        "quantity": quantity,
        "invoice_at": invoice_at,
        "unit_price": price,
        "source_line_value": line_value,
        "currency": currency,
        "customer_reference": raw.get("Customer ID", "").strip() or None,
        "country": raw.get("Country", "").strip() or None,
        "native_invoice_type": "CANCELLATION_INVOICE" if cancellation else "NORMAL_INVOICE",
        "line_classification": line_classification,
        "stock_code_classification": stock_category,
        "commerce_projection_eligible": projection_eligible,
        "normalization_errors": errors,
    }, tuple(errors)


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(type(value).__name__)


def _write_line(handle, value: Mapping[str, Any]) -> None:
    handle.write(json.dumps(value, default=_json_default, sort_keys=True,
                            separators=(",", ":"), allow_nan=False) + "\n")


def _issue_line_observations(dq: DqObservations, line: Mapping[str, Any]) -> None:
    record_id = str(line["line_id"])
    for error in line["normalization_errors"]:
        dq.add(error, record_id)
    if line["customer_reference"] is None:
        dq.add("missing_customer_id", record_id)
    if line["description"] is None:
        dq.add("missing_description", record_id)
    quantity = line["quantity"]
    price = line["unit_price"]
    if price == 0:
        dq.add("zero_price_line", record_id)
    if price is not None and price < 0:
        dq.add("negative_price_line", record_id)
    if line["native_invoice_type"] == "CANCELLATION_INVOICE":
        dq.add("cancellation_invoice_line", record_id)
        if quantity is not None and quantity > 0:
            dq.add("positive_quantity_on_c_invoice", record_id)
    if quantity is not None and quantity < 0:
        dq.add("negative_quantity_line", record_id)
        if line["native_invoice_type"] != "CANCELLATION_INVOICE":
            dq.add("non_c_negative_quantity_line", record_id)
    if line["stock_code_classification"] != "PRODUCT":
        dq.add("special_stock_code_line", record_id)


def _anomaly_rows(daily: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not daily:
        return []
    ordered = sorted(daily, key=lambda row: row["event_date"])
    definitions = (
        ("daily_invoice_count", "invoice_count", "completed_order_volume"),
        ("daily_positive_merchandise_value", "positive_merchandise_value", "gross_revenue"),
        ("daily_cancellation_value", "cancellation_value", "daily_cancellation_value"),
    )
    output = []
    for metric_name, field_name, policy_name in definitions:
        current, history = ordered[-1], ordered[:-1]
        observed_at = datetime.fromisoformat(str(current["event_date"])).replace(tzinfo=timezone.utc)
        history_times = tuple(datetime.fromisoformat(str(row["event_date"])).replace(tzinfo=timezone.utc)
                              for row in history)
        series = MetricSeries(
            metric_name=metric_name, dataset_name="uci_online_retail_ii",
            layer="benchmark_gold", current_value=float(current[field_name]),
            history=tuple(float(row[field_name]) for row in history),
            observed_at_utc=observed_at, history_observed_at_utc=history_times,
            dimensions={"business_id": UCI_BUSINESS_ID, "currency": "GBP"},
        )
        policy = policy_for(policy_name, minimum_history=7)
        identity = uuid5(NAMESPACE_URL, f"uci-anomaly|{metric_name}|{current['event_date']}")
        result = evaluate(series, policy, identity)
        output.append({
            "metric_name": metric_name, "history_count": result.history_count,
            "baseline_strategy": result.baseline_strategy,
            "status": result.status.value, "severity": result.severity.value,
            "current_value": result.current_value, "expected_value": result.expected_value,
            "threshold": result.threshold, "thresholds_unchanged": True,
        })
    return output


def run_benchmark(*, fixture: bool = False, root: Path | None = None,
                  output_root: Path | None = None, write_outputs: bool = True,
                  enforce_acceptance: bool = False,
                  core_compatibility: bool | None = None) -> dict[str, Any]:
    manifest = UciManifest.load()
    selected_root = Path(root).resolve() if root else manifest.root(fixture=fixture)
    adapter = UciOnlineRetailAdapter(selected_root, manifest, fixture=fixture)
    errors = adapter.validate_config()
    if errors:
        suffix = " Use --fixture for the committed CI dataset." if not fixture else ""
        raise ValueError("; ".join(errors) + suffix)
    output = (Path(output_root).resolve() if output_root else
              PROJECT_ROOT / "data" / "benchmarks" / "uci_online_retail_ii" /
              ("fixture" if fixture else "full")).resolve()
    exercise_core = fixture if core_compatibility is None else core_compatibility
    currency = str(manifest.raw["currency"])
    zone = ZoneInfo(str(manifest.raw["source_timezone"]))
    extracted_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    bronze_seconds = 0.0
    silver_seconds = 0.0
    source_rows = 0
    sheet_counts: Counter[str] = Counter()
    raw_invoice_sheets: dict[str, set[str]] = defaultdict(set)
    raw_cancellation_ids: set[str] = set()
    invoices: dict[tuple[str, str], InvoiceSummary] = {}
    last_source_row_by_sheet: dict[str, int] = {}
    known_customers: set[str] = set()
    classification_counts: Counter[str] = Counter()
    classification_values: dict[str, Decimal] = defaultdict(Decimal)
    stock_counts: Counter[str] = Counter()
    zero_quantity_rows = 0
    country_line_counts: Counter[str] = Counter()
    country_values: dict[str, Decimal] = defaultdict(Decimal)
    daily_values: dict[date, dict[str, Any]] = defaultdict(lambda: {
        "line_count": 0, "positive_merchandise_value": Decimal(0),
        "cancellation_value": Decimal(0), "adjustment_value": Decimal(0),
        "non_merchandise_value": Decimal(0), "net_ledger_value": Decimal(0),
        "zero_price_line_count": 0, "negative_price_line_count": 0,
    })
    dq = DqObservations()
    projected_lines: dict[str, list[OrderLine]] = defaultdict(list)
    canonical_orders: list[CommerceOrder] = []
    behavior_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    with ExitStack() as stack:
        bronze_handle = silver_handle = None
        if write_outputs:
            (output / "bronze").mkdir(parents=True, exist_ok=True)
            (output / "silver").mkdir(parents=True, exist_ok=True)
            (output / "gold").mkdir(parents=True, exist_ok=True)
            bronze_handle = stack.enter_context((output / "bronze" / "ledger_rows.jsonl").open(
                "w", encoding="utf-8", newline="\n"))
            silver_handle = stack.enter_context((output / "silver" / "ledger_entries.jsonl").open(
                "w", encoding="utf-8", newline="\n"))

        iterator = iter(adapter.iter_source_rows())
        while True:
            parse_started = time.perf_counter()
            try:
                source = next(iterator)
            except StopIteration:
                break
            source_parse_elapsed = time.perf_counter() - parse_started
            source_rows += 1
            sheet_counts[source.worksheet] += 1
            if bronze_handle is not None:
                stage = time.perf_counter()
                _write_line(bronze_handle, adapter.envelope(source, extracted_at).to_dict())
                bronze_seconds += time.perf_counter() - stage
            stage = time.perf_counter()
            line, _ = normalize_source_row(source, currency=currency, zone=zone)
            line_id = str(line["line_id"])
            previous_source_row = last_source_row_by_sheet.get(source.worksheet, 0)
            if source.source_row_number <= previous_source_row:
                dq.add("duplicate_source_row_identity", line_id)
            last_source_row_by_sheet[source.worksheet] = source.source_row_number
            _issue_line_observations(dq, line)
            if line["quantity"] == 0:
                zero_quantity_rows += 1
            sample_names = []
            if line["unit_price"] is not None and line["unit_price"] < 0:
                sample_names.append("negative_price")
            if (line["native_invoice_type"] == "CANCELLATION_INVOICE" and
                    line["quantity"] is not None and line["quantity"] > 0):
                sample_names.append("positive_quantity_on_c_invoice")
            if line["line_classification"] == "NON_C_NEGATIVE_ADJUSTMENT":
                sample_names.append("non_c_negative_adjustment")
            for sample_name in sample_names:
                if len(behavior_samples[sample_name]) < 10:
                    behavior_samples[sample_name].append({
                        "line_id": line_id, "stock_code": line["stock_code"],
                        "description": line["description"],
                        "quantity": float(line["quantity"]),
                        "unit_price": float(line["unit_price"]),
                        "source_line_value": float(line["source_line_value"]),
                    })
            if silver_handle is not None:
                _write_line(silver_handle, line)

            invoice = line["raw_invoice_id"]
            if invoice:
                raw_invoice_sheets[invoice].add(source.worksheet)
                if line["native_invoice_type"] == "CANCELLATION_INVOICE":
                    raw_cancellation_ids.add(invoice)
                key = (source.worksheet, invoice)
                summary = invoices.get(key)
                if summary is None:
                    summary = InvoiceSummary(
                        worksheet=source.worksheet, raw_invoice_id=invoice,
                        scoped_invoice_id=str(line["scoped_invoice_id"]),
                        native_invoice_type=str(line["native_invoice_type"]),
                    )
                    invoices[key] = summary
                summary.line_count += 1
                customer = line["customer_reference"]
                if customer:
                    summary.known_customers.add(str(customer)); known_customers.add(str(customer))
                else:
                    summary.has_missing_customer = True
                if line["country"]:
                    summary.countries.add(str(line["country"]))
                if line["invoice_at"]:
                    summary.timestamps.add(line["invoice_at"])
                if line["source_line_value"] is not None:
                    value = line["source_line_value"]
                    summary.net_ledger_value += value
                    category = str(line["line_classification"])
                    if category == "MERCHANDISE_SALE":
                        summary.positive_merchandise_value += value
                    elif category == "CANCELLATION_OR_REVERSAL":
                        summary.cancellation_value += value
                    elif category == "NON_C_NEGATIVE_ADJUSTMENT":
                        summary.adjustment_value += value
                    else:
                        summary.non_merchandise_value += value
                    if line["commerce_projection_eligible"]:
                        summary.projected_order_value += value
                        summary.projected_line_count += 1
                        if exercise_core:
                            projected_lines[summary.scoped_invoice_id].append(OrderLine(
                                business_id=UCI_BUSINESS_ID, order_id=summary.scoped_invoice_id,
                                line_id=line_id, product_id=str(line["stock_code"]),
                                variant_id=None, sku=str(line["stock_code"]),
                                quantity=int(line["quantity"]),
                                unit_price=float(line["unit_price"]), currency=currency,
                            ))
            category = str(line["line_classification"])
            classification_counts[category] += 1
            stock_counts[str(line["stock_code_classification"])] += 1
            country = str(line["country"] or "UNKNOWN")
            country_line_counts[country] += 1
            if line["source_line_value"] is not None:
                value = line["source_line_value"]
                classification_values[category] += value
                country_values[country] += value
                if line["invoice_at"]:
                    day = line["invoice_at"].astimezone(zone).date()
                    daily = daily_values[day]
                    daily["line_count"] += 1
                    daily["net_ledger_value"] += value
                    if category == "MERCHANDISE_SALE":
                        daily["positive_merchandise_value"] += value
                    elif category == "CANCELLATION_OR_REVERSAL":
                        daily["cancellation_value"] += value
                    elif category == "NON_C_NEGATIVE_ADJUSTMENT":
                        daily["adjustment_value"] += value
                    else:
                        daily["non_merchandise_value"] += value
                    if line["unit_price"] == 0:
                        daily["zero_price_line_count"] += 1
                    if line["unit_price"] is not None and line["unit_price"] < 0:
                        daily["negative_price_line_count"] += 1
            silver_seconds += time.perf_counter() - stage

    loop_seconds = time.perf_counter() - started
    source_parsing_seconds = max(0.0, loop_seconds - bronze_seconds - silver_seconds)
    gold_started = time.perf_counter()

    invoice_dates: dict[date, dict[str, int]] = defaultdict(lambda: {
        "invoice_count": 0, "normal_invoice_count": 0,
        "cancellation_invoice_count": 0, "anonymous_customer_invoice_count": 0,
    })
    country_invoice_counts: Counter[str] = Counter()
    invoice_rows: list[dict[str, Any]] = []
    projected_invoice_count = 0
    for summary in invoices.values():
        if len(summary.known_customers) > 1:
            dq.add("mixed_invoice_customer", summary.scoped_invoice_id)
        if summary.has_missing_customer and summary.known_customers:
            dq.add("mixed_invoice_customer_presence", summary.scoped_invoice_id)
        if len(summary.countries) > 1:
            dq.add("mixed_invoice_country", summary.scoped_invoice_id)
        if len(summary.timestamps) > 1:
            dq.add("mixed_invoice_timestamp", summary.scoped_invoice_id)
        invoice_at = min(summary.timestamps) if summary.timestamps else None
        country = next(iter(summary.countries)) if len(summary.countries) == 1 else "MIXED_OR_UNKNOWN"
        country_invoice_counts[country] += 1
        anonymous = not summary.known_customers
        if invoice_at:
            day = invoice_at.astimezone(zone).date()
            row = invoice_dates[day]
            row["invoice_count"] += 1
            if summary.native_invoice_type == "CANCELLATION_INVOICE":
                row["cancellation_invoice_count"] += 1
            else:
                row["normal_invoice_count"] += 1
            if anonymous:
                row["anonymous_customer_invoice_count"] += 1
        eligible = (summary.native_invoice_type == "NORMAL_INVOICE" and
                    summary.projected_line_count > 0)
        if eligible:
            projected_invoice_count += 1
            if exercise_core:
                canonical_orders.append(CommerceOrder(
                    business_id=UCI_BUSINESS_ID, source_type="commerce_dataset",
                    source_id=UCI_SOURCE_ID, provider=PROVIDER,
                    order_id=summary.scoped_invoice_id,
                    external_order_id=summary.raw_invoice_id,
                    order_created_at=invoice_at or extracted_at,
                    order_updated_at=invoice_at or extracted_at,
                    source_timezone=str(manifest.raw["source_timezone"]),
                    reporting_timezone=str(manifest.raw["source_timezone"]),
                    currency=currency, order_value=float(summary.projected_order_value),
                    payment_type=PaymentType.OTHER,
                    customer_reference=(next(iter(summary.known_customers))
                                        if len(summary.known_customers) == 1 else None),
                    lines=tuple(projected_lines[summary.scoped_invoice_id]),
                ))
        invoice_rows.append({
            "business_id": UCI_BUSINESS_ID, "scoped_invoice_id": summary.scoped_invoice_id,
            "worksheet": summary.worksheet, "raw_invoice_id": summary.raw_invoice_id,
            "native_invoice_type": summary.native_invoice_type,
            "invoice_at": invoice_at, "currency": currency, "country": country,
            "anonymous_customer": anonymous, "line_count": summary.line_count,
            "positive_merchandise_value": float(summary.positive_merchandise_value),
            "cancellation_value": float(summary.cancellation_value),
            "adjustment_value": float(summary.adjustment_value),
            "non_merchandise_value": float(summary.non_merchandise_value),
            "net_ledger_value": float(summary.net_ledger_value),
            "commerce_projection_eligible": eligible,
            "projected_order_value": float(summary.projected_order_value),
            "projected_line_count": summary.projected_line_count,
        })

    daily_rows = []
    for day in sorted(set(daily_values) | set(invoice_dates)):
        values = daily_values[day]
        counts = invoice_dates[day]
        daily_rows.append({
            "business_id": UCI_BUSINESS_ID, "event_date": day, "currency": currency,
            **counts, "line_count": values["line_count"],
            "positive_merchandise_value": float(values["positive_merchandise_value"]),
            "cancellation_value": float(values["cancellation_value"]),
            "adjustment_value": float(values["adjustment_value"]),
            "non_merchandise_value": float(values["non_merchandise_value"]),
            "net_ledger_value": float(values["net_ledger_value"]),
            "zero_price_line_count": values["zero_price_line_count"],
            "negative_price_line_count": values["negative_price_line_count"],
        })
    classification_rows = [
        {"business_id": UCI_BUSINESS_ID, "line_classification": name,
         "currency": currency, "line_count": count,
         "signed_line_value": float(classification_values[name])}
        for name, count in sorted(classification_counts.items())
    ]
    country_rows = [
        {"business_id": UCI_BUSINESS_ID, "country": country, "currency": currency,
         "line_count": count, "invoice_count": country_invoice_counts[country],
         "net_ledger_value": float(country_values[country])}
        for country, count in sorted(country_line_counts.items())
    ]
    quality_rows = dq.rows()
    economics_rows = [{
        "business_id": UCI_BUSINESS_ID, "currency": currency,
        "economic_status": "INCOMPLETE_COSTS", "invoice_count": len(invoices),
        "cogs_available": False, "merchant_shipping_cost_available": False,
        "attribution_available": False, "cod_available": False,
        "remittance_available": False, "profit_calculated": False,
    }]
    anomaly_rows = _anomaly_rows(daily_rows)
    line_counts = [summary.line_count for summary in invoices.values()]
    net_ledger_value = sum(classification_values.values(), Decimal(0))
    invoice_net_value = sum((summary.net_ledger_value for summary in invoices.values()), Decimal(0))
    daily_net_value = sum((values["net_ledger_value"] for values in daily_values.values()), Decimal(0))
    stripped_matches = sum(1 for invoice in raw_cancellation_ids
                           if invoice[1:] in raw_invoice_sheets)
    metrics = {
        "source_rows": source_rows,
        "distinct_raw_invoice_ids": len(raw_invoice_sheets),
        "worksheet_scoped_invoices": len(invoices),
        "raw_invoice_ids_reused_across_worksheets": sum(len(sheets) > 1 for sheets in raw_invoice_sheets.values()),
        "worksheet_scoped_cancellation_invoices": sum(
            summary.native_invoice_type == "CANCELLATION_INVOICE" for summary in invoices.values()),
        "normal_invoice_count": sum(
            summary.native_invoice_type == "NORMAL_INVOICE" for summary in invoices.values()),
        "distinct_raw_cancellation_invoice_ids": len(raw_cancellation_ids),
        "negative_quantity_rows": dq.counts["negative_quantity_line"],
        "non_c_negative_quantity_rows": dq.counts["non_c_negative_quantity_line"],
        "positive_quantity_rows_on_c_invoice": dq.counts["positive_quantity_on_c_invoice"],
        "zero_quantity_rows": zero_quantity_rows,
        "negative_price_rows": dq.counts["negative_price_line"],
        "zero_price_rows": dq.counts["zero_price_line"],
        "missing_customer_id_rows": dq.counts["missing_customer_id"],
        "missing_description_rows": dq.counts["missing_description"],
        "stripped_c_invoice_direct_matches": stripped_matches,
        "invoices_missing_customer": sum(not summary.known_customers for summary in invoices.values()),
        "invoices_with_multiple_known_customers": dq.counts["mixed_invoice_customer"],
        "invoices_with_mixed_known_and_missing_customer": dq.counts["mixed_invoice_customer_presence"],
        "invoices_with_multiple_countries": dq.counts["mixed_invoice_country"],
        "known_customer_count": len(known_customers),
        "minimum_lines_per_invoice": min(line_counts, default=0),
        "maximum_lines_per_invoice": max(line_counts, default=0),
        "average_lines_per_invoice": source_rows / len(invoices) if invoices else 0,
        "positive_merchandise_value": float(classification_values["MERCHANDISE_SALE"]),
        "cancellation_value": float(classification_values["CANCELLATION_OR_REVERSAL"]),
        "adjustment_value": float(classification_values["NON_C_NEGATIVE_ADJUSTMENT"]),
        "non_merchandise_value": float(sum(
            value for name, value in classification_values.items()
            if name not in {"MERCHANDISE_SALE", "CANCELLATION_OR_REVERSAL",
                            "NON_C_NEGATIVE_ADJUSTMENT"}
        )),
        "net_source_ledger_value": float(net_ledger_value),
    }
    invoice_values = [summary.net_ledger_value for summary in invoices.values()]
    metrics.update({
        "minimum_invoice_value": float(min(invoice_values, default=Decimal(0))),
        "maximum_invoice_value": float(max(invoice_values, default=Decimal(0))),
        "average_invoice_value": float(invoice_net_value / len(invoices)) if invoices else 0.0,
        "minimum_invoice_date": min(daily_values).isoformat() if daily_values else None,
        "maximum_invoice_date": max(daily_values).isoformat() if daily_values else None,
    })
    worksheet_scoped_metrics: dict[str, dict[str, Any]] = {}
    for worksheet in manifest.worksheets:
        scoped = [summary for summary in invoices.values() if summary.worksheet == worksheet]
        values = [summary.net_ledger_value for summary in scoped]
        lines = [summary.line_count for summary in scoped]
        worksheet_scoped_metrics[worksheet] = {
            "invoice_count": len(scoped),
            "cancellation_invoice_count": sum(
                summary.native_invoice_type == "CANCELLATION_INVOICE" for summary in scoped),
            "normal_invoice_count": sum(
                summary.native_invoice_type == "NORMAL_INVOICE" for summary in scoped),
            "invoices_missing_customer": sum(not summary.known_customers for summary in scoped),
            "invoices_with_multiple_known_customers": sum(
                len(summary.known_customers) > 1 for summary in scoped),
            "invoices_with_mixed_known_and_missing_customer": sum(
                bool(summary.known_customers) and summary.has_missing_customer for summary in scoped),
            "invoices_with_multiple_countries": sum(len(summary.countries) > 1 for summary in scoped),
            "minimum_lines_per_invoice": min(lines, default=0),
            "maximum_lines_per_invoice": max(lines, default=0),
            "average_lines_per_invoice": sum(lines) / len(lines) if lines else 0.0,
            "net_invoice_value": float(sum(values, Decimal(0))),
            "minimum_invoice_value": float(min(values, default=Decimal(0))),
            "maximum_invoice_value": float(max(values, default=Decimal(0))),
            "average_invoice_value": float(sum(values, Decimal(0)) / len(values)) if values else 0.0,
        }
    reconciliation = {
        "classification_value_total": float(net_ledger_value),
        "invoice_value_total": float(invoice_net_value),
        "daily_value_total": float(daily_net_value),
        "signed_value_reconciled": net_ledger_value == invoice_net_value == daily_net_value,
    }
    fanout = {
        "invoice_count_matches_scoped_grain": len(invoice_rows) == len(invoices),
        "line_count_matches_source": sum(classification_counts.values()) == source_rows,
        "country_line_count_matches_source": sum(country_line_counts.values()) == source_rows,
        "signed_value_reconciled": reconciliation["signed_value_reconciled"],
    }
    acceptance = []
    if not fixture:
        expected = dict(manifest.raw["acceptance"])
        actual = {name: (metrics["source_rows"] if name == "total_rows" else metrics[name])
                  for name in expected}
        for name, expected_value in expected.items():
            acceptance.append({"metric": name, "expected": expected_value,
                               "actual": actual[name], "passed": actual[name] == expected_value})
        for worksheet, spec in manifest.worksheets.items():
            acceptance.append({"metric": f"rows:{worksheet}", "expected": spec["expected_rows"],
                               "actual": sheet_counts[worksheet],
                               "passed": sheet_counts[worksheet] == spec["expected_rows"]})
        if enforce_acceptance and not all(row["passed"] for row in acceptance):
            failed = [row for row in acceptance if not row["passed"]]
            raise ValueError(f"UCI full-data acceptance failed: {failed}")

    gold_seconds = time.perf_counter() - gold_started
    report = {
        "benchmark_id": manifest.raw["benchmark_id"], "business_id": UCI_BUSINESS_ID,
        "source_id": UCI_SOURCE_ID, "fixture": fixture, "network_access": False,
        "currency": currency, "input_rows": dict(sheet_counts), "metrics": metrics,
        "worksheet_scoped_metrics": worksheet_scoped_metrics,
        "line_classifications": dict(classification_counts),
        "stock_code_classifications": dict(stock_counts),
        "behavior_samples": dict(behavior_samples),
        "reconciliation": reconciliation, "fanout": fanout,
        "quality": quality_rows, "anomaly": anomaly_rows,
        "economics": economics_rows[0],
        "canonical_projection": {
            "eligible_order_count": projected_invoice_count,
            "eligible_line_count": sum(summary.projected_line_count for summary in invoices.values()),
            "core_compatibility_exercised": exercise_core,
            "commerce_order_models": len(canonical_orders) if exercise_core else None,
            "order_line_models": sum(len(order.lines) for order in canonical_orders) if exercise_core else None,
            "operational_events": 0, "cod_rows": 0, "remittance_rows": 0,
            "attribution_rows": 0,
        },
        "acceptance": acceptance,
        "timings_seconds": {
            "source_parsing": round(source_parsing_seconds, 6),
            "bronze": round(bronze_seconds, 6),
            "silver": round(silver_seconds, 6),
            "gold_dq": round(gold_seconds, 6),
            "total": round(time.perf_counter() - started, 6),
        },
    }
    if write_outputs:
        files = {
            "invoice_summary.jsonl": invoice_rows,
            "retail_daily.jsonl": daily_rows,
            "line_classification.jsonl": classification_rows,
            "country_distribution.jsonl": country_rows,
            "data_quality.jsonl": quality_rows,
            "economic_completeness.jsonl": economics_rows,
            "anomaly.jsonl": anomaly_rows,
        }
        for filename, rows in files.items():
            with (output / "gold" / filename).open("w", encoding="utf-8", newline="\n") as handle:
                for row in rows:
                    _write_line(handle, row)
        with (output / "report.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
            _write_line(handle, report)
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "profile", "dry-run", "benchmark"))
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--enforce-acceptance", action="store_true")
    parser.add_argument("--core-compatibility", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "validate":
        manifest = UciManifest.load()
        root = args.root.resolve() if args.root else manifest.root(fixture=args.fixture)
        errors = manifest.validate(root, fixture=args.fixture)
        payload = {"valid": not errors, "errors": errors, "root": str(root),
                   "fixture": args.fixture, "network_access": False}
        print(json.dumps(payload, indent=2))
        return int(bool(errors))
    report = run_benchmark(
        fixture=args.fixture, root=args.root, output_root=args.output_root,
        write_outputs=args.command == "benchmark" and not args.no_write,
        enforce_acceptance=args.enforce_acceptance,
        core_compatibility=(True if args.core_compatibility else None),
    )
    print(json.dumps(report, default=_json_default, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Small deterministic input boundary for aggregate-only analyst questions."""

from __future__ import annotations

import re


BUSINESS_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE = re.compile(r"(?<!\d)(?:\+?\d[\s().-]*){8,}\d(?!\d)")
CUSTOMER_LEVEL = re.compile(
    r"\b(?:customer[ -]?id|order[ -]?id|tracking number|phone number|"
    r"email address|street address|raw (?:row|record|order)|"
    r"individual (?:customer|order)|customer-level)\b",
    re.IGNORECASE,
)


def valid_business_id(value: str) -> bool:
    return bool(BUSINESS_ID.fullmatch(value))


def is_aggregate_question(question: str) -> bool:
    return not (
        EMAIL.search(question)
        or PHONE.search(question)
        or CUSTOMER_LEVEL.search(question)
    )

"""Resolve source credential references without exposing secret values."""

from __future__ import annotations

from collections.abc import Mapping
import os
import re

from src.onboarding.models import SourceConfig


REFERENCE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")


class CredentialResolutionError(RuntimeError):
    """A named runtime credential is unavailable; messages never contain values."""


def resolve_credential(
    config: SourceConfig, environ: Mapping[str, str] | None = None
) -> str:
    """Return the runtime value for a validated environment-variable reference."""

    reference = config.credential_ref
    if not REFERENCE_PATTERN.fullmatch(reference or ""):
        raise CredentialResolutionError(
            f"Source {config.source_id!r} has an invalid credential reference name"
        )
    environment = os.environ if environ is None else environ
    value = environment.get(reference)
    if value is None or not value.strip():
        raise CredentialResolutionError(
            f"Credential reference {reference!r} is not set for source {config.source_id!r}"
        )
    return value

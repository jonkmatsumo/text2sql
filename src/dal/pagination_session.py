"""Server-issued pagination session model and bounded in-memory registry."""

from __future__ import annotations

import re
import secrets
from collections import OrderedDict
from dataclasses import dataclass, replace
from threading import Lock
from typing import Callable, Protocol

from dal.pagination_cursor import (
    CURSOR_MAX_SIGNED_INT,
    DEFAULT_CURSOR_TTL_MS,
    MAX_CURSOR_TTL_MS,
    cursor_now_epoch_milliseconds,
)

PAGINATION_SESSION_MISSING = "PAGINATION_SESSION_MISSING"
PAGINATION_SESSION_UNKNOWN = "PAGINATION_SESSION_UNKNOWN"
PAGINATION_SESSION_REVOKED = "PAGINATION_SESSION_REVOKED"

PAGINATION_SESSION_ID_MIN_LENGTH = 16
PAGINATION_SESSION_ID_MAX_LENGTH = 64
PAGINATION_SESSION_ID_BYTES = 24
PAGINATION_SESSION_PROVIDER_MAX_LENGTH = 64
PAGINATION_SESSION_MODE_MAX_LENGTH = 32
PAGINATION_SESSION_SCOPE_FP_MAX_LENGTH = 128
PAGINATION_SESSION_POLICY_FP_MAX_LENGTH = 128
PAGINATION_SESSION_TENANT_MAX_LENGTH = 128
PAGINATION_SESSION_DEFAULT_MAX_ENTRIES = 10_000
PAGINATION_SESSION_MAX_MAX_ENTRIES = 100_000

_SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class PaginationSession:
    """Bounded server-issued pagination session identity and scope bindings."""

    session_id: str
    created_at_ms: int
    tenant_id: str | None
    provider_name: str
    pagination_mode: str
    query_scope_fp: str
    policy_snapshot_fp: str
    revocation_epoch: int
    is_revoked: bool = False

    def __post_init__(self) -> None:
        """Fail closed for malformed, unbounded, or unsafe session payloads."""
        if not isinstance(self.session_id, str):
            raise ValueError("Pagination session id must be a string.")
        if not (
            PAGINATION_SESSION_ID_MIN_LENGTH
            <= len(self.session_id)
            <= PAGINATION_SESSION_ID_MAX_LENGTH
        ):
            raise ValueError("Pagination session id length is out of bounds.")
        if _SESSION_ID_PATTERN.fullmatch(self.session_id) is None:
            raise ValueError("Pagination session id contains unsupported characters.")
        if not isinstance(self.created_at_ms, int):
            raise ValueError("Pagination session created_at_ms must be an integer.")
        if self.created_at_ms < 0 or self.created_at_ms > CURSOR_MAX_SIGNED_INT:
            raise ValueError("Pagination session created_at_ms is out of bounds.")
        if self.tenant_id is not None:
            if not isinstance(self.tenant_id, str):
                raise ValueError("Pagination session tenant_id must be a string when provided.")
            normalized_tenant = self.tenant_id.strip()
            if not normalized_tenant:
                raise ValueError("Pagination session tenant_id must not be empty when provided.")
            if len(normalized_tenant) > PAGINATION_SESSION_TENANT_MAX_LENGTH:
                raise ValueError("Pagination session tenant_id exceeds maximum length.")
        self._validate_required_field(
            "provider_name",
            self.provider_name,
            PAGINATION_SESSION_PROVIDER_MAX_LENGTH,
        )
        self._validate_required_field(
            "pagination_mode",
            self.pagination_mode,
            PAGINATION_SESSION_MODE_MAX_LENGTH,
        )
        self._validate_required_field(
            "query_scope_fp",
            self.query_scope_fp,
            PAGINATION_SESSION_SCOPE_FP_MAX_LENGTH,
        )
        self._validate_required_field(
            "policy_snapshot_fp",
            self.policy_snapshot_fp,
            PAGINATION_SESSION_POLICY_FP_MAX_LENGTH,
        )
        if not isinstance(self.revocation_epoch, int):
            raise ValueError("Pagination session revocation_epoch must be an integer.")
        if self.revocation_epoch < 0 or self.revocation_epoch > CURSOR_MAX_SIGNED_INT:
            raise ValueError("Pagination session revocation_epoch is out of bounds.")
        if not isinstance(self.is_revoked, bool):
            raise ValueError("Pagination session is_revoked must be a boolean.")

    @staticmethod
    def _validate_required_field(name: str, value: str, max_length: int) -> None:
        if not isinstance(value, str):
            raise ValueError(f"Pagination session {name} must be a string.")
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"Pagination session {name} must not be empty.")
        if len(normalized) > max_length:
            raise ValueError(f"Pagination session {name} exceeds maximum length.")


def generate_pagination_session_id() -> str:
    """Generate a bounded opaque non-guessable session id."""
    session_id = secrets.token_urlsafe(PAGINATION_SESSION_ID_BYTES).rstrip("=")
    if len(session_id) > PAGINATION_SESSION_ID_MAX_LENGTH:
        session_id = session_id[:PAGINATION_SESSION_ID_MAX_LENGTH]
    if len(session_id) < PAGINATION_SESSION_ID_MIN_LENGTH:
        # Defensive fallback; token_urlsafe should not usually produce short values here.
        session_id = f"{session_id}{secrets.token_hex(8)}"
        session_id = session_id[:PAGINATION_SESSION_ID_MAX_LENGTH]
    return session_id


def create_pagination_session(
    *,
    tenant_id: str | None,
    provider_name: str,
    pagination_mode: str,
    query_scope_fp: str,
    policy_snapshot_fp: str,
    revocation_epoch: int,
    now_epoch_milliseconds: int | None = None,
) -> PaginationSession:
    """Construct a new bounded pagination session with a server-issued id."""
    created_at_ms = cursor_now_epoch_milliseconds(
        now_epoch_milliseconds=now_epoch_milliseconds,
    )
    normalized_tenant_id: str | None = None
    if tenant_id is not None:
        normalized_tenant_id = tenant_id.strip()
        if not normalized_tenant_id:
            normalized_tenant_id = None
    return PaginationSession(
        session_id=generate_pagination_session_id(),
        created_at_ms=created_at_ms,
        tenant_id=normalized_tenant_id,
        provider_name=provider_name.strip().lower(),
        pagination_mode=pagination_mode.strip().lower(),
        query_scope_fp=query_scope_fp.strip(),
        policy_snapshot_fp=policy_snapshot_fp.strip(),
        revocation_epoch=int(revocation_epoch),
    )


class PaginationSessionRegistry(Protocol):
    """Bounded registry contract for server-issued pagination sessions."""

    def get(self, session_id: str) -> PaginationSession | None:
        """Return a session when present and not expired."""

    def put(self, session: PaginationSession) -> PaginationSession:
        """Persist a bounded session object."""

    def revoke(self, session_id: str) -> PaginationSession | None:
        """Mark a session revoked when present and not expired."""


class InMemoryPaginationSessionRegistry(PaginationSessionRegistry):
    """Thread-safe in-memory registry with TTL and bounded entry capacity."""

    def __init__(
        self,
        *,
        ttl_ms: int = DEFAULT_CURSOR_TTL_MS,
        max_entries: int = PAGINATION_SESSION_DEFAULT_MAX_ENTRIES,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        """Initialize bounded in-memory storage with deterministic time controls."""
        normalized_ttl_ms = int(ttl_ms)
        if normalized_ttl_ms <= 0:
            normalized_ttl_ms = DEFAULT_CURSOR_TTL_MS
        self._ttl_ms = min(normalized_ttl_ms, MAX_CURSOR_TTL_MS)
        normalized_max_entries = int(max_entries)
        if normalized_max_entries <= 0:
            normalized_max_entries = PAGINATION_SESSION_DEFAULT_MAX_ENTRIES
        self._max_entries = min(normalized_max_entries, PAGINATION_SESSION_MAX_MAX_ENTRIES)
        self._now_ms: Callable[[], int] = now_ms or cursor_now_epoch_milliseconds
        self._sessions: OrderedDict[str, PaginationSession] = OrderedDict()
        self._lock = Lock()

    @property
    def ttl_ms(self) -> int:
        """Expose normalized TTL for deterministic tests and telemetry."""
        return self._ttl_ms

    def get(self, session_id: str) -> PaginationSession | None:
        """Return a non-expired session by id, or None when absent/invalid."""
        normalized_session_id = _normalize_session_id(session_id)
        if normalized_session_id is None:
            return None
        now_ms = self._safe_now_ms()
        with self._lock:
            self._evict_expired_locked(now_ms)
            session = self._sessions.get(normalized_session_id)
            if session is None:
                return None
            self._sessions.move_to_end(normalized_session_id)
            return session

    def put(self, session: PaginationSession) -> PaginationSession:
        """Store or replace a session and enforce bounded registry capacity."""
        now_ms = self._safe_now_ms()
        with self._lock:
            self._evict_expired_locked(now_ms)
            self._sessions[session.session_id] = session
            self._sessions.move_to_end(session.session_id)
            while len(self._sessions) > self._max_entries:
                self._sessions.popitem(last=False)
        return session

    def revoke(self, session_id: str) -> PaginationSession | None:
        """Mark a stored session revoked and return the updated session object."""
        normalized_session_id = _normalize_session_id(session_id)
        if normalized_session_id is None:
            return None
        now_ms = self._safe_now_ms()
        with self._lock:
            self._evict_expired_locked(now_ms)
            session = self._sessions.get(normalized_session_id)
            if session is None:
                return None
            if session.is_revoked:
                self._sessions.move_to_end(normalized_session_id)
                return session
            revoked = replace(session, is_revoked=True)
            self._sessions[normalized_session_id] = revoked
            self._sessions.move_to_end(normalized_session_id)
            return revoked

    def clear(self) -> None:
        """Clear all in-memory state (test-only helper)."""
        with self._lock:
            self._sessions.clear()

    def _safe_now_ms(self) -> int:
        now_ms = int(self._now_ms())
        if now_ms < 0:
            return 0
        return min(now_ms, CURSOR_MAX_SIGNED_INT)

    def _evict_expired_locked(self, now_ms: int) -> None:
        while self._sessions:
            session_id, session = next(iter(self._sessions.items()))
            if now_ms - session.created_at_ms <= self._ttl_ms:
                break
            self._sessions.pop(session_id, None)


_DEFAULT_REGISTRY_LOCK = Lock()
_DEFAULT_REGISTRY: InMemoryPaginationSessionRegistry | None = None


def get_default_pagination_session_registry() -> InMemoryPaginationSessionRegistry:
    """Return a process-wide in-memory registry singleton."""
    global _DEFAULT_REGISTRY
    if _DEFAULT_REGISTRY is not None:
        return _DEFAULT_REGISTRY
    with _DEFAULT_REGISTRY_LOCK:
        if _DEFAULT_REGISTRY is None:
            _DEFAULT_REGISTRY = InMemoryPaginationSessionRegistry()
        return _DEFAULT_REGISTRY


def _normalize_session_id(session_id: str) -> str | None:
    if not isinstance(session_id, str):
        return None
    normalized = session_id.strip()
    if not normalized:
        return None
    if (
        len(normalized) < PAGINATION_SESSION_ID_MIN_LENGTH
        or len(normalized) > PAGINATION_SESSION_ID_MAX_LENGTH
    ):
        return None
    if _SESSION_ID_PATTERN.fullmatch(normalized) is None:
        return None
    return normalized

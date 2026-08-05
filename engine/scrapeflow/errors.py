"""Stable public error hierarchy for the engine."""

from __future__ import annotations

from collections.abc import Sequence


class ScraperError(RuntimeError):
    """An expected error safe to show to the local user."""


class ApiError(ScraperError):
    """A remote API request failed."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class PlanError(ScraperError):
    """A generated or loaded operation plan is unsafe or incomplete."""


class PartialMoveError(ApiError):
    """A remote move only completed for part of the requested names."""

    def __init__(self, message: str, moved_names: Sequence[str]) -> None:
        super().__init__(message)
        self.moved_names = list(moved_names)


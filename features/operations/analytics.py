"""Compatibility facade for legacy operations analytics imports."""

from features.operations.domain.imports import build_imports_summary
from features.operations.domain.otd import on_time_delivery_summary

__all__ = ["on_time_delivery_summary", "build_imports_summary"]

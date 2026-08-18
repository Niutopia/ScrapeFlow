"""Automatic provider search and task-owned materialization."""

from .materialize import LocalTorrentMaterializer
from .search import ReplenishmentSearchService
from .subtitle_provider import (
    PROVIDER_SUBTITLE_A4K,
    PROVIDER_SUBTITLE_ANIMETOSHO,
    PROVIDER_SUBTITLE_ASSRT,
    PROVIDER_SUBTITLE_OPENSUBTITLES,
    PROVIDER_SUBTITLE_SUBDOG,
    PROVIDER_SUBTITLE_SUBHD,
    PROVIDER_SUBTITLE_ZIMUKU,
    SubtitleDiscoveryService,
    SubtitleInfrastructureError,
    SubtitleMaterializer,
    SubtitlePauseRequested,
    SubtitleProviderError,
    score_subtitle_candidate,
)

__all__ = [
    "LocalTorrentMaterializer",
    "PROVIDER_SUBTITLE_A4K",
    "PROVIDER_SUBTITLE_ANIMETOSHO",
    "PROVIDER_SUBTITLE_ASSRT",
    "PROVIDER_SUBTITLE_OPENSUBTITLES",
    "PROVIDER_SUBTITLE_SUBDOG",
    "PROVIDER_SUBTITLE_SUBHD",
    "PROVIDER_SUBTITLE_ZIMUKU",
    "ReplenishmentSearchService",
    "SubtitleDiscoveryService",
    "SubtitleInfrastructureError",
    "SubtitleMaterializer",
    "SubtitlePauseRequested",
    "SubtitleProviderError",
    "score_subtitle_candidate",
]



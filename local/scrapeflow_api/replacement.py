"""API-layer import surface for the pure replacement manifest domain.

The manifest itself lives in ``engine.scrapeflow`` so planner/writer callers
and the local HTTP composition root share exactly one validator and recovery
model.  This module intentionally adds no endpoint or second writer.
"""

from engine.scrapeflow.replacement import *  # noqa: F401,F403
from engine.scrapeflow.replacement import __all__


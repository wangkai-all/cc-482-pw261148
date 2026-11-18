"""File merge service: snapshot-isolated compaction for append-only segment stores."""

from .planner import MergeConfig
from .service import MergeService

__all__ = ["MergeConfig", "MergeService"]
__version__ = "0.1.0"

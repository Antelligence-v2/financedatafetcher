"""Detection modules for analyzing web pages and finding data sources."""

from .network_inspector import NetworkInspector, CandidateEndpoint
from .data_detector import DataDetector

__all__ = [
    "NetworkInspector",
    "CandidateEndpoint",
    "DataDetector",
]


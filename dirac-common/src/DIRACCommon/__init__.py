"""
DIRACCommon - Stateless utilities for DIRAC

This package contains stateless utilities extracted from DIRAC that can be used
by DiracX and other projects without triggering DIRAC's global state initialization.

The utilities here should not depend on:
- gConfig (Configuration system)
- gLogger (Global logging)
- gMonitor (Monitoring)
- Database connections
- Any other global state
"""

import importlib.metadata

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    # package is not installed
    __version__ = "Unknown"

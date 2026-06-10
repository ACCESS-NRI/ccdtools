from . import catalog
import importlib.metadata

try:
    __version__ = importlib.metadata.version("access-ccdtools")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

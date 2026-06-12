"""Make the repo-root modules (config, soccer_model, scrapers, …) importable.

Run the suite from anywhere with: python -m pytest tests/
"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

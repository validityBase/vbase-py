"""Run the public forwarder test modules against the selected vbase package."""

from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from types import ModuleType

TEST_MODULES = (
    "vbase.tests.test_vbase_client",
    "vbase.tests.test_indexing_service",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _configure_test_package() -> ModuleType:
    """Load vbase from the installation and expose checkout-only test modules."""
    repo_root = _repo_root()
    package_source = os.environ.get("VBASE_PACKAGE_SOURCE", "source").strip().lower()
    if package_source not in {"source", "pypi"}:
        raise RuntimeError(
            f"Unsupported VBASE_PACKAGE_SOURCE={package_source!r}; "
            "expected 'source' or 'pypi'"
        )

    # The runner is invoked by absolute path, so the checkout is not normally
    # on sys.path. Add it only for the source leg; the PyPI leg must import from
    # site-packages before the checkout's test modules are exposed below.
    if package_source == "source":
        sys.path.insert(0, str(repo_root))

    vbase = importlib.import_module("vbase")
    package_path = Path(vbase.__file__).resolve()
    package_in_checkout = repo_root in package_path.parents

    if package_source == "pypi" and package_in_checkout:
        raise RuntimeError(
            "PyPI test run imported vbase from the repository checkout: "
            f"{package_path}"
        )
    if package_source == "source" and not package_in_checkout:
        raise RuntimeError(
            "Source test run imported vbase outside the repository checkout: "
            f"{package_path}"
        )

    test_package_path = str(repo_root / "vbase")
    if test_package_path not in vbase.__path__:
        vbase.__path__.append(test_package_path)

    print(f"vbase imported from {package_path}")
    return vbase


def main() -> int:
    """Run the shared public forwarder test suite."""
    _configure_test_package()
    loader = unittest.defaultTestLoader
    suite = unittest.TestSuite(
        loader.loadTestsFromName(module_name) for module_name in TEST_MODULES
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())

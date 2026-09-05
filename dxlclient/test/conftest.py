"""
pytest configuration mirroring the nose settings in ``setup.cfg``: tests
tagged with the nose ``manual`` or ``load`` attribute and the wildcard
performance test are excluded from regular runs.
"""

import pytest

_EXCLUDED_ATTRS = ("manual", "load")
_EXCLUDED_TESTS = ("test_wildcard_performance",)


def pytest_collection_modifyitems(items):
    skip = pytest.mark.skip(reason="manual / load test (excluded by default)")
    for item in items:
        func = getattr(item, "function", None)
        if item.name in _EXCLUDED_TESTS or \
                any(getattr(func, attr, False) for attr in _EXCLUDED_ATTRS):
            item.add_marker(skip)

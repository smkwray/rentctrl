from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.option.markexpr:
        return

    skip_integration = pytest.mark.skip(reason="integration test; run with `pytest -m integration`")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)

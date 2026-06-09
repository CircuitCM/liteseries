from __future__ import annotations

import importlib
import os

import pytest

import liteseries._sql as sql


@pytest.fixture()
def reloaded_sql():
    """Reload `_sql` around a test so env-picked globals do not leak."""
    original = os.environ.get("LITESERIES_PROTECTNAMES")
    yield lambda: importlib.reload(sql)
    if original is None:
        os.environ.pop("LITESERIES_PROTECTNAMES", None)
    else:
        os.environ["LITESERIES_PROTECTNAMES"] = original
    importlib.reload(sql)


def test_qident_defaults_to_plain_identifier(
    monkeypatch: pytest.MonkeyPatch,
    reloaded_sql,
) -> None:
    """Covers the default no-allocation identifier path."""
    monkeypatch.delenv("LITESERIES_PROTECTNAMES", raising=False)

    module = reloaded_sql()

    assert module.qident("price_table") == "price_table"
    assert module.colreq(("instrument", "ts")) == "instrument, ts"


@pytest.mark.parametrize("value", ["true", "TRUE", "TrUe"])
def test_qident_uses_original_quoting_when_protection_is_true(
    monkeypatch: pytest.MonkeyPatch,
    reloaded_sql,
    value: str,
) -> None:
    """Covers the case-insensitive protected-name compatibility path."""
    monkeypatch.setenv("LITESERIES_PROTECTNAMES", value)

    module = reloaded_sql()

    assert module.qident('Adj "Close"') == '"Adj ""Close"""'
    assert module.colreq(("Adj Close", "ts")) == '"Adj Close", "ts"'


def test_qident_ignores_non_true_protection_values(
    monkeypatch: pytest.MonkeyPatch,
    reloaded_sql,
) -> None:
    """Covers non-true env values staying on the plain identifier path."""
    monkeypatch.setenv("LITESERIES_PROTECTNAMES", "1")

    module = reloaded_sql()

    assert module.qident("Adj Close") == "Adj Close"

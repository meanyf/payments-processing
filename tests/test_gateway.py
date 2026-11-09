"""Проверки эмуляции платёжного шлюза."""

from unittest.mock import AsyncMock

import pytest

from payments_processing.db.models import PaymentStatus
from payments_processing.services import gateway


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("draw", "delay", "expected"),
    [
        (0.0, 2.0, PaymentStatus.SUCCEEDED),
        (0.8999, 3.5, PaymentStatus.SUCCEEDED),
        (0.9, 5.0, PaymentStatus.FAILED),
        (0.9999, 4.0, PaymentStatus.FAILED),
    ],
)
async def test_simulate_payment(monkeypatch, draw, delay, expected):
    """Шлюз ждёт 2–5 секунд и выделяет 10% диапазона на отказ платежа."""
    sleep = AsyncMock()

    def uniform(lower, upper):
        assert (lower, upper) == (2, 5)
        return delay

    monkeypatch.setattr(gateway, "sleep", sleep)
    monkeypatch.setattr(gateway.random, "uniform", uniform)
    monkeypatch.setattr(gateway.random, "random", lambda: draw)

    result = await gateway.simulate_payment()

    assert result == expected
    sleep.assert_awaited_once_with(delay)

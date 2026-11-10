"""Эмуляция внешнего платёжного шлюза."""

import random
from asyncio import sleep

from payments_processing.db.models import PaymentStatus


async def simulate_payment() -> PaymentStatus:
    """Эмулирует обработку платежа за 2–5 секунд.

    Returns:
        Статус succeeded с вероятностью 90% или failed с вероятностью 10%.
    """
    await sleep(random.uniform(2, 5))
    return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED

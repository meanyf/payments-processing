"""Маршруты API платежей."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from payments_processing.api.schemas import (
    PaymentCreate,
    PaymentCreated,
    PaymentDetails,
)
from payments_processing.db.session import get_session
from payments_processing.services import payments

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])


@router.post("", response_model=PaymentCreated, status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentCreated:
    """Принимает запрос создания платежа и возвращает подтверждение.

    Args:
        data: Данные нового платежа.
        idempotency_key: Ключ идемпотентности запроса.
        session: Сессия БД для сохранения платежа и события.

    Returns:
        Идентификатор, статус и дата создания принятого платежа.
    """
    try:
        payment = await payments.create_payment(
            session,
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata=data.metadata,
            webhook_url=str(data.webhook_url),
            idempotency_key=idempotency_key,
        )
    except payments.IdempotencyConflict as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key already used with different request data",
        ) from error
    return PaymentCreated(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get("/{payment_id}", response_model=PaymentDetails)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentDetails:
    """Возвращает подробную информацию о платеже.

    Args:
        payment_id: Идентификатор платежа из пути запроса.
        session: Сессия БД для чтения платежа.

    Returns:
        Данные платежа, его текущий статус и даты.

    Raises:
        HTTPException: Если платёж не найден.
    """
    payment = await payments.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )
    return PaymentDetails(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )

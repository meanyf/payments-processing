"""Исключения сервисов обработки платежей."""


class PaymentNotFound(Exception):
    """Платёж из сообщения отсутствует в БД."""


class AttemptMismatch(Exception):
    """Номер повтора из сообщения несовместим с состоянием платежа."""


class IdempotencyConflict(Exception):
    """Ключ идемпотентности уже использован с другими данными платежа."""

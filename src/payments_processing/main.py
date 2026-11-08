"""Точка входа HTTP-приложения."""

from fastapi import Depends, FastAPI

from payments_processing.api.auth import verify_api_key
from payments_processing.api.payments import router as payments_router

app = FastAPI(title="Payments Processing", dependencies=[Depends(verify_api_key)])
app.include_router(payments_router)

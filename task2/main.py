from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Optional

import httpx
import jwt
from jwt import PyJWKClient
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

import clickhouse_connect


CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

REPORTS_PIPELINE = os.getenv("REPORTS_PIPELINE", "reports_etl")

KEYCLOAK_ISSUER = os.getenv("KEYCLOAK_ISSUER", "")
KEYCLOAK_AUDIENCE = os.getenv("KEYCLOAK_AUDIENCE", "")

JWKS_CACHE_SECONDS = 300


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )


class ReportRow(BaseModel):
    day: date
    prosthesis_id: str
    steps_sum: int
    active_minutes: int
    avg_load: Optional[float] = None
    max_load: Optional[float] = None
    errors_count: int
    battery_min: Optional[float] = None
    battery_avg: Optional[float] = None


class ReportResponse(BaseModel):
    user_id: str
    from_date: date
    to_date: date
    available_to: Optional[datetime]
    rows: list[ReportRow]


auth_scheme = HTTPBearer(auto_error=True)

_jwks_client: Optional[PyJWKClient] = None
_jwks_loaded_at: float = 0


async def _get_jwks_client() -> PyJWKClient:
    global _jwks_client, _jwks_loaded_at

    if _jwks_client and (time.time() - _jwks_loaded_at) < JWKS_CACHE_SECONDS:
        return _jwks_client

    if not KEYCLOAK_ISSUER:
        raise RuntimeError("KEYCLOAK_ISSUER not configured")

    openid_url = KEYCLOAK_ISSUER.rstrip("/") + "/.well-known/openid-configuration"

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(openid_url)
        resp.raise_for_status()
        data = resp.json()

    jwks_uri = data["jwks_uri"]

    _jwks_client = PyJWKClient(jwks_uri)
    _jwks_loaded_at = time.time()
    return _jwks_client


async def get_current_user_id(
    credentials: HTTPAuthorizationCredentials = Depends(auth_scheme),
) -> str:
    token = credentials.credentials

    try:
        jwks_client = await _get_jwks_client()
        signing_key = jwks_client.get_signing_key_from_jwt(token).key

        decoded = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=KEYCLOAK_AUDIENCE or None,
            issuer=KEYCLOAK_ISSUER,
            options={
                "verify_aud": bool(KEYCLOAK_AUDIENCE),
                "verify_iss": True,
            },
        )

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = decoded.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token has no sub claim")

    return str(user_id)


def get_watermark(ch) -> Optional[datetime]:
    q = """
    SELECT max_time
    FROM etl_watermark
    WHERE pipeline = %(pipeline)s
    ORDER BY updated_at DESC
    LIMIT 1
    """
    res = ch.query(q, parameters={"pipeline": REPORTS_PIPELINE})
    if not res.result_rows:
        return None
    return res.result_rows[0][0]


def ensure_period_processed(wm: Optional[datetime], to_date: date):
    if wm is None:
        return

    requested = datetime.combine(to_date, datetime.min.time())
    if requested > wm:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Requested period not processed yet",
                "available_to": wm.isoformat(),
            },
        )


app = FastAPI(title="Reports API", version="1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/reports", response_model=ReportResponse)
def get_report(
    user_id: str = Depends(get_current_user_id),
    from_date: date = Query(..., alias="from"),
    to_date: date = Query(..., alias="to"),
):
    if from_date > to_date:
        raise HTTPException(status_code=400, detail="'from' must be <= 'to'")

    ch = get_ch_client()
    wm = get_watermark(ch)

    ensure_period_processed(wm, to_date)

    q = """
    SELECT
        day,
        prosthesis_id,
        steps_sum,
        active_minutes,
        avg_load,
        max_load,
        errors_count,
        battery_min,
        battery_avg
    FROM reports_user_daily
    WHERE user_id = %(user_id)s
      AND day >= toDate(%(from)s)
      AND day <= toDate(%(to)s)
    ORDER BY day
    """

    res = ch.query(
        q,
        parameters={
            "user_id": user_id,
            "from": from_date.isoformat(),
            "to": to_date.isoformat(),
        },
    )

    rows = [
        ReportRow(
            day=r[0],
            prosthesis_id=r[1],
            steps_sum=int(r[2] or 0),
            active_minutes=int(r[3] or 0),
            avg_load=r[4],
            max_load=r[5],
            errors_count=int(r[6] or 0),
            battery_min=r[7],
            battery_avg=r[8],
        )
        for r in res.result_rows
    ]

    return ReportResponse(
        user_id=user_id,
        from_date=from_date,
        to_date=to_date,
        available_to=wm,
        rows=rows,
    )
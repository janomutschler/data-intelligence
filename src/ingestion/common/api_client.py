import logging
import time
from typing import Any, Callable, Dict, Optional

import requests

from common.utils import utc_now_str
from config import (
    BASE_BACKOFF,
    MAX_RETRIES,
    NO_DATA_STATUSES,
    REQUEST_TIMEOUT,
    RETRYABLE_STATUSES,
)

logger = logging.getLogger(__name__)

TOKEN_URL = "https://api.lufthansa.com/v1/oauth/token"
_TOKEN_REFRESH_BUFFER_SECONDS = 60


def _fetch_token(client_id: str, client_secret: str) -> tuple[str, int]:
    """Request a new OAuth2 Bearer token and return (access_token, expires_in)."""
    response = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    response.raise_for_status()
    token_data = response.json()
    return token_data["access_token"], int(token_data["expires_in"])


def _refresh_session_token(session: requests.Session) -> None:
    """Re-authenticate using credentials stored on the session and update its headers."""
    access_token, expires_in = _fetch_token(
        session._lh_client_id,
        session._lh_client_secret,
    )
    session.headers.update({"Authorization": f"Bearer {access_token}"})
    session.token_expires_at = time.time() + expires_in
    logger.info("Token refreshed — expires in %d seconds.", expires_in)


def _ensure_token_valid(session: requests.Session) -> None:
    """Proactively refresh the token before it expires."""
    if time.time() >= session.token_expires_at - _TOKEN_REFRESH_BUFFER_SECONDS:
        logger.info("Token nearing expiry — refreshing proactively.")
        _refresh_session_token(session)


def create_session(client_id: str, client_secret: str) -> requests.Session:
    """
    Create a configured requests session for the Lufthansa API using OAuth2.
    Credentials are stored on the session to enable automatic token refresh.
    """
    access_token, expires_in = _fetch_token(client_id, client_secret)
    logger.info("Initial token acquired — expires in %d seconds.", expires_in)

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
    )
    session.token_expires_at = time.time() + expires_in
    # Store credentials for automatic token refresh mid-run.
    session._lh_client_id = client_id
    session._lh_client_secret = client_secret

    return session


def request_with_retry(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]],
    context: Dict[str, Any],
    log_func: Callable,
) -> Optional[requests.Response]:
    """
    Execute a GET request with exponential-backoff retry for transient failures.
    The token is proactively refreshed before each attempt if it is close to expiry.
    """
    backoff = BASE_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        _ensure_token_valid(session)

        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            logger.warning("Request exception on attempt %d/%d: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                logger.info("Retrying in %d seconds...", backoff)
                time.sleep(backoff)
                backoff *= 2
            log_func(
                {
                    "timestamp_utc": utc_now_str(),
                    "status": "failed" if attempt == MAX_RETRIES else "retry",
                    "http_status": None,
                    "attempt": attempt,
                    "url": url,
                    "params": params,
                    "exception": str(exc),
                    **context,
                }
            )
            continue

        if response.status_code == 200:
            return response

        if response.status_code in NO_DATA_STATUSES:
            # The API returned "resource not found" — this is an expected empty result
            # (e.g. no flights in this window, no reference data for this offset).
            # Log it as informational and stop pagination cleanly.
            logger.info(
                "HTTP %d (no data) for %s — stopping pagination.",
                response.status_code,
                url,
            )
            log_func(
                {
                    "timestamp_utc": utc_now_str(),
                    "status": "no_data",
                    "http_status": response.status_code,
                    "attempt": attempt,
                    "url": url,
                    "params": params,
                    **context,
                }
            )
            return None

        if response.status_code in RETRYABLE_STATUSES:
            logger.warning(
                "Retryable HTTP %d on attempt %d/%d — %s",
                response.status_code,
                attempt,
                MAX_RETRIES,
                response.text[:200],
            )
            status = "retry" if attempt < MAX_RETRIES else "failed"
            log_func(
                {
                    "timestamp_utc": utc_now_str(),
                    "status": status,
                    "http_status": response.status_code,
                    "attempt": attempt,
                    "url": url,
                    "params": params,
                    "response_text": response.text[:500],
                    **context,
                }
            )
            if attempt < MAX_RETRIES:
                logger.info("Sleeping for %d seconds...", backoff)
                time.sleep(backoff)
                backoff *= 2
            continue

        # Non-retryable failure (4xx other than retryable ones)
        logger.error(
            "Non-retryable HTTP %d: %s", response.status_code, response.text[:300]
        )
        log_func(
            {
                "timestamp_utc": utc_now_str(),
                "status": "failed",
                "http_status": response.status_code,
                "attempt": attempt,
                "url": url,
                "params": params,
                "response_text": response.text[:500],
                **context,
            }
        )
        return None

    return None

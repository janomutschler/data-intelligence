import time
import requests
from typing import Optional, Dict, Any
from common.utils import utc_now_str
from config.settings import (
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    BASE_BACKOFF,
)

def create_session(password: str) -> requests.Session:
    """
    Create a configured requests session for the Lufthansa proxy.
    """
    session = requests.Session()

    session.headers.update(
        {
            "password": password,
            "Accept": "application/json",
        }
    )

    return session

def request_with_retry(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]],
    context: Dict[str, Any],
    log_func,
) -> Optional[requests.Response]:
    """
    Make a request with retry logic
    Logs the request and response details to the corresponding log function
    """

    backoff = BASE_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:

            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200:
                return response

            if response.status_code in RETRYABLE_STATUSAS:
                print(f"Request failed for Attempt {attempt}/{MAX_RETRIES}: {response.status_code} {response.text}")
                if attempt < MAX_RETRIES:
                    print(f"Sleeping for {backoff} seconds...")
                    time.sleep(backoff)
                    backoff *= 2
                
                log_func(
                    {
                        "timestamp_utc": utc_now_str(),
                        "status": "retry" if attempt < MAX_RETRIES else "failed",
                        "http_status": response.status_code,
                        "attempt": attempt,
                        "url": url,
                        "params": params,
                        "response_text": response.text[:500],
                        **context,
                    }
                )
                continue

            else:
                print (f"Request failed: {response.status_code} {response.text}")
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
                return response

        except requests.exceptions.RequestException as e:
            print(f"Request exception on attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                print(f"Sleeping for {backoff} seconds...")
                time.sleep(backoff)
                backoff *= 2
            log_func(
                {
                    "timestamp_utc": utc_now_str(),
                    "status": "retry_exception" if attempt < MAX_RETRIES else "failed",
                    "attempt": attempt,
                    "url": url,
                    "params": params,
                    "exception": str(e),
                    **context,
                }
            )
    return None



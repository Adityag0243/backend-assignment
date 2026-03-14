"""
retry_utils.py
──────────────
WHAT THIS FILE IS:
  A standalone utility module. Nothing here knows about FastAPI, SQLAlchemy,
  or the ETL pipeline. It only wraps functions that make HTTP calls.

WHAT COMES IN:
  - Any callable (function) that may raise an exception or return a
    requests.Response with a bad status code.

WHAT GOES OUT:
  → retry_with_backoff  decorator  →  imported by etl_pipeline.py
                                      applied to extract_from_api()

RETRY STRATEGY:
  Attempt 1  → fails → wait 1s
  Attempt 2  → fails → wait 2s
  Attempt 3  → fails → wait 4s  (2 ** attempt)
  Attempt 4  → gives up, re-raises the last exception

SPECIAL CASE — HTTP 429 Too Many Requests:
  CoinGecko rate-limits free-tier callers.
  When the API returns 429, we sleep 10s (not the normal backoff)
  before retrying — giving the rate-limit window time to reset.

USAGE:
  from app.retry_utils import retry_with_backoff

  @retry_with_backoff(max_attempts=3)
  def my_api_call():
      response = requests.get("https://...")
      response.raise_for_status()
      return response.json()
"""

import logging
import time
import functools
from typing import Callable, TypeVar, Any

import requests

logger = logging.getLogger(__name__)

# Type variable so the decorator preserves the wrapped function's return type
F = TypeVar("F", bound=Callable[..., Any])


def retry_with_backoff(max_attempts: int = 3, base_delay: float = 1.0) -> Callable:
    """
    Decorator factory — returns a decorator that retries the wrapped function.

    Args:
        max_attempts:  Total number of tries before giving up. Default 3.
        base_delay:    Seconds to wait after attempt 1. Doubles each retry.
                       Attempt 1 fail → sleep base_delay    (1s)
                       Attempt 2 fail → sleep base_delay*2  (2s)
                       Attempt 3 fail → sleep base_delay*4  (4s)

    Special behaviour:
        - If the wrapped function raises a requests.HTTPError with status 429,
          sleep RATE_LIMIT_DELAY seconds instead of the normal backoff delay,
          then retry — regardless of which attempt number we're on.
        - Any other exception follows the normal exponential backoff schedule.

    Raises:
        The last exception from the final attempt, so the caller knows what
        went wrong rather than receiving a silent None.
    """

    # How long to pause when the API says "slow down" (HTTP 429)
    RATE_LIMIT_DELAY = 10.0

    def decorator(func: F) -> F:
        @functools.wraps(func)  # keeps func.__name__, __doc__, etc. intact
        def wrapper(*args, **kwargs):
            last_exception: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    # ── Happy path ─────────────────────────────────────────
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(
                            f"{func.__name__} succeeded on attempt {attempt}"
                        )
                    return result

                except requests.HTTPError as exc:
                    last_exception = exc
                    status = exc.response.status_code if exc.response is not None else 0

                    # ── 429 Too Many Requests ──────────────────────────────
                    # Don't count against our backoff schedule.
                    # Sleep a fixed window then retry immediately.
                    if status == 429:
                        logger.warning(
                            f"{func.__name__} hit rate limit (429) on attempt "
                            f"{attempt}. Sleeping {RATE_LIMIT_DELAY}s before retry."
                        )
                        time.sleep(RATE_LIMIT_DELAY)
                        continue  # retry without incrementing the backoff

                    # ── Other HTTP errors (4xx, 5xx) ───────────────────────
                    delay = base_delay * (2 ** (attempt - 1))  # 1s, 2s, 4s
                    logger.warning(
                        f"{func.__name__} failed on attempt {attempt}/{max_attempts} "
                        f"with HTTP {status}. "
                        f"{'Retrying in ' + str(delay) + 's.' if attempt < max_attempts else 'Giving up.'}"
                    )

                except Exception as exc:
                    # ── Non-HTTP errors (network timeout, DNS, etc.) ───────
                    last_exception = exc
                    delay = base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        f"{func.__name__} raised {type(exc).__name__} on attempt "
                        f"{attempt}/{max_attempts}: {exc}. "
                        f"{'Retrying in ' + str(delay) + 's.' if attempt < max_attempts else 'Giving up.'}"
                    )

                # ── Wait before next attempt (skip sleep on final attempt) ─
                if attempt < max_attempts:
                    time.sleep(delay)

            # All attempts exhausted — re-raise so the caller can log + handle
            logger.error(
                f"{func.__name__} failed after {max_attempts} attempts. "
                f"Last error: {last_exception}"
            )
            raise last_exception

        return wrapper  # type: ignore[return-value]

    return decorator
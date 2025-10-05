import asyncio
import logging
from typing import Any, Sequence, Literal

import aiohttp
import requests

logger = logging.getLogger("ETL-Hyperliquid-API")
logger.setLevel(logging.INFO)

MAINNET_URL = "https://stats-data.hyperliquid.xyz/Mainnet/vaults"
API_URL = "https://api.hyperliquid.xyz/info"
HEADERS = {"Content-Type": "application/json"}

MAX_CONCURRENCY = 2
MAX_RETRIES = 3
INITIAL_BACKOFF = 0.5
READ_TIMEOUT = 15
TOTAL_TIMEOUT = 20
RETRYABLE_STATUS = {429, 500, 502, 503, 504}



def get_vault_addresses() -> list[str]:
    """Return all vault addresses published on the mainnet endpoint."""
    response = requests.get(MAINNET_URL, timeout=TOTAL_TIMEOUT)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list):
        logger.warning("Unexpected payload type when fetching vault addresses: %s", type(payload))
        return []

    addresses: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        summary = item.get("summary")
        if isinstance(summary, dict):
            address = summary.get("vaultAddress")
            if isinstance(address, str):
                addresses.append(address)
    return addresses


async def _fetch_with_retry(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    body_field: Literal["user", "vaultAddress"],
    address: str,
) -> dict[str, Any]:
    backoff = INITIAL_BACKOFF

    match body_field:
        case "user":
            payload = {"type": "userFills", "user": address}
        case "vaultAddress":
            payload = {"type": "vaultDetails", "vaultAddress": address}


    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.post(API_URL, json=payload) as response:
                    logger.info(f"{body_field} - Parse response: {response.status}")
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, dict):
                            data.setdefault(body_field, address)
                        return data

                    body = await response.text()
        except asyncio.TimeoutError:
            error = {"error": "timeout", body_field: address}
        except aiohttp.ClientError as exc:
            error = {
                "error": "client_error",
                "details": str(exc),
                body_field: address,
            }
        else:
            if response.status in RETRYABLE_STATUS and attempt < MAX_RETRIES:
                logger.warning(
                    "Retrying detail fetch for %s after %.1fs due to HTTP %s",
                    address,
                    backoff,
                    response.status,
                )
                await asyncio.sleep(backoff)
                backoff *= 2
                continue

            return {
                "error": f"HTTP {response.status}",
                "details": body,
                body_field: address,
            }

        if attempt < MAX_RETRIES:
            logger.warning(
                "Retrying detail fetch for %s after %.1fs due to %s",
                address,
                backoff,
                error["error"],
            )
            await asyncio.sleep(backoff)
            backoff *= 2
            continue

        return error

    return {"error": "unknown", body_field: address}


async def fetch_details_async(
    body_field: Literal["user", "vaultAddress"],
    addresses: Sequence[str],
    concurrency: int = MAX_CONCURRENCY,
) -> list[dict[str, Any]]:
    """Fetch vaultDetails or userFills for the supplied vault/users addresses with bounded concurrency."""
    if not addresses:
        return []

    timeout = aiohttp.ClientTimeout(sock_read=READ_TIMEOUT, total=TOTAL_TIMEOUT)
    semaphore = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        tasks = [
            asyncio.create_task(_fetch_with_retry(session, semaphore, body_field, address))
            for address in addresses
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        output: list[dict[str, Any]] = []
        for address, result in zip(addresses, results):
            if isinstance(result, Exception):
                logger.exception("Unhandled exception when fetching details for %s", address)
                output.append(
                    {
                        "error": "unhandled_exception",
                        "details": str(result),
                        body_field: address,
                    }
                )
            else:
                output.append(result)
        return output


def fetch_details(
    body_field: Literal["user", "vaultAddress"],
    addresses: Sequence[str],
    concurrency: int = MAX_CONCURRENCY,
) -> list[dict[str, Any]]:
    """
    Synchronous helper that runs the async fetcher.
    Applied both for vaults and users addresses.
    """
    try:
        return asyncio.run(fetch_details_async(body_field, addresses, concurrency))
    except RuntimeError as exc:
        if "asyncio.run() cannot be called" in str(exc):
            raise RuntimeError(
                "fetch_details() cannot run inside an active event loop; use fetch_details_async instead."
            ) from exc
        raise
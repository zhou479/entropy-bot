"""Official Hyperliquid HTTP info client. Be polite: cache + small pauses."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from entropy_bot.coins import ALLOWED_DEX, assert_no_foreign_venue, assert_tradable
from entropy_bot.errors import (ExchangeOutcomeUnknown, RateLimited, RequestWeightLimited,
                                error_text, is_weight_limit_error)

log = logging.getLogger("entropy_bot.rest")

DEFAULT_TIMEOUT = 15.0
INFO_PAUSE_S = 0.15


class InfoClient:
    def __init__(self, base_url: str, *, timeout: float = DEFAULT_TIMEOUT) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self._perp_dexs: list[Any] | None = None
        self._meta: dict[str, Any] | None = None
        self._meta_and_ctxs: Any | None = None
        self._info_backoff_until = 0.0

    def close(self) -> None:
        self.session.close()

    def post_info(
        self,
        payload: dict[str, Any],
        *,
        cacheable: bool = False,
        optional: bool = False,
    ) -> Any:
        assert_no_foreign_venue(payload)
        if time.monotonic() < self._info_backoff_until:
            if optional:
                return None
            raise RateLimited("info local cooldown after 429")
        url = f"{self.base_url}/info"
        try:
            response = self.session.post(url, json=payload, timeout=self.timeout)
        except requests.RequestException as exc:
            if optional:
                log.warning("info request failed: %s", exc)
                return None
            raise
        if response.status_code == 429:
            self._info_backoff_until = time.monotonic() + 5.0
            if optional:
                log.warning("info 429 for %s", payload.get("type"))
                return None
            raise RateLimited(f"info 429 for {payload.get('type')}")
        if optional and not response.ok:
            log.warning("info HTTP %s for %s", response.status_code, payload.get("type"))
            return None
        if not optional:
            response.raise_for_status()
        if not response.content or response.content.strip() in {b"", b"null"}:
            return None
        data = response.json()
        if not cacheable:
            time.sleep(INFO_PAUSE_S)
        return data

    def perp_dexs(self, *, refresh: bool = False) -> list[Any]:
        if self._perp_dexs is None or refresh:
            self._perp_dexs = self.post_info({"type": "perpDexs"}, cacheable=True)
        return self._perp_dexs

    def meta(self, dex: str = ALLOWED_DEX, *, refresh: bool = False) -> dict[str, Any]:
        if dex != ALLOWED_DEX:
            raise ValueError(f"this client only queries dex={ALLOWED_DEX!r}")
        if self._meta is None or refresh:
            self._meta = self.post_info({"type": "meta", "dex": dex}, cacheable=True)
        return self._meta

    def meta_and_asset_ctxs(self, dex: str = ALLOWED_DEX, *, refresh: bool = False) -> Any:
        if dex != ALLOWED_DEX:
            raise ValueError(f"this client only queries dex={ALLOWED_DEX!r}")
        if self._meta_and_ctxs is None or refresh:
            self._meta_and_ctxs = self.post_info(
                {"type": "metaAndAssetCtxs", "dex": dex}, cacheable=True
            )
            if isinstance(self._meta_and_ctxs, list) and self._meta_and_ctxs:
                self._meta = self._meta_and_ctxs[0]
        return self._meta_and_ctxs

    def l2_book(self, coin: str) -> dict[str, Any]:
        coin = assert_tradable(coin)
        return self.post_info({"type": "l2Book", "coin": coin})

    def clearinghouse_state(
        self,
        user: str,
        dex: str = ALLOWED_DEX,
        *,
        optional: bool = False,
    ) -> dict[str, Any] | None:
        if dex != ALLOWED_DEX:
            raise ValueError(f"this client only queries dex={ALLOWED_DEX!r}")
        return self.post_info(
            {"type": "clearinghouseState", "user": user, "dex": dex},
            optional=optional,
        )

    def open_orders(
        self,
        user: str,
        dex: str = ALLOWED_DEX,
        *,
        optional: bool = False,
    ) -> list[dict[str, Any]] | None:
        """frontendOpenOrders with dex:io. null/429 returns None — callers must keep cache."""
        if dex != ALLOWED_DEX:
            raise ValueError(f"this client only queries dex={ALLOWED_DEX!r}")
        data = self.post_info(
            {"type": "frontendOpenOrders", "user": user, "dex": dex},
            optional=optional,
        )
        if data is None:
            return None
        if not isinstance(data, list):
            if optional:
                log.warning("frontendOpenOrders returned non-list; treating as null")
                return None
            raise ValueError("frontendOpenOrders expected a list")
        return data

    def user_fills(self, user: str, *, optional: bool = True) -> list[dict[str, Any]] | None:
        """Recent fills for the master account. Observe-only; 429/null → None."""
        data = self.post_info({"type": "userFills", "user": user}, optional=optional)
        if data is None:
            return None
        if not isinstance(data, list):
            if optional:
                log.warning("userFills returned non-list; treating as null")
                return None
            raise ValueError("userFills expected a list")
        return [row for row in data if isinstance(row, dict)]

    def extra_agents(self, user: str, *, optional: bool = True) -> list[Any] | None:
        data = self.post_info({"type": "extraAgents", "user": user}, optional=optional, cacheable=True)
        if data is None:
            return None
        return data if isinstance(data, list) else None

    def user_rate_limit(self, user: str) -> Any:
        return self.post_info({"type": "userRateLimit", "user": user}, optional=True)

    def post_exchange(self, payload: dict[str, Any]) -> Any:
        assert_no_foreign_venue(payload)
        url = f"{self.base_url}/exchange"
        try:
            response = self.session.post(url, json=payload, timeout=self.timeout)
        except requests.RequestException as exc:
            raise ExchangeOutcomeUnknown("exchange transport failed; reconcile before retry") from exc
        if response.status_code == 429:
            text = response.text or "exchange 429"
            if is_weight_limit_error(text):
                raise RequestWeightLimited(text)
            raise RateLimited("exchange 429")
        if not response.ok and is_weight_limit_error(response.text):
            raise RequestWeightLimited(response.text)
        if not response.ok:
            raise ExchangeOutcomeUnknown(f"exchange HTTP {response.status_code}; outcome unknown")
        if not response.content or response.content.strip() in {b"", b"null"}:
            raise ExchangeOutcomeUnknown("exchange empty response; outcome unknown")
        try:
            data = response.json()
        except ValueError as exc:
            raise ExchangeOutcomeUnknown("exchange invalid JSON; outcome unknown") from exc
        if not isinstance(data, dict) or data.get("status") not in ("ok", "err"):
            raise ExchangeOutcomeUnknown("exchange unexpected response schema")
        if data.get("status") == "err" and is_weight_limit_error(data):
            raise RequestWeightLimited(error_text(data))
        return data

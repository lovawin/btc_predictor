from __future__ import annotations

import os
import math
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

from .config import settings


@dataclass
class OrderResult:
    ok: bool
    mode: str
    side: str
    amount_usd: float
    market_id: str
    message: str


class PolymarketClient:
    def __init__(self, live_trading: bool) -> None:
        self.live_trading = live_trading

    @staticmethod
    @contextmanager
    def _proxy_env(proxy_url: str):
        if not proxy_url:
            yield
            return

        keys = ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]
        previous = {key: os.environ.get(key) for key in keys}

        # Also patch the httpx singleton in py_clob_client so it routes through proxy
        try:
            from py_clob_client.http_helpers import helpers as _clob_helpers
            _old_client = _clob_helpers._http_client
        except Exception:
            _clob_helpers = None  # type: ignore[assignment]
            _old_client = None

        try:
            for key in keys:
                os.environ[key] = proxy_url

            if _clob_helpers is not None:
                import httpx as _httpx
                # Convert socks5h to socks5 for httpx (httpx doesn't support socks5h)
                httpx_proxy = proxy_url.replace("socks5h://", "socks5://")
                _clob_helpers._http_client = _httpx.Client(
                    http2=True, proxy=httpx_proxy,
                    timeout=_httpx.Timeout(15.0, connect=10.0),
                )

            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            if _clob_helpers is not None and _old_client is not None:
                _clob_helpers._http_client = _old_client

    @staticmethod
    def _friendly_error(exc: Exception) -> str:
        raw = str(exc or "").strip()
        lower = raw.lower()
        if "trading restricted in your region" in lower or "geoblock" in lower:
            return "Trading restricted in your region (geoblocked). Your current exit IP is blocked by Polymarket; switch to a different MX exit IP/proxy endpoint and retry."
        if "not enough balance / allowance" in lower:
            return "Insufficient collateral balance/allowance for this live order."
        key_markers = ["private key", "invalid key", "32 bytes", "bytes long", "non-hexadecimal", "fromhex"]
        if any(marker in lower for marker in key_markers):
            return "Invalid EOA private key format. Use your regular wallet private key with /setkey <private_key>."
        return raw or "Unknown error"

    @staticmethod
    def _extract_numeric_value(payload: object, keys: tuple[str, ...]) -> float | None:
        if isinstance(payload, dict):
            for key in keys:
                if key in payload:
                    try:
                        return float(str(payload.get(key)))
                    except Exception:
                        continue
            for value in payload.values():
                nested = PolymarketClient._extract_numeric_value(value, keys)
                if nested is not None:
                    return nested
        if isinstance(payload, list):
            for value in payload:
                nested = PolymarketClient._extract_numeric_value(value, keys)
                if nested is not None:
                    return nested
        return None

    @staticmethod
    def _balance_allowance_diagnostic(
        client,
        required_usd: float,
        signature_type_value: int,
        funder_value: str,
    ) -> str:
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams  # type: ignore

            signature_type = signature_type_value if signature_type_value > 0 else -1
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=signature_type)
            try:
                client.update_balance_allowance(params)
            except Exception:
                pass
            info = client.get_balance_allowance(params)

            balance = PolymarketClient._extract_numeric_value(info, ("balance", "available", "availableBalance"))
            allowance = PolymarketClient._extract_numeric_value(info, ("allowance", "approved", "approvedAmount"))

            parts: list[str] = [f"required≈${required_usd:.2f}"]
            if balance is not None:
                parts.append(f"balance≈{balance:.6f}")
            if allowance is not None:
                parts.append(f"allowance≈{allowance:.6f}")
            if funder_value:
                parts.append(f"funder={funder_value}")
            parts.append(f"sig_type={signature_type_value}")
            return "; ".join(parts)
        except Exception:
            return "could not fetch balance/allowance diagnostic"

    @staticmethod
    def _egress_hint(proxy_url: str = "") -> str:
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        ip = "unknown"
        country = "unknown"
        try:
            ip_resp = requests.get("https://api.ipify.org", proxies=proxies, timeout=6)
            if ip_resp.ok and ip_resp.text.strip():
                ip = ip_resp.text.strip()
        except Exception:
            pass

        try:
            c_resp = requests.get("https://ipapi.co/country", proxies=proxies, timeout=6)
            if c_resp.ok and c_resp.text.strip():
                country = c_resp.text.strip()
        except Exception:
            pass

        return f"egress_ip={ip} country={country}"

    @staticmethod
    def _token_for_side(market: dict, side: str) -> Optional[str]:
        side_up = side.upper() == "YES"
        bullish_labels = {
            "yes",
            "up",
            "above",
            "higher",
            "increase",
            "true",
        }
        bearish_labels = {
            "no",
            "down",
            "below",
            "lower",
            "decrease",
            "false",
        }

        tokens = market.get("tokens", []) or []
        if not isinstance(tokens, list):
            tokens = []

        for token in tokens:
            outcome = str(token.get("outcome", "")).strip().lower()
            if side_up and outcome in bullish_labels:
                token_id = str(token.get("token_id", "")).strip()
                if token_id:
                    return token_id
            if (not side_up) and outcome in bearish_labels:
                token_id = str(token.get("token_id", "")).strip()
                if token_id:
                    return token_id

        if len(tokens) == 2:
            idx = 0 if side_up else 1
            token_id = str(tokens[idx].get("token_id", "")).strip()
            if token_id:
                return token_id

        return None

    @staticmethod
    def _best_ask_price(order_book) -> Optional[float]:
        asks = getattr(order_book, "asks", []) or []
        if not asks:
            return None
        prices: list[float] = []
        for ask in asks:
            try:
                prices.append(float(getattr(ask, "price", 0.0)))
            except Exception:
                continue
        if not prices:
            return None
        return min(prices)

    @staticmethod
    def _best_bid_price(order_book) -> Optional[float]:
        bids = getattr(order_book, "bids", []) or []
        if not bids:
            return None
        prices: list[float] = []
        for bid in bids:
            try:
                prices.append(float(getattr(bid, "price", 0.0)))
            except Exception:
                continue
        if not prices:
            return None
        return max(prices)

    @staticmethod
    def _market_has_live_orderbook(condition_id: str) -> bool:
        try:
            from py_clob_client.client import ClobClient  # type: ignore

            client = ClobClient(host=settings.polymarket_host, chain_id=settings.polymarket_chain_id)
            market = client.get_market(condition_id)
            if not isinstance(market, dict):
                return False
            for token in market.get("tokens", []) or []:
                token_id = str(token.get("token_id", "")).strip()
                if not token_id:
                    continue
                try:
                    order_book = client.get_order_book(token_id)
                    asks = getattr(order_book, "asks", []) or []
                    bids = getattr(order_book, "bids", []) or []
                    if asks or bids:
                        return True
                except Exception:
                    continue
            return False
        except Exception:
            return False

    def _discover_from_slug_template(self, slug_template: str, proxy_url: str = "") -> Optional[tuple[str, str]]:
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        now_ts = int(datetime.now(timezone.utc).timestamp())
        slot = (now_ts // 300) * 300

        seconds_left_in_slot = (slot + 300) - now_ts
        roll_early = seconds_left_in_slot <= settings.polymarket_roll_to_next_seconds
        if roll_early:
            candidates = [slot + 300, slot + 600, slot, slot + 900, slot - 300]
        else:
            candidates = [slot, slot + 300, slot + 600, slot + 900, slot - 300]
        for ts in candidates:
            slug = slug_template.replace("{ts}", str(ts))
            try:
                res = requests.get(
                    f"https://gamma-api.polymarket.com/markets?slug={slug}",
                    proxies=proxies,
                    timeout=12,
                )
                if not res.ok:
                    continue
                rows = res.json()
                if not isinstance(rows, list) or not rows:
                    continue
                row = rows[0]
                if row.get("closed") or row.get("archived"):
                    continue
                if not row.get("active", True):
                    continue

                market_id = str(row.get("conditionId") or row.get("condition_id") or row.get("id") or "").strip()
                if not market_id:
                    continue
                title = str(row.get("question") or row.get("title") or slug)
                if self._market_has_live_orderbook(market_id):
                    return market_id, title
            except Exception:
                continue
        return None

    def discover_current_btc_5m_market(self, proxy_url: str = "", slug_template: str = "") -> Optional[tuple[str, str]]:
        if slug_template:
            by_slug = self._discover_from_slug_template(slug_template, proxy_url)
            if by_slug:
                return by_slug

        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

        def parse_end(value: str) -> datetime:
            raw = (value or "").strip()
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(raw)
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                return datetime.max.replace(tzinfo=timezone.utc)

        def harvest(rows: list[dict]) -> list[tuple[str, str, datetime, bool]]:
            out: list[tuple[str, str, datetime, bool]] = []
            for market in rows:
                if market.get("closed") or market.get("archived"):
                    continue
                if not market.get("active", False):
                    continue
                if not market.get("accepting_orders", market.get("acceptingOrders", True)):
                    continue

                text = " ".join(
                    [
                        str(market.get("question", "")),
                        str(market.get("title", "")),
                        str(market.get("market_slug", "")),
                        str(market.get("slug", "")),
                        str(market.get("groupItemTitle", "")),
                    ]
                ).lower()
                if "btc" not in text and "bitcoin" not in text:
                    continue

                is_five_min = any(
                    tag in text for tag in ["5m", "5 min", "5-minute", "5 minute", "minute", "up or down"]
                )

                market_id = (
                    str(market.get("condition_id", ""))
                    or str(market.get("conditionId", ""))
                    or str(market.get("id", ""))
                ).strip()
                if not market_id:
                    continue

                end_dt = parse_end(
                    str(
                        market.get("end_date_iso")
                        or market.get("endDate")
                        or market.get("endDateIso")
                        or market.get("end_date")
                        or ""
                    )
                )
                title = str(market.get("question") or market.get("title") or market.get("market_slug") or market_id)
                out.append((market_id, title, end_dt, is_five_min))
            return out

        def harvest_gamma(rows: list[dict]) -> list[tuple[str, str, datetime, bool]]:
            normalized: list[dict] = []
            for row in rows:
                normalized.append(
                    {
                        "closed": row.get("closed", False),
                        "archived": row.get("archived", False),
                        "active": row.get("active", True),
                        "accepting_orders": row.get("acceptingOrders", row.get("accepting_orders", True)),
                        "question": row.get("question", ""),
                        "title": row.get("title", ""),
                        "market_slug": row.get("market_slug", row.get("slug", "")),
                        "slug": row.get("slug", ""),
                        "groupItemTitle": row.get("groupItemTitle", ""),
                        "condition_id": row.get("conditionId", row.get("condition_id", row.get("id", ""))),
                        "end_date_iso": row.get("endDateIso", row.get("end_date_iso", row.get("endDate", ""))),
                    }
                )
            return harvest(normalized)

        candidates: list[tuple[str, str, datetime, bool]] = []

        try:
            res = requests.get("https://clob.polymarket.com/sampling-markets", proxies=proxies, timeout=12)
            if res.ok:
                candidates.extend(harvest(res.json().get("data", [])))
        except Exception:
            pass

        gamma_urls = [
            "https://gamma-api.polymarket.com/markets?limit=1000&active=true&closed=false&archived=false&search=bitcoin",
            "https://gamma-api.polymarket.com/markets?limit=1000&active=true&closed=false&archived=false&search=btc",
        ]
        for url in gamma_urls:
            try:
                res = requests.get(url, proxies=proxies, timeout=12)
                if res.ok and isinstance(res.json(), list):
                    candidates.extend(harvest_gamma(res.json()))
            except Exception:
                continue

        if not candidates:
            cursor = ""
            for _ in range(40):
                try:
                    url = "https://clob.polymarket.com/markets"
                    if cursor:
                        url = f"{url}?next_cursor={cursor}"
                    res = requests.get(url, proxies=proxies, timeout=12)
                    if not res.ok:
                        break
                    payload = res.json()
                    rows = payload.get("data", [])
                    if not rows:
                        break
                    candidates.extend(harvest(rows))
                    cursor = payload.get("next_cursor")
                    if not cursor or cursor == "LTE=":
                        break
                except Exception:
                    break

        if not candidates:
            return None

        now = datetime.now(timezone.utc)
        live = [row for row in candidates if row[2] >= now]
        pool = live or candidates

        price_action = [
            row
            for row in pool
            if any(k in row[1].lower() for k in [" above ", " below ", " up ", " down ", " hit ", " reach ", "$", "price", "minute", "5m"])
        ]
        pool = price_action or pool

        short_horizon = [row for row in pool if row[2] >= now and row[2] <= now + timedelta(minutes=90)]
        near_horizon = [row for row in pool if row[2] >= now and row[2] <= now + timedelta(hours=6)]

        five_min_pool = [row for row in pool if row[3]]
        if five_min_pool:
            pool = five_min_pool
        elif short_horizon:
            pool = short_horizon
        elif near_horizon:
            pool = near_horizon
        else:
            return None

        for chosen in sorted(pool, key=lambda x: x[2]):
            if self._market_has_live_orderbook(chosen[0]):
                return chosen[0], chosen[1]

        return None

    def place_order(
        self,
        *,
        private_key: str,
        market_id: str,
        side: str,
        amount_usd: float,
        proxy_url: str = "",
        signature_type: int | None = None,
        funder: str = "",
    ) -> OrderResult:
        if not self.live_trading:
            via = " via AU proxy" if proxy_url else ""
            return OrderResult(
                ok=True,
                mode="paper",
                side=side,
                amount_usd=amount_usd,
                market_id=market_id,
                message=f"Paper order accepted{via} at {datetime.now(timezone.utc).isoformat()}",
            )

        if not private_key:
            return OrderResult(
                ok=False,
                mode="live",
                side=side,
                amount_usd=amount_usd,
                market_id=market_id,
                message="Missing private key",
            )

        try:
            from py_clob_client.client import ClobClient  # type: ignore
            from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType, PartialCreateOrderOptions  # type: ignore
            from py_clob_client.order_builder.constants import BUY  # type: ignore

            signature_type_value = settings.polymarket_signature_type if signature_type is None else int(signature_type)
            funder_value = (funder or settings.polymarket_funder).strip()
            signature_type_for_client = signature_type_value if signature_type_value > 0 else None
            funder_for_client = funder_value or None

            with self._proxy_env(proxy_url):
                client = ClobClient(
                    host=settings.polymarket_host,
                    chain_id=settings.polymarket_chain_id,
                    key=private_key,
                    signature_type=signature_type_for_client,
                    funder=funder_for_client,
                )

                creds = client.create_or_derive_api_creds()
                client.set_api_creds(creds)

                market = client.get_market(market_id)
            if not isinstance(market, dict):
                return OrderResult(
                    ok=False,
                    mode="live",
                    side=side,
                    amount_usd=amount_usd,
                    market_id=market_id,
                    message="Live execution unavailable: invalid market payload",
                )

            token_id = self._token_for_side(market, side)
            if not token_id:
                return OrderResult(
                    ok=False,
                    mode="live",
                    side=side,
                    amount_usd=amount_usd,
                    market_id=market_id,
                    message=f"Live execution unavailable: missing {side} token id",
                )

            try:
                with self._proxy_env(proxy_url):
                    order_book = client.get_order_book(token_id)
            except Exception:
                return OrderResult(
                    ok=False,
                    mode="live",
                    side=side,
                    amount_usd=amount_usd,
                    market_id=market_id,
                    message="Live execution unavailable: selected outcome has no active CLOB orderbook yet. Bot will retry next market slot.",
                )
            tick_size = float(getattr(order_book, "tick_size", "0.001") or 0.001)
            best_ask = self._best_ask_price(order_book)
            best_bid = self._best_bid_price(order_book)

            order_type = getattr(OrderType, "FAK")
            liquidity_note = ""
            if best_ask is not None:
                aggressive_price = min(0.99, best_ask + tick_size)
            elif best_bid is not None:
                aggressive_price = min(0.99, best_bid + tick_size)
                order_type = getattr(OrderType, "GTC", getattr(OrderType, "FAK"))
                liquidity_note = " (no asks; posted resting bid)"
            else:
                return OrderResult(
                    ok=False,
                    mode="live",
                    side=side,
                    amount_usd=amount_usd,
                    market_id=market_id,
                    message="Live execution unavailable: no ask/bid liquidity on selected outcome",
                )

            if aggressive_price <= 0:
                return OrderResult(
                    ok=False,
                    mode="live",
                    side=side,
                    amount_usd=amount_usd,
                    market_id=market_id,
                    message="Live execution unavailable: invalid price",
                )

            spend_usd = round(max(0.01, amount_usd), 2)

            if tick_size >= 0.1:
                order_tick = "0.1"
                price_decimals = 1
            else:
                order_tick = "0.01"
                price_decimals = 2

            limit_price = round(max(0.01, aggressive_price), price_decimals)

            est_shares = round(spend_usd / max(limit_price, 1e-9), 4)
            est_spend = spend_usd

            with self._proxy_env(proxy_url):
                try:
                    order = client.create_market_order(
                        MarketOrderArgs(
                            token_id=token_id,
                            amount=spend_usd,
                            side=BUY,
                            price=limit_price,
                            order_type=order_type,
                        ),
                        PartialCreateOrderOptions(
                            tick_size=order_tick,
                        )
                    )
                    posted = client.post_order(order, orderType=order_type)
                except Exception as order_exc:
                    if "invalid amounts" not in str(order_exc).lower():
                        raise

                    fallback_size = math.floor((spend_usd / max(limit_price, 1e-9)) * 100) / 100

                    order = client.create_order(
                        OrderArgs(
                            token_id=token_id,
                            size=fallback_size,
                            side=BUY,
                            price=limit_price,
                        )
                    )
                    posted = client.post_order(order, orderType=order_type)
                    est_shares = round(fallback_size, 2)
                    est_spend = round(fallback_size * limit_price, 2)

            via = " via AU proxy" if proxy_url else ""
            order_id = ""
            if isinstance(posted, dict):
                order_id = str(posted.get("orderID") or posted.get("id") or "")
            order_ref = f" order_id={order_id}" if order_id else ""
            return OrderResult(
                ok=True,
                mode="live",
                side=side,
                amount_usd=amount_usd,
                market_id=market_id,
                message=(
                    f"Live order posted{via}: {side} token={token_id[:10]}... "
                    f"limit={limit_price:.{price_decimals}f} spend=${est_spend:.2f} est_size={est_shares:.4f}{order_ref}{liquidity_note}"
                ),
            )
        except Exception as exc:
            friendly = self._friendly_error(exc)
            if "insufficient collateral balance/allowance" in friendly.lower():
                try:
                    diag = self._balance_allowance_diagnostic(
                        client,
                        spend_usd if 'spend_usd' in locals() else amount_usd,
                        signature_type_value,
                        funder_value,
                    )
                except Exception:
                    diag = "could not fetch balance/allowance diagnostic"
                friendly = (
                    f"{friendly} Add USDC on Polygon and approve CLOB allowance for your trading wallet. "
                    f"If using proxy/smart wallet mode, set POLYMARKET_FUNDER to the funded address. ({diag})"
                )
            if "geoblocked" in friendly.lower() or "trading restricted" in friendly.lower():
                friendly = f"{friendly} ({self._egress_hint(proxy_url)})"
            return OrderResult(
                ok=False,
                mode="live",
                side=side,
                amount_usd=amount_usd,
                market_id=market_id,
                message=f"Live execution unavailable: {friendly}",
            )

    def dry_run_order(
        self,
        *,
        private_key: str,
        market_id: str,
        side: str,
        amount_usd: float,
        proxy_url: str = "",
        signature_type: int | None = None,
        funder: str = "",
    ) -> OrderResult:
        if not private_key:
            return OrderResult(
                ok=False,
                mode="live",
                side=side,
                amount_usd=amount_usd,
                market_id=market_id,
                message="Missing private key",
            )

        try:
            from py_clob_client.client import ClobClient  # type: ignore

            signature_type_value = settings.polymarket_signature_type if signature_type is None else int(signature_type)
            funder_value = (funder or settings.polymarket_funder).strip()
            signature_type_for_client = signature_type_value if signature_type_value > 0 else None
            funder_for_client = funder_value or None
            with self._proxy_env(proxy_url):
                client = ClobClient(
                    host=settings.polymarket_host,
                    chain_id=settings.polymarket_chain_id,
                    key=private_key,
                    signature_type=signature_type_for_client,
                    funder=funder_for_client,
                )

                creds = client.create_or_derive_api_creds()
                client.set_api_creds(creds)

                market = client.get_market(market_id)
            if not isinstance(market, dict):
                return OrderResult(
                    ok=False,
                    mode="live",
                    side=side,
                    amount_usd=amount_usd,
                    market_id=market_id,
                    message="Dry run failed: invalid market payload",
                )

            token_id = self._token_for_side(market, side)
            if not token_id:
                return OrderResult(
                    ok=False,
                    mode="live",
                    side=side,
                    amount_usd=amount_usd,
                    market_id=market_id,
                    message=f"Dry run failed: missing {side} token id",
                )

            try:
                with self._proxy_env(proxy_url):
                    order_book = client.get_order_book(token_id)
            except Exception:
                return OrderResult(
                    ok=False,
                    mode="live",
                    side=side,
                    amount_usd=amount_usd,
                    market_id=market_id,
                    message="Dry run failed: selected outcome has no active CLOB orderbook yet",
                )
            tick_size = float(getattr(order_book, "tick_size", "0.001") or 0.001)
            best_ask = self._best_ask_price(order_book)
            best_bid = self._best_bid_price(order_book)
            if best_ask is not None:
                aggressive_price = min(0.99, best_ask + tick_size)
                best_ask_text = f"{best_ask:.3f}"
                liquidity_note = ""
            elif best_bid is not None:
                aggressive_price = min(0.99, best_bid + tick_size)
                best_ask_text = "n/a"
                liquidity_note = " (no asks; would post resting bid)"
            else:
                return OrderResult(
                    ok=False,
                    mode="live",
                    side=side,
                    amount_usd=amount_usd,
                    market_id=market_id,
                    message="Dry run failed: no ask/bid liquidity",
                )

            shares = round(max(1.0, amount_usd / max(aggressive_price, 1e-9)), 3)

            via = " via AU proxy" if proxy_url else ""
            return OrderResult(
                ok=True,
                mode="live",
                side=side,
                amount_usd=amount_usd,
                market_id=market_id,
                message=(
                    f"Dry run OK{via}: {side} token={token_id[:10]}... "
                    f"best_ask={best_ask_text} limit={aggressive_price:.3f} size={shares:.3f}{liquidity_note}"
                ),
            )
        except Exception as exc:
            friendly = self._friendly_error(exc)
            if "geoblocked" in friendly.lower() or "trading restricted" in friendly.lower():
                friendly = f"{friendly} ({self._egress_hint(proxy_url)})"
            return OrderResult(
                ok=False,
                mode="live",
                side=side,
                amount_usd=amount_usd,
                market_id=market_id,
                message=f"Dry run failed: {friendly}",
            )

    def wallet_diagnostics(
        self,
        *,
        private_key: str,
        proxy_url: str = "",
        signature_type: int | None = None,
        funder: str = "",
    ) -> str:
        if not private_key:
            return "Wallet check failed: missing private key"

        try:
            from eth_account import Account  # type: ignore
            from py_clob_client.client import ClobClient  # type: ignore
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams  # type: ignore

            signature_type_value = settings.polymarket_signature_type if signature_type is None else int(signature_type)
            signature_type_for_client = signature_type_value if signature_type_value > 0 else None
            sig_for_query = signature_type_value if signature_type_value > 0 else -1
            funder_value = (funder or settings.polymarket_funder).strip()
            funder_for_client = funder_value or None
            signer_address = Account.from_key(private_key).address

            with self._proxy_env(proxy_url):
                client = ClobClient(
                    host=settings.polymarket_host,
                    chain_id=settings.polymarket_chain_id,
                    key=private_key,
                    signature_type=signature_type_for_client,
                    funder=funder_for_client,
                )

                creds = client.create_or_derive_api_creds()
                client.set_api_creds(creds)

                params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=sig_for_query)
                try:
                    client.update_balance_allowance(params)
                except Exception:
                    pass
                info = client.get_balance_allowance(params)

            balance = self._extract_numeric_value(info, ("balance", "available", "availableBalance"))
            allowance = self._extract_numeric_value(info, ("allowance", "approved", "approvedAmount"))

            return "\n".join(
                [
                    "Wallet check:",
                    f"Signer: {signer_address}",
                    f"Signature type: {signature_type_value}",
                    f"Funder: {funder_value or signer_address}",
                    f"Collateral balance: {balance:.6f}" if balance is not None else "Collateral balance: unknown",
                    f"Collateral allowance: {allowance:.6f}" if allowance is not None else "Collateral allowance: unknown",
                ]
            )
        except Exception as exc:
            return f"Wallet check failed: {self._friendly_error(exc)}"

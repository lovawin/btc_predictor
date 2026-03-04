from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import Optional
import time

import requests
from web3 import Web3

from .config import settings


@dataclass
class MarketSnapshot:
    prices_5m: list[float]
    binance_spot: float
    coingecko_spot: float
    chainlink_spot: Optional[float]

    @property
    def fused_spot(self) -> float:
        values = [self.binance_spot, self.coingecko_spot]
        if self.chainlink_spot and self.chainlink_spot > 0:
            values.append(self.chainlink_spot)
        return mean(values)


class MarketDataClient:
    def __init__(self, timeout: int = 10) -> None:
        self.timeout = timeout
        self._cg_spot_cache: Optional[float] = None
        self._cg_spot_backoff_until: float = 0.0

    def fetch_binance_5m_prices(self, limit: int = 60) -> list[float]:
        for base in ("https://api.binance.com", "https://api.binance.us"):
            url = f"{base}/api/v3/klines"
            params = {"symbol": settings.binance_symbol, "interval": "5m", "limit": limit}
            try:
                response = requests.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                rows = response.json()
                return [float(row[4]) for row in rows]
            except Exception:
                continue
        raise RuntimeError("Binance klines unavailable (both .com and .us)")

    def _cg_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if settings.coingecko_api_key:
            headers["x-cg-demo-api-key"] = settings.coingecko_api_key
        return headers

    def fetch_coingecko_recent_prices(self, limit: int = 60) -> list[float]:
        url = f"https://api.coingecko.com/api/v3/coins/{settings.coingecko_coin_id}/market_chart"
        params = {"vs_currency": "usd", "days": "1", "interval": "minute"}
        response = requests.get(url, params=params, headers=self._cg_headers(), timeout=self.timeout)
        response.raise_for_status()
        prices = response.json().get("prices", [])
        values = [float(row[1]) for row in prices if isinstance(row, list) and len(row) >= 2]
        if len(values) < limit:
            raise RuntimeError("Insufficient CoinGecko history for fallback")
        return values[-limit:]

    def fetch_binance_spot(self) -> float:
        for base in ("https://api.binance.com", "https://api.binance.us"):
            url = f"{base}/api/v3/ticker/price"
            params = {"symbol": settings.binance_symbol}
            try:
                response = requests.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return float(response.json()["price"])
            except Exception:
                continue
        raise RuntimeError("Binance spot unavailable (both .com and .us)")

    def fetch_coingecko_spot(self) -> float:
        now = time.time()
        if self._cg_spot_cache is not None and now < self._cg_spot_backoff_until:
            return self._cg_spot_cache

        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {"ids": settings.coingecko_coin_id, "vs_currencies": "usd"}
        try:
            response = requests.get(url, params=params, headers=self._cg_headers(), timeout=self.timeout)
            response.raise_for_status()
            value = float(response.json()[settings.coingecko_coin_id]["usd"])
            self._cg_spot_cache = value
            self._cg_spot_backoff_until = 0.0
            return value
        except requests.HTTPError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code == 429:
                retry_after_raw = ""
                if getattr(exc, "response", None) is not None:
                    retry_after_raw = str(exc.response.headers.get("Retry-After", "") or "")
                try:
                    retry_after = max(30, int(retry_after_raw))
                except ValueError:
                    retry_after = 120
                self._cg_spot_backoff_until = time.time() + retry_after
                if self._cg_spot_cache is not None:
                    return self._cg_spot_cache
            raise
        except Exception:
            if self._cg_spot_cache is not None:
                return self._cg_spot_cache
            raise

    def fetch_chainlink_spot(self) -> Optional[float]:
        if not settings.chainlink_rpc_url:
            return None

        abi = [
            {
                "inputs": [],
                "name": "latestRoundData",
                "outputs": [
                    {"internalType": "uint80", "name": "roundId", "type": "uint80"},
                    {"internalType": "int256", "name": "answer", "type": "int256"},
                    {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
                    {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
                    {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
                ],
                "stateMutability": "view",
                "type": "function",
            },
            {
                "inputs": [],
                "name": "decimals",
                "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
                "stateMutability": "view",
                "type": "function",
            },
        ]

        w3 = Web3(Web3.HTTPProvider(settings.chainlink_rpc_url, request_kwargs={"timeout": self.timeout}))
        contract = w3.eth.contract(address=Web3.to_checksum_address(settings.chainlink_btc_usd_feed), abi=abi)
        decimals = contract.functions.decimals().call()
        round_data = contract.functions.latestRoundData().call()
        answer = round_data[1]
        if answer <= 0:
            return None
        return float(answer) / (10 ** decimals)

    def snapshot(self) -> MarketSnapshot:
        prices_5m = None
        for fetcher in (self.fetch_binance_5m_prices, self.fetch_coingecko_recent_prices):
            try:
                prices_5m = fetcher(limit=60)
                break
            except Exception:
                continue
        if prices_5m is None:
            raise RuntimeError("All price sources failed for 5m history")

        try:
            binance_spot = self.fetch_binance_spot()
        except Exception:
            binance_spot = prices_5m[-1]

        try:
            coingecko_spot = self.fetch_coingecko_spot()
        except Exception:
            coingecko_spot = binance_spot

        try:
            chainlink_spot = self.fetch_chainlink_spot()
        except Exception:
            chainlink_spot = None

        return MarketSnapshot(
            prices_5m=prices_5m,
            binance_spot=binance_spot,
            coingecko_spot=coingecko_spot,
            chainlink_spot=chainlink_spot,
        )

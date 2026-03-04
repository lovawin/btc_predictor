"""
fee_manager.py — Per-trade protocol fee collection.

Flow:
  1. Before each order is placed, FeeManager.collect() is called with the
     trade amount and the user's private key.
  2. If the user's signer address is in FEE_EXEMPT_ADDRESSES, no fee is taken.
  3. Otherwise, FEE_PCT of the trade amount is transferred in USDC (Polygon)
     to FEE_RECIPIENT_ADDRESS before the order goes through.
  4. If the fee transfer fails (e.g. low gas, no USDC), it returns a soft
     warning — the trade is still blocked to avoid free-riding.  Set
     FEE_SOFT_FAIL=true in env to let trades through even on fee failure
     (not recommended for production).

Configuration (env or direct):
  FEE_RECIPIENT      = 0x274b21BeE479afce248BdeADC42e633fF8020e68
  FEE_PCT            = 0.03          # 3%
  FEE_SOFT_FAIL      = false
  FEE_EXEMPT         = 0xABC...,0xDEF...  (comma-separated, case-insensitive)
  POLYGON_RPC_URL    = https://polygon-rpc.com  (fallback public RPC)
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# ── USDC on Polygon (native USDC, 6 decimals) ─────────────────────────────────
USDC_POLYGON = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
USDC_ABI = [
    {
        "name": "transfer",
        "type": "function",
        "inputs": [
            {"name": "recipient", "type": "address"},
            {"name": "amount",    "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
    },
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
]

# ── Config ─────────────────────────────────────────────────────────────────────
FEE_RECIPIENT: str = os.getenv(
    "FEE_RECIPIENT", "0x274b21BeE479afce248BdeADC42e633fF8020e68"
)
FEE_PCT: float = float(os.getenv("FEE_PCT", "0.03"))          # 3%
FEE_SOFT_FAIL: bool = os.getenv("FEE_SOFT_FAIL", "false").lower() == "true"
POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")

# Pre-seeded exempt addresses (owner wallet).  Users can also be added via bot command.
_BUILTIN_EXEMPT: set[str] = {
    "0xc0568b5b8274a7db58b1c794d9b6a493af8ed81f",
}

def _load_exempt_from_env() -> set[str]:
    raw = os.getenv("FEE_EXEMPT", "")
    return {a.strip().lower() for a in raw.split(",") if a.strip()}

FEE_EXEMPT_ADDRESSES: set[str] = _BUILTIN_EXEMPT | _load_exempt_from_env()


@dataclass
class FeeResult:
    ok: bool
    skipped: bool       # True when user is exempt
    fee_usd: float
    tx_hash: Optional[str]
    message: str


class FeeManager:
    """Collects protocol fees on Polygon before each trade."""

    def __init__(self) -> None:
        self._w3 = None   # lazy-init

    def _get_w3(self):
        if self._w3 is None:
            from web3 import Web3
            self._w3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL, request_kwargs={"timeout": 10}))
        return self._w3

    def is_exempt(self, address: str) -> bool:
        return address.lower() in FEE_EXEMPT_ADDRESSES

    def add_exempt(self, address: str) -> None:
        FEE_EXEMPT_ADDRESSES.add(address.lower())

    def remove_exempt(self, address: str) -> None:
        FEE_EXEMPT_ADDRESSES.discard(address.lower())

    def calculate_fee(self, trade_usd: float) -> float:
        """Return the fee amount in USD (not yet rounded to 6-dec USDC)."""
        return round(trade_usd * FEE_PCT, 6)

    def collect(
        self,
        private_key: str,
        trade_usd: float,
        proxy_url: str | None = None,
    ) -> FeeResult:
        """
        Transfer the protocol fee in USDC from the signer to FEE_RECIPIENT.
        Returns FeeResult — caller decides whether to block on failure.
        """
        fee_usd = self.calculate_fee(trade_usd)
        if fee_usd < 0.000001:
            return FeeResult(ok=True, skipped=True, fee_usd=0.0, tx_hash=None,
                             message="Fee below dust threshold, skipped.")

        try:
            from eth_account import Account  # type: ignore
            acct = Account.from_key(private_key)
            signer = acct.address
        except Exception as exc:
            return FeeResult(ok=False, skipped=False, fee_usd=fee_usd, tx_hash=None,
                             message=f"Fee: could not derive address — {exc}")

        if self.is_exempt(signer):
            return FeeResult(ok=True, skipped=True, fee_usd=0.0, tx_hash=None,
                             message=f"Fee exempt: {signer}")

        try:
            w3 = self._get_w3()
            usdc = w3.eth.contract(
                address=w3.to_checksum_address(USDC_POLYGON),
                abi=USDC_ABI,
            )
            recipient = w3.to_checksum_address(FEE_RECIPIENT)

            # USDC has 6 decimals
            amount_raw = int(fee_usd * 1_000_000)

            nonce = w3.eth.get_transaction_count(signer)
            gas_price = w3.eth.gas_price

            tx = usdc.functions.transfer(recipient, amount_raw).build_transaction({
                "from":     signer,
                "nonce":    nonce,
                "gas":      80_000,
                "gasPrice": gas_price,
                "chainId":  137,
            })

            from eth_account import Account  # type: ignore
            signed = Account.sign_transaction(tx, private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)

            if receipt.status == 1:
                return FeeResult(
                    ok=True, skipped=False, fee_usd=fee_usd,
                    tx_hash=tx_hash.hex(),
                    message=f"Fee ${fee_usd:.4f} USDC sent → {FEE_RECIPIENT[:10]}… tx={tx_hash.hex()[:12]}…",
                )
            else:
                return FeeResult(ok=False, skipped=False, fee_usd=fee_usd, tx_hash=tx_hash.hex(),
                                 message=f"Fee tx reverted. tx={tx_hash.hex()[:12]}…")

        except Exception as exc:
            logger.warning("Fee collection error: %s", exc)
            return FeeResult(ok=False, skipped=False, fee_usd=fee_usd, tx_hash=None,
                             message=f"Fee transfer failed: {exc}")


# Singleton
fee_manager = FeeManager()

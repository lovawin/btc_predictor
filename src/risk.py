from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class PositionState:
    side: str | None = None   # "YES" or "NO"
    size_usd: float = 0.0
    entry_price: float = 0.0  # BTC spot at entry — used only for BTC-spot PnL tracking
    entry_token_price: float = 0.0   # NEW: actual Polymarket token price paid (0.0–1.0)
    opened_at: datetime | None = None  # NEW: when position was opened


class RiskManager:
    # ── Kelly fraction ────────────────────────────────────────────────────────
    # Full Kelly is mathematically optimal but practically ruinous in noisy
    # markets. Half-Kelly (0.5) is the standard conservative choice.
    KELLY_FRACTION = 0.5

    # ── Minimum position size worth hedging ───────────────────────────────────
    # Below this threshold the cost of an exit order exceeds expected benefit.
    MIN_HEDGE_SIZE_USD = 3.0

    # ── Minimum confidence flip margin ───────────────────────────────────────
    # Signal must be confidently AGAINST us, not just slightly tipping.
    MIN_FLIP_CONFIDENCE = 0.65  # was hedge_confidence_flip from settings — kept configurable

    def __init__(self, max_drawdown_pct: float, hedge_confidence_flip: float) -> None:
        self.max_drawdown_pct = max_drawdown_pct
        self.hedge_confidence_flip = max(hedge_confidence_flip, self.MIN_FLIP_CONFIDENCE)

    # ── Bet sizing: fractional Kelly on binary token ───────────────────────────
    def bet_size(
        self,
        max_bet_usd: float,
        confidence: float,
        token_price: float = 0.50,   # NEW: current market price of the token
    ) -> float:
        """
        Fractional Kelly sizing for a binary Polymarket token.

        Kelly formula for a binary bet:
            f = (p * b - q) / b
        where:
            p = probability of winning (our confidence)
            q = 1 - p
            b = net odds on a $1 bet = (1 - token_price) / token_price

        We then apply KELLY_FRACTION and cap at max_bet_usd.
        """
        p = max(0.50, min(0.99, confidence))
        q = 1.0 - p

        # Clamp token price to avoid division by zero or nonsensical odds
        token_price = max(0.01, min(0.99, token_price))
        b = (1.0 - token_price) / token_price  # net odds per dollar risked

        kelly_f = (p * b - q) / b              # full Kelly fraction of bankroll
        kelly_f = max(0.0, kelly_f)            # can't be negative
        fractional_kelly = kelly_f * self.KELLY_FRACTION

        sized = max_bet_usd * fractional_kelly
        return round(max(1.0, min(sized, max_bet_usd)), 2)

    # ── Hedge / exit decision ─────────────────────────────────────────────────
    def should_hedge(
        self,
        position: PositionState,
        current_spot: float,            # BTC spot — kept for API compatibility
        signal_direction: str,
        signal_confidence: float,
        current_token_price: float = 0.0,  # NEW: live token price (preferred)
    ) -> bool:
        """
        Returns True if we should hedge/exit the open position.

        Uses token price movement when available (current_token_price > 0),
        falls back to BTC spot movement only as a last resort.
        The original code used BTC spot exclusively, which meant the drawdown
        guard almost never fired in a 5-minute window.
        """
        if position.side is None or position.size_usd <= 0:
            return False
        if position.entry_price <= 0:
            return False

        # ── Guard: don't waste an order on a tiny position ────────────────────
        if position.size_usd < self.MIN_HEDGE_SIZE_USD:
            return False

        # ── Drawdown check: prefer token-price PnL, fall back to BTC spot ─────
        if current_token_price > 0 and position.entry_token_price > 0:
            # Token-price based: accurate for prediction market positions
            token_move = (current_token_price / position.entry_token_price) - 1.0
            if position.side == "NO":
                token_move *= -1
            drawdown_hit = token_move <= -self.max_drawdown_pct
        else:
            # BTC-spot fallback: coarse but available when token price unknown
            btc_move = (current_spot / position.entry_price) - 1.0
            if position.side == "NO":
                btc_move *= -1
            drawdown_hit = btc_move <= -self.max_drawdown_pct

        # ── Confidence flip: signal has strongly reversed against our position ─
        confidence_flip = (
            signal_confidence >= self.hedge_confidence_flip
            and (
                (position.side == "YES" and signal_direction == "DOWN")
                or (position.side == "NO" and signal_direction == "UP")
            )
        )

        # ── Age-based urgency: near market expiry, be more aggressive exiting ──
        # A position held for >4 minutes in a 5-minute market is near resolution.
        age_exit = False
        if position.opened_at is not None:
            age_seconds = (datetime.now(timezone.utc) - position.opened_at).total_seconds()
            age_exit = age_seconds >= 240  # exit if position is 4+ minutes old

        return drawdown_hit or confidence_flip or age_exit
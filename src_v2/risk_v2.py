"""
Hardened Risk Manager with Circuit Breakers
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class PositionState:
    side: str | None = None
    size_usd: float = 0.0
    entry_price: float = 0.0
    entry_token_price: float = 0.0
    opened_at: datetime | None = None


@dataclass
class CircuitBreakerState:
    """Prevents death spiral trading"""
    daily_loss_usd: float = 0.0
    consecutive_losses: int = 0
    daily_wins: int = 0
    daily_losses: int = 0
    last_trade_date: str = ""
    cooldown_until: str | None = None
    
    # Limits
    MAX_DAILY_LOSS_MULTIPLIER: float = 2.0  # 2x max_bet
    MAX_CONSECUTIVE_LOSSES: int = 3
    COOLDOWN_MINUTES: int = 30
    
    def check(self, max_bet_usd: float) -> str | None:
        """Returns block reason or None if clear"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Reset daily counters
        if self.last_trade_date != today:
            self.daily_loss_usd = 0.0
            self.consecutive_losses = 0
            self.daily_wins = 0
            self.daily_losses = 0
            self.last_trade_date = today
            self.cooldown_until = None
        
        # Check cooldown
        if self.cooldown_until:
            now = datetime.now(timezone.utc).isoformat()
            if now < self.cooldown_until:
                return f"COOLDOWN: Locked until {self.cooldown_until}"
            self.cooldown_until = None
        
        # Circuit breakers
        max_daily_loss = max_bet_usd * self.MAX_DAILY_LOSS_MULTIPLIER
        if self.daily_loss_usd >= max_daily_loss:
            return f"CIRCUIT BREAKER: Daily loss ${self.daily_loss_usd:.2f} >= ${max_daily_loss:.2f}"
        
        if self.consecutive_losses >= self.MAX_CONSECUTIVE_LOSSES:
            # Activate cooldown
            cooldown_end = datetime.now(timezone.utc)
            cooldown_end = cooldown_end.replace(minute=cooldown_end.minute + self.COOLDOWN_MINUTES)
            self.cooldown_until = cooldown_end.isoformat()
            return f"CIRCUIT BREAKER: {self.consecutive_losses} losses in a row. Cooldown for {self.COOLDOWN_MINUTES}min"
        
        return None
    
    def record_trade(self, pnl: float) -> None:
        """Record trade result for tracking"""
        if pnl > 0:
            self.daily_wins += 1
            self.consecutive_losses = 0
        elif pnl < 0:
            self.daily_losses += 1
            self.consecutive_losses += 1
            self.daily_loss_usd += abs(pnl)


class RiskManager:
    KELLY_FRACTION = 0.3  # More conservative (was 0.5)
    MIN_HEDGE_SIZE_USD = 5.0
    MIN_FLIP_CONFIDENCE = 0.70  # Higher threshold
    
    # Early exit targets
    PROFIT_TARGET_PCT = 0.20  # Exit 50% at +20%
    STOP_LOSS_PCT = 0.10  # Exit full at -10%
    
    def __init__(self, max_drawdown_pct: float, hedge_confidence_flip: float) -> None:
        self.max_drawdown_pct = max_drawdown_pct
        self.hedge_confidence_flip = max(hedge_confidence_flip, self.MIN_FLIP_CONFIDENCE)
    
    def bet_size(self, max_bet_usd: float, confidence: float, token_price: float = 0.50) -> float:
        """Conservative Kelly sizing"""
        p = max(0.50, min(0.99, confidence))
        q = 1.0 - p
        
        token_price = max(0.01, min(0.99, token_price))
        b = (1.0 - token_price) / token_price
        
        kelly_f = (p * b - q) / b
        kelly_f = max(0.0, kelly_f)
        fractional_kelly = kelly_f * self.KELLY_FRACTION
        
        sized = max_bet_usd * fractional_kelly
        return round(max(1.0, min(sized, max_bet_usd)), 2)
    
    def should_exit_early(
        self,
        position: PositionState,
        current_token_price: float,
    ) -> tuple[bool, str, float]:
        """
        Check if position should exit early.
        Returns: (should_exit, reason, exit_pct)
        """
        if position.side is None or position.size_usd <= 0:
            return False, "", 0.0
        if position.entry_token_price <= 0:
            return False, "", 0.0
        
        # Calculate return based on token price
        token_return = (current_token_price / position.entry_token_price) - 1.0
        if position.side == "NO":
            token_return *= -1
        
        # Take profit: exit 50% at +20%
        if token_return >= self.PROFIT_TARGET_PCT:
            return True, f"PROFIT_TARGET: +{token_return:.1%}", 0.50
        
        # Stop loss: exit 100% at -10%
        if token_return <= -self.STOP_LOSS_PCT:
            return True, f"STOP_LOSS: {token_return:.1%}", 1.0
        
        # Age-based: if 3+ minutes old and barely moving, exit
        if position.opened_at:
            age_seconds = (datetime.now(timezone.utc) - position.opened_at).total_seconds()
            if age_seconds >= 180 and abs(token_return) < 0.05:
                return True, f"TIME_DECAY: {age_seconds:.0f}s old, flat", 1.0
        
        return False, "", 0.0

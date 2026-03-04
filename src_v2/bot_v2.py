from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

from .config import settings
from .data_sources import MarketDataClient
from .polymarket_client import PolymarketClient
from .risk_v2 import PositionState, CircuitBreakerState
from .strategy import BtcFiveMinuteStrategy, Signal


@dataclass
class ChatState:
    user_id: int = 0
    chat_id: int = 0
    private_key: str = ""
    polymarket_signature_type: int = settings.polymarket_signature_type
    polymarket_funder: str = settings.polymarket_funder
    take_profit_pct: float = 0.20  # Token-based, not spot
    stop_loss_pct: float = 0.10
    block_dca_on_loss: bool = True
    dca_block_loss_pct: float = 0.05
    max_bet_usd: float = settings.default_max_bet_usd
    market_id: str = settings.polymarket_default_market_id
    market_slug_template: str = settings.polymarket_5m_slug_template
    auto_enabled: bool = False
    trading_enabled: bool = False
    force_all_trades: bool = False
    vpn_region: str = settings.vpn_default_region if settings.vpn_default_region in {"default", "mx", "au"} else "default"
    position: PositionState = field(default_factory=PositionState)
    circuit_breaker: CircuitBreakerState = field(default_factory=CircuitBreakerState)
    last_signal: Signal | None = None
    last_run_at: str | None = None
    realized_pnl_usd: float = 0.0
    trades_executed: int = 0
    wins: int = 0
    losses: int = 0
    volume_usd: float = 0.0
    last_message: str | None = None
    _cycle_running: bool = False


class TradingBot:
    def __init__(self) -> None:
        self.market = MarketDataClient()
        self.strategy = BtcFiveMinuteStrategy()
        self._proxy_url = settings.vpn_mx_proxy_url or os.getenv("POLYMARKET_SOCKS_PROXY", "")
        self.polymarket = PolymarketClient(
            live_trading=settings.live_trading,
            proxy_url=self._proxy_url,
        )
        self.chat_state: dict[int, ChatState] = {}

    def state_for(self, user_id: int, chat_id: int) -> ChatState:
        if user_id not in self.chat_state:
            self.chat_state[user_id] = ChatState(user_id=user_id, chat_id=chat_id)
        else:
            self.chat_state[user_id].chat_id = chat_id
        return self.chat_state[user_id]

    async def runonce(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        msg = await self.execute_cycle(user_id, chat_id)
        await update.message.reply_text(msg)

    async def execute_cycle(self, user_id: int, chat_id: int) -> str:
        return await asyncio.to_thread(self._execute_cycle_sync, user_id, chat_id)

    def _execute_cycle_sync(self, user_id: int, chat_id: int) -> str:
        state = self.state_for(user_id, chat_id)
        
        # Prevent concurrent cycles
        if state._cycle_running:
            return "SKIP: Cycle already running"
        state._cycle_running = True
        
        try:
            # Circuit breaker check
            block = state.circuit_breaker.check(state.max_bet_usd)
            if block:
                return f"BLOCKED: {block}"
            
            # Get signal
            snapshot = self.market.snapshot()
            signal = self.strategy.generate(snapshot)
            state.last_signal = signal
            state.last_run_at = datetime.now(timezone.utc).isoformat()
            
            if not state.market_id:
                return "No market ID set"
            
            desired_side = "YES" if signal.direction == "UP" else "NO"
            
            # Check for existing position
            if state.position.side and state.position.size_usd > 0:
                # Get current token price for P&L calculation
                current_token_price = self.polymarket.get_token_price(
                    state.market_id, state.position.side, ""
                )
                
                if current_token_price and state.position.entry_token_price > 0:
                    token_return = (current_token_price / state.position.entry_token_price) - 1.0
                    if state.position.side == "NO":
                        token_return *= -1
                    
                    # Take profit at +20%
                    if token_return >= state.take_profit_pct:
                        pnl = state.position.size_usd * token_return
                        state.realized_pnl_usd += pnl
                        state.circuit_breaker.record_trade(pnl)
                        state.position = PositionState()  # Reset
                        return f"PROFIT: +{token_return:.1%} (${pnl:.2f})"
                    
                    # Stop loss at -10%
                    if token_return <= -state.stop_loss_pct:
                        pnl = state.position.size_usd * token_return
                        state.realized_pnl_usd += pnl
                        state.circuit_breaker.record_trade(pnl)
                        state.position = PositionState()
                        return f"STOP LOSS: {token_return:.1%} (${pnl:.2f})"
                
                # Already in position, don't add more
                return f"HOLDING: {state.position.side} ${state.position.size_usd:.2f}"
            
            # No position - check if we should enter
            if signal.confidence < 0.65:
                return f"SKIP: Confidence {signal.confidence:.2%} < 65%"
            
            # Place order
            result = self.polymarket.place_order(
                private_key=state.private_key,
                market_id=state.market_id,
                side=desired_side,
                amount_usd=state.max_bet_usd,
                proxy_url=self._proxy_url,
            )
            
            if result.ok:
                state.position = PositionState(
                    side=desired_side,
                    size_usd=state.max_bet_usd,
                    entry_price=snapshot.fused_spot,
                    entry_token_price=result.fill_price if hasattr(result, 'fill_price') else 0.5,
                    opened_at=datetime.now(timezone.utc)
                )
                state.trades_executed += 1
                return f"ENTERED: {desired_side} ${state.max_bet_usd:.2f}"
            else:
                return f"ORDER FAILED: {result.message}"
        finally:
            state._cycle_running = False


async def run_bot_v2() -> None:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")
    bot = TradingBot()
    app = Application.builder().token(settings.telegram_bot_token).build()
    app.add_handler(CommandHandler("runonce", bot.runonce))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    while True:
        await asyncio.sleep(3600)

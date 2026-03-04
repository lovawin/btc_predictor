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
from .risk import CircuitBreakerState, PositionState, RiskManager
from .strategy import BtcFiveMinuteStrategy, Signal
from .fee_manager import fee_manager, FEE_PCT, FEE_RECIPIENT, FEE_EXEMPT_ADDRESSES
from .key_vault import key_vault

# ─────────────────────────────────────────────
# CONSTANTS — tune these without touching logic
# ─────────────────────────────────────────────



@dataclass
class ChatState:
    user_id: int = 0
    chat_id: int = 0
    private_key: str = ""
    polymarket_signature_type: int = settings.polymarket_signature_type
    polymarket_funder: str = settings.polymarket_funder
    take_profit_pct: float = 0.60
    stop_loss_pct: float = 0.15
    block_dca_on_loss: bool = True
    dca_block_loss_pct: float = 0.05
    min_confidence: float = 0.65        # skip entry if signal confidence < this
    order_size_pct: float = 0.25        # per-order cap as fraction of max_bet_usd
    token_dca_block_pct: float = 0.20   # block DCA if avg token dropped > this% from cost
    min_edge_pct: float = 0.01          # skip entry if net expected edge < this (1%)
    resolve_buffer_sec: int = 90        # skip NEW entries if < this many seconds left in the 5m slot
    trend_lock_pct: float = 0.015       # block entries that fight a strong 5h trend (1.5%)
    max_bet_usd: float = settings.default_max_bet_usd
    market_id: str = settings.polymarket_default_market_id
    market_slug_template: str = settings.polymarket_5m_slug_template
    auto_enabled: bool = False
    trading_enabled: bool = False
    force_all_trades: bool = False
    vpn_region: str = settings.vpn_default_region if settings.vpn_default_region in {"default", "mx", "au"} else "default"
    position: PositionState = field(default_factory=PositionState)
    last_signal: Signal | None = None
    last_run_at: str | None = None
    realized_pnl_usd: float = 0.0
    trades_executed: int = 0
    wins: int = 0
    losses: int = 0
    hedge_count: int = 0
    volume_usd: float = 0.0
    last_message: str | None = None

    # ── Death-spiral guards ────────────────────────────────────────────────────
    # Total USD deployed into the currently active market_id
    exposure_in_current_market: float = 0.0
    # Weighted-average TOKEN price we paid (0.0–1.0 scale)
    avg_token_price: float = 0.0
    # The market_id that owns the above counters (reset when market changes)
    _tracked_market_id: str = ""
    # Circuit breaker — enforces daily loss limits and cooldowns
    circuit_breaker: CircuitBreakerState = field(default_factory=CircuitBreakerState)


class TradingBot:
    def __init__(self) -> None:
        self.market = MarketDataClient()
        self.strategy = BtcFiveMinuteStrategy()
        self.risk = RiskManager(
            max_drawdown_pct=settings.max_drawdown_pct,
            hedge_confidence_flip=settings.hedge_confidence_flip,
        )
        self.polymarket = PolymarketClient(live_trading=settings.live_trading)
        self.chat_state: dict[int, ChatState] = {}
        self._user_key_store_path = settings.user_key_store_path
        self._stored_user_keys: dict[int, str] = self._load_user_keys()
        self._user_wallet_mode_store_path = settings.user_wallet_mode_store_path
        self._stored_user_wallet_modes: dict[int, dict[str, object]] = self._load_user_wallet_modes()

    # ── helpers (unchanged) ────────────────────────────────────────────────────

    def _load_user_keys(self) -> dict[int, str]:
        try:
            with open(self._user_key_store_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if not isinstance(raw, dict):
                return {}
            loaded: dict[int, str] = {}
            for key, value in raw.items():
                try:
                    user_id = int(key)
                except (TypeError, ValueError):
                    continue
                if isinstance(value, str) and value.strip():
                    loaded[user_id] = value.strip()
            return loaded
        except Exception:
            return {}

    def _save_user_keys(self) -> None:
        try:
            store_dir = os.path.dirname(self._user_key_store_path)
            if store_dir:
                os.makedirs(store_dir, exist_ok=True)
            serializable = {str(user_id): key for user_id, key in self._stored_user_keys.items() if key}
            with open(self._user_key_store_path, "w", encoding="utf-8") as fh:
                json.dump(serializable, fh)
        except Exception:
            pass

    def _persist_user_key(self, user_id: int, key: str) -> None:
        key = key.strip()
        if key:
            self._stored_user_keys[user_id] = key
        else:
            self._stored_user_keys.pop(user_id, None)
        self._save_user_keys()

    def _load_user_wallet_modes(self) -> dict[int, dict[str, object]]:
        try:
            with open(self._user_wallet_mode_store_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if not isinstance(raw, dict):
                return {}
            loaded: dict[int, dict[str, object]] = {}
            for key, value in raw.items():
                try:
                    user_id = int(key)
                except (TypeError, ValueError):
                    continue
                if not isinstance(value, dict):
                    continue
                sig_type = value.get("signature_type", settings.polymarket_signature_type)
                funder = str(value.get("funder", "") or "").strip()
                take_profit_pct = value.get("take_profit_pct", 0.60)
                stop_loss_pct = value.get("stop_loss_pct", 0.15)
                block_dca_on_loss = bool(value.get("block_dca_on_loss", True))
                dca_block_loss_pct = value.get("dca_block_loss_pct", 0.05)
                try:
                    sig_type_int = int(sig_type)
                except (TypeError, ValueError):
                    sig_type_int = settings.polymarket_signature_type
                if sig_type_int not in {0, 1, 2}:
                    sig_type_int = settings.polymarket_signature_type
                try:
                    take_profit_pct_f = float(take_profit_pct)
                except (TypeError, ValueError):
                    take_profit_pct_f = 0.60
                try:
                    stop_loss_pct_f = float(stop_loss_pct)
                except (TypeError, ValueError):
                    stop_loss_pct_f = 0.15
                if take_profit_pct_f <= 0:
                    take_profit_pct_f = 0.60
                if stop_loss_pct_f <= 0:
                    stop_loss_pct_f = 0.15
                try:
                    dca_block_loss_pct_f = float(dca_block_loss_pct)
                except (TypeError, ValueError):
                    dca_block_loss_pct_f = 0.05
                if dca_block_loss_pct_f <= 0:
                    dca_block_loss_pct_f = 0.05
                try:
                    min_confidence_f = float(value.get("min_confidence", 0.65))
                except (TypeError, ValueError):
                    min_confidence_f = 0.65
                if not (0.50 <= min_confidence_f <= 0.95):
                    min_confidence_f = 0.65
                try:
                    order_size_pct_f = float(value.get("order_size_pct", 0.25))
                except (TypeError, ValueError):
                    order_size_pct_f = 0.25
                if not (0.01 <= order_size_pct_f <= 1.0):
                    order_size_pct_f = 0.25
                try:
                    token_dca_block_pct_f = float(value.get("token_dca_block_pct", 0.20))
                except (TypeError, ValueError):
                    token_dca_block_pct_f = 0.20
                if not (0.01 <= token_dca_block_pct_f <= 1.0):
                    token_dca_block_pct_f = 0.20
                loaded[user_id] = {
                    "signature_type": sig_type_int,
                    "funder": funder,
                    "take_profit_pct": take_profit_pct_f,
                    "stop_loss_pct": stop_loss_pct_f,
                    "block_dca_on_loss": block_dca_on_loss,
                    "dca_block_loss_pct": dca_block_loss_pct_f,
                    "min_confidence": min_confidence_f,
                    "order_size_pct": order_size_pct_f,
                    "token_dca_block_pct": token_dca_block_pct_f,
                }
            return loaded
        except Exception:
            return {}

    def _save_user_wallet_modes(self) -> None:
        try:
            store_dir = os.path.dirname(self._user_wallet_mode_store_path)
            if store_dir:
                os.makedirs(store_dir, exist_ok=True)
            serializable = {
                str(user_id): {
                    "signature_type": int(mode.get("signature_type", settings.polymarket_signature_type)),
                    "funder": str(mode.get("funder", "") or "").strip(),
                    "take_profit_pct": float(mode.get("take_profit_pct", 0.60)),
                    "stop_loss_pct": float(mode.get("stop_loss_pct", 0.15)),
                    "block_dca_on_loss": bool(mode.get("block_dca_on_loss", True)),
                    "dca_block_loss_pct": float(mode.get("dca_block_loss_pct", 0.05)),
                    "min_confidence": float(mode.get("min_confidence", 0.65)),
                    "order_size_pct": float(mode.get("order_size_pct", 0.25)),
                    "token_dca_block_pct": float(mode.get("token_dca_block_pct", 0.20)),
                }
                for user_id, mode in self._stored_user_wallet_modes.items()
            }
            with open(self._user_wallet_mode_store_path, "w", encoding="utf-8") as fh:
                json.dump(serializable, fh)
        except Exception:
            pass

    def _persist_user_wallet_mode(
        self,
        user_id: int,
        signature_type: int,
        funder: str,
        take_profit_pct: float,
        stop_loss_pct: float,
        block_dca_on_loss: bool,
        dca_block_loss_pct: float,
        min_confidence: float = 0.65,
        order_size_pct: float = 0.25,
        token_dca_block_pct: float = 0.20,
    ) -> None:
        self._stored_user_wallet_modes[user_id] = {
            "signature_type": signature_type,
            "funder": funder.strip(),
            "take_profit_pct": float(take_profit_pct),
            "stop_loss_pct": float(stop_loss_pct),
            "block_dca_on_loss": bool(block_dca_on_loss),
            "dca_block_loss_pct": float(dca_block_loss_pct),
            "min_confidence": float(min_confidence),
            "order_size_pct": float(order_size_pct),
            "token_dca_block_pct": float(token_dca_block_pct),
        }
        self._save_user_wallet_modes()

    def _user_chat_ids(self, update: Update) -> tuple[int, int]:
        if update.effective_user is None or update.effective_chat is None:
            raise ValueError("Missing effective user/chat")
        return update.effective_user.id, update.effective_chat.id

    @staticmethod
    def menu_commands() -> list[BotCommand]:
        return [
            BotCommand("start", "Start your private trading session"),
            BotCommand("help", "Show command guide and what each command does"),
            BotCommand("whoami", "Show your Telegram user/chat ids"),
            BotCommand("myconfig", "Show your current bot settings"),
            BotCommand("mode", "Show PAPER or LIVE mode"),
            BotCommand("setpin", "Set your encryption PIN (required before setkey)"),
            BotCommand("setkey", "Store your key encrypted with your PIN"),
            BotCommand("unlock", "Decrypt key into session memory"),
            BotCommand("lock", "Wipe decrypted key from memory"),
            BotCommand("vaultstatus", "Show vault lock state"),
            BotCommand("clearkey", "Permanently delete your encrypted key"),
            BotCommand("setsigtype", "Set signature mode: 0,1,2"),
            BotCommand("setfunder", "Set funded wallet address"),
            BotCommand("clearfunder", "Clear per-user funder override"),
            BotCommand("setstops", "Set take-profit and stop-loss"),
            BotCommand("setdca", "Block or allow DCA on loss"),
            BotCommand("keyhelp", "Explain key usage and safety"),
            BotCommand("setvpn", "Set route preference: default or mx"),
            BotCommand("vpnon", "Enable VPN now (no params)"),
            BotCommand("vpnoff", "Disable VPN route preference"),
            BotCommand("vpnstatus", "Show VPN routing status"),
            BotCommand("setmax", "Set max bet amount in USD"),
            BotCommand("setmarket", "Set market id manually"),
            BotCommand("setmarketurl", "Set market from Polymarket URL"),
            BotCommand("tradecurrent", "Pin market and start auto trading"),
            BotCommand("starttrade", "Start background trading"),
            BotCommand("stoptrade", "Stop background trading"),
            BotCommand("alltradeson", "Trade every cycle (high risk)"),
            BotCommand("alltradesoff", "Return to profit-only trade filter"),
            BotCommand("markethelp", "Explain market id setup"),
            BotCommand("status", "Show latest signal and position"),
            BotCommand("pnl", "Show your private PnL and stats"),
            BotCommand("dryrunlive", "Validate live order path without trading"),
            BotCommand("walletcheck", "Show signer and collateral diagnostics"),
            BotCommand("runonce", "Run one analysis + trade cycle"),
            BotCommand("autoon", "Enable fast auto loop"),
            BotCommand("autooff", "Disable the auto loop"),
            BotCommand("reset", "Reset your local session state"),
            BotCommand("setminedge", "Min net expected edge % to trade"),
            BotCommand("setresolvebuf", "Skip entries if <N sec left in slot"),
            BotCommand("settrendlock", "Block trades fighting strong 5h trend"),
            BotCommand("setminconf", "Min signal confidence to trade (50-95%)"),
            BotCommand("setorderpct", "Per-order size as % of max_bet"),
            BotCommand("settokendca", "DCA block: token drop % from avg cost"),
            BotCommand("setcb", "Circuit breaker limits: mult consec cooldown"),
            BotCommand("cbstatus", "Show live circuit breaker state"),
        ]

    async def register_command_menu(self, application: Application) -> None:
        try:
            await application.bot.set_my_commands(self.menu_commands())
        except Exception:
            pass

    def state_for(self, user_id: int, chat_id: int) -> ChatState:
        mode = self._stored_user_wallet_modes.get(user_id, {})
        sig_type = int(mode.get("signature_type", settings.polymarket_signature_type)) if isinstance(mode, dict) else settings.polymarket_signature_type
        if sig_type not in {0, 1, 2}:
            sig_type = settings.polymarket_signature_type
        funder = str(mode.get("funder", settings.polymarket_funder) if isinstance(mode, dict) else settings.polymarket_funder)
        try:
            take_profit_pct = float(mode.get("take_profit_pct", 0.60)) if isinstance(mode, dict) else 0.60
        except (TypeError, ValueError):
            take_profit_pct = 0.60
        try:
            stop_loss_pct = float(mode.get("stop_loss_pct", 0.15)) if isinstance(mode, dict) else 0.15
        except (TypeError, ValueError):
            stop_loss_pct = 0.15
        block_dca_on_loss = bool(mode.get("block_dca_on_loss", True)) if isinstance(mode, dict) else True
        try:
            dca_block_loss_pct = float(mode.get("dca_block_loss_pct", 0.05)) if isinstance(mode, dict) else 0.05
        except (TypeError, ValueError):
            dca_block_loss_pct = 0.05
        try:
            min_confidence = float(mode.get("min_confidence", 0.65)) if isinstance(mode, dict) else 0.65
        except (TypeError, ValueError):
            min_confidence = 0.65
        try:
            order_size_pct = float(mode.get("order_size_pct", 0.25)) if isinstance(mode, dict) else 0.25
        except (TypeError, ValueError):
            order_size_pct = 0.25
        try:
            token_dca_block_pct = float(mode.get("token_dca_block_pct", 0.20)) if isinstance(mode, dict) else 0.20
        except (TypeError, ValueError):
            token_dca_block_pct = 0.20
        if user_id not in self.chat_state:
            self.chat_state[user_id] = ChatState(
                user_id=user_id,
                chat_id=chat_id,
                private_key=self._stored_user_keys.get(user_id, ""),
                polymarket_signature_type=sig_type,
                polymarket_funder=funder,
                take_profit_pct=take_profit_pct,
                stop_loss_pct=stop_loss_pct,
                block_dca_on_loss=block_dca_on_loss,
                dca_block_loss_pct=dca_block_loss_pct,
                min_confidence=min_confidence,
                order_size_pct=order_size_pct,
                token_dca_block_pct=token_dca_block_pct,
            )
        else:
            self.chat_state[user_id].chat_id = chat_id
        return self.chat_state[user_id]

    # ── NEW: reset per-market exposure counters when market changes ────────────
    def _sync_market_counters(self, state: ChatState) -> None:
        """Reset order/exposure counters whenever the market_id changes."""
        if state.market_id != state._tracked_market_id:
            state.exposure_in_current_market = 0.0
            state.avg_token_price = 0.0
            state._tracked_market_id = state.market_id

    # ── NEW: update token price tracking after a successful fill ───────────────
    def _update_token_tracking(
        self, state: ChatState, *, fill_usd: float, token_price: float
    ) -> None:
        """
        Maintain a weighted-average token price so DCA blocking is based on
        actual Polymarket token cost, NOT on BTC spot movement.
        """
        prev_exposure = state.exposure_in_current_market
        new_exposure = prev_exposure + fill_usd
        if new_exposure > 0:
            state.avg_token_price = (
                (state.avg_token_price * prev_exposure + token_price * fill_usd)
                / new_exposure
            )
        state.exposure_in_current_market = new_exposure

    # ── NEW: the actual guard that stops the death spiral ─────────────────────
    def _check_spiral_guards(
        self,
        state: ChatState,
        desired_side: str,
        current_token_price: float,
    ) -> str | None:
        """
        Returns a block reason string if trading should be skipped,
        or None if it's safe to proceed.
        """
        # Auto-clear stale cumulative exposure when no position is open
        if state.position.size_usd <= 0:
            state.exposure_in_current_market = 0.0
            state.avg_token_price = 0.0

        # 1. Hard cap: use actual open position size, not cumulative counter
        #    Use > (not >=) so a $1 position at $1 max doesn't block the next market
        open_exposure = state.position.size_usd
        if open_exposure > state.max_bet_usd:
            return (
                f"BLOCKED: exposure ${open_exposure:.2f} > max "
                f"${state.max_bet_usd:.2f} for this market. Use /setmax to adjust."
            )

        # 3. Token-price DCA block: compare current token price to avg cost basis
        if (
            state.block_dca_on_loss
            and state.avg_token_price > 0
            and desired_side == state.position.side
        ):
            token_move = (current_token_price / state.avg_token_price) - 1.0
            if token_move <= -state.token_dca_block_pct:
                return (
                    f"BLOCKED DCA: token price {current_token_price:.3f} is "
                    f"{token_move:.1%} vs avg cost {state.avg_token_price:.3f} "
                    f"(threshold={-state.token_dca_block_pct:.0%})."
                )

        return None  # all clear

    # ─────────────────────────────────────────────────────────────────────────
    # All command handlers are unchanged — only _execute_cycle_sync is modified
    # ─────────────────────────────────────────────────────────────────────────

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        self.state_for(user_id, chat_id)
        await update.message.reply_text(
            "⚡ BTC Predictor Bot\n"
            "Automated BTC/USD prediction trading on Polymarket.\n\n"
            "Quick setup:\n"
            "1️⃣ /setpin <PIN> — create your encryption PIN\n"
            "2️⃣ /setkey <private_key> <PIN> — store key encrypted\n"
            "3️⃣ /setmax <usd> — set your max bet size\n"
            "4️⃣ /starttrade — let it run\n\n"
            "🔐 Your key is encrypted with your PIN before storage.\n"
            "We never have access to your unencrypted key.\n\n"
            "Type /help for the full command list or /keyhelp for key setup guide."
        )

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "BTC Predictor Bot — Command Guide\n\n"
            "🔐 KEY VAULT (setup first)\n"
            "/setpin <PIN> — create your encryption PIN\n"
            "/setkey <key> <PIN> — store key encrypted with your PIN\n"
            "/unlock <PIN> — load key into session memory\n"
            "/lock — wipe key from session memory\n"
            "/vaultstatus — show vault state\n"
            "/clearkey — permanently delete your key\n"
            "/keyhelp — full key setup walkthrough\n\n"
            "⚙️ CONFIGURATION\n"
            "/setmax <usd> — max bet size\n"
            "/setstops <tp%> <sl%> — take profit / stop loss\n"
            "/setminconf <0-100> — min confidence to enter\n"
            "/setminedge <pct> — min net expected edge\n"
            "/setresolvebuf <sec> — skip entry if slot ending soon\n"
            "/settrendlock <pct> — block trades against macro trend\n"
            "/setdca <block|allow> [loss%] — DCA guard\n"
            "/setcb <mult> <consec> <cooldown> — circuit breaker\n"
            "/myconfig — show all current settings\n\n"
            "💹 TRADING\n"
            "/starttrade — start auto trading loop\n"
            "/stoptrade — pause auto trading\n"
            "/runonce — run one manual cycle\n"
            "/status — live signal, confidence & position\n"
            "/pnl — your win/loss record and P&L\n"
            "/cbstatus — circuit breaker state\n\n"
            "🌐 WALLET & MARKET\n"
            "/walletcheck — balance, allowance, signer info\n"
            "/setfunder <0x...> — set funded wallet\n"
            "/setsigtype <0|1|2> — signature mode\n"
            "/setmarket <market_id> — pin a specific market\n"
            "/setmarketurl <url> — set market from URL\n\n"
            "/whoami — your Telegram user ID\n"
            "/mode — PAPER or LIVE mode\n"
            "/reset — clear session data\n"
        )

    async def whoami(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        await update.message.reply_text(
            f"user_id={user_id}\nchat_id={chat_id}\nState isolation key: user_id"
        )

    async def keyhelp(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        mode = "LIVE" if settings.live_trading else "PAPER"
        await update.message.reply_text(
            "\n".join(
                [
                    f"Mode: {mode}",
                    "",
                    "🔐 HOW KEY SECURITY WORKS",
                    "Your private key is encrypted with AES-256-GCM using a key",
                    "derived from your personal PIN (PBKDF2, 310,000 iterations).",
                    "Only the encrypted blob is written to disk — never the raw key.",
                    "Even with full server access, your key cannot be read without your PIN.",
                    "",
                    "SETUP (do this once):",
                    "1. /setpin <PIN>          — choose any PIN, min 4 chars",
                    "2. /setkey <key> <PIN>    — encrypts and stores your key",
                    "   Done. Trading is active for this session.",
                    "",
                    "EACH SESSION AFTER RESTART:",
                    "1. /unlock <PIN>          — decrypts key into memory",
                    "2. /starttrade            — start the bot",
                    "",
                    "WHEN DONE:",
                    "/lock                     — wipes decrypted key from memory",
                    "/vaultstatus              — check current state",
                    "/clearkey                 — permanently delete everything",
                    "",
                    "⚠️  There is no PIN recovery. Store it somewhere safe.",
                    "Your key never leaves your device unencrypted.",
                    "Use /clearkey anytime to remove it.",
                ]
            )
        )

    async def myconfig(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        mode = "LIVE" if settings.live_trading else "PAPER"
        await update.message.reply_text(
            "\n".join(
                [
                    f"Mode: {mode}",
                    f"Max bet: ${state.max_bet_usd:.2f}",
                    f"Market ID: {state.market_id or 'NOT SET'}",
                    f"Market holder: {state.market_slug_template or 'NOT SET'}",
                    f"Private key set: {'yes' if bool(state.private_key or settings.polymarket_private_key) else 'no'}",
                    f"Signature type: {state.polymarket_signature_type}",
                    f"Funder: {state.polymarket_funder or 'DEFAULT (signer)'}",
                    f"Take profit: {state.take_profit_pct:.2%}",
                    f"Stop loss: {state.stop_loss_pct:.2%}",
                    f"DCA on loss: {'BLOCKED' if state.block_dca_on_loss else 'ALLOWED'} (threshold={state.dca_block_loss_pct:.2%})",
                    f"Min confidence: {state.min_confidence:.0%}",
                    f"Min edge: {state.min_edge_pct:.2%}",
                    f"Resolve buffer: {state.resolve_buffer_sec}s (skip entry if <N sec left in slot)",
                    f"Trend lock: {state.trend_lock_pct:.2%} (0=off)",
                    f"Order size: {state.order_size_pct:.0%} of max_bet (cap=${state.max_bet_usd * state.order_size_pct:.2f})",
                    f"Token DCA block: {state.token_dca_block_pct:.0%} drop from cost",
                    f"Circuit breaker: daily_loss={state.circuit_breaker.MAX_DAILY_LOSS_MULTIPLIER}x max_bet, max_consec={state.circuit_breaker.MAX_CONSECUTIVE_LOSSES}, cooldown={state.circuit_breaker.COOLDOWN_MINUTES}min",
                    f"VPN route: {state.vpn_region}",
                    f"Auto: {'ON' if state.auto_enabled else 'OFF'}",
                    f"Trading enabled: {'ON' if state.trading_enabled else 'OFF'}",
                    f"All-trades mode: {'ON' if state.force_all_trades else 'OFF'}",
                    # NEW: show spiral guard stats
                    f"Exposure in current market: ${state.exposure_in_current_market:.2f}/${state.max_bet_usd:.2f}",
                    f"Avg token price (cost basis): {state.avg_token_price:.4f}",
                ]
            )
        )

    def _proxy_for_state(self, state: ChatState) -> str:
        if state.vpn_region in {"au", "mx"}:
            return settings.vpn_mx_proxy_url
        return ""

    @staticmethod
    def _host_dns_nameservers() -> list[str]:
        nameservers: list[str] = []
        try:
            with open("/etc/resolv.conf", "r", encoding="utf-8") as fh:
                for raw_line in fh:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split()
                    if len(parts) >= 2 and parts[0].lower() == "nameserver":
                        nameservers.append(parts[1])
        except Exception:
            return []
        return nameservers

    @staticmethod
    def _wg_interface_from_conf(conf_path: str) -> str:
        if not conf_path:
            return ""
        name = Path(conf_path).name
        if name.endswith(".conf"):
            name = name[:-5]
        return name.strip()

    @staticmethod
    def _is_interface_up(interface: str) -> bool:
        if not interface:
            return False
        try:
            result = subprocess.run(
                ["ip", "link", "show", "dev", interface],
                capture_output=True,
                text=True,
            )
        except Exception:
            return False
        if result.returncode != 0:
            return False
        output = (result.stdout or "").upper()
        return "<" in output and ",UP" in output

    def _is_private_chat(self, update: Update) -> bool:
        return bool(update.effective_chat and update.effective_chat.type == "private")

    @staticmethod
    def _signed_move_pct(side: str, entry_price: float, current_spot: float) -> float:
        if entry_price <= 0:
            return 0.0
        move = (current_spot / entry_price) - 1.0
        if side == "NO":
            move *= -1
        return move

    def _record_realized_pnl(
        self,
        state: ChatState,
        *,
        side: str,
        entry_price: float,
        current_spot: float,
        closed_size_usd: float,
    ) -> float:
        if closed_size_usd <= 0:
            return 0.0
        pnl = closed_size_usd * self._signed_move_pct(side, entry_price, current_spot)
        state.realized_pnl_usd += pnl
        if pnl > 0:
            state.wins += 1
        elif pnl < 0:
            state.losses += 1
        return pnl

    def _unrealized_pnl(self, state: ChatState, current_spot: float) -> float:
        if state.position.side is None or state.position.size_usd <= 0 or state.position.entry_price <= 0:
            return 0.0
        move = self._signed_move_pct(state.position.side, state.position.entry_price, current_spot)
        return state.position.size_usd * move

    @staticmethod
    def _has_open_position(state: ChatState) -> bool:
        return bool(state.position.side and state.position.size_usd > 0 and state.position.entry_price > 0)

    def _attempt_risk_exit(
        self,
        *,
        state: ChatState,
        effective_key: str,
        market_id: str,
        exit_side: str,
        exit_size: float,
        proxy_url: str,
        max_attempts: int = 3,
    ):
        last_result = None
        attempt_size = round(max(1.0, exit_size), 2)
        for _ in range(max_attempts):
            result = self.polymarket.place_order(
                private_key=effective_key,
                market_id=market_id,
                side=exit_side,
                amount_usd=attempt_size,
                proxy_url=proxy_url,
                signature_type=state.polymarket_signature_type,
                funder=state.polymarket_funder,
            )
            last_result = result
            if result.ok:
                return result, attempt_size
            attempt_size = round(max(1.0, attempt_size * 0.8), 2)
        return last_result, attempt_size

    async def markethelp(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "\n".join(
                [
                    "Market ID = the unique Polymarket market identifier your bot will trade.",
                    "Find your target BTC 5m market in Polymarket and copy its market id from API/URL metadata.",
                    "Then set it with: /setmarket <market_id>",
                    "Without market id, /runonce will only do analysis and no order execution.",
                    "/pnl - show your private PnL + trade stats",
                ]
            )
        )

    async def setpin(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Set or change the encryption PIN for your private key vault."""
        user_id, _ = self._user_chat_ids(update)
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text(
                "Usage: /setpin <PIN>\n"
                "Your PIN encrypts your private key — we never see your unencrypted key.\n"
                "Minimum 4 characters. Remember it — there is no recovery."
            )
            return
        pin = args[0].strip()
        if not key_vault.set_pin(user_id, pin):
            await update.message.reply_text("PIN must be at least 4 characters.")
            return
        # If user already has an encrypted key, they need to re-encrypt with new PIN
        await update.message.reply_text(
            "✅ PIN set.\n"
            "Now use /setkey <private_key> to store your key encrypted with this PIN.\n\n"
            "⚠️ Store your PIN somewhere safe — it cannot be recovered."
        )

    async def unlock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Decrypt your key into session memory so the bot can trade."""
        user_id, chat_id = self._user_chat_ids(update)
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text("Usage: /unlock <PIN>")
            return
        pin = args[0].strip()
        if not key_vault.has_encrypted_key(user_id):
            await update.message.reply_text("No encrypted key found. Use /setpin then /setkey first.")
            return
        success = key_vault.unlock(user_id, pin)
        if not success:
            await update.message.reply_text("❌ Wrong PIN. Key not unlocked.")
            return
        # Sync decrypted key into active session state
        state = self.state_for(user_id, chat_id)
        state.private_key = key_vault.get_session_key(user_id) or ""
        await update.message.reply_text(
            "🔓 Unlocked. Key is active in session memory.\n"
            "Use /lock to wipe it from memory when done."
        )

    async def lock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Wipe your decrypted key from session memory."""
        user_id, chat_id = self._user_chat_ids(update)
        key_vault.lock(user_id)
        state = self.state_for(user_id, chat_id)
        state.private_key = ""
        await update.message.reply_text(
            "🔒 Locked. Decrypted key wiped from memory.\n"
            "Use /unlock <PIN> to re-activate trading."
        )

    async def vaultstatus(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show your vault state — PIN set, key stored, locked/unlocked."""
        user_id, _ = self._user_chat_ids(update)
        await update.message.reply_text(key_vault.vault_status(user_id))

    async def setkey(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if not context.args:
            await update.message.reply_text("Usage: /setkey <private_key>")
            return
        raw_key = context.args[0].strip()

        # ── Vault path: encrypt with PIN ─────────────────────────────────────
        if key_vault.has_pin(user_id):
            # Ask user to confirm with PIN inline: /setkey <key> <pin>
            if len(context.args) < 2:
                await update.message.reply_text(
                    "PIN detected. Usage: /setkey <private_key> <PIN>\n"
                    "Your key will be encrypted before storage — we never see the plaintext."
                )
                return
            pin = context.args[1].strip()
            ok = key_vault.store_key(user_id, raw_key, pin)
            if not ok:
                await update.message.reply_text("❌ Wrong PIN. Key not saved.")
                return
            state = self.state_for(user_id, chat_id)
            state.private_key = raw_key   # active in session memory
            await update.message.reply_text(
                "✅ Key encrypted and stored.\n"
                "🔐 Your unencrypted key never touches our disk.\n"
                "Session is active — use /lock to wipe from memory."
            )
            return

        # ── Legacy path: no PIN set yet ───────────────────────────────────────
        state = self.state_for(user_id, chat_id)
        state.private_key = raw_key
        self._persist_user_key(user_id, raw_key)
        await update.message.reply_text(
            "Key saved (unencrypted).\n"
            "💡 Tip: Set a PIN with /setpin to encrypt your key — then we cannot access it."
        )

    async def clearkey(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        key_vault.delete_key(user_id)
        state = self.state_for(user_id, chat_id)
        state.private_key = ""
        self._persist_user_key(user_id, "")
        await update.message.reply_text("🗑️ Key permanently deleted — encrypted vault and session memory cleared.")

    async def setsigtype(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if not context.args:
            await update.message.reply_text("Usage: /setsigtype <0|1|2>")
            return
        try:
            value = int(context.args[0].strip())
        except ValueError:
            await update.message.reply_text("Invalid value. Use 0, 1, or 2.")
            return
        if value not in {0, 1, 2}:
            await update.message.reply_text("Invalid value. Use 0, 1, or 2.")
            return
        state = self.state_for(user_id, chat_id)
        state.polymarket_signature_type = value
        self._persist_user_wallet_mode(
            user_id,
            state.polymarket_signature_type,
            state.polymarket_funder,
            state.take_profit_pct,
            state.stop_loss_pct,
            state.block_dca_on_loss,
            state.dca_block_loss_pct,
        )
        await update.message.reply_text(f"Signature type set to {value}.")

    async def setfunder(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if not context.args:
            await update.message.reply_text("Usage: /setfunder <0x...>")
            return
        address = context.args[0].strip()
        if not re.fullmatch(r"0x[a-fA-F0-9]{40}", address):
            await update.message.reply_text("Invalid address format. Expected 0x + 40 hex chars.")
            return
        state = self.state_for(user_id, chat_id)
        state.polymarket_funder = address
        self._persist_user_wallet_mode(
            user_id,
            state.polymarket_signature_type,
            state.polymarket_funder,
            state.take_profit_pct,
            state.stop_loss_pct,
            state.block_dca_on_loss,
            state.dca_block_loss_pct,
        )
        await update.message.reply_text(f"Funder set to {address}")

    async def clearfunder(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        state.polymarket_funder = ""
        self._persist_user_wallet_mode(
            user_id,
            state.polymarket_signature_type,
            state.polymarket_funder,
            state.take_profit_pct,
            state.stop_loss_pct,
            state.block_dca_on_loss,
            state.dca_block_loss_pct,
        )
        await update.message.reply_text("Funder override cleared for your user profile.")

    async def setstops(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /setstops <take_profit%> <stop_loss%>. Example: /setstops 60 15")
            return
        try:
            take_raw = float(context.args[0].strip())
            stop_raw = float(context.args[1].strip())
        except ValueError:
            await update.message.reply_text("Invalid values. Example: /setstops 60 15")
            return
        take_pct = take_raw / 100.0 if take_raw > 1 else take_raw
        stop_pct = stop_raw / 100.0 if stop_raw > 1 else stop_raw
        if take_pct <= 0 or stop_pct <= 0 or take_pct > 5 or stop_pct > 0.95:
            await update.message.reply_text("Out-of-range values.")
            return
        state = self.state_for(user_id, chat_id)
        state.take_profit_pct = take_pct
        state.stop_loss_pct = stop_pct
        self._persist_user_wallet_mode(
            user_id,
            state.polymarket_signature_type,
            state.polymarket_funder,
            state.take_profit_pct,
            state.stop_loss_pct,
            state.block_dca_on_loss,
            state.dca_block_loss_pct,
        )
        await update.message.reply_text(
            f"Stops updated: take-profit={state.take_profit_pct:.2%}, stop-loss={state.stop_loss_pct:.2%}"
        )

    async def setdca(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if not context.args:
            await update.message.reply_text("Usage: /setdca <block|allow> [loss%]")
            return
        choice = context.args[0].strip().lower()
        if choice not in {"block", "allow"}:
            await update.message.reply_text("Invalid option. Use /setdca block or /setdca allow")
            return
        state = self.state_for(user_id, chat_id)
        state.block_dca_on_loss = choice == "block"
        if len(context.args) >= 2:
            try:
                raw_threshold = float(context.args[1].strip())
            except ValueError:
                await update.message.reply_text("Invalid threshold.")
                return
            threshold = raw_threshold / 100.0 if raw_threshold > 1 else raw_threshold
            if threshold <= 0 or threshold > 0.5:
                await update.message.reply_text("Threshold out of range.")
                return
            state.dca_block_loss_pct = threshold
        self._persist_user_wallet_mode(
            user_id,
            state.polymarket_signature_type,
            state.polymarket_funder,
            state.take_profit_pct,
            state.stop_loss_pct,
            state.block_dca_on_loss,
            state.dca_block_loss_pct,
        )
        await update.message.reply_text(
            f"DCA on loss is now {'BLOCKED' if state.block_dca_on_loss else 'ALLOWED'} (threshold={state.dca_block_loss_pct:.2%})."
        )

    async def setvpn(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if not context.args:
            await update.message.reply_text("Usage: /setvpn <default|mx>")
            return
        choice = context.args[0].strip().lower()
        if choice == "au":
            choice = "mx"
        if choice not in {"default", "mx"}:
            await update.message.reply_text("Invalid option.")
            return
        state = self.state_for(user_id, chat_id)
        state.vpn_region = choice
        if choice == "mx" and not settings.vpn_mx_proxy_url and not settings.vpn_wg_conf:
            await update.message.reply_text(
                "VPN set to MX, but no route backend is configured yet."
            )
            return
        await update.message.reply_text(f"VPN route set to: {choice}")

    async def vpnon(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        if context.args:
            await update.message.reply_text("/vpnon takes no parameters. Enabling VPN now.")
        state.vpn_region = "mx"

        # If a SOCKS proxy is configured (e.g. Tor), use that directly — no need for WireGuard.
        if settings.vpn_mx_proxy_url:
            await update.message.reply_text("VPN ON (routing trades via SOCKS proxy).")
            return

        if settings.vpn_wg_conf:
            wg_interface = self._wg_interface_from_conf(settings.vpn_wg_conf)
            if self._is_interface_up(wg_interface):
                await update.message.reply_text("VPN ON (WireGuard profile already active).")
                return
            cmd = ["sudo", "-n", "wg-quick", "up", settings.vpn_wg_conf]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                await update.message.reply_text("VPN ON (WireGuard profile up).")
                return
            stderr = (result.stderr or "").strip()
            await update.message.reply_text(
                f"WireGuard up failed. Details: {stderr or 'unknown error'}"
            )
            return
        await update.message.reply_text("VPN ON requested, but no active route backend configured.")

    async def vpnoff(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        state.vpn_region = "default"
        if settings.vpn_wg_conf:
            cmd = ["sudo", "-n", "wg-quick", "down", settings.vpn_wg_conf]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                await update.message.reply_text("VPN OFF (WireGuard profile down).")
                return
        await update.message.reply_text("VPN route OFF (default network).")

    async def vpnstatus(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        proxy = self._proxy_for_state(state)
        dns = self._host_dns_nameservers()
        await update.message.reply_text(
            "\n".join(
                [
                    f"VPN route: {state.vpn_region}",
                    f"MX proxy configured: {'yes' if bool(settings.vpn_mx_proxy_url) else 'no'}",
                    f"WireGuard profile configured: {'yes' if bool(settings.vpn_wg_conf) else 'no'}",
                    f"Routing active: {'yes' if bool(proxy) else 'no'}",
                    f"Host DNS: {', '.join(dns) if dns else 'n/a'}",
                ]
            )
        )

    async def setmax(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if not context.args:
            await update.message.reply_text("Usage: /setmax <usd>")
            return
        try:
            value = float(context.args[0])
            if value <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Invalid amount.")
            return
        state = self.state_for(user_id, chat_id)
        state.max_bet_usd = value
        await update.message.reply_text(f"Max bet set to ${value:.2f}")

    async def setmarket(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if not context.args:
            await update.message.reply_text("Usage: /setmarket <market_id>")
            return
        market_id = context.args[0].strip()
        state = self.state_for(user_id, chat_id)
        state.market_id = market_id
        state.market_slug_template = ""
        self._sync_market_counters(state)  # reset counters for new market
        await update.message.reply_text(f"Market ID set to: {market_id}")

    async def setmarketurl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if not context.args:
            await update.message.reply_text("Usage: /setmarketurl <polymarket_url_or_text_with_condition_id>")
            return
        raw = " ".join(context.args).strip()
        slug_match = re.search(r"polymarket\.com/event/([a-zA-Z0-9\-]+)", raw)
        if slug_match:
            slug = slug_match.group(1)
            slot_match = re.search(r"^(.*-)(\d{9,12})$", slug)
            state = self.state_for(user_id, chat_id)
            if slot_match:
                template = f"{slot_match.group(1)}{{ts}}"
                state.market_slug_template = template
                state.market_id = ""
                now_ts = int(datetime.now(timezone.utc).timestamp())
                current_slot = (now_ts // 300) * 300
                next_slot = current_slot + 300
                self._sync_market_counters(state)
                await update.message.reply_text(
                    "\n".join(
                        [
                            f"5m holder template set: {template}",
                            f"Current slot: {current_slot}",
                            f"Next slot: {next_slot}",
                        ]
                    )
                )
                return
        match = re.search(r"0x[a-fA-F0-9]{64}", raw)
        if not match:
            await update.message.reply_text("Could not find a condition id in that input.")
            return
        market_id = match.group(0)
        state = self.state_for(user_id, chat_id)
        state.market_id = market_id
        state.market_slug_template = ""
        self._sync_market_counters(state)
        await update.message.reply_text(f"Market ID extracted and set: {market_id}")

    async def tradecurrent(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        if not context.args:
            await update.message.reply_text("Usage: /tradecurrent <market_id>")
            return
        state = self.state_for(user_id, chat_id)
        state.market_id = context.args[0].strip()
        state.auto_enabled = True
        state.trading_enabled = True
        state.force_all_trades = False
        self._sync_market_counters(state)
        await update.message.reply_text(
            f"Current market pinned: {state.market_id}\nAuto loop: ON\nTrade mode: profit-only"
        )

    async def starttrade(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        if context.args:
            state.market_id = context.args[0].strip()
            self._sync_market_counters(state)
        if not state.market_id:
            discovered = await asyncio.to_thread(
                self.polymarket.discover_current_btc_5m_market,
                self._proxy_for_state(state),
                state.market_slug_template,
            )
            if not discovered:
                if not settings.live_trading:
                    state.market_id = "paper-btc-5m-auto"
                    self._sync_market_counters(state)
                    await update.message.reply_text("Paper mode auto-market enabled.")
                else:
                    await update.message.reply_text("Live auto mode enabled. Scanning for market...")
            else:
                state.market_id = discovered[0]
                self._sync_market_counters(state)
                await update.message.reply_text(f"Auto-selected market: {discovered[1]}\nID: {state.market_id}")
        state.auto_enabled = True
        state.trading_enabled = True
        state.force_all_trades = False
        await update.message.reply_text("Background trading started.")

    async def stoptrade(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        state.trading_enabled = False
        state.auto_enabled = False
        await update.message.reply_text("Background trading stopped.")

    async def alltradeson(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        state.force_all_trades = True
        state.trading_enabled = True
        await update.message.reply_text("All-trades mode ON (high risk).")

    async def alltradesoff(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        state.force_all_trades = False
        await update.message.reply_text("All-trades mode OFF.")

    def _expected_edge_pct(self, signal: Signal) -> float:
        confidence_strength = max(0.0, (signal.confidence - 0.5) * 2.0)
        return confidence_strength * signal.expected_move_pct

    async def mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        mode = "LIVE" if settings.live_trading else "PAPER"
        await update.message.reply_text(f"Trading mode: {mode}")

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        if state.last_signal is None:
            await update.message.reply_text("No signal yet. Run /runonce")
            return
        sig = state.last_signal
        await update.message.reply_text(
            "\n".join(
                [
                    f"Last run: {state.last_run_at}",
                    f"Signal: {sig.direction} ({sig.confidence:.2%})",
                    f"Expected move: {sig.expected_move_pct:.3%}",
                    f"Position: {state.position.side or 'NONE'} size=${state.position.size_usd:.2f}",
                    f"Auto: {'ON' if state.auto_enabled else 'OFF'}",
                    f"Realized PnL: ${state.realized_pnl_usd:.2f}",
                    f"All-trades: {'ON' if state.force_all_trades else 'OFF'}",
                    f"Last action: {state.last_message or 'n/a'}",
                    f"Exposure this market: ${state.exposure_in_current_market:.2f}/${state.max_bet_usd:.2f}",
                ]
            )
        )

    async def pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_private_chat(update):
            await update.message.reply_text("For privacy, use /pnl in a direct DM with the bot.")
            return
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        spot = None
        try:
            snapshot = self.market.snapshot()
            spot = snapshot.fused_spot
        except Exception:
            spot = None
        unrealized = self._unrealized_pnl(state, spot) if spot is not None else 0.0
        total = state.realized_pnl_usd + unrealized
        closed = state.wins + state.losses
        win_rate = (state.wins / closed) if closed else 0.0
        lines = [
            "Your private PnL stats",
            f"Realized PnL: ${state.realized_pnl_usd:.2f}",
            f"Unrealized PnL: ${unrealized:.2f}" if spot is not None else "Unrealized PnL: n/a",
            f"Total PnL: ${total:.2f}",
            f"Trades executed: {state.trades_executed}",
            f"Hedges executed: {state.hedge_count}",
            f"Win/Loss (closed): {state.wins}/{state.losses} ({win_rate:.1%} win rate)",
            f"Total notional volume: ${state.volume_usd:.2f}",
        ]
        if state.position.side and state.position.size_usd > 0:
            lines.append(
                f"Open position: {state.position.side} ${state.position.size_usd:.2f} @ {state.position.entry_price:.2f}"
            )
        else:
            lines.append("Open position: NONE")
        await update.message.reply_text("\n".join(lines))

    async def dryrunlive(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        if not settings.live_trading:
            await update.message.reply_text("Dry run is for LIVE mode only.")
            return
        market_id = state.market_id
        side_override = ""
        amount_override = None
        if context.args:
            first = context.args[0].strip()
            if first.lower() not in {"yes", "no"}:
                market_id = first
            else:
                side_override = first.upper()
        if len(context.args) >= 2:
            second = context.args[1].strip()
            if second.lower() in {"yes", "no"}:
                side_override = second.upper()
            else:
                try:
                    amount_override = float(second)
                except ValueError:
                    amount_override = None
        if len(context.args) >= 3:
            try:
                amount_override = float(context.args[2].strip())
            except ValueError:
                amount_override = None
        if not market_id:
            discovered = self.polymarket.discover_current_btc_5m_market(
                self._proxy_for_state(state), state.market_slug_template,
            )
            if discovered:
                market_id = discovered[0]
            else:
                await update.message.reply_text("Dry run failed: no eligible live BTC market found.")
                return
        snapshot = self.market.snapshot()
        signal = self.strategy.generate(snapshot)
        side = side_override or ("YES" if signal.direction == "UP" else "NO")
        amount_usd = amount_override if (amount_override and amount_override > 0) else self.risk.bet_size(
            state.max_bet_usd, signal.confidence
        )
        effective_key = state.private_key or settings.polymarket_private_key
        result = self.polymarket.dry_run_order(
            private_key=effective_key,
            market_id=market_id,
            side=side,
            amount_usd=amount_usd,
            proxy_url=self._proxy_for_state(state),
            signature_type=state.polymarket_signature_type,
            funder=state.polymarket_funder,
        )
        await update.message.reply_text(
            "\n".join(
                [
                    f"Mode: LIVE dry run",
                    f"Market: {market_id}",
                    f"Side: {side}",
                    f"Amount: ${amount_usd:.2f}",
                    result.message,
                ]
            )
        )

    async def feestatus(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show current fee config and exempt list."""
        exempt_list = "\n".join(f"  • {a}" for a in sorted(FEE_EXEMPT_ADDRESSES)) or "  (none)"
        await update.message.reply_text(
            f"Protocol Fee Config\n"
            f"Rate: {FEE_PCT:.1%} per trade\n"
            f"Recipient: {FEE_RECIPIENT}\n"
            f"Exempt addresses:\n{exempt_list}"
        )

    async def feeexempt(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Usage: /feeexempt add|remove <address>"""
        args = context.args or []
        if len(args) != 2 or args[0] not in ("add", "remove"):
            await update.message.reply_text("Usage: /feeexempt add <address>\n       /feeexempt remove <address>")
            return
        action, addr = args[0], args[1].strip()
        if not addr.startswith("0x") or len(addr) != 42:
            await update.message.reply_text("Invalid address format. Must be 0x... (42 chars)")
            return
        if action == "add":
            fee_manager.add_exempt(addr)
            await update.message.reply_text(f"✅ {addr} added to fee exempt list.")
        else:
            fee_manager.remove_exempt(addr)
            await update.message.reply_text(f"✅ {addr} removed from fee exempt list.")

    async def setminedge(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Usage: /setminedge <pct>  e.g. /setminedge 1"""
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text(
                f"Usage: /setminedge <pct>\n"
                f"Example: /setminedge 1 → only trade when net expected edge ≥1%\n"
                f"Range: 0–20\n"
                f"Current: {state.min_edge_pct:.2%}"
            )
            return
        try:
            val = float(args[0])
            if not (0 <= val <= 20):
                raise ValueError
        except ValueError:
            await update.message.reply_text("Value must be 0–20 (percent). Example: /setminedge 1")
            return
        state.min_edge_pct = val / 100.0
        await update.message.reply_text(
            f"Min edge set to {state.min_edge_pct:.2%}.\n"
            f"Bot will skip signals where net expected return is below this."
        )

    async def setresolvebuf(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Usage: /setresolvebuf <seconds>  e.g. /setresolvebuf 90"""
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text(
                f"Usage: /setresolvebuf <seconds>\n"
                f"Example: /setresolvebuf 90 → skip new entries if <90s left in slot\n"
                f"Range: 0–270\n"
                f"Current: {state.resolve_buffer_sec}s"
            )
            return
        try:
            val = int(args[0])
            if not (0 <= val <= 270):
                raise ValueError
        except ValueError:
            await update.message.reply_text("Value must be 0–270 (seconds). Example: /setresolvebuf 90")
            return
        state.resolve_buffer_sec = val
        await update.message.reply_text(
            f"Resolution buffer set to {val}s.\n"
            f"Bot will skip new entries when <{val}s left in the 5-min slot."
        )

    async def settrendlock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Usage: /settrendlock <pct>  e.g. /settrendlock 1.5"""
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text(
                f"Usage: /settrendlock <pct>\n"
                f"Example: /settrendlock 1.5 → block trades that fight a 5h trend ≥1.5%\n"
                f"Set to 0 to disable.\n"
                f"Range: 0–10\n"
                f"Current: {state.trend_lock_pct:.2%}"
            )
            return
        try:
            val = float(args[0])
            if not (0 <= val <= 10):
                raise ValueError
        except ValueError:
            await update.message.reply_text("Value must be 0–10 (percent). Example: /settrendlock 1.5")
            return
        state.trend_lock_pct = val / 100.0
        msg = (
            f"Trend lock set to {state.trend_lock_pct:.2%}.\n"
            f"Bot will skip entries that fight a 5h BTC trend stronger than this."
            if val > 0 else
            "Trend lock disabled. Bot will trade in any trend direction."
        )
        await update.message.reply_text(msg)

    async def setminconf(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Usage: /setminconf <pct>  e.g. /setminconf 65"""
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text(
                f"Usage: /setminconf <pct>\n"
                f"Example: /setminconf 65 → bot only trades when confidence ≥65%\n"
                f"Range: 50–95\n"
                f"Current: {state.min_confidence:.0%}"
            )
            return
        try:
            val = float(args[0])
            if not (50 <= val <= 95):
                raise ValueError
        except ValueError:
            await update.message.reply_text("Value must be 50–95. Example: /setminconf 65")
            return
        state.min_confidence = val / 100.0
        self._persist_user_wallet_mode(
            user_id, state.polymarket_signature_type, state.polymarket_funder,
            state.take_profit_pct, state.stop_loss_pct, state.block_dca_on_loss,
            state.dca_block_loss_pct, state.min_confidence, state.order_size_pct,
            state.token_dca_block_pct,
        )
        await update.message.reply_text(
            f"Min confidence set to {state.min_confidence:.0%}.\n"
            f"Bot will skip any signal below this threshold."
        )

    async def setorderpct(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Usage: /setorderpct <pct>  e.g. /setorderpct 25"""
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text(
                f"Usage: /setorderpct <pct>\n"
                f"Example: /setorderpct 25 → each order uses up to 25% of max_bet\n"
                f"Range: 1–100\n"
                f"Current: {state.order_size_pct:.0%} (cap=${state.max_bet_usd * state.order_size_pct:.2f})"
            )
            return
        try:
            val = float(args[0])
            if not (1 <= val <= 100):
                raise ValueError
        except ValueError:
            await update.message.reply_text("Value must be 1–100. Example: /setorderpct 25")
            return
        state.order_size_pct = val / 100.0
        self._persist_user_wallet_mode(
            user_id, state.polymarket_signature_type, state.polymarket_funder,
            state.take_profit_pct, state.stop_loss_pct, state.block_dca_on_loss,
            state.dca_block_loss_pct, state.min_confidence, state.order_size_pct,
            state.token_dca_block_pct,
        )
        await update.message.reply_text(
            f"Order size set to {state.order_size_pct:.0%} of max_bet.\n"
            f"With max_bet=${state.max_bet_usd:.2f}, each order is capped at ${state.max_bet_usd * state.order_size_pct:.2f}."
        )

    async def settokendca(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Usage: /settokendca <pct>  e.g. /settokendca 20"""
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text(
                f"Usage: /settokendca <pct>\n"
                f"Example: /settokendca 20 → block DCA if token dropped >20% from your avg cost\n"
                f"Range: 1–99\n"
                f"Current: {state.token_dca_block_pct:.0%}"
            )
            return
        try:
            val = float(args[0])
            if not (1 <= val <= 99):
                raise ValueError
        except ValueError:
            await update.message.reply_text("Value must be 1–99. Example: /settokendca 20")
            return
        state.token_dca_block_pct = val / 100.0
        self._persist_user_wallet_mode(
            user_id, state.polymarket_signature_type, state.polymarket_funder,
            state.take_profit_pct, state.stop_loss_pct, state.block_dca_on_loss,
            state.dca_block_loss_pct, state.min_confidence, state.order_size_pct,
            state.token_dca_block_pct,
        )
        await update.message.reply_text(f"Token DCA block set to {state.token_dca_block_pct:.0%} drop from avg cost.")

    async def setcb(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Usage: /setcb <daily_loss_mult> <max_consec_losses> <cooldown_minutes>"""
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        args = context.args or []
        if len(args) != 3:
            await update.message.reply_text(
                f"Usage: /setcb <daily_mult> <max_consec> <cooldown_min>\n"
                f"Example: /setcb 2 3 30\n"
                f"  daily_mult: max daily loss = mult × max_bet\n"
                f"  max_consec: consecutive losses before cooldown\n"
                f"  cooldown_min: minutes to pause after hitting consec limit\n"
                f"Current: {state.circuit_breaker.MAX_DAILY_LOSS_MULTIPLIER}x / "
                f"{state.circuit_breaker.MAX_CONSECUTIVE_LOSSES} / "
                f"{state.circuit_breaker.COOLDOWN_MINUTES}min"
            )
            return
        try:
            mult = float(args[0])
            consec = int(args[1])
            cooldown = int(args[2])
            if mult <= 0 or consec < 1 or cooldown < 1:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Invalid. Example: /setcb 2 3 30")
            return
        state.circuit_breaker.MAX_DAILY_LOSS_MULTIPLIER = mult
        state.circuit_breaker.MAX_CONSECUTIVE_LOSSES = consec
        state.circuit_breaker.COOLDOWN_MINUTES = cooldown
        await update.message.reply_text(
            f"Circuit breaker updated:\n"
            f"Daily loss limit: {mult}x max_bet (${state.max_bet_usd * mult:.2f})\n"
            f"Max consecutive losses before cooldown: {consec}\n"
            f"Cooldown duration: {cooldown} min"
        )

    async def cbstatus(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show live circuit breaker counters and limits."""
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        cb = state.circuit_breaker
        block = cb.check(state.max_bet_usd)
        lines = [
            "Circuit Breaker Status",
            f"Status: {'🔴 BLOCKED — ' + block if block else '🟢 OPEN (trading allowed)'}",
            f"Daily loss: ${cb.daily_loss_usd:.2f} / ${state.max_bet_usd * cb.MAX_DAILY_LOSS_MULTIPLIER:.2f} limit",
            f"Consecutive losses: {cb.consecutive_losses} / {cb.MAX_CONSECUTIVE_LOSSES} max",
            f"Today: {cb.daily_wins}W / {cb.daily_losses}L",
            f"Cooldown until: {cb.cooldown_until or 'none'}",
            f"Settings: {cb.MAX_DAILY_LOSS_MULTIPLIER}x daily, {cb.MAX_CONSECUTIVE_LOSSES} consec, {cb.COOLDOWN_MINUTES}min cooldown",
            f"Use /setcb to adjust limits.",
        ]
        await update.message.reply_text("\n".join(lines))

    async def reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        self.chat_state[user_id] = ChatState(
            user_id=user_id,
            chat_id=chat_id,
            private_key=self._stored_user_keys.get(user_id, ""),
            polymarket_signature_type=int(
                self._stored_user_wallet_modes.get(user_id, {}).get("signature_type", settings.polymarket_signature_type)
            ),
            polymarket_funder=str(self._stored_user_wallet_modes.get(user_id, {}).get("funder", settings.polymarket_funder)),
            take_profit_pct=float(self._stored_user_wallet_modes.get(user_id, {}).get("take_profit_pct", 0.60)),
            stop_loss_pct=float(self._stored_user_wallet_modes.get(user_id, {}).get("stop_loss_pct", 0.15)),
            block_dca_on_loss=bool(self._stored_user_wallet_modes.get(user_id, {}).get("block_dca_on_loss", True)),
            dca_block_loss_pct=float(self._stored_user_wallet_modes.get(user_id, {}).get("dca_block_loss_pct", 0.05)),
        )
        await update.message.reply_text("State cleared for your user session.")

    async def walletcheck(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        effective_key = state.private_key or settings.polymarket_private_key
        if not effective_key:
            await update.message.reply_text("Wallet check failed: missing key.")
            return
        message = self.polymarket.wallet_diagnostics(
            private_key=effective_key,
            proxy_url=self._proxy_for_state(state),
            signature_type=state.polymarket_signature_type,
            funder=state.polymarket_funder,
        )
        await update.message.reply_text(message)

    async def runonce(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        msg = await self.execute_cycle(user_id, chat_id)
        await update.message.reply_text(msg)

    async def autoon(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        state.auto_enabled = True
        await update.message.reply_text(f"Auto loop enabled ({settings.auto_loop_interval_sec}s checks).")

    async def autooff(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id, chat_id = self._user_chat_ids(update)
        state = self.state_for(user_id, chat_id)
        state.auto_enabled = False
        await update.message.reply_text("Auto loop disabled.")

    async def execute_cycle(self, user_id: int, chat_id: int) -> str:
        return await asyncio.to_thread(self._execute_cycle_sync, user_id, chat_id)

    # ══════════════════════════════════════════════════════════════════════════
    # CORE TRADING CYCLE — this is where the death spiral was happening
    # ══════════════════════════════════════════════════════════════════════════
    def _execute_cycle_sync(self, user_id: int, chat_id: int) -> str:
        state = self.state_for(user_id, chat_id)
        proxy_url = self._proxy_for_state(state)

        market_update_note = ""

        if settings.live_trading and (state.auto_enabled or state.trading_enabled) and (not self._has_open_position(state)):
            discovered_live = self.polymarket.discover_current_btc_5m_market(proxy_url, state.market_slug_template)
            if discovered_live and discovered_live[0] != state.market_id:
                state.market_id = discovered_live[0]
                market_update_note = f"Market auto-updated: {discovered_live[1]} ({state.market_id}) | "

        if not state.market_id:
            discovered = self.polymarket.discover_current_btc_5m_market(proxy_url, state.market_slug_template)
            if discovered:
                state.market_id = discovered[0]
            elif not settings.live_trading:
                state.market_id = "paper-btc-5m-auto"

        # ── CRITICAL: sync market counters whenever market_id may have changed ─
        self._sync_market_counters(state)

        # ── Circuit breaker check ─────────────────────────────────────────────
        cb_block = state.circuit_breaker.check(state.max_bet_usd)
        if cb_block:
            state.last_message = cb_block
            return state.last_message

        snapshot = self.market.snapshot()
        signal = self.strategy.generate(snapshot)
        expected_edge = self._expected_edge_pct(signal)
        net_expected_edge = expected_edge - settings.cost_buffer_pct
        state.last_signal = signal
        state.last_run_at = datetime.now(timezone.utc).isoformat()

        if not state.market_id:
            state.last_message = (
                f"No trade: {signal.direction} conf={signal.confidence:.2%}. No eligible market."
            )
            return state.last_message

        effective_key = state.private_key or settings.polymarket_private_key
        if settings.live_trading and not effective_key:
            state.last_message = "Live mode requires your key: use /setkey <private_key>"
            return state.last_message

        # ── Minimum confidence filter ─────────────────────────────────────────
        if signal.confidence < state.min_confidence:
            state.last_message = (
                f"No trade: confidence {signal.confidence:.2%} < min threshold "
                f"{state.min_confidence:.2%} | {signal.reason}"
            )
            return state.last_message

        # ── Edge filter — only enter when expected return justifies the cost ──
        # (Exits / holds are allowed regardless of edge)
        if net_expected_edge < state.min_edge_pct and not (state.position.side and state.position.size_usd > 0):
            state.last_message = (
                f"No trade: edge {net_expected_edge:.3%} < min {state.min_edge_pct:.3%} | "
                f"conf={signal.confidence:.2%} move={signal.expected_move_pct:.3%}"
            )
            return state.last_message

        # ── Market resolution timing — skip NEW entries near slot expiry ──────
        # Each 5-min Polymarket slot ends at the next multiple of 300 seconds.
        # Don't open a new position when there's less than resolve_buffer_sec left.
        if not (state.position.side and state.position.size_usd > 0):
            now_ts = int(datetime.now(timezone.utc).timestamp())
            secs_left = ((now_ts // 300) + 1) * 300 - now_ts
            if secs_left < state.resolve_buffer_sec:
                state.last_message = (
                    f"No trade: only {secs_left}s left in slot (buffer={state.resolve_buffer_sec}s). "
                    f"Waiting for next 5m window."
                )
                return state.last_message

        # ── Macro trend lock — don't fight a strong 5h trend ─────────────────
        # If the 5h return strongly disagrees with the trade direction, skip it.
        # We still allow exits / hedges to proceed (only blocks new entries).
        if not (state.position.side and state.position.size_usd > 0):
            trend_against = (
                signal.direction == "UP"   and signal.ret_5h < -state.trend_lock_pct
            ) or (
                signal.direction == "DOWN" and signal.ret_5h >  state.trend_lock_pct
            )
            if trend_against:
                state.last_message = (
                    f"No trade: {signal.direction} signal fights 5h trend "
                    f"(ret_5h={signal.ret_5h:.3%}, lock={state.trend_lock_pct:.3%})"
                )
                return state.last_message

        desired_side = "YES" if signal.direction == "UP" else "NO"
        # per_order_cap: user-adjustable fraction of max_bet (default 25%)
        per_order_cap = round(state.max_bet_usd * state.order_size_pct, 2)
        size = max(0.01, min(self.risk.bet_size(state.max_bet_usd, signal.confidence), per_order_cap))

        position_move_pct = 0.0
        if state.position.side and state.position.size_usd > 0 and state.position.entry_price > 0:
            position_move_pct = self._signed_move_pct(
                state.position.side,
                state.position.entry_price,
                snapshot.fused_spot,
            )

        # ── take-profit exit (unchanged logic) ────────────────────────────────
        if state.position.side and state.position.size_usd > 0 and position_move_pct >= state.take_profit_pct:
            exit_side = "NO" if state.position.side == "YES" else "YES"
            exit_size = round(max(1.0, state.position.size_usd), 2)
            exit_result, used_exit_size = self._attempt_risk_exit(
                state=state,
                effective_key=effective_key,
                market_id=state.market_id,
                exit_side=exit_side,
                exit_size=exit_size,
                proxy_url=proxy_url,
            )
            if exit_result.ok:
                close_size = min(state.position.size_usd, used_exit_size)
                realized = self._record_realized_pnl(
                    state,
                    side=state.position.side,
                    entry_price=state.position.entry_price,
                    current_spot=snapshot.fused_spot,
                    closed_size_usd=close_size,
                )
                state.circuit_breaker.record_trade(realized)
                state.position.size_usd = round(max(0.0, state.position.size_usd - close_size), 2)
                if state.position.size_usd == 0:
                    state.position.side = None
                    state.position.entry_price = 0.0
                    state.position.entry_token_price = 0.0
                    state.position.opened_at = None
                state.trades_executed += 1
                state.volume_usd += close_size
                state.last_message = (
                    f"TAKE PROFIT {exit_side} ${close_size:.2f} @ {snapshot.fused_spot:.2f} | "
                    f"move={position_move_pct:.2%} | realized=${realized:.2f} | {exit_result.message}"
                )
                return state.last_message
            # If exit failed because market expired, clear position and move on
            if "no active clob orderbook" in exit_result.message.lower():
                state.position.side = None
                state.position.size_usd = 0.0
                state.position.entry_price = 0.0
                state.position.entry_token_price = 0.0
                state.position.opened_at = None
                state.market_id = ""
                state.last_message = f"TAKE PROFIT exit failed (market expired, position cleared): {exit_result.message}"
                return state.last_message
            state.last_message = f"TAKE PROFIT exit failed: {exit_result.message}"
            return state.last_message

        # ── stop-loss exit (unchanged logic) ──────────────────────────────────
        if state.position.side and state.position.size_usd > 0 and position_move_pct <= -state.stop_loss_pct:
            exit_side = "NO" if state.position.side == "YES" else "YES"
            exit_size = round(max(1.0, state.position.size_usd), 2)
            exit_result, used_exit_size = self._attempt_risk_exit(
                state=state,
                effective_key=effective_key,
                market_id=state.market_id,
                exit_side=exit_side,
                exit_size=exit_size,
                proxy_url=proxy_url,
            )
            if exit_result.ok:
                close_size = min(state.position.size_usd, used_exit_size)
                realized = self._record_realized_pnl(
                    state,
                    side=state.position.side,
                    entry_price=state.position.entry_price,
                    current_spot=snapshot.fused_spot,
                    closed_size_usd=close_size,
                )
                state.circuit_breaker.record_trade(realized)
                state.position.size_usd = round(max(0.0, state.position.size_usd - close_size), 2)
                if state.position.size_usd == 0:
                    state.position.side = None
                    state.position.entry_price = 0.0
                    state.position.entry_token_price = 0.0
                    state.position.opened_at = None
                state.trades_executed += 1
                state.volume_usd += close_size
                state.last_message = (
                    f"STOP LOSS {exit_side} ${close_size:.2f} @ {snapshot.fused_spot:.2f} | "
                    f"move={position_move_pct:.2%} | realized=${realized:.2f} | {exit_result.message}"
                )
                return state.last_message
            # If exit failed because market expired, clear position and move on
            if "no active clob orderbook" in exit_result.message.lower():
                state.position.side = None
                state.position.size_usd = 0.0
                state.position.entry_price = 0.0
                state.position.entry_token_price = 0.0
                state.position.opened_at = None
                state.market_id = ""
                state.last_message = f"STOP LOSS exit failed (market expired, position cleared): {exit_result.message}"
                return state.last_message
            state.last_message = f"STOP LOSS exit failed: {exit_result.message}"
            return state.last_message

        # ── Estimate current token price from signal confidence ───────────────
        # YES token at confidence X ≈ X (markets are roughly calibrated).
        # Used for both hedge logic and spiral guards below.
        estimated_token_price = signal.confidence if desired_side == "YES" else (1.0 - signal.confidence)

        # ── hedge logic ───────────────────────────────────────────────────────
        if self.risk.should_hedge(
            position=state.position,
            current_spot=snapshot.fused_spot,
            signal_direction=signal.direction,
            signal_confidence=signal.confidence,
            current_token_price=estimated_token_price,
        ):
            hedge_side = "NO" if state.position.side == "YES" else "YES"
            hedge_size = min(round(max(1.0, state.position.size_usd * 0.5), 2), per_order_cap)
            hedge_result = self.polymarket.place_order(
                private_key=effective_key,
                market_id=state.market_id,
                side=hedge_side,
                amount_usd=hedge_size,
                proxy_url=proxy_url,
                signature_type=state.polymarket_signature_type,
                funder=state.polymarket_funder,
            )
            if hedge_result.ok and state.position.side:
                close_size = min(state.position.size_usd, hedge_size)
                realized = self._record_realized_pnl(
                    state,
                    side=state.position.side,
                    entry_price=state.position.entry_price,
                    current_spot=snapshot.fused_spot,
                    closed_size_usd=close_size,
                )
                state.circuit_breaker.record_trade(realized)
                state.position.size_usd = round(max(0.0, state.position.size_usd - close_size), 2)
                if state.position.size_usd == 0:
                    state.position.side = None
                    state.position.entry_price = 0.0
                    state.position.entry_token_price = 0.0
                    state.position.opened_at = None
                state.trades_executed += 1
                state.hedge_count += 1
                state.volume_usd += close_size
                state.last_message = (
                    f"HEDGE {hedge_side} ${close_size:.2f} -> {hedge_result.message} | realized=${realized:.2f}"
                )
                return state.last_message
            # If hedge failed because market expired, abandon stale position and move on
            if "no active clob orderbook" in hedge_result.message.lower():
                state.position.side = None
                state.position.size_usd = 0.0
                state.position.entry_price = 0.0
                state.position.entry_token_price = 0.0
                state.position.opened_at = None
                state.market_id = ""
                state.last_message = (
                    f"HEDGE {hedge_side} ${hedge_size:.2f} -> market expired, position cleared. "
                    f"Bot will discover next market slot automatically."
                )
                return state.last_message
            state.last_message = f"HEDGE {hedge_side} ${hedge_size:.2f} -> {hedge_result.message}"
            return state.last_message

        # ── DEATH SPIRAL GUARDS — run BEFORE placing any new entry order ──────
        block_reason = self._check_spiral_guards(state, desired_side, estimated_token_price)
        if block_reason:
            state.last_message = block_reason
            return state.last_message

        # ── legacy BTC-spot DCA block (kept as secondary guard) ───────────────
        if (
            state.block_dca_on_loss
            and state.position.side is not None
            and state.position.side == desired_side
            and state.position.size_usd > 0
            and position_move_pct <= -state.dca_block_loss_pct
        ):
            state.last_message = (
                f"No trade: DCA into losing position blocked "
                f"(side={state.position.side}, btc_move={position_move_pct:.2%}, "
                f"threshold={state.dca_block_loss_pct:.2%})."
            )
            return state.last_message

        # ── Protocol fee — collect before placing entry order ─────────────────
        fee_result = fee_manager.collect(
            private_key=effective_key,
            trade_usd=size,
            proxy_url=proxy_url,
        )
        if not fee_result.ok and not fee_result.skipped:
            from .fee_manager import FEE_SOFT_FAIL
            if not FEE_SOFT_FAIL:
                state.last_message = f"Trade blocked: fee collection failed — {fee_result.message}"
                return state.last_message
            # Soft-fail: log warning, proceed anyway
            import logging as _logging
            _logging.getLogger(__name__).warning("Fee soft-fail: %s", fee_result.message)

        # ── place the entry order ─────────────────────────────────────────────
        result = self.polymarket.place_order(
            private_key=effective_key,
            market_id=state.market_id,
            side=desired_side,
            amount_usd=size,
            proxy_url=proxy_url,
            signature_type=state.polymarket_signature_type,
            funder=state.polymarket_funder,
        )

        if (not result.ok) and settings.live_trading and "no active clob orderbook" in result.message.lower():
            state.market_id = ""

        if (not result.ok) and settings.live_trading and "geoblocked" in result.message.lower():
            # Geoblock — don't clear market, just skip this tick and retry
            pass

        if result.ok:
            # ── Update token price tracking with the actual fill ──────────────
            # If your result object carries fill_price, use it here.
            # Otherwise we fall back to the signal-derived estimate.
            fill_token_price = getattr(result, "fill_price", None) or estimated_token_price
            self._update_token_tracking(state, fill_usd=size, token_price=fill_token_price)

            prev_side = state.position.side
            prev_size = state.position.size_usd
            prev_entry = state.position.entry_price

            if prev_side is None or prev_size <= 0:
                state.position.side = desired_side
                state.position.size_usd = size
                state.position.entry_price = snapshot.fused_spot
                state.position.entry_token_price = fill_token_price
                state.position.opened_at = datetime.now(timezone.utc)
            elif prev_side == desired_side:
                total_size = prev_size + size
                weighted_entry = ((prev_entry * prev_size) + (snapshot.fused_spot * size)) / max(total_size, 1e-9)
                # Weighted average token price too
                prev_token = state.position.entry_token_price or fill_token_price
                weighted_token = ((prev_token * prev_size) + (fill_token_price * size)) / max(total_size, 1e-9)
                state.position.side = desired_side
                state.position.size_usd = round(total_size, 2)
                state.position.entry_price = weighted_entry
                state.position.entry_token_price = weighted_token
                # keep original opened_at — position age from first entry
            else:
                close_size = min(prev_size, size)
                self._record_realized_pnl(
                    state,
                    side=prev_side,
                    entry_price=prev_entry,
                    current_spot=snapshot.fused_spot,
                    closed_size_usd=close_size,
                )
                remaining_prev = round(max(0.0, prev_size - close_size), 2)
                remaining_new = round(max(0.0, size - close_size), 2)
                if remaining_prev > 0:
                    state.position.side = prev_side
                    state.position.size_usd = remaining_prev
                    state.position.entry_price = prev_entry
                elif remaining_new > 0:
                    state.position.side = desired_side
                    state.position.size_usd = remaining_new
                    state.position.entry_price = snapshot.fused_spot
                    state.position.entry_token_price = fill_token_price
                    state.position.opened_at = datetime.now(timezone.utc)
                else:
                    state.position.side = None
                    state.position.size_usd = 0.0
                    state.position.entry_price = 0.0
                    state.position.entry_token_price = 0.0
                    state.position.opened_at = None

            state.trades_executed += 1
            state.volume_usd += size

        fee_note = (
            f" | fee=${fee_result.fee_usd:.4f}" if (fee_result.ok and not fee_result.skipped)
            else " | fee=exempt" if fee_result.skipped
            else f" | fee=FAILED({fee_result.message[:30]})"
        )
        state.last_message = (
            f"{market_update_note}{desired_side} ${size:.2f} @ {snapshot.fused_spot:.2f} | "
            f"conf={signal.confidence:.2%} | gross_edge={expected_edge:.3%} | "
            f"net_edge={net_expected_edge:.3%}{fee_note} | "
            f"{result.message}"
        )
        return state.last_message

    async def auto_tick(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        app = context.application
        for user_id, state in list(self.chat_state.items()):
            if not state.auto_enabled:
                continue
            try:
                if state.trading_enabled or state.force_all_trades:
                    msg = await self.execute_cycle(user_id, state.chat_id)
                    await app.bot.send_message(chat_id=state.chat_id, text=f"[AUTO] {msg}")
            except Exception as exc:
                await app.bot.send_message(chat_id=state.chat_id, text=f"[AUTO] cycle error: {exc}")

    def build_app(self) -> Application:
        application = Application.builder().token(settings.telegram_bot_token).build()
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help))
        application.add_handler(CommandHandler("whoami", self.whoami))
        application.add_handler(CommandHandler("myconfig", self.myconfig))
        application.add_handler(CommandHandler("keyhelp", self.keyhelp))
        application.add_handler(CommandHandler("setkey", self.setkey))
        application.add_handler(CommandHandler("clearkey", self.clearkey))
        application.add_handler(CommandHandler("setsigtype", self.setsigtype))
        application.add_handler(CommandHandler("setfunder", self.setfunder))
        application.add_handler(CommandHandler("clearfunder", self.clearfunder))
        application.add_handler(CommandHandler("setstops", self.setstops))
        application.add_handler(CommandHandler("setdca", self.setdca))
        application.add_handler(CommandHandler("setvpn", self.setvpn))
        application.add_handler(CommandHandler("vpnon", self.vpnon))
        application.add_handler(CommandHandler("vpnoff", self.vpnoff))
        application.add_handler(CommandHandler("vpnstatus", self.vpnstatus))
        application.add_handler(CommandHandler("setmax", self.setmax))
        application.add_handler(CommandHandler("setmarket", self.setmarket))
        application.add_handler(CommandHandler("setmarketurl", self.setmarketurl))
        application.add_handler(CommandHandler("tradecurrent", self.tradecurrent))
        application.add_handler(CommandHandler("starttrade", self.starttrade))
        application.add_handler(CommandHandler("stoptrade", self.stoptrade))
        application.add_handler(CommandHandler("alltradeson", self.alltradeson))
        application.add_handler(CommandHandler("alltradesoff", self.alltradesoff))
        application.add_handler(CommandHandler("markethelp", self.markethelp))
        application.add_handler(CommandHandler("mode", self.mode))
        application.add_handler(CommandHandler("status", self.status))
        application.add_handler(CommandHandler("pnl", self.pnl))
        application.add_handler(CommandHandler("dryrunlive", self.dryrunlive))
        application.add_handler(CommandHandler("walletcheck", self.walletcheck))
        application.add_handler(CommandHandler("runonce", self.runonce))
        application.add_handler(CommandHandler("autoon", self.autoon))
        application.add_handler(CommandHandler("autooff", self.autooff))
        application.add_handler(CommandHandler("reset", self.reset))
        application.add_handler(CommandHandler("setpin", self.setpin))
        application.add_handler(CommandHandler("unlock", self.unlock))
        application.add_handler(CommandHandler("lock", self.lock))
        application.add_handler(CommandHandler("vaultstatus", self.vaultstatus))
        application.add_handler(CommandHandler("feestatus", self.feestatus))
        application.add_handler(CommandHandler("feeexempt", self.feeexempt))
        application.add_handler(CommandHandler("setminedge", self.setminedge))
        application.add_handler(CommandHandler("setresolvebuf", self.setresolvebuf))
        application.add_handler(CommandHandler("settrendlock", self.settrendlock))
        application.add_handler(CommandHandler("setminconf", self.setminconf))
        application.add_handler(CommandHandler("setorderpct", self.setorderpct))
        application.add_handler(CommandHandler("settokendca", self.settokendca))
        application.add_handler(CommandHandler("setcb", self.setcb))
        application.add_handler(CommandHandler("cbstatus", self.cbstatus))
        first_run = max(1, min(5, settings.auto_loop_interval_sec))
        application.job_queue.run_repeating(
            self.auto_tick,
            interval=settings.auto_loop_interval_sec,
            first=first_run,
        )
        return application


async def run_bot() -> None:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")
    bot = TradingBot()
    app = bot.build_app()
    await app.initialize()
    await bot.register_command_menu(app)
    await app.start()
    await app.updater.start_polling()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
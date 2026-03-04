# BTC 5m Polymarket Agent (Local)

Local Telegram trading agent for 5-minute BTC prediction markets with:
- Signal generation from last 5 hours (60 candles) of BTC price
- Multi-source checks (Binance + CoinGecko + optional Chainlink)
- Risk controls (max bet, confidence threshold, drawdown guard, hedge trigger)
- Paper mode by default; live mode is opt-in

## Important
This is **not** guaranteed profit software. Real-money trading can lose funds quickly.
Start in paper mode and validate behavior before enabling live execution.

## Multi-user command separation
- This bot now stores state per Telegram user ID (not shared per chat), so each user has isolated key, limits, and position state.
- For clean separation from your personal OpenClaw control commands, use a dedicated bot token for this project (recommended).
- If you must reuse one bot, run only one poller process at a time per token.

## Commands
- `/start` - Register chat + show quick help
- `/help` - Show command list
- `/keyhelp` - Explain key requirements by mode
- `/setkey <private_key>` - Set your regular EOA private key for your user profile (persisted locally on this host)
- `/clearkey` - Clear your key from this bot process
- `/setvpn <default|mx>` - Set routing preference for your requests
- `/vpnstatus` - Show whether VPN/proxy routing is active
- `/setmax <usd>` - Set max bet amount in USD for this chat
- `/setmarket <market_id>` - Set market id for trading
- `/tradecurrent <market_id>` - Pin one current 5m market and enable auto
- `/starttrade [market_id]` - Start background trading (profit-only gate)
- `/stoptrade` - Stop background trading
- `/mode` - Show current mode (paper/live)
- `/status` - Show latest signal and risk state
- `/runonce` - Run one prediction + optional execution cycle now
- `/autoon` - Enable automatic monitoring/execution loop
- `/autooff` - Disable automatic monitoring/execution loop
- `/alltradeson` - Trade every cycle (bypasses profit/confidence gates, high risk)
- `/alltradesoff` - Restore gated trading behavior
- `/reset` - Clear chat settings and local position state

Profit-only gate: default trading requires expected edge > `MIN_EXPECTED_EDGE_PCT + COST_BUFFER_PCT`.

Key behavior:
- Paper mode: no private key required.
- Live mode: each user must set a private key with `/setkey` before trading.

VPN behavior:
- Per-user `/setvpn mx` uses MX proxy routing when server env `VPN_MX_PROXY_URL` is configured.
- Backward compatibility: `VPN_AU_PROXY_URL` still works if `VPN_MX_PROXY_URL` is empty.
- This is proxy-based request routing, not OS-level VPN namespace isolation per user.
- If `VPN_WG_CONF` is set, `/vpnon` and `/vpnoff` attempt host WireGuard up/down using `sudo -n wg-quick`.
- For passwordless command execution, add a sudoers rule for your bot user.

## Setup
1. Create bot with BotFather and set token.
2. Copy `.env.example` to `.env` and fill values.
3. Install deps:
   ```bash
   pip install -r requirements.txt
   ```
4. Run:
   ```bash
   python -m src.main
   ```

If `TELEGRAM_BOT_TOKEN` is empty, the app automatically falls back to your existing OpenClaw token from `~/.openclaw/openclaw.json` (same bot you already configured).

## Live trading note
`LIVE_TRADING=true` enables live path. The default implementation includes a conservative execution interface and paper fallback. You should extend `src/polymarket_client.py` for your exact market/order flow and testnet-first rollout.

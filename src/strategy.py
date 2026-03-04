from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, pstdev

from .data_sources import MarketSnapshot


@dataclass
class Signal:
    direction: str        # "UP" or "DOWN"
    confidence: float     # 0.50 – 0.95
    expected_move_pct: float
    reason: str


class BtcFiveMinuteStrategy:
    # ── Lookback windows (in bars, 1 bar = 5 minutes) ─────────────────────────
    BARS_1H  = 12   # 60 min  — was wrongly 13 in the original (off-by-one)
    BARS_5H  = 60   # 300 min
    SHORT_MA = 6    # 30 min short moving average
    LONG_MA  = 24   # 2h long moving average
    MIN_BARS = 25   # minimum snapshot length before we'll produce a signal

    # ── raw_score weighting: ret_1h, ret_5h, ma_bias ──────────────────────────
    W_RET_1H  = 2.0
    W_RET_5H  = 1.0
    W_MA_BIAS = 3.0

    # ── confidence tuning ─────────────────────────────────────────────────────
    NORM_SCALE        = 15.0   # divisor that converts raw_score/vol → norm
    CONF_SENSITIVITY  = 0.07   # how much each unit of |norm| lifts confidence
    CONF_BASE         = 0.50   # floor before any signal adjustment
    CONF_MAX          = 0.95   # ceiling
    AGREEMENT_BONUS   = 0.06   # bonus when all 3 indicators agree on direction
    DIV_PENALTY       = 3.0    # multiplier on source divergence penalty
    DIV_ABORT_THRESH  = 0.015  # if src divergence > 1.5%, skip the trade entirely

    # ── expected move tuning ──────────────────────────────────────────────────
    EXP_MOVE_RET_WEIGHT = 1 / 12
    EXP_MOVE_MA_WEIGHT  = 1 / 8
    EXP_MOVE_VOL_WEIGHT = 0.5   # NEW: volatility contribution to expected move

    def generate(self, snapshot: MarketSnapshot) -> Signal:
        prices = snapshot.prices_5m

        # ── Guard: not enough data ────────────────────────────────────────────
        if len(prices) < self.MIN_BARS:
            return Signal(
                direction="UP",
                confidence=self.CONF_BASE,
                expected_move_pct=0.0,
                reason=f"insufficient data: {len(prices)} bars < {self.MIN_BARS} required",
            )

        latest = prices[-1]

        # ── Returns ───────────────────────────────────────────────────────────
        # FIX: was prices[-13] → 65-min lookback. Correct 1h = 12 bars.
        ret_1h = (latest / prices[-self.BARS_1H]) - 1.0
        ret_5h = (latest / prices[-min(self.BARS_5H, len(prices))]) - 1.0

        # ── Moving averages ───────────────────────────────────────────────────
        short_ma = mean(prices[-self.SHORT_MA:])
        long_ma  = mean(prices[-min(self.LONG_MA, len(prices)):])
        ma_bias  = (short_ma / long_ma) - 1.0

        # ── Volatility ────────────────────────────────────────────────────────
        returns = [prices[i] / prices[i - 1] - 1.0 for i in range(1, len(prices))]
        vol = max(pstdev(returns), 1e-6)

        # ── Source divergence ─────────────────────────────────────────────────
        source_divergence = (
            abs(snapshot.binance_spot - snapshot.coingecko_spot)
            / max(snapshot.fused_spot, 1)
        )

        # ── Hard abort on extreme data divergence ─────────────────────────────
        # Original code just penalised confidence and clipped to 0.50, meaning
        # the bot still traded when Binance/CG wildly disagreed. Now we skip.
        if source_divergence > self.DIV_ABORT_THRESH:
            return Signal(
                direction="UP",
                confidence=self.CONF_BASE,
                expected_move_pct=0.0,
                reason=(
                    f"SKIPPED: source divergence {source_divergence:.4%} "
                    f"> threshold {self.DIV_ABORT_THRESH:.4%}"
                ),
            )

        # ── Directional score ─────────────────────────────────────────────────
        raw_score = (
            (ret_1h   * self.W_RET_1H)
            + (ret_5h * self.W_RET_5H)
            + (ma_bias * self.W_MA_BIAS)
        )
        norm      = raw_score / (vol * self.NORM_SCALE)
        direction = "UP" if norm >= 0 else "DOWN"

        # ── Signal agreement bonus ────────────────────────────────────────────
        # If all three sub-signals agree on direction, we have more conviction.
        signals_up = sum([ret_1h > 0, ret_5h > 0, ma_bias > 0])
        all_agree  = signals_up == 3 or signals_up == 0
        agreement_bonus = self.AGREEMENT_BONUS if all_agree else 0.0

        # ── Confidence ────────────────────────────────────────────────────────
        confidence = max(
            self.CONF_BASE,
            min(
                self.CONF_MAX,
                self.CONF_BASE
                + (abs(norm) * self.CONF_SENSITIVITY)
                + agreement_bonus
                - (source_divergence * self.DIV_PENALTY),
            ),
        )

        # ── Expected move ─────────────────────────────────────────────────────
        # FIX: original ignored volatility entirely.
        # Now: blend of 1h momentum, MA divergence, and current vol regime.
        expected_move_pct = (
            abs(ret_1h)  * self.EXP_MOVE_RET_WEIGHT
            + abs(ma_bias) * self.EXP_MOVE_MA_WEIGHT
            + vol          * self.EXP_MOVE_VOL_WEIGHT
        )

        reason = (
            f"ret_1h={ret_1h:.4%}, ret_5h={ret_5h:.4%}, ma_bias={ma_bias:.4%}, "
            f"vol={vol:.4%}, src_div={source_divergence:.4%}, "
            f"norm={norm:.4f}, agreement={'YES' if all_agree else 'NO'}, "
            f"conf={confidence:.4f}"
        )

        return Signal(
            direction=direction,
            confidence=confidence,
            expected_move_pct=expected_move_pct,
            reason=reason,
        )
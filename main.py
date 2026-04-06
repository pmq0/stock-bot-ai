import os
import time
import json
import logging
import threading
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, request
import telebot
import pytz
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import yfinance as yf
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import re
import xml.etree.ElementTree as ET
from collections import deque

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("⚠️ Missing TELEGRAM_TOKEN or CHAT_ID")

CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5
DAILY_LOSS_LIMIT = 200.0

# --- FIX: Scoring thresholds separated for penny vs regular ---
MIN_SCORE_REGULAR = 75
MIN_SCORE_PENNY = 80          # Penny stocks require higher confidence
SIGNAL_COOLDOWN = 3600
STATE_FILE = "state.json"
MAX_SCAN_SYMBOLS = 150

plt.style.use('dark_background')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

app = Flask(__name__)
bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
executor = ThreadPoolExecutor(max_workers=3)

# ================= STATE =================
state_lock = threading.RLock()

# --- FIX: cache_lock added to protect data_cache ---
cache_lock = threading.RLock()
data_cache = {}

state = {
    "open_trades": {},
    "performance": {},
    "weights": {
        "trend":     20,
        "rsi":       15,
        "volume":    15,
        "obv":       10,
        "breakout":  15,
        "macd":      10,   # NEW
        "bb_pos":    10,   # NEW: Bollinger Band position
        "adx":        5,   # NEW: trend strength
    },
    "seen_signals":       {},
    "halted_alerts":      {},
    "last_accumulation":  {},
    "daily_loss":         0.0,
    "last_reset":         None,
    "backtest_results":   {},   # NEW
    "equity_curve":       [],   # NEW
}

def save_state():
    with state_lock:
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(state, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Save state error: {e}")

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                loaded = json.load(f)
                with state_lock:
                    state.update(loaded)
            logger.info("State loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")

def reset_daily_loss_if_needed():
    today = datetime.now().date().isoformat()
    with state_lock:
        if state["last_reset"] != today:
            state["daily_loss"] = 0.0
            state["last_reset"] = today
            save_state()

# ================= UNIVERSE =================
large_universe = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "BRK-B", "LLY", "JPM",
    "V",    "XOM",  "UNH",  "WMT",  "PG",   "JNJ",   "MA",   "HD",    "CVX", "BAC",
    "KO",   "PEP",  "COST", "ADBE", "CRM",  "NFLX",  "DIS",  "AMD",   "INTC","CMCSA",
    "PFE",  "TMO",  "ABT",  "DHR",  "NKE",  "LIN",   "UPS",  "SBUX",  "LOW", "RTX",
    "HON",  "CAT",  "GS",   "MS",   "C",    "WFC",   "SPGI", "BLK",   "AXP", "BK",
    "DE",   "GE",   "MMM",  "AMGN", "TXN",  "QCOM",  "MU",   "ADI",   "NXPI","MRVL",
    "LRCX", "KLAC", "AMAT", "ON",   "PLTR", "SNOW",  "NET",  "DDOG",  "ZS",  "MDB",
    "CRWD", "PANW", "OKTA", "DOCU", "TEAM", "ROKU",  "SHOP", "SQ",    "AFRM","UPST",
    "SOFI", "PYPL", "UBER", "LYFT", "DASH", "ABNB",  "RBLX", "GME",   "AMC", "BB",
    "NIO",  "RIVN", "LCID", "PLUG", "RIOT", "MARA",  "CLSK", "COIN",  "HOOD","BA",
    "LMT",  "NOC",
]
dynamic_universe = large_universe[:MAX_SCAN_SYMBOLS]

# ================= DATA ENGINE =================
def fetch_yfinance_data(symbol: str, period: str = '5d', interval: str = '15m') -> pd.DataFrame | None:
    """Fetch OHLCV data with retry logic."""
    for attempt in range(3):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval, auto_adjust=True)
            if df.empty:
                return None
            df = df.rename(columns={
                'Open': 'open', 'High': 'high',
                'Low': 'low', 'Close': 'close', 'Volume': 'volume'
            })
            df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
            df.dropna(inplace=True)
            return df
        except Exception as e:
            if attempt == 2:
                logger.warning(f"fetch_yfinance_data failed for {symbol}: {e}")
            time.sleep(0.5 * (attempt + 1))
    return None

def fetch_yfinance_quick(symbol: str) -> dict | None:
    """Lightweight fetch for pre-filter."""
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period='5d')
        if data.empty or len(data) < 2:
            return None
        return {
            'price':      float(data['Close'].iloc[-1]),
            'volume':     float(data['Volume'].iloc[-1]),
            'avg_volume': float(data['Volume'].mean()),
            'change_pct': float((data['Close'].iloc[-1] - data['Close'].iloc[-2]) / data['Close'].iloc[-2] * 100),
        }
    except Exception:
        return None

# ================= INDICATORS =================
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # EMA
    df["ema9"]  = df["close"].ewm(span=9,  adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()   # NEW

    # RSI
    delta = df["close"].diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / (loss + 1e-9)
    df["rsi"] = 100 - (100 / (1 + rs))

    # ATR
    df["tr"]  = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift(1)),
            abs(df["low"]  - df["close"].shift(1))
        )
    )
    df["atr"] = df["tr"].rolling(14).mean()

    # OBV
    obv = [0.0]
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i - 1]:
            obv.append(obv[-1] + df["volume"].iloc[i])
        elif df["close"].iloc[i] < df["close"].iloc[i - 1]:
            obv.append(obv[-1] - df["volume"].iloc[i])
        else:
            obv.append(obv[-1])
    df["obv"]    = obv
    df["obv_ma"] = pd.Series(obv, index=df.index).rolling(10).mean()  # NEW

    # Volume MA
    df["vol_ma"] = df["volume"].rolling(20).mean()

    # Bollinger Bands
    df["sma"]        = df["close"].rolling(20).mean()
    df["std"]        = df["close"].rolling(20).std()
    df["upper_band"] = df["sma"] + (df["std"] * 2)
    df["lower_band"] = df["sma"] - (df["std"] * 2)
    df["bandwidth"]  = (df["upper_band"] - df["lower_band"]) / (df["sma"] + 1e-9)
    df["bb_pct"]     = (df["close"] - df["lower_band"]) / (df["upper_band"] - df["lower_band"] + 1e-9)  # NEW

    # MACD (NEW)
    ema12        = df["close"].ewm(span=12, adjust=False).mean()
    ema26        = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"]   = ema12 - ema26
    df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["signal"]

    # ADX (NEW) — simplified
    plus_dm  = df["high"].diff().clip(lower=0)
    minus_dm = (-df["low"].diff()).clip(lower=0)
    tr_smooth   = df["tr"].rolling(14).mean()
    plus_di  = 100 * plus_dm.rolling(14).mean()  / (tr_smooth + 1e-9)
    minus_di = 100 * minus_dm.rolling(14).mean() / (tr_smooth + 1e-9)
    dx       = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)
    df["adx"] = dx.rolling(14).mean()

    return df

# ================= SCORING ENGINE (IMPROVED) =================
def score_signal(df: pd.DataFrame) -> tuple[int, dict]:
    """
    Returns (total_score 0-100, breakdown dict).
    Each component is independent so we can inspect what fired.
    """
    if df is None or len(df) < 50:
        return 0, {}

    c   = df.iloc[-1]
    c_5 = df.iloc[-5] if len(df) > 5 else c

    with state_lock:
        w = state["weights"].copy()

    breakdown = {}
    score = 0

    # 1. Trend: EMA alignment (9 > 21 > 50)
    if c["ema9"] > c["ema21"] > c["ema50"]:
        breakdown["trend"] = w["trend"]
        score += w["trend"]
    elif c["ema9"] > c["ema21"]:
        breakdown["trend"] = w["trend"] // 2
        score += w["trend"] // 2

    # 2. RSI: momentum zone 52–68 (avoid overbought)
    if 52 < c["rsi"] < 68:
        breakdown["rsi"] = w["rsi"]
        score += w["rsi"]
    elif 48 < c["rsi"] <= 52:
        breakdown["rsi"] = w["rsi"] // 3
        score += w["rsi"] // 3

    # 3. Volume surge
    if c["volume"] > c["vol_ma"] * 1.5:
        breakdown["volume"] = w["volume"]
        score += w["volume"]
    elif c["volume"] > c["vol_ma"] * 1.2:
        breakdown["volume"] = w["volume"] // 2
        score += w["volume"] // 2

    # 4. OBV rising vs its own MA
    if c["obv"] > c["obv_ma"]:
        breakdown["obv"] = w["obv"]
        score += w["obv"]

    # 5. Breakout: close above 20-period high
    recent_high = df["high"].iloc[-20:-1].max()
    if c["close"] > recent_high:
        breakdown["breakout"] = w["breakout"]
        score += w["breakout"]

    # 6. MACD bullish crossover
    prev_hist = df["macd_hist"].iloc[-2]
    if c["macd_hist"] > 0 and prev_hist <= 0:        # fresh cross
        breakdown["macd"] = w["macd"]
        score += w["macd"]
    elif c["macd_hist"] > 0 and c["macd"] > 0:       # already above
        breakdown["macd"] = w["macd"] // 2
        score += w["macd"] // 2

    # 7. Bollinger Band position (0.5–0.8 = healthy uptrend, not overextended)
    if 0.5 < c["bb_pct"] < 0.8:
        breakdown["bb_pos"] = w["bb_pos"]
        score += w["bb_pos"]

    # 8. ADX: trend strength > 25
    if c["adx"] > 25:
        breakdown["adx"] = w["adx"]
        score += w["adx"]
    elif c["adx"] > 20:
        breakdown["adx"] = w["adx"] // 2
        score += w["adx"] // 2

    # Penalty: RSI overbought
    if c["rsi"] > 75:
        score = int(score * 0.7)
        breakdown["penalty_overbought"] = True

    # Penalty: price below EMA50 (weak trend)
    if c["close"] < c["ema50"]:
        score = int(score * 0.8)
        breakdown["penalty_below_ema50"] = True

    return min(100, max(0, score)), breakdown

# ================= PENNY STOCK =================
def is_penny_stock(price: float) -> bool:
    return price < 5.0

# ================= ACCUMULATION =================
def detect_accumulation(df: pd.DataFrame) -> tuple[bool, int]:
    if len(df) < 30:
        return False, 0

    last_10 = df.tail(10)
    last_20 = df.tail(20)

    price_change  = abs((df['close'].iloc[-1] - df['close'].iloc[-20]) / (df['close'].iloc[-20] + 1e-9))
    price_stable  = price_change < 0.015
    volume_surge  = last_10['volume'].mean() > last_20['volume'].mean() * 1.2
    obv_rising    = df['obv'].iloc[-1] > df['obv'].iloc[-10]
    recent_range  = (last_10['high'].max() - last_10['low'].min()) / (df['close'].iloc[-1] + 1e-9)
    tight_range   = recent_range < 0.04

    acc_score = 0
    if price_stable: acc_score += 25
    if volume_surge: acc_score += 30
    if obv_rising:   acc_score += 25
    if tight_range:  acc_score += 20

    return acc_score >= 60, acc_score

def pre_breakout_detection(df: pd.DataFrame) -> bool:
    if len(df) < 20:
        return False
    is_squeezing  = df['bandwidth'].iloc[-1] < df['bandwidth'].iloc[-10] * 0.7
    approaching   = df['close'].iloc[-1] > df['upper_band'].iloc[-1] * 0.97
    volume_surge  = df['volume'].iloc[-3:].mean() > df['volume'].rolling(20).mean().iloc[-1] * 1.5
    return is_squeezing and approaching and volume_surge

# ================= BACKTESTING (NEW) =================
def simple_backtest(symbol: str, df: pd.DataFrame) -> dict:
    """
    Walk-forward backtest on historical data.
    Returns summary stats to validate the strategy before going live.
    """
    if df is None or len(df) < 80:
        return {}

    trades      = []
    in_trade    = False
    entry_price = 0.0
    entry_idx   = 0
    tp_mult     = 2.0
    sl_mult     = 2.0

    for i in range(50, len(df)):
        window = df.iloc[:i]
        score_val, _ = score_signal(window)
        price = df['close'].iloc[i]
        atr   = df['atr'].iloc[i]
        if pd.isna(atr) or atr == 0:
            continue

        if not in_trade and score_val >= MIN_SCORE_REGULAR:
            entry_price = price
            tp_price    = price + atr * tp_mult
            sl_price    = price - atr * sl_mult
            in_trade    = True
            entry_idx   = i

        elif in_trade:
            if price >= tp_price:
                trades.append({"pnl": tp_price - entry_price, "bars": i - entry_idx, "result": "win"})
                in_trade = False
            elif price <= sl_price:
                trades.append({"pnl": sl_price - entry_price, "bars": i - entry_idx, "result": "loss"})
                in_trade = False
            # Max hold: 20 bars
            elif i - entry_idx >= 20:
                trades.append({"pnl": price - entry_price, "bars": 20, "result": "timeout"})
                in_trade = False

    if not trades:
        return {"total_trades": 0, "win_rate": 0, "avg_pnl": 0}

    wins     = sum(1 for t in trades if t["result"] == "win")
    total    = len(trades)
    avg_pnl  = sum(t["pnl"] for t in trades) / total
    win_rate = wins / total * 100

    return {
        "symbol":       symbol,
        "total_trades": total,
        "wins":         wins,
        "losses":       total - wins,
        "win_rate":     round(win_rate, 1),
        "avg_pnl":      round(avg_pnl, 4),
        "profitable":   avg_pnl > 0,
    }

# ================= TRADING HALTS =================
def check_trading_halt(symbol: str) -> tuple:
    try:
        rss_url  = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
        response = requests.get(rss_url, timeout=10)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            for item in root.findall('.//item'):
                title       = item.find('title').text or ''
                description = item.find('description').text or ''
                if symbol in title or symbol in description:
                    price_match  = re.search(r'PauseThresholdPrice:\s*\$?([0-9]+\.[0-9]+)', description)
                    halt_price   = price_match.group(1) if price_match else "N/A"
                    time_match   = re.search(r'(\d{1,2}:\d{2}:\d{2})\s*ET', description)
                    halt_time    = time_match.group(1) if time_match else "N/A"
                    reason_match = re.search(r'Reason:\s*(.+?)(?:\.|$)', description)
                    reason       = reason_match.group(1) if reason_match else "Trading Pause"
                    return True, datetime.now().strftime('%Y-%m-%d'), halt_time, reason, halt_price
    except Exception as e:
        logger.warning(f"Halt check error: {e}")
    return False, None, None, None, None

# ================= SIGNAL CONTROL =================
def is_seen(symbol: str) -> bool:
    now = time.time()
    with state_lock:
        ts = state["seen_signals"].get(symbol)
        return ts is not None and now - ts < SIGNAL_COOLDOWN

def mark_seen(symbol: str):
    with state_lock:
        state["seen_signals"][symbol] = time.time()
        save_state()

def is_halt_alerted(symbol: str) -> bool:
    now = time.time()
    with state_lock:
        ts = state["halted_alerts"].get(symbol)
        return ts is not None and now - ts < 3600

def mark_halt_alerted(symbol: str):
    with state_lock:
        state["halted_alerts"][symbol] = time.time()
        save_state()

def should_send_accumulation_alert(symbol: str, new_score: int) -> bool:
    with state_lock:
        last = state["last_accumulation"].get(symbol)
        if not last:
            return True
        if abs(new_score - last.get("score", 0)) >= 10:
            return True
        if time.time() - last.get("time", 0) > 4 * 3600:
            return True
    return False

def update_accumulation_alert(symbol: str, new_score: int):
    with state_lock:
        state["last_accumulation"][symbol] = {"score": new_score, "time": time.time()}
        save_state()

# ================= MARKET HOURS =================
def is_market_open() -> bool:
    tz  = pytz.timezone("US/Eastern")
    now = datetime.now(tz)
    if now.weekday() >= 5:
        return False
    open_t  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_t = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return open_t <= now <= close_t

def get_market_session() -> tuple[str, str]:
    tz   = pytz.timezone("US/Eastern")
    now  = datetime.now(tz)
    hour = now.hour
    if 9 <= hour < 16:
        return "REGULAR HOURS", "🟢 تداول نشط"
    elif 4 <= hour < 9:
        return "PRE-MARKET", "🟡 تحليل فقط"
    else:
        return "AFTER-HOURS", "🟡 تحليل فقط"

# ================= RISK MANAGEMENT =================
def can_trade(price: float, atr: float) -> tuple[bool, int]:
    reset_daily_loss_if_needed()
    with state_lock:
        if len(state["open_trades"]) >= MAX_OPEN_TRADES:
            return False, 0
        if state["daily_loss"] >= DAILY_LOSS_LIMIT:
            return False, 0

    risk_amount   = CAPITAL * RISK_PER_TRADE
    stop_distance = atr * 2 if atr > 0 else price * 0.02
    size          = int(risk_amount / stop_distance)

    if size < 1:
        return False, 0

    # Cap at 20% of capital per trade
    if size * price > CAPITAL * 0.2:
        size = int(CAPITAL * 0.2 / price)

    return (size >= 1), max(1, size)

# ================= CHART =================
def generate_chart(symbol: str, df: pd.DataFrame, entry: float, tp: float, sl: float,
                   is_penny: bool = False, is_accumulating: bool = False,
                   session: str = "", breakdown: dict = None) -> io.BytesIO | None:
    try:
        df_plot = df.tail(80).copy()
        fig, axes = plt.subplots(3, 1, figsize=(14, 12),
                                 gridspec_kw={'height_ratios': [3, 1, 1]})
        ax1, ax2, ax3 = axes

        # --- Price panel ---
        ax1.plot(df_plot.index, df_plot['close'],      color='cyan',   linewidth=1.5, label='Price')
        ax1.plot(df_plot.index, df_plot['ema9'],       color='yellow', linewidth=1,   alpha=0.8, label='EMA 9')
        ax1.plot(df_plot.index, df_plot['ema21'],      color='orange', linewidth=1,   alpha=0.8, label='EMA 21')
        ax1.plot(df_plot.index, df_plot['ema50'],      color='magenta',linewidth=1,   alpha=0.6, label='EMA 50')
        ax1.fill_between(df_plot.index, df_plot['upper_band'], df_plot['lower_band'],
                         alpha=0.08, color='gray', label='Bollinger')
        ax1.axhline(y=entry, color='lime',  linestyle='--', linewidth=1.5, label=f'Entry ${entry:.2f}')
        ax1.axhline(y=tp,    color='green', linestyle='--', linewidth=1.5, label=f'TP ${tp:.2f}')
        ax1.axhline(y=sl,    color='red',   linestyle='--', linewidth=1.5, label=f'SL ${sl:.2f}')
        ax1.fill_between(df_plot.index, entry, tp, alpha=0.15, color='green')
        ax1.fill_between(df_plot.index, sl, entry, alpha=0.15, color='red')

        if df_plot['bandwidth'].iloc[-1] < df_plot['bandwidth'].iloc[-10] * 0.7:
            ax1.axvspan(df_plot.index[-5], df_plot.index[-1], alpha=0.25, color='yellow', label='Pre-Breakout')

        ax1.set_title(f'{symbol}  |  {session}', fontsize=13, color='white')
        ax1.set_ylabel('Price ($)', color='white')
        ax1.legend(loc='upper left', fontsize=8, ncol=2)
        ax1.grid(True, alpha=0.15)
        ax1.tick_params(colors='white')

        # --- Volume panel ---
        colors = ['green' if df_plot['close'].iloc[i] >= df_plot['open'].iloc[i] else 'red'
                  for i in range(len(df_plot))]
        ax2.bar(df_plot.index, df_plot['volume'], color=colors, alpha=0.7)
        ax2.axhline(y=df_plot['vol_ma'].iloc[-1], color='blue', linestyle='--', linewidth=1, label='Avg Vol')
        ax2.set_ylabel('Volume', color='white')
        ax2.legend(loc='upper left', fontsize=8)
        ax2.grid(True, alpha=0.15)
        ax2.tick_params(colors='white')

        # --- MACD panel (NEW) ---
        ax3.plot(df_plot.index, df_plot['macd'],   color='cyan',  linewidth=1,   label='MACD')
        ax3.plot(df_plot.index, df_plot['signal'], color='orange',linewidth=1,   label='Signal')
        hist_colors = ['green' if v >= 0 else 'red' for v in df_plot['macd_hist']]
        ax3.bar(df_plot.index, df_plot['macd_hist'], color=hist_colors, alpha=0.6)
        ax3.axhline(0, color='white', linewidth=0.5)
        ax3.set_ylabel('MACD', color='white')
        ax3.set_xlabel('Time', color='white')
        ax3.legend(loc='upper left', fontsize=8)
        ax3.grid(True, alpha=0.15)
        ax3.tick_params(colors='white')

        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100, facecolor='#0d1117')
        buf.seek(0)
        plt.close(fig)
        return buf
    except Exception as e:
        logger.error(f"Chart generation error for {symbol}: {e}")
        return None

def send_chart(symbol: str, df: pd.DataFrame, entry: float, tp: float, sl: float,
               is_penny: bool, is_accumulating: bool, session: str, breakdown: dict):
    if not bot:
        return
    buf = generate_chart(symbol, df, entry, tp, sl, is_penny, is_accumulating, session, breakdown)
    if buf is None:
        return
    caption = f"📊 *{symbol}*\n💰 Entry: ${entry:.2f}  🎯 TP: ${tp:.2f}  🛑 SL: ${sl:.2f}"
    if is_penny:
        caption += "\n🔥 *Penny Stock*"
    if is_accumulating:
        caption += "\n📦 *Accumulation Detected*"
    try:
        bot.send_photo(chat_id=CHAT_ID, photo=buf, caption=caption, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"send_photo error: {e}")

# ================= TRADE MANAGEMENT =================
def open_trade(symbol: str, price: float, atr: float, score_val: int,
               size: int, df: pd.DataFrame, is_penny: bool,
               is_accumulating: bool, breakdown: dict):
    tp = round(price + atr * 2, 4)
    sl = round(price - atr * 2, 4)
    session, _ = get_market_session()

    with state_lock:
        state["open_trades"][symbol] = {
            "entry": price, "tp": tp, "sl": sl,
            "size":  size,  "time": time.time(),
            "score": score_val, "atr": atr,
        }
        save_state()

    send_chart(symbol, df, price, tp, sl, is_penny, is_accumulating, session, breakdown)

    score_breakdown = " | ".join(f"{k}:{v}" for k, v in breakdown.items()
                                 if isinstance(v, (int, float)))
    msg = (
        f"🚀 *NEW TRADE* ({session})\n"
        f"📊 {symbol}\n"
        f"💰 Entry: ${price:.2f}\n"
        f"🎯 TP:    ${tp:.2f}\n"
        f"🛑 SL:    ${sl:.2f}\n"
        f"⭐ Score: {score_val}/100\n"
        f"📦 Shares: {size}\n"
        f"📈 Breakdown: {score_breakdown}"
    )
    send_telegram(msg)

def close_trade(symbol: str, price: float):
    with state_lock:
        trade = state["open_trades"].pop(symbol, None)
        if not trade:
            return
        pnl  = round((price - trade["entry"]) * trade["size"], 2)
        perf = state["performance"].setdefault(symbol, {"wins": 0, "losses": 0, "total_pnl": 0.0})
        if pnl >= 0:
            perf["wins"] += 1
        else:
            perf["losses"] += 1
            state["daily_loss"] += abs(pnl)
        perf["total_pnl"] = round(perf.get("total_pnl", 0) + pnl, 2)
        state["equity_curve"].append({"time": time.time(), "pnl": pnl})
        save_state()

    result = "WIN 🎉" if pnl >= 0 else "LOSS ❌"
    msg = (
        f"🔒 *TRADE CLOSED*\n"
        f"📊 {symbol}\n"
        f"📉 Exit: ${price:.2f}\n"
        f"🏆 {result}\n"
        f"💰 PnL: ${pnl:+.2f}"
    )
    send_telegram(msg)

def update_positions():
    """Check open trades against cached prices."""
    with cache_lock:
        cache_snapshot = dict(data_cache)

    for symbol, trade in list(state["open_trades"].items()):
        df = cache_snapshot.get(symbol)
        if df is None:
            continue
        price = df["close"].iloc[-1]
        if price >= trade["tp"] or price <= trade["sl"]:
            close_trade(symbol, price)

# ================= CACHE BUILD (THREAD-SAFE) =================
def build_cache(symbols: list, period: str = '5d', interval: str = '15m'):
    """Fetch and compute indicators for all symbols, then swap cache atomically."""
    new_cache = {}
    for symbol in symbols:
        df = fetch_yfinance_data(symbol, period, interval)
        if df is not None and len(df) >= 50:
            new_cache[symbol] = compute_indicators(df)
        time.sleep(0.2)

    with cache_lock:
        data_cache.clear()
        data_cache.update(new_cache)

    logger.info(f"Cache refreshed: {len(data_cache)} symbols")

# ================= SCAN =================
def quick_filter_stocks(symbols: list) -> list:
    promising = []
    for i, symbol in enumerate(symbols):
        try:
            quick = fetch_yfinance_quick(symbol)
            if quick:
                price    = quick['price']
                volume   = quick['volume']
                avg_vol  = quick['avg_volume']
                change   = quick['change_pct']
                # Filter: price range, volume activity, positive momentum
                if (1.0 < price < 200 and
                        volume > 150_000 and
                        volume > avg_vol * 1.1 and
                        change > -3):
                    promising.append(symbol)
            if (i + 1) % 50 == 0:
                logger.info(f"Quick filter: {i+1}/{len(symbols)}")
        except Exception:
            pass
        time.sleep(0.1)
    logger.info(f"Quick filter: {len(promising)} / {len(symbols)} passed")
    return promising[:100]

def deep_analysis(symbols: list) -> list:
    results = []
    for symbol in symbols:
        if is_seen(symbol):
            continue
        with cache_lock:
            df = data_cache.get(symbol)

        if df is None:
            df = fetch_yfinance_data(symbol, '5d', '15m')
            if df is None or len(df) < 50:
                continue
            df = compute_indicators(df)
            with cache_lock:
                data_cache[symbol] = df

        score_val, breakdown = score_signal(df)
        if score_val >= MIN_SCORE_REGULAR - 10:
            results.append((symbol, score_val, breakdown, df))
        time.sleep(0.05)

    return sorted(results, key=lambda x: x[1], reverse=True)

def scan_large_universe():
    session, _ = get_market_session()
    logger.info(f"🔄 Scan started ({len(dynamic_universe)} symbols) [{session}]")

    # Step 1: quick filter
    promising = quick_filter_stocks(dynamic_universe)
    if not promising:
        logger.warning("No promising stocks from quick filter")
        return

    # Step 2: build cache for filtered list
    build_cache(promising)

    # Step 3: deep analysis
    top_signals = deep_analysis(promising)
    logger.info(f"Top signals: {len(top_signals)}")

    # Step 4: process top 5
    for symbol, score_val, breakdown, df in top_signals[:5]:
        price     = df["close"].iloc[-1]
        atr       = df["atr"].iloc[-1]
        is_penny  = is_penny_stock(price)
        threshold = MIN_SCORE_PENNY if is_penny else MIN_SCORE_REGULAR

        if score_val < threshold or is_seen(symbol):
            continue
        if pd.isna(atr) or atr <= 0:
            atr = price * 0.02

        # Halt check
        halted, *halt_info = check_trading_halt(symbol)
        if halted and not is_halt_alerted(symbol):
            send_telegram(f"🚨 *TRADING HALT*: {symbol} — {halt_info[2]}")
            mark_halt_alerted(symbol)
            continue

        is_acc, acc_score = detect_accumulation(df)
        ok, size          = can_trade(price, atr)

        if ok:
            open_trade(symbol, price, atr, score_val, size, df, is_penny, is_acc, breakdown)
            mark_seen(symbol)

# ================= TELEGRAM =================
def send_telegram(msg: str):
    if not bot:
        print(f"[Telegram] {msg}")
        return
    try:
        bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

def generate_analysis_message(symbol: str) -> str:
    df = fetch_yfinance_data(symbol, '10d', '15m')
    if df is None or len(df) < 50:
        return f"⚠️ No sufficient data for {symbol}"

    df        = compute_indicators(df)
    session, _= get_market_session()
    score_val, breakdown = score_signal(df)
    price     = df["close"].iloc[-1]
    atr       = df["atr"].iloc[-1]
    volume    = df["volume"].iloc[-1]
    avg_vol   = df["vol_ma"].iloc[-1]
    rsi       = df["rsi"].iloc[-1]
    adx       = df["adx"].iloc[-1]
    is_penny  = is_penny_stock(price)
    is_acc, acc_score    = detect_accumulation(df)
    pre_bo    = pre_breakout_detection(df)

    # Backtest summary
    bt = simple_backtest(symbol, df)
    bt_line = ""
    if bt.get("total_trades", 0) > 0:
        bt_line = (f"🔬 Backtest: {bt['total_trades']} trades | "
                   f"WR {bt['win_rate']}% | Avg PnL ${bt['avg_pnl']:.3f}\n")

    breakdown_str = " | ".join(f"{k}:{v}" for k, v in breakdown.items()
                               if isinstance(v, (int, float)))

    msg  = f"📊 *{symbol} Analysis* ({session})\n"
    msg += f"💰 Price:   ${price:.2f}\n"
    msg += f"📊 Volume:  {volume:,.0f}  (avg {avg_vol:,.0f})\n"
    msg += f"🎢 ATR:     ${atr:.2f}\n"
    msg += f"⚡ RSI:     {rsi:.1f}\n"
    msg += f"💪 ADX:     {adx:.1f}\n"
    msg += f"🧠 Score:   {score_val}/100\n"
    msg += f"📐 Detail:  {breakdown_str}\n"
    msg += bt_line
    if is_penny:     msg += "🔥 *Penny Stock*\n"
    if is_acc:       msg += f"📦 *Accumulation* ({acc_score}%)\n"
    if pre_bo:       msg += "⚡ *Pre-Breakout Detected*\n"
    return msg

# ================= COMMANDS =================
@bot.message_handler(commands=['start'])
def cmd_start(message):
    session, session_msg = get_market_session()
    welcome = (
        "🚀 *AI Trading Bot v19*\n\n"
        f"*Session:* {session}\n{session_msg}\n\n"
        "*Commands:*\n"
        "/scan \\<symbol\\> — Technical analysis + backtest\n"
        "/status — Bot overview\n"
        "/positions — Open trades\n"
        "/performance — Win/loss stats\n"
        "/backtest \\<symbol\\> — Run backtest only\n"
        "/close \\<symbol\\> — Manually close trade\n"
    )
    bot.reply_to(message, welcome, parse_mode='Markdown')

@bot.message_handler(commands=['scan'])
def cmd_scan(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "⚠️ Usage: /scan <SYMBOL>")
        return
    symbol = parts[1].upper().strip()
    msg_obj = bot.reply_to(message, f"🔍 Analyzing {symbol}…")

    def do_analysis():
        analysis = generate_analysis_message(symbol)
        try:
            bot.edit_message_text(chat_id=message.chat.id,
                                  message_id=msg_obj.message_id,
                                  text=analysis, parse_mode='Markdown')
        except Exception:
            bot.send_message(chat_id=message.chat.id, text=analysis, parse_mode='Markdown')

    future = executor.submit(do_analysis)
    try:
        future.result(timeout=25)
    except TimeoutError:
        bot.edit_message_text(chat_id=message.chat.id,
                              message_id=msg_obj.message_id,
                              text="⚠️ Timeout. Try again.")

@bot.message_handler(commands=['backtest'])
def cmd_backtest(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "⚠️ Usage: /backtest <SYMBOL>")
        return
    symbol  = parts[1].upper().strip()
    msg_obj = bot.reply_to(message, f"🔬 Running backtest for {symbol}…")

    def do_bt():
        df = fetch_yfinance_data(symbol, '60d', '1h')
        if df is None or len(df) < 80:
            bot.edit_message_text(chat_id=message.chat.id,
                                  message_id=msg_obj.message_id,
                                  text="⚠️ Not enough data for backtest (need 60d 1h).")
            return
        df  = compute_indicators(df)
        bt  = simple_backtest(symbol, df)
        if not bt or bt.get("total_trades", 0) == 0:
            text = f"⚠️ No trades triggered for {symbol} in backtest period."
        else:
            emoji = "✅" if bt["profitable"] else "❌"
            text  = (
                f"🔬 *Backtest: {symbol}*\n"
                f"📊 Trades:   {bt['total_trades']}\n"
                f"✅ Wins:     {bt['wins']}\n"
                f"❌ Losses:   {bt['losses']}\n"
                f"📈 Win Rate: {bt['win_rate']}%\n"
                f"💰 Avg PnL:  ${bt['avg_pnl']:.4f}\n"
                f"{emoji} Strategy: {'PROFITABLE' if bt['profitable'] else 'UNPROFITABLE'}"
            )
        bot.edit_message_text(chat_id=message.chat.id,
                              message_id=msg_obj.message_id,
                              text=text, parse_mode='Markdown')

    future = executor.submit(do_bt)
    try:
        future.result(timeout=60)
    except TimeoutError:
        bot.edit_message_text(chat_id=message.chat.id,
                              message_id=msg_obj.message_id,
                              text="⚠️ Backtest timed out.")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    reset_daily_loss_if_needed()
    session, session_msg = get_market_session()
    with state_lock:
        total_wins   = sum(p["wins"]   for p in state["performance"].values())
        total_losses = sum(p["losses"] for p in state["performance"].values())
        total        = total_wins + total_losses
        winrate      = (total_wins / total * 100) if total else 0
        total_pnl    = sum(p.get("total_pnl", 0) for p in state["performance"].values())
        open_count   = len(state["open_trades"])
        daily_loss   = state["daily_loss"]

    msg = (
        f"📊 *Bot Status* ({session})\n"
        f"{session_msg}\n\n"
        f"💰 Capital:     ${CAPITAL:,.0f}\n"
        f"📈 Open Trades: {open_count}/{MAX_OPEN_TRADES}\n"
        f"✅ Wins:        {total_wins}\n"
        f"❌ Losses:      {total_losses}\n"
        f"📈 Win Rate:    {winrate:.1f}%\n"
        f"💵 Total PnL:   ${total_pnl:+.2f}\n"
        f"📉 Daily Loss:  ${daily_loss:.2f} / ${DAILY_LOSS_LIMIT}\n"
        f"🌐 Universe:    {len(dynamic_universe)} stocks\n"
        f"📦 Cached:      {len(data_cache)} symbols"
    )
    bot.reply_to(message, msg, parse_mode='Markdown')

@bot.message_handler(commands=['positions'])
def cmd_positions(message):
    with state_lock:
        trades = dict(state["open_trades"])
    if not trades:
        bot.reply_to(message, "📭 No open positions")
        return
    lines = ["*Open Positions*\n"]
    for sym, t in trades.items():
        age = int((time.time() - t["time"]) / 60)
        lines.append(
            f"🔹 *{sym}*\n"
            f"   Entry ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f}\n"
            f"   Shares {t['size']} | Age {age}m | Score {t['score']}"
        )
    bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')

@bot.message_handler(commands=['performance'])
def cmd_performance(message):
    with state_lock:
        perf = dict(state["performance"])
    if not perf:
        bot.reply_to(message, "📭 No completed trades yet")
        return
    lines = ["*Per-Symbol Performance*\n"]
    for sym, p in sorted(perf.items(), key=lambda x: x[1].get("total_pnl", 0), reverse=True):
        total = p["wins"] + p["losses"]
        wr    = (p["wins"] / total * 100) if total else 0
        lines.append(
            f"*{sym}*: {p['wins']}W / {p['losses']}L "
            f"({wr:.1f}%) | PnL ${p.get('total_pnl', 0):+.2f}"
        )
    bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')

@bot.message_handler(commands=['close'])
def cmd_close(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "⚠️ Usage: /close <SYMBOL>")
        return
    symbol = parts[1].upper().strip()
    with state_lock:
        if symbol not in state["open_trades"]:
            bot.reply_to(message, f"📭 No open trade for {symbol}")
            return
    df = fetch_yfinance_data(symbol, '1d', '5m')
    if df is None or df.empty:
        bot.reply_to(message, f"⚠️ Cannot fetch current price for {symbol}")
        return
    price = df["close"].iloc[-1]
    close_trade(symbol, price)
    bot.reply_to(message, f"✅ Manually closed *{symbol}* at ${price:.2f}", parse_mode='Markdown')

# ================= BACKGROUND SCANNER =================
def background_scanner():
    last_scan        = 0
    last_cache_build = 0

    while True:
        try:
            now = time.time()

            # Refresh cache every 10 minutes even outside scan
            if now - last_cache_build > 600 and is_market_open():
                build_cache(dynamic_universe[:50])   # top-50 always warm
                last_cache_build = now

            # Full scan every 5 minutes during market hours
            if now - last_scan > 300 and is_market_open():
                logger.info("🔄 Full scan triggered")
                scan_large_universe()
                update_positions()
                last_scan = now

            time.sleep(60)
        except Exception as e:
            logger.error(f"Scanner loop error: {e}", exc_info=True)
            time.sleep(60)

# ================= FLASK =================
@app.route("/", methods=['GET'])
def home():
    return f"AI Trading Bot v19 | Cache: {len(data_cache)} | Trades: {len(state['open_trades'])}"

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = telebot.types.Update.de_json(request.data.decode("utf-8"))
        bot.process_new_updates([update])
        return "ok", 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return "error", 500

@app.route("/health", methods=['GET'])
def health():
    return json.dumps({
        "status":      "ok",
        "cache_size":  len(data_cache),
        "open_trades": len(state["open_trades"]),
        "daily_loss":  state["daily_loss"],
        "market_open": is_market_open(),
    })

# ================= ENTRY POINT =================
if __name__ == "__main__":
    load_state()
    reset_daily_loss_if_needed()
    threading.Thread(target=background_scanner, daemon=True).start()
    if bot:
        threading.Thread(
            target=lambda: bot.infinity_polling(timeout=10, long_polling_timeout=5),
            daemon=True
        ).start()
    send_telegram("✅ *AI Trading Bot v19 is LIVE!*\nImprovements: MACD + ADX + Backtest + Thread-safe cache")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

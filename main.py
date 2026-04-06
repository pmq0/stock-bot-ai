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
import finnhub
import io
import matplotlib.pyplot as plt
import re
import xml.etree.ElementTree as ET

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

if not TELEGRAM_TOKEN or not CHAT_ID or not FINNHUB_API_KEY:
    raise ValueError("Missing required environment variables")

CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5
DAILY_LOSS_LIMIT = 200.0
MIN_SCORE = 75
PENNY_THRESHOLD = 65
SIGNAL_COOLDOWN = 3600
STATE_FILE = "state.json"

# Chart settings
plt.style.use('dark_background')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Flask and Telegram bot
app = Flask(__name__)
bot = telebot.TeleBot(TELEGRAM_TOKEN)

# ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=2)

# Finnhub client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

# ================= STATE MANAGEMENT =================
state_lock = threading.RLock()
state = {
    "open_trades": {},
    "performance": {},
    "weights": {
        "trend": 20,
        "rsi": 15,
        "volume": 15,
        "obv": 10,
        "breakout": 15
    },
    "seen_signals": {},
    "halted_alerts": {},
    "last_accumulation": {},
    "daily_loss": 0.0,
    "last_reset": None
}

def save_state():
    with state_lock:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                loaded = json.load(f)
                with state_lock:
                    state.update(loaded)
                logger.info("State loaded")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")

def reset_daily_loss_if_needed():
    today = datetime.now().date().isoformat()
    with state_lock:
        if state["last_reset"] != today:
            state["daily_loss"] = 0.0
            state["last_reset"] = today
            save_state()

# ================= GLOBAL UNIVERSE =================
initial_universe = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA",
    "GME", "AMC", "PLTR", "SOFI", "NIO", "RIOT", "MARA", "CLSK"
]

dynamic_universe = initial_universe.copy()

# ================= RATE LIMIT =================
_last_api_call = 0
_api_lock = threading.Lock()

def rate_limit():
    global _last_api_call
    with _api_lock:
        now = time.time()
        wait = 1.0 - (now - _last_api_call)
        if wait > 0:
            time.sleep(wait)
        _last_api_call = time.time()

# ================= FINNHUB DATA =================
def fetch_finnhub_candles(symbol, resolution='15', count=100):
    rate_limit()
    try:
        to_time = int(time.time())
        from_time = to_time - (count * 60 * 15)
        res = finnhub_client.stock_candles(symbol, resolution, from_time, to_time)
        if res and res.get('s') == 'ok':
            df = pd.DataFrame({
                't': res['t'],
                'o': res['o'],
                'h': res['h'],
                'l': res['l'],
                'c': res['c'],
                'v': res['v']
            })
            df = df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
            return df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        else:
            return None
    except Exception as e:
        logger.error(f"Fetch error {symbol}: {e}")
        return None

def fetch_quote(symbol):
    rate_limit()
    try:
        return finnhub_client.quote(symbol)
    except Exception as e:
        logger.error(f"Quote error {symbol}: {e}")
        return None

# ================= INDICATORS =================
def compute_indicators(df):
    df = df.copy()
    df["ema9"] = df["close"].ewm(9).mean()
    df["ema21"] = df["close"].ewm(21).mean()
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / (loss + 1e-9)
    df["rsi"] = 100 - (100/(1+rs))
    df["atr"] = (df["high"] - df["low"]).rolling(14).mean()
    obv = [0]
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i-1]:
            obv.append(obv[-1] + df["volume"].iloc[i])
        else:
            obv.append(obv[-1] - df["volume"].iloc[i])
    df["obv"] = obv
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["sma"] = df["close"].rolling(20).mean()
    df["std"] = df["close"].rolling(20).std()
    df["upper_band"] = df["sma"] + (df["std"] * 2)
    df["lower_band"] = df["sma"] - (df["std"] * 2)
    df["bandwidth"] = (df["upper_band"] - df["lower_band"]) / df["sma"]
    return df

def score_from_cached(df):
    if df is None or len(df) < 30:
        return 0
    c = df.iloc[-1]
    s = 0
    if c["ema9"] > c["ema21"]:
        s += 20
    if 45 < c["rsi"] < 75:
        s += 15
    if c["volume"] > c["vol_ma"] * 1.3:
        s += 15
    if c["obv"] > df["obv"].rolling(10).mean().iloc[-1]:
        s += 10
    recent_high = df["high"].iloc[-10:].max()
    if c["close"] > recent_high:
        s += 15
    return min(100, s)

def is_penny_stock(price):
    return price < 5.0

def detect_accumulation(df):
    if len(df) < 30:
        return False, 0
    last_10 = df.tail(10)
    last_20 = df.tail(20)
    price_change = abs((df['close'].iloc[-1] - df['close'].iloc[-20]) / df['close'].iloc[-20])
    price_stable = price_change < 0.01
    volume_surge = last_10['volume'].mean() > last_20['volume'].mean() * 1.2
    obv_rising = df['obv'].iloc[-1] > df['obv'].iloc[-10]
    recent_high = last_10['high'].max()
    recent_low = last_10['low'].min()
    tight_range = (recent_high - recent_low) / df['close'].iloc[-1] < 0.03
    acc_score = 0
    if price_stable: acc_score += 25
    if volume_surge: acc_score += 30
    if obv_rising: acc_score += 25
    if tight_range: acc_score += 20
    return acc_score >= 60, acc_score

def pre_breakout_detection(df):
    if len(df) < 20:
        return False
    is_squeezing = df['bandwidth'].iloc[-1] < df['bandwidth'].iloc[-10] * 0.7
    approaching = df['close'].iloc[-1] > df['upper_band'].iloc[-1] * 0.98
    volume_surge = df['volume'].iloc[-3:].mean() > df['volume'].rolling(20).mean().iloc[-1] * 1.5
    return is_squeezing and approaching and volume_surge

# ================= SIGNAL CONTROL =================
def is_seen(symbol):
    with state_lock:
        if symbol in state["seen_signals"]:
            if time.time() - state["seen_signals"][symbol] < SIGNAL_COOLDOWN:
                return True
    return False

def mark_seen(symbol):
    with state_lock:
        state["seen_signals"][symbol] = time.time()
        save_state()

def should_send_accumulation_alert(symbol, new_score):
    with state_lock:
        last = state["last_accumulation"].get(symbol)
        if not last:
            return True
        old_score = last.get("score", 0)
        if abs(new_score - old_score) >= 10:
            return True
        if time.time() - last.get("time", 0) > 4 * 3600:
            return True
        return False

def update_accumulation_alert(symbol, new_score):
    with state_lock:
        state["last_accumulation"][symbol] = {"score": new_score, "time": time.time()}
        save_state()

# ================= MARKET HOURS =================
def is_market_open():
    tz = pytz.timezone("US/Eastern")
    now = datetime.now(tz)
    if now.weekday() >= 5:
        return False
    open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_time <= now <= close_time

def get_market_session():
    tz = pytz.timezone("US/Eastern")
    now = datetime.now(tz)
    hour = now.hour
    if 9 <= hour < 16:
        return "REGULAR HOURS", "🟢 تداول نشط"
    elif 4 <= hour < 9:
        return "PRE-MARKET", "🟡 تحليل فقط"
    else:
        return "AFTER-HOURS", "🟡 تحليل فقط"

# ================= RISK MANAGEMENT =================
def can_trade(price, atr):
    reset_daily_loss_if_needed()
    with state_lock:
        if len(state["open_trades"]) >= MAX_OPEN_TRADES:
            return False, 0
        if state["daily_loss"] >= DAILY_LOSS_LIMIT:
            return False, 0
    risk_amount = CAPITAL * RISK_PER_TRADE
    stop_distance = atr * 2 if atr > 0 else price * 0.02
    size = int(risk_amount / stop_distance)
    if size < 1:
        return False, 0
    if size * price > CAPITAL * 0.2:
        size = int(CAPITAL * 0.2 / price)
        if size < 1:
            return False, 0
    return True, size

# ================= TELEGRAM FUNCTIONS =================
def send_telegram(msg):
    try:
        bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram error: {e}")

def generate_and_send_chart(symbol, df, entry, tp, sl, is_penny=False, is_accumulating=False, session=""):
    try:
        df_plot = df.tail(60).copy()
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]})
        ax1.plot(df_plot.index, df_plot['close'], 'cyan', linewidth=1.5, label='Price')
        ax1.plot(df_plot.index, df_plot['ema9'], 'yellow', linewidth=1, alpha=0.7, label='EMA 9')
        ax1.plot(df_plot.index, df_plot['ema21'], 'orange', linewidth=1, alpha=0.7, label='EMA 21')
        ax1.fill_between(df_plot.index, df_plot['upper_band'], df_plot['lower_band'], alpha=0.1, color='gray', label='Bollinger Bands')
        ax1.axhline(y=entry, color='lime', linestyle='--', linewidth=1.5, label=f'ENTRY: ${entry:.2f}')
        ax1.axhline(y=tp, color='green', linestyle='--', linewidth=1.5, label=f'TP: ${tp:.2f}')
        ax1.axhline(y=sl, color='red', linestyle='--', linewidth=1.5, label=f'SL: ${sl:.2f}')
        ax1.fill_between(df_plot.index, entry, tp, alpha=0.2, color='green', label='Profit Zone')
        ax1.fill_between(df_plot.index, sl, entry, alpha=0.2, color='red', label='Loss Zone')
        if df['bandwidth'].iloc[-1] < df['bandwidth'].iloc[-10] * 0.7:
            ax1.axvspan(df_plot.index[-5], df_plot.index[-1], alpha=0.3, color='yellow', label='Pre-Breakout')
        ax1.set_title(f'{symbol} - Trading Signal ({session})', fontsize=14, color='white')
        ax1.set_ylabel('Price ($)', color='white')
        ax1.legend(loc='upper left', fontsize=9)
        ax1.grid(True, alpha=0.2)
        ax1.tick_params(colors='white')
        colors = ['green' if df_plot['close'].iloc[i] >= df_plot['close'].iloc[i-1] else 'red' for i in range(len(df_plot))]
        ax2.bar(df_plot.index, df_plot['volume'], color=colors, alpha=0.7)
        ax2.axhline(y=df_plot['vol_ma'].iloc[-1], color='blue', linestyle='--', linewidth=1, label=f'Avg Volume')
        ax2.set_ylabel('Volume', color='white')
        ax2.set_xlabel('Time', color='white')
        ax2.legend(loc='upper left', fontsize=9)
        ax2.grid(True, alpha=0.2)
        ax2.tick_params(colors='white')
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100, facecolor='#1a1a2e')
        buf.seek(0)
        caption = f"📊 **{symbol}**\n💰 Entry: ${entry:.2f} | 🎯 TP: ${tp:.2f} | 🛑 SL: ${sl:.2f}"
        if is_penny:
            caption += "\n🔥 **PENNY STOCK**"
        if is_accumulating:
            caption += "\n📦 **ACCUMULATION DETECTED**"
        bot.send_photo(chat_id=CHAT_ID, photo=buf, caption=caption, parse_mode='Markdown')
        plt.close()
    except Exception as e:
        logger.error(f"Chart error: {e}")

def open_trade(symbol, price, atr, score_val, size, df, is_penny=False, is_accumulating=False):
    tp = price + atr * 2
    sl = price - atr * 2
    session, _ = get_market_session()
    with state_lock:
        state["open_trades"][symbol] = {"entry": price, "tp": tp, "sl": sl, "size": size, "time": time.time(), "score": score_val}
        save_state()
    generate_and_send_chart(symbol, df, price, tp, sl, is_penny, is_accumulating, session)
    msg = f"🚀 **NEW TRADE** ({session})\n📊 {symbol}\n💰 Entry: ${price:.2f}\n🎯 TP: ${tp:.2f}\n🛑 SL: ${sl:.2f}\n⭐ Score: {score_val}\n📦 Shares: {size}"
    send_telegram(msg)

def close_trade(symbol, price, win=False):
    with state_lock:
        trade = state["open_trades"].pop(symbol, None)
        if not trade:
            return
        pnl = (price - trade["entry"]) * trade["size"]
        perf = state["performance"].setdefault(symbol, {"wins": 0, "losses": 0})
        if pnl >= 0:
            perf["wins"] += 1
        else:
            perf["losses"] += 1
            state["daily_loss"] += abs(pnl)
        save_state()
    result = "WIN 🎉" if pnl >= 0 else "LOSS ❌"
    msg = f"🔒 **TRADE CLOSED**\n📊 {symbol}\n📉 Exit: ${price:.2f}\n🏆 {result}\n💰 PnL: ${pnl:.2f}"
    send_telegram(msg)

# ================= UPDATE UNIVERSE =================
def update_universe():
    global dynamic_universe
    logger.info("Updating universe...")
    try:
        all_symbols = finnhub_client.stock_symbols('US')
        if all_symbols and len(all_symbols) > 0:
            symbols_list = [item['symbol'] for item in all_symbols if 'symbol' in item]
            new_symbols = symbols_list[:100]
            dynamic_universe = list(set(initial_universe + new_symbols))
            logger.info(f"Universe updated: {len(dynamic_universe)} symbols")
    except Exception as e:
        logger.error(f"Universe update error: {e}")

# ================= SCAN =================
data_cache = {}

def build_cache(symbols):
    global data_cache
    new_cache = {}
    for symbol in symbols:
        df = fetch_finnhub_candles(symbol, '15', 100)
        if df is not None and len(df) >= 30:
            new_cache[symbol] = compute_indicators(df)
        time.sleep(0.05)
    data_cache = new_cache
    logger.info(f"Cached {len(data_cache)} symbols")

def scan_symbols(symbols, allow_trading=True):
    build_cache(symbols)
    session, _ = get_market_session()
    for symbol in symbols:
        if symbol not in data_cache:
            continue
        if is_seen(symbol):
            continue
        df = data_cache[symbol]
        price = df["close"].iloc[-1]
        quote = fetch_quote(symbol)
        current_price = quote['c'] if quote else price
        is_penny = is_penny_stock(current_price)
        is_accumulating, acc_score = detect_accumulation(df)
        pre_breakout = pre_breakout_detection(df)
        score_val = score_from_cached(df)
        final_score = score_val + (10 if is_penny else 0) + (15 if is_accumulating else 0) + (10 if pre_breakout else 0)
        if pre_breakout and final_score >= 60:
            send_telegram(f"⚡ **Pre-Breakout** {symbol} - Score: {final_score} ({session})")
        if is_accumulating and acc_score >= 60:
            if should_send_accumulation_alert(symbol, acc_score):
                send_telegram(f"📦 **Accumulation** {symbol} - {acc_score}% ({session})")
                update_accumulation_alert(symbol, acc_score)
        if allow_trading and is_market_open():
            threshold = PENNY_THRESHOLD if is_penny else MIN_SCORE
            if final_score >= threshold and not is_seen(symbol):
                atr = df["atr"].iloc[-1]
                if pd.isna(atr):
                    atr = current_price * 0.02
                ok, size = can_trade(current_price, atr)
                if ok:
                    open_trade(symbol, current_price, atr, final_score, size, df, is_penny, is_accumulating)
                    mark_seen(symbol)

# ================= TELEGRAM COMMANDS =================
@bot.message_handler(commands=['start'])
def cmd_start(message):
    welcome = "🚀 **AI Trading Bot v15**\n\n/scan SYMBOL - تحليل سهم\n/status - حالة البوت\n/positions - الصفقات المفتوحة\n/performance - الأداء\n/close SYMBOL - إغلاق صفقة"
    bot.reply_to(message, welcome, parse_mode='Markdown')

@bot.message_handler(commands=['scan'])
def cmd_scan(message):
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "⚠️ استخدم: /scan <symbol>")
            return
        symbol = parts[1].upper()
        msg = bot.reply_to(message, f"🔍 Analyzing {symbol}...")
        df = fetch_finnhub_candles(symbol, '15', 100)
        if df is None or len(df) < 30:
            bot.edit_message_text(f"⚠️ No data for {symbol}", chat_id=message.chat.id, message_id=msg.message_id)
            return
        df = compute_indicators(df)
        score_val = score_from_cached(df)
        price = df["close"].iloc[-1]
        analysis = f"📊 **{symbol}**\n💰 Price: ${price:.2f}\n🧠 Score: {score_val}/100\n⚡️ RSI: {df['rsi'].iloc[-1]:.1f}\n📊 Volume: {df['volume'].iloc[-1]:.0f}"
        bot.edit_message_text(analysis, chat_id=message.chat.id, message_id=msg.message_id, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Scan error: {e}")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    with state_lock:
        msg = f"📊 **Status**\n💰 Capital: ${CAPITAL}\n📈 Open: {len(state['open_trades'])}/{MAX_OPEN_TRADES}\n📉 Daily Loss: ${state['daily_loss']:.2f}\n📊 Universe: {len(dynamic_universe)} stocks"
    bot.reply_to(message, msg, parse_mode='Markdown')

@bot.message_handler(commands=['positions'])
def cmd_positions(message):
    with state_lock:
        if not state["open_trades"]:
            bot.reply_to(message, "No open positions")
            return
        msg = "**Open Positions**\n"
        for sym, t in state["open_trades"].items():
            msg += f"\n🔹 {sym} | Entry ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f}"
    bot.reply_to(message, msg, parse_mode='Markdown')

@bot.message_handler(commands=['close'])
def cmd_close(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /close <symbol>")
        return
    symbol = parts[1].upper()
    with state_lock:
        if symbol not in state["open_trades"]:
            bot.reply_to(message, f"No open trade for {symbol}")
            return
    quote = fetch_quote(symbol)
    if not quote:
        bot.reply_to(message, f"Cannot get price for {symbol}")
        return
    close_trade(symbol, quote['c'], win=False)
    bot.reply_to(message, f"Closed {symbol} at ${quote['c']:.2f}")

# ================= MAIN =================
def background_scanner():
    global dynamic_universe
    last_update = 0
    while True:
        try:
            if time.time() - last_update > 3600:
                update_universe()
                last_update = time.time()
            scan_symbols(dynamic_universe, allow_trading=is_market_open())
            time.sleep(60)
        except Exception as e:
            logger.error(f"Scanner error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    load_state()
    update_universe()
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=lambda: bot.infinity_polling(timeout=10), daemon=True).start()
    send_telegram("✅ Bot Started - Finnhub Mode")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

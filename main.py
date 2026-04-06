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
import matplotlib.pyplot as plt
import re
import xml.etree.ElementTree as ET

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("⚠️ Missing TELEGRAM_TOKEN or CHAT_ID")

CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5
DAILY_LOSS_LIMIT = 200.0
MIN_SCORE = 75
PENNY_THRESHOLD = 65
SIGNAL_COOLDOWN = 3600
STATE_FILE = "state.json"
MAX_SCAN_SYMBOLS = 150

plt.style.use('dark_background')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

app = Flask(__name__)
bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
executor = ThreadPoolExecutor(max_workers=2)

# ================= STATE =================
state_lock = threading.RLock()
state = {
    "open_trades": {},
    "performance": {},
    "weights": {"trend": 20, "rsi": 15, "volume": 15, "obv": 10, "breakout": 15},
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

# ================= UNIVERSE =================
large_universe = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "BRK-B", "LLY", "JPM",
    "V", "XOM", "UNH", "WMT", "PG", "JNJ", "MA", "HD", "CVX", "BAC", "KO", "PEP",
    "COST", "ADBE", "CRM", "NFLX", "DIS", "AMD", "INTC", "CMCSA", "PFE", "TMO",
    "ABT", "DHR", "NKE", "LIN", "UPS", "SBUX", "LOW", "RTX", "HON", "CAT", "GS",
    "MS", "C", "WFC", "SPGI", "BLK", "AXP", "BK", "DE", "GE", "MMM", "AMGN",
    "TXN", "QCOM", "MU", "ADI", "NXPI", "MRVL", "LRCX", "KLAC", "AMAT", "ON",
    "PLTR", "SNOW", "NET", "DDOG", "ZS", "MDB", "CRWD", "PANW", "OKTA", "DOCU",
    "TEAM", "ROKU", "SHOP", "SQ", "AFRM", "UPST", "SOFI", "PYPL", "UBER", "LYFT",
    "DASH", "ABNB", "RBLX", "GME", "AMC", "BB", "NIO", "RIVN", "LCID", "PLUG",
    "RIOT", "MARA", "CLSK", "COIN", "HOOD", "BA", "LMT", "NOC"
]

dynamic_universe = large_universe[:MAX_SCAN_SYMBOLS]

# ================= DATA ENGINE =================
def fetch_yfinance_data(symbol, period='5d', interval='15m'):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval)
        if df.empty:
            return None
        df = df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
        return df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    except Exception as e:
        return None

def fetch_yfinance_quick(symbol):
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period='5d')
        if data.empty:
            return None
        return {'price': data['Close'].iloc[-1], 'volume': data['Volume'].iloc[-1], 'avg_volume': data['Volume'].mean()}
    except:
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
    with state_lock:
        w = state["weights"]
    s = 0
    if df["close"].iloc[-1] > df["close"].iloc[-5]:
        s += 5
    if c["ema9"] > c["ema21"]:
        s += w["trend"]
    if 50 < c["rsi"] < 70:
        s += w["rsi"]
    if c["volume"] > c["vol_ma"] * 1.3:
        s += w["volume"]
    if c["obv"] > df["obv"].rolling(10).mean().iloc[-1]:
        s += w["obv"]
    recent_high = df["high"].iloc[-10:].max()
    if c["close"] > recent_high:
        s += w["breakout"]
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

# ================= TRADING HALTS =================
def check_trading_halt(symbol):
    try:
        rss_url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
        response = requests.get(rss_url, timeout=10)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            for item in root.findall('.//item'):
                title = item.find('title').text if item.find('title') is not None else ''
                description = item.find('description').text if item.find('description') is not None else ''
                if symbol in title or symbol in description:
                    price_match = re.search(r'PauseThresholdPrice:\s*\$?([0-9]+\.[0-9]+)', description)
                    halt_price = price_match.group(1) if price_match else "N/A"
                    time_match = re.search(r'(\d{1,2}:\d{2}:\d{2})\s*ET', description)
                    halt_time = time_match.group(1) if time_match else "N/A"
                    reason_match = re.search(r'Reason:\s*(.+?)(?:\.|$)', description)
                    reason = reason_match.group(1) if reason_match else "Trading Pause"
                    return True, datetime.now().strftime('%Y-%m-%d'), halt_time, reason, halt_price
    except Exception as e:
        pass
    return False, None, None, None, None

# ================= SIGNAL CONTROL =================
def is_seen(symbol):
    now = time.time()
    with state_lock:
        if symbol in state["seen_signals"]:
            if now - state["seen_signals"][symbol] < SIGNAL_COOLDOWN:
                return True
    return False

def mark_seen(symbol):
    with state_lock:
        state["seen_signals"][symbol] = time.time()
        save_state()

def is_halt_alerted(symbol):
    now = time.time()
    with state_lock:
        if symbol in state["halted_alerts"]:
            if now - state["halted_alerts"][symbol] < 3600:
                return True
    return False

def mark_halt_alerted(symbol):
    with state_lock:
        state["halted_alerts"][symbol] = time.time()
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

# ================= CHART =================
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

# ================= SCAN =================
data_cache = {}

def build_cache(symbols, period='5d', interval='15m'):
    global data_cache
    new_cache = {}
    for symbol in symbols:
        df = fetch_yfinance_data(symbol, period, interval)
        if df is not None and len(df) >= 30:
            new_cache[symbol] = compute_indicators(df)
        time.sleep(0.2)
    data_cache = new_cache
    logger.info(f"Cached {len(data_cache)} symbols")

def update_positions():
    for symbol, trade in list(state["open_trades"].items()):
        if symbol not in data_cache:
            continue
        price = data_cache[symbol]["close"].iloc[-1]
        if price >= trade["tp"]:
            close_trade(symbol, price, win=True)
        elif price <= trade["sl"]:
            close_trade(symbol, price, win=False)

def quick_filter_stocks(symbols):
    promising = []
    total = len(symbols)
    for i, symbol in enumerate(symbols):
        try:
            quick = fetch_yfinance_quick(symbol)
            if quick and quick['price']:
                price = quick['price']
                volume = quick['volume']
                avg_volume = quick['avg_volume']
                if 2 < price < 150 and volume > 100000 and volume > avg_volume * 1.2:
                    promising.append(symbol)
            if (i + 1) % 50 == 0:
                logger.info(f"Quick filter progress: {i+1}/{total} symbols")
        except Exception as e:
            continue
        time.sleep(0.1)
    logger.info(f"Quick filter: {len(promising)} promising stocks out of {total}")
    return promising[:100]

def deep_analysis(symbols):
    results = []
    for symbol in symbols:
        if is_seen(symbol):
            continue
        df = fetch_yfinance_data(symbol, '5d', '15m')
        if df is None or len(df) < 30:
            continue
        df = compute_indicators(df)
        score_val = score_from_cached(df)
        if score_val >= MIN_SCORE - 10:
            results.append((symbol, score_val, df))
        time.sleep(0.1)
    return sorted(results, key=lambda x: x[1], reverse=True)

def scan_large_universe():
    session, _ = get_market_session()
    logger.info(f"🚀 Starting scan ({len(dynamic_universe)} symbols) - {session}")
    promising_stocks = quick_filter_stocks(dynamic_universe)
    if not promising_stocks:
        logger.warning("No promising stocks found")
        return
    logger.info(f"Step 2: Deep analysis of {len(promising_stocks)} stocks...")
    top_signals = deep_analysis(promising_stocks)
    logger.info(f"Step 3: Processing {len(top_signals)} signals...")
    for symbol, score_val, df in top_signals[:5]:
        if score_val >= MIN_SCORE and not is_seen(symbol):
            price = df["close"].iloc[-1]
            atr = df["atr"].iloc[-1]
            if pd.isna(atr):
                atr = price * 0.02
            is_penny = is_penny_stock(price)
            is_accumulating, acc_score = detect_accumulation(df)
            ok, size = can_trade(price, atr)
            if ok:
                open_trade(symbol, price, atr, score_val, size, df, is_penny, is_accumulating)
                mark_seen(symbol)

# ================= TELEGRAM =================
def send_telegram(msg):
    if not bot:
        print(f"Telegram message: {msg}")
        return
    try:
        bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

def generate_simple_analysis(symbol):
    df = fetch_yfinance_data(symbol, '5d', '15m')
    if df is None or len(df) < 30:
        return f"⚠️ No sufficient data for {symbol}"
    df = compute_indicators(df)
    session, _ = get_market_session()
    score_val = score_from_cached(df)
    price = df["close"].iloc[-1]
    atr = df["atr"].iloc[-1]
    volume = df["volume"].iloc[-1]
    avg_volume = df["vol_ma"].iloc[-1]
    rsi = df["rsi"].iloc[-1]
    is_penny = is_penny_stock(price)
    is_accumulating, acc_score = detect_accumulation(df)
    pre_breakout = pre_breakout_detection(df)
    analysis = f"📊 **{symbol} Analysis** ({session})\n💰 Price: ${price:.2f}\n📊 Volume: {volume:,.0f}\n⚖️ Avg Volume: {avg_volume:,.0f}\n🎢 ATR: {atr:.2f}\n⚡️ RSI: {rsi:.2f}\n🧠 Score: {score_val}/100\n"
    if is_penny:
        analysis += "🔥 **Penny Stock** 🔥\n"
    if is_accumulating:
        analysis += f"📦 **Accumulation Pattern** ({acc_score}%)\n"
    if pre_breakout:
        analysis += "⚡ **Pre-Breakout Detected** ⚡\n"
    return analysis

# ================= COMMANDS =================
@bot.message_handler(commands=['start'])
def cmd_start(message):
    session, session_msg = get_market_session()
    welcome = f"""
🚀 **AI Trading Bot v18 - 150 Stocks Scanner**

**Current Session:** {session}
{session_msg}

**Commands:**
/scan <symbol> - Technical analysis
/status - Bot status
/positions - Open positions
/performance - Performance stats
/close <symbol> - Close position
"""
    bot.reply_to(message, welcome, parse_mode='Markdown')

@bot.message_handler(commands=['scan'])
def cmd_scan(message):
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "⚠️ Usage: /scan <symbol>")
            return    
            symbol = parts[1].upper()
        processing_msg = bot.reply_to(message, f"🔍 Analyzing {symbol}...")
        def do_analysis():
            analysis = generate_simple_analysis(symbol)
            bot.edit_message_text(chat_id=message.chat.id, message_id=processing_msg.message_id, text=analysis, parse_mode='Markdown')
        future = executor.submit(do_analysis)
        try:
            future.result(timeout=15)
        except TimeoutError:
            bot.edit_message_text(chat_id=message.chat.id, message_id=processing_msg.message_id, text="⚠️ Timeout, try again")
    except Exception as e:
        bot.reply_to(message, "❌ Error")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    reset_daily_loss_if_needed()
    session, session_msg = get_market_session()
    with state_lock:
        total_wins = sum(p["wins"] for p in state["performance"].values())
        total_losses = sum(p["losses"] for p in state["performance"].values())
        total = total_wins + total_losses
        winrate = (total_wins / total * 100) if total else 0
        msg = f"📊 **Bot Status** ({session})\n{session_msg}\n💰 Capital: ${CAPITAL}\n📈 Open Trades: {len(state['open_trades'])}/{MAX_OPEN_TRADES}\n✅ Wins: {total_wins}\n❌ Losses: {total_losses}\n📈 Winrate: {winrate:.1f}%\n📉 Daily Loss: ${state['daily_loss']:.2f} / ${DAILY_LOSS_LIMIT}\n📊 Universe: {len(dynamic_universe)} stocks"
    bot.reply_to(message, msg, parse_mode='Markdown')

@bot.message_handler(commands=['positions'])
def cmd_positions(message):
    with state_lock:
        if not state["open_trades"]:
            bot.reply_to(message, "No open positions")
            return
        msg = "**Open Positions**\n"
        for sym, t in state["open_trades"].items():
            msg += f"\n🔹 {sym} | Entry ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f} | Shares {t['size']}"
    bot.reply_to(message, msg, parse_mode='Markdown')

@bot.message_handler(commands=['performance'])
def cmd_performance(message):
    with state_lock:
        if not state["performance"]:
            bot.reply_to(message, "No completed trades yet")
            return
        msg = "**Per-Symbol Performance**\n"
        for sym, p in state["performance"].items():
            total = p["wins"] + p["losses"]
            wr = (p["wins"] / total * 100) if total else 0
            msg += f"\n{sym}: {p['wins']}W / {p['losses']}L ({wr:.1f}%)"
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
    df = fetch_yfinance_data(symbol, '1d', '5m')
    if df is None or len(df) == 0:
        bot.reply_to(message, f"Cannot fetch price for {symbol}")
        return
    price = df["close"].iloc[-1]
    close_trade(symbol, price, win=False)
    bot.reply_to(message, f"Manually closed {symbol} at ${price:.2f}")

# ================= BACKGROUND SCANNER =================
def background_scanner():
    last_scan = 0
    while True:
        try:
            if time.time() - last_scan > 300 and is_market_open():
                logger.info(f"🔄 Scanning {len(dynamic_universe)} symbols...")
                scan_large_universe()
                last_scan = time.time()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Scanner error: {e}")
            time.sleep(60)

# ================= WEBHOOK =================
@app.route("/", methods=['GET'])
def home():
    return "AI Trading Bot v18 Running"

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = telebot.types.Update.de_json(request.data.decode("utf-8"))
        bot.process_new_updates([update])
        return "ok", 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return "error", 500

# ================= START =================
if __name__ == "__main__":
    load_state()
    reset_daily_loss_if_needed()
    threading.Thread(target=background_scanner, daemon=True).start()
    if bot:
        threading.Thread(target=lambda: bot.infinity_polling(timeout=10, long_polling_timeout=5), daemon=True).start()
    send_telegram("✅ **AI Trading Bot v18 - 150 Stocks Scanner is LIVE!**")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

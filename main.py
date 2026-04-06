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
import io
import matplotlib.pyplot as plt
import re
import xml.etree.ElementTree as ET

# ================= CONFIGURATION =================
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
POLYGON_API = os.getenv("POLYGON_API")

if not TOKEN or not CHAT_ID or not POLYGON_API:
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
bot = telebot.TeleBot(TOKEN)

# ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=2)

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
    "last_accumulation": {},  # 🔥 NEW: لمنع تكرار تنبيهات التجميع
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

# ================= RATE LIMIT =================
_last_api_call = 0
_api_lock = threading.Lock()

def rate_limit():
    global _last_api_call
    with _api_lock:
        now = time.time()
        wait = 12.5 - (now - _last_api_call)
        if wait > 0:
            time.sleep(wait)
        _last_api_call = time.time()

# ================= DATA ENGINE =================
def fetch_polygon(symbol):
    rate_limit()
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/15/minute/{start}/{end}?adjusted=true&limit=300&apiKey={POLYGON_API}"
    try:
        resp = requests.get(url, timeout=15).json()
        if "results" not in resp or not resp["results"]:
            return None
        df = pd.DataFrame(resp["results"])
        df.rename(columns={"c":"close","h":"high","l":"low","o":"open","v":"volume"}, inplace=True)
        df = df[["open","high","low","close","volume"]].astype(float)
        return compute_indicators(df)
    except Exception as e:
        logger.error(f"Fetch error {symbol}: {e}")
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

# ================= SCORING =================
def score_from_cached(df):
    if df is None or len(df) < 30:
        return 0
    c = df.iloc[-1]
    with state_lock:
        w = state["weights"]
    s = 0
    if c["ema9"] > c["ema21"]:
        s += w["trend"]
    if 45 < c["rsi"] < 75:
        s += w["rsi"]
    if c["volume"] > c["vol_ma"] * 1.3:
        s += w["volume"]
    if c["obv"] > df["obv"].rolling(10).mean().iloc[-1]:
        s += w["obv"]
    recent_high = df["high"].iloc[-10:].max()
    if c["close"] > recent_high:
        s += w["breakout"]
    return min(100, s)

# ================= PENNY STOCK =================
def is_penny_stock(price):
    return price < 5.0

# ================= ACCUMULATION DETECTION =================
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

# ================= PRE-BREAKOUT =================
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
        logger.error(f"Error checking halt for {symbol}: {e}")
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

# ================= ACCUMULATION ALERT CONTROL (NEW) =================
def should_send_accumulation_alert(symbol, new_score):
    """تتحقق إذا كان يجب إرسال تنبيه التجميع (يمنع التكرار بنسبة 10%)"""
    with state_lock:
        last = state["last_accumulation"].get(symbol)
        if not last:
            return True
        old_score = last.get("score", 0)
        # إذا تغيرت النسبة بـ 10% على الأقل، أرسل
        if abs(new_score - old_score) >= 10:
            return True
        # إذا مر أكثر من 4 ساعات على آخر إرسال، أرسل (تحديث دوري)
        if time.time() - last.get("time", 0) > 4 * 3600:
            return True
        return False

def update_accumulation_alert(symbol, new_score):
    """تحديث آخر تنبيه تم إرساله"""
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
        return "REGULAR HOURS", "🟢 تداول نشط - يمكن فتح صفقات"
    elif 4 <= hour < 9:
        return "PRE-MARKET", "🟡 تحليل فقط - انتظر الافتتاح"
    elif 16 <= hour < 20:
        return "AFTER-HOURS", "🟡 تحليل فقط - سيولة منخفضة"
    else:
        return "OVERNIGHT", "🔵 تحضير لليوم التالي"

# ================= RISK MANAGEMENT =================
def can_trade(price, atr):
    reset_daily_loss_if_needed()
    with state_lock:
        if len(state["open_trades"]) >= MAX_OPEN_TRADES:
            return False, 0
        if state["daily_loss"] >= DAILY_LOSS_LIMIT:
            return False, 0
    risk_amount = CAPITAL * RISK_PER_TRADE
    stop_distance = atr * 2
    if stop_distance <= 0:
        stop_distance = price * 0.02
    size = int(risk_amount / stop_distance)
    if size < 1:
        return False, 0
    if size * price > CAPITAL * 0.2:
        size = int(CAPITAL * 0.2 / price)
        if size < 1:
            return False, 0
    return True, size

# ================= CHART GENERATION =================
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
        
        caption = f"📊 **{symbol} - Technical Analysis**\n💰 Entry: ${entry:.2f} | 🎯 TP: ${tp:.2f} | 🛑 SL: ${sl:.2f}\n"
        if is_penny:
            caption += "🔥 **PENNY STOCK ALERT** 🔥\n"
        if is_accumulating:
            caption += "📦 **Accumulation Pattern Detected** 📦\n"
        
        bot.send_photo(chat_id=CHAT_ID, photo=buf, caption=caption, parse_mode='Markdown')
        plt.close()
    except Exception as e:
        logger.error(f"Chart error {symbol}: {e}")

# ================= TRADE MANAGEMENT =================
def open_trade(symbol, price, atr, score_val, size, df, is_penny=False, is_accumulating=False):
    tp = price + atr * 2
    sl = price - atr * 2
    session, _ = get_market_session()
    
    with state_lock:
        state["open_trades"][symbol] = {
            "entry": price, "tp": tp, "sl": sl, "size": size,
            "time": time.time(), "score": score_val, "is_penny": is_penny
        }
        save_state()
    
    generate_and_send_chart(symbol, df, price, tp, sl, is_penny, is_accumulating, session)
    
    msg = f"""
🚀 **NEW TRADE OPENED** ({session})
📊 Symbol: `{symbol}`
💰 Entry: ${price:.2f}
🎯 TP: ${tp:.2f}
🛑 SL: ${sl:.2f}
⭐ Score: {score_val}/100
📦 Shares: {size}
💵 Risk: ${size * (price - sl):.2f}
"""
    if is_penny:
        msg += "🔥 **Penny Stock - Higher Risk/Reward** 🔥\n"
    if is_accumulating:
        msg += "📦 **Accumulation Pattern - Pre-Breakout** 📦\n"
    
    send_telegram(msg)
    logger.info(f"✅ OPENED {symbol} at {price}")

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
        total = perf["wins"] + perf["losses"]
        if total >= 5:
            winrate = perf["wins"] / total
            if winrate > 0.6:
                state["weights"]["breakout"] = min(state["weights"]["breakout"] + 1, 30)
            elif winrate < 0.4:
                state["weights"]["breakout"] = max(state["weights"]["breakout"] - 1, 5)
        save_state()
    
    result = "WIN 🎉" if pnl >= 0 else "LOSS ❌"
    msg = f"""
🔒 **TRADE CLOSED**
📊 {symbol}
📉 Exit: ${price:.2f}
🏆 Result: {result}
💰 PnL: ${pnl:.2f}
📊 Daily Loss: ${state['daily_loss']:.2f} / ${DAILY_LOSS_LIMIT}
"""
    send_telegram(msg)
    logger.info(f"🔒 CLOSED {symbol}: {result} PnL ${pnl:.2f}")

# ================= CACHE & SCAN =================
data_cache = {}

def build_cache(symbols):
    global data_cache
    new_cache = {}
    for s in symbols:
        df = fetch_polygon(s)
        if df is not None:
            new_cache[s] = df
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

# ================= ENHANCED SYMBOLS =================
def get_enhanced_universe():
    main_symbols = ["TSLA", "NVDA", "AMD", "AAPL", "MSFT", "META", "GOOGL"]
    penny_symbols = ["SOFI", "NIO", "PLTR", "RIOT", "MARA", "CLSK", "AMC", "GME", "BB", "SNDL"]
    return list(set(main_symbols + penny_symbols))

def scan_symbols(symbols, allow_trading=True):
    build_cache(symbols)
    update_positions()
    session, session_msg = get_market_session()
    
    for symbol in symbols:
        if symbol not in data_cache:
            continue
        if is_seen(symbol):
            continue
        
        df = data_cache[symbol]
        price = df["close"].iloc[-1]
        
        # Trading Halts Check
        halted, halt_date, halt_time, reason, halt_price = check_trading_halt(symbol)
        if halted:
            if not is_halt_alerted(symbol):
                halt_msg = f"""
⛔ **TRADING HALT DETECTED** ⛔
📊 Symbol: `{symbol}`
📅 Date: {halt_date}
⏱️ Time: {halt_time}
💲 Halt Price: ${halt_price}
📄 Reason: {reason}
"""
                send_telegram(halt_msg)
                mark_halt_alerted(symbol)
            continue
        
        # Technical Analysis
        is_penny = is_penny_stock(price)
        is_accumulating, acc_score = detect_accumulation(df)
        pre_breakout = pre_breakout_detection(df)
        score_val = score_from_cached(df)
        
        penny_bonus = 10 if is_penny else 0
        accumulation_bonus = 15 if is_accumulating else 0
        pre_breakout_bonus = 10 if pre_breakout else 0
        final_score = score_val + penny_bonus + accumulation_bonus + pre_breakout_bonus
        
        # Alerts (with anti-spam)
        if pre_breakout and final_score >= 60:
            send_telegram(f"⚡ **Pre-Breakout Alert!** {symbol} - Score: {final_score} ({session})")
        
        # 🔥 FIXED: Accumulation Alert with anti-spam
        if is_accumulating and acc_score >= 60:
            if should_send_accumulation_alert(symbol, acc_score):
                send_telegram(f"📦 **Accumulation Detected!** {symbol} - Score: {acc_score}% ({session})")
                update_accumulation_alert(symbol, acc_score)
        
        # Open Trades (only when market is open)
        if allow_trading and is_market_open():
            threshold = PENNY_THRESHOLD if is_penny else MIN_SCORE
            if final_score >= threshold and not is_seen(symbol):
                atr = df["atr"].iloc[-1]
                if pd.isna(atr):
                    atr = price * 0.02
                ok, size = can_trade(price, atr)
                if ok:
                    open_trade(symbol, price, atr, final_score, size, df, is_penny, is_accumulating)
                    mark_seen(symbol)
        elif final_score >= 70 and not is_seen(symbol):
            msg = f"""
📊 **Pre-Market Signal** ({session})
📊 Symbol: `{symbol}`
💰 Price: ${price:.2f}
⭐ Score: {final_score}/100
🔥 Status: {session_msg}
"""
            send_telegram(msg)
            mark_seen(symbol)

# ================= TELEGRAM FUNCTIONS =================
def send_telegram(msg):
    try:
        bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram send error: {e}")
def generate_simple_analysis(symbol):
    df = fetch_polygon(symbol)
    if df is None or len(df) < 30:
        return f"⚠️ No sufficient data for {symbol}"
    
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
    
    analysis = f"""
📊 **{symbol} Analysis** ({session})
💰 Price: ${price:.2f}
📊 Volume: {volume:,.0f}
⚖️ Avg Volume: {avg_volume:,.0f}
🎢 ATR: {atr:.2f}
⚡️ RSI: {rsi:.2f}
🧠 Score: {score_val}/100
"""
    if is_penny:
        analysis += "🔥 **Penny Stock** 🔥\n"
    if is_accumulating:
        analysis += f"📦 **Accumulation Pattern** ({acc_score}%)\n"
    if pre_breakout:
        analysis += "⚡ **Pre-Breakout Detected** ⚡\n"
    
    return analysis

# ================= TELEGRAM COMMANDS =================
@bot.message_handler(commands=['start'])
def cmd_start(message):
    session, session_msg = get_market_session()
    welcome = f"""
🚀 **AI Trading Bot v15 - 24/7 Mode (No Spam)**

**Current Session:** {session}
{session_msg}

**Commands:**
/scan <symbol> - Technical analysis
/status - Bot status
/positions - Open positions
/performance - Performance stats
/close <symbol> - Close position

**Auto Alerts:**
✅ Accumulation Alert (only when score changes by 10% or every 4 hours)
✅ Pre-Breakout Alert
✅ Trading Halt Alert
✅ Trade Open/Close
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
            bot.edit_message_text(chat_id=message.chat.id, message_id=processing_msg.message_id, text="⚠️ Analysis timeout, try again")
    except Exception as e:
        logger.error(f"Scan error: {e}")
        bot.reply_to(message, "❌ Error occurred")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    reset_daily_loss_if_needed()
    session, session_msg = get_market_session()
    with state_lock:
        total_wins = sum(p["wins"] for p in state["performance"].values())
        total_losses = sum(p["losses"] for p in state["performance"].values())
        total = total_wins + total_losses
        winrate = (total_wins / total * 100) if total else 0
        msg = f"""
📊 **Bot Status** ({session})
{session_msg}
💰 Capital: ${CAPITAL}
📈 Open Trades: {len(state['open_trades'])}/{MAX_OPEN_TRADES}
✅ Wins: {total_wins}
❌ Losses: {total_losses}
📈 Winrate: {winrate:.1f}%
📉 Daily Loss: ${state['daily_loss']:.2f} / ${DAILY_LOSS_LIMIT}
⚙️ Weights: {state['weights']}
"""
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
    df = fetch_polygon(symbol)
    if df is None:
        bot.reply_to(message, f"Cannot fetch price for {symbol}")
        return
    price = df["close"].iloc[-1]
    close_trade(symbol, price, win=False)
    bot.reply_to(message, f"Manually closed {symbol} at ${price:.2f}")

# ================= BACKGROUND SCANNER (24/7) =================
def background_scanner():
    while True:
        try:
            symbols = get_enhanced_universe()
            session, _ = get_market_session()
            logger.info(f"🔄 [{session}] Scanning {len(symbols)} symbols...")
            allow_trading = is_market_open()
            scan_symbols(symbols, allow_trading=allow_trading)
            time.sleep(60)
        except Exception as e:
            logger.error(f"Scanner error: {e}")
            time.sleep(60)

# ================= WEBHOOK =================
@app.route("/", methods=['GET'])
def home():
    session, _ = get_market_session()
    return f"AI Trading Bot v15 is running - {session}"

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
    
    def run_bot():
        logger.info("Starting bot polling...")
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
    
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=background_scanner, daemon=True).start()
    
    session, session_msg = get_market_session()
    send_telegram(f"✅ **AI Trading Bot v15 is LIVE! (No Spam Mode)**\n\n🕐 Session: {session}\n{session_msg}\n\n✅ Accumulation alerts will NOT repeat unless score changes by 10% or 4 hours pass")
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

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
SIGNAL_COOLDOWN = 3600
STATE_FILE = "state.json"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Flask and Telegram bot
app = Flask(__name__)
bot = telebot.TeleBot(TOKEN)

# ThreadPoolExecutor for /scan command
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
    "daily_loss": 0.0,
    "last_reset": None
}

def save_state():
    with state_lock:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    logger.debug("State saved")

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
            logger.warning(f"No data for {symbol}")
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

def open_trade(symbol, price, atr, score_val, size):
    tp = price + atr * 2
    sl = price - atr * 2
    with state_lock:
        state["open_trades"][symbol] = {
            "entry": price,
            "tp": tp,
            "sl": sl,
            "size": size,
            "time": time.time(),
            "score": score_val
        }
        save_state()
    msg = f"🚀 {symbol} OPEN\nEntry ${price:.2f}\nTP ${tp:.2f}\nSL ${sl:.2f}\nScore {score_val}\nShares {size}"
    send_telegram(msg)
    logger.info(f"Opened {symbol} at {price}")

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
    msg = f"🔒 {symbol} CLOSED at ${price:.2f}\nPnL ${pnl:.2f}\n{result}\nDaily loss: ${state['daily_loss']:.2f}"
    send_telegram(msg)
    logger.info(f"Closed {symbol}: {result} PnL {pnl:.2f}")

# ================= MARKET HOURS =================
def is_market_open():
    tz = pytz.timezone("US/Eastern")
    now = datetime.now(tz)
    if now.weekday() >= 5:
        return False
    open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_time <= now <= close_time

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

def scan_symbols(symbols):
    build_cache(symbols)
    update_positions()
    for symbol in symbols:
        if symbol not in data_cache:
            continue
        if is_seen(symbol):
            continue
        df = data_cache[symbol]
        score_val = score_from_cached(df)
        if score_val < MIN_SCORE:
            continue
        price = df["close"].iloc[-1]
        atr = df["atr"].iloc[-1]
        if pd.isna(atr):
            atr = price * 0.02
        ok, size = can_trade(price, atr)
        if not ok:
            continue
        open_trade(symbol, price, atr, score_val, size)
        mark_seen(symbol)

# ================= TELEGRAM FUNCTIONS =================
def send_telegram(msg):
    try:
        bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

def generate_simple_analysis(symbol):
    df = fetch_polygon(symbol)
    if df is None or len(df) < 30:
        return f"⚠️ لا توجد بيانات كافية للسهم {symbol}. تأكد من الرمز."
    score_val = score_from_cached(df)
    price = df["close"].iloc[-1]
    atr = df["atr"].iloc[-1]
    volume = df["volume"].iloc[-1]
    avg_volume = df["vol_ma"].iloc[-1]
    rsi = df["rsi"].iloc[-1]
    return f"""
📊 <b>تحليل سهم {symbol}</b>
💰 السعر الحالي: ${price:.2f}
📊 الحجم: {volume:,.0f}
⚖️ متوسط الحجم: {avg_volume:,.0f}
🎢 ATR: {atr:.2f}
⚡️ RSI: {rsi:.2f}
🧠 النتيجة: {score_val}/100
"""

# ================= TELEGRAM COMMANDS =================
@bot.message_handler(commands=['start'])
def cmd_start(message):
    welcome = """
🚀 <b>بوت التداول الآلي v14</b>

<b>الأوامر المتاحة:</b>
/scan &lt;رمز&gt; – تحليل فني
/status – حالة البوت
/positions – الصفقات المفتوحة
/performance – أداء كل رمز
/close &lt;رمز&gt; – إغلاق صفقة
"""
    bot.reply_to(message, welcome, parse_mode='HTML')

@bot.message_handler(commands=['scan'])
def cmd_scan(message):
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "⚠️ استخدم: /scan <رمز_السهم>")
            return
        symbol = parts[1].upper()
        processing_msg = bot.reply_to(message, f"🔍 جاري تحليل {symbol}...")
        def do_analysis():
            analysis = generate_simple_analysis(symbol)
            bot.edit_message_text(chat_id=message.chat.id, message_id=processing_msg.message_id, text=analysis, parse_mode='HTML')
        future = executor.submit(do_analysis)
        try:
            future.result(timeout=15)
        except TimeoutError:
            bot.edit_message_text(chat_id=message.chat.id, message_id=processing_msg.message_id, text="⚠️ استغرق التحليل وقتاً طويلاً")
    except Exception as e:
        logger.error(f"Scan error: {e}")
        bot.reply_to(message, "❌ حدث خطأ")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    reset_daily_loss_if_needed()
    with state_lock:
        total_wins = sum(p["wins"] for p in state["performance"].values())
        total_losses = sum(p["losses"] for p in state["performance"].values())
        total = total_wins + total_losses
        winrate = (total_wins / total * 100) if total else 0
        msg = f"""
📊 <b>حالة البوت</b>
💰 رأس المال: ${CAPITAL}
📈 الصفقات المفتوحة: {len(state['open_trades'])}/{MAX_OPEN_TRADES}
✅ فوز: {total_wins}
❌ خسارة: {total_losses}
📈 نسبة الفوز: {winrate:.1f}%
📉 خسارة اليوم: ${state['daily_loss']:.2f}
"""
    bot.reply_to(message, msg, parse_mode='HTML')

@bot.message_handler(commands=['positions'])
def cmd_positions(message):
    with state_lock:
        if not state["open_trades"]:
            bot.reply_to(message, "لا توجد صفقات مفتوحة")
            return
        msg = "<b>الصفقات المفتوحة</b>\n"
        for sym, t in state["open_trades"].items():
            msg += f"\n🔹 {sym} | دخول ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f}"
    bot.reply_to(message, msg, parse_mode='HTML')

@bot.message_handler(commands=['performance'])
def cmd_performance(message):
    with state_lock:
        if not state["performance"]:
            bot.reply_to(message, "لا توجد صفقات مكتملة")
            return
        msg = "<b>أداء كل رمز</b>\n"
        for sym, p in state["performance"].items():
            total = p["wins"] + p["losses"]
            wr = (p["wins"] / total * 100) if total else 0
            msg += f"\n{sym}: {p['wins']}W / {p['losses']}L ({wr:.1f}%)"
    bot.reply_to(message, msg, parse_mode='HTML')

@bot.message_handler(commands=['close'])
def cmd_close(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "استخدم: /close <رمز>")
        return
    symbol = parts[1].upper()
    with state_lock:
        if symbol not in state["open_trades"]:
            bot.reply_to(message, f"لا توجد صفقة لـ {symbol}")
            return
    df = fetch_polygon(symbol)
    if df is None:
        bot.reply_to(message, f"لا يمكن جلب سعر {symbol}")
        return
    price = df["close"].iloc[-1]
    close_trade(symbol, price, win=False)
    bot.reply_to(message, f"تم إغلاق {symbol} عند ${price:.2f}")

# ================= BACKGROUND SCANNER =================
def background_scanner():
    symbols = ["TSLA","NVDA","AMD","PLTR","AAPL","MSFT","META","GOOGL"]
    while True:
        try:
            if is_market_open():
                scan_symbols(symbols)
            else:
                logger.info("Market closed, sleeping")
            time.sleep(60)
        except Exception as e:
            logger.error(f"Scanner error: {e}")
            time.sleep(60)

# ================= WEBHOOK ENDPOINT =================
@app.route("/", methods=['GET'])
def home():
    return "Bot is running"

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
    
    # بدء البوت بطريقة Polling
    def run_bot():
        logger.info("Starting bot polling...")
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
    
    polling_thread = threading.Thread(target=run_bot, daemon=True)
    polling_thread.start()
    
    # بدء الماسح الضوئي
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    
    # إرسال رسالة بدء التشغيل
    send_telegram("✅ بوت التداول v14 شغال الآن!")
    
    # تشغيل Flask
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

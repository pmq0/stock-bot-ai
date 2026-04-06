import os
import time
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
import telebot
from collections import deque
import threading

# ================= CONFIGURATION =================
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
POLYGON_API = os.getenv("POLYGON_API")

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

if not TOKEN or not CHAT_ID or not POLYGON_API:
    logger.error("Missing Environment Variables! Please set TOKEN, CHAT_ID, and POLYGON_API.")
    exit(1)

bot = telebot.TeleBot(TOKEN)

# ================= TELEGRAM =================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
        )
        logger.info("Telegram message sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

@bot.message_handler(commands=['start'])
def start_command(message):
    bot.reply_to(message, "🚀 <b>AI Institutional Ultra Pro v6.0 Active!</b>\n\nI am scanning the US Stock Market for high-probability setups. Signals will be sent automatically.")

# ================= UNIVERSE =================
def get_universe():
    return [
        "AAPL", "MSFT", "NVDA", "AMD", "META", "TSLA",
        "AMZN", "GOOGL", "SPY", "QQQ", "PLTR", "SOFI",
        "BABA", "NIO", "RIOT", "MARA", "INTC", "JPM", "BAC",
        "COIN", "UBER", "NFLX", "DIS", "PYPL", "SQ", "CRM", "ADBE",
        "CMCSA", "CSCO", "XOM", "CVX", "PG", "KO", "PEP", "WMT", "HD"
    ]

# ================= MARKET HOURS =================
def is_market_open():
    tz = pytz.timezone("US/Eastern")
    now = datetime.now(tz)
    if now.weekday() >= 5:
        return False
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close

# ================= POLYGON DATA =================
data_cache = {}

def get_data(symbol, timespan="minute", multiplier=15, days_back=10):
    cache_key = f"{symbol}_{timespan}_{multiplier}_{days_back}"
    now_utc = datetime.now(pytz.utc)
    
    if cache_key in data_cache:
        cached_time, df = data_cache[cache_key]
        if (now_utc - cached_time).total_seconds() < 300:
            return df

    tz = pytz.timezone("US/Eastern")
    end_date = datetime.now(tz)
    start_date = end_date - timedelta(days=days_back)
    
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?adjusted=true&sort=asc&limit=1000&apiKey={POLYGON_API}"
    
    try:
        r = requests.get(url, timeout=10).json()
        if "results" not in r or len(r["results"]) == 0:
            return None
            
        df = pd.DataFrame(r["results"])
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "vw": "vwap", "t": "timestamp"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms', utc=True)
        df = df.set_index("timestamp")
        
        # Liquidity Filter: Skip low volume stocks
        if df["volume"].iloc[-1] < 200000:
            return None
            
        data_cache[cache_key] = (now_utc, df)
        return df
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None

# ================= INDICATORS =================
def calculate_indicators(df):
    if len(df) < 50:
        return df
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df["atr"] = true_range.rolling(14).mean()
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))
    df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()
    ema_12 = df["close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema_12 - ema_26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    df["bb_middle"] = df["close"].rolling(window=20).mean()
    bb_std = df["close"].rolling(window=20).std()
    df["bb_upper"] = df["bb_middle"] + (bb_std * 2)
    df["bb_lower"] = df["bb_middle"] - (bb_std * 2)
    df["vol_sma_20"] = df["volume"].rolling(window=20).mean()
    return df

# ================= SCORING ENGINE =================
def score_setup(df):
    score = 0
    reasons = []
    if len(df) < 200:
        return 0, ["Insufficient data"]
    current = df.iloc[-1]
    prev = df.iloc[-2]
    if current["close"] > current["ema_50"] and current["ema_50"] > current["ema_200"]:
        score += 15
        reasons.append("Strong Uptrend (Price > EMA50 > EMA200)")
    if current["ema_9"] > current["ema_21"] and prev["ema_9"] <= prev["ema_21"]:
        score += 15
        reasons.append("Bullish EMA Cross (9 over 21)")
    if 45 < current["rsi"] < 65:
        score += 10
        reasons.append("Healthy RSI (45-65)")
    if current["macd"] > current["macd_signal"] and current["macd_hist"] > 0 and prev["macd_hist"] <= 0:
        score += 15
        reasons.append("Bullish MACD Crossover")
    if current["volume"] > current["vol_sma_20"] * 2:
        score += 15
        reasons.append("Significant Volume Spike")
    recent_high_50 = df["high"].iloc[-50:-1].max()
    if current["close"] > recent_high_50:
        score += 15
        reasons.append("50-Period Price Breakout")
    return score, reasons

# ================= RISK MANAGEMENT =================
def calculate_trade_params(price, atr_val, account_balance=10000, risk_per_trade_percent=1.0):
    if atr_val == 0 or np.isnan(atr_val):
        return None
    sl_dist = atr_val * 1.5
    sl = price - sl_dist
    if sl <= 0:
        return None
    t1 = price + (sl_dist * 1.0)
    t2 = price + (sl_dist * 1.5)
    t3 = price + (sl_dist * 2.0)
    risk_amount = account_balance * (risk_per_trade_percent / 100)
    risk_per_share = price - sl
    if risk_per_share <= 0:
        return None
    suggested_shares = int(risk_amount / risk_per_share)
    if suggested_shares <= 0:
        return None
    rr = round((t2 - price) / (price - sl), 2) if price > sl else 0
    return {"entry": price, "sl": sl, "t1": t1, "t2": t2, "t3": t3, "shares": suggested_shares, "risk_reward": rr}

# ================= ANALYSIS PIPELINE =================
def analyze(symbol):
    df = get_data(symbol, days_back=10)
    if df is None or len(df) < 200:
        return None
    df = calculate_indicators(df)
    df = df.dropna()
    if len(df) < 50:
        return None
    score, reasons = score_setup(df)
    if score < 70:
        return None
    current = df.iloc[-1]
    trade_params = calculate_trade_params(current["close"], current["atr"])
    if trade_params is None:
        return None
    return {"symbol": symbol, "score": score, "price": current["close"], "reasons": reasons, "params": trade_params, "time": datetime.now(pytz.timezone("US/Eastern")).strftime("%H:%M:%S EST")}

# ================= MAIN ENGINE =================
def run_engine():
    startup_msg = "🚀 <b>AI Institutional Ultra Pro v6.0 Started</b>\n\n✅ Advanced Indicators\n✅ Dynamic Risk Management\n✅ Market Hours Filtering\n✅ Railway Deployment Ready\n✅ Polling Active"
    send(startup_msg)
    seen_signals = deque(maxlen=100)
    while True:
        try:
            if not is_market_open():
                logger.info("Market is closed. Waiting...")
                time.sleep(300)
                continue
            universe = get_universe()
            now_utc = datetime.now(pytz.utc)
            for symbol in universe:
                # Fixed seen_signals logic using UTC comparison
                if any(s["symbol"] == symbol and (now_utc - s["timestamp"]).total_seconds() < 7200 for s in seen_signals):
                    continue
                r = analyze(symbol)
                if r:
                    seen_signals.append({"symbol": r["symbol"], "timestamp": now_utc})
                    p = r["params"]
                    reasons_str = "\n".join([f"🔹 {reason}" for reason in r["reasons"]])
                    msg = f"🚨 <b>AI ULTRA PRO SIGNAL</b> 🚨\n\n📊 <b>Ticker:</b> #{r['symbol']}\n⭐ <b>Score:</b> {r['score']}/100\n⏰ <b>Time:</b> {r['time']}\n\n💰 <b>Entry:</b> ${p['entry']:.2f}\n\n🎯 <b>Targets:</b>\nT1: ${p['t1']:.2f}\nT2: ${p['t2']:.2f}\nT3: ${p['t3']:.2f}\n\n🛑 <b>Stop Loss:</b> ${p['sl']:.2f}\n\n⚖️ <b>R/R:</b> 1:{p['risk_reward']}\n📦 <b>Size:</b> {p['shares']} shares\n\n🧠 <b>Confluence:</b>\n{reasons_str}"
                    send(msg)
                    time.sleep(12) # Respect Polygon Free Tier (5 calls/min)
            time.sleep(300)
        except Exception as e:
            logger.error(f"Error in engine: {e}")
            time.sleep(60)

if __name__ == "__main__":
    # Start the engine in a separate thread
    engine_thread = threading.Thread(target=run_engine, daemon=True)
    engine_thread.start()
    
    # Start Telegram Polling in the main thread
    logger.info("Starting Telegram Polling...")
    try:
        bot.infinity_polling()
    except Exception as e:
        logger.error(f"Polling error: {e}")
        time.sleep(10)
        

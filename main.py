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
    bot.reply_to(message, "🚀 <b>AI Institutional Ultra Pro v7.5 Active!</b>\n\nScanning 24/7 for Penny Stocks, Accumulation, and Explosive Breakouts.\n\n⚠️ <b>Halt Radar:</b> Active (Monitoring for Trading Halts & Resumes)")

# ================= UNIVERSE (DYNAMIC & EXPANDED) =================
def get_universe():
    return [
        "AAPL", "MSFT", "NVDA", "AMD", "META", "TSLA", "AMZN", "GOOGL", "PLTR", "SOFI",
        "BABA", "NIO", "RIOT", "MARA", "COIN", "UBER", "NFLX", "PYPL", "SQ", "CRM",
        "F", "T", "PFE", "BAC", "JPM", "WMT", "KO", "DIS", "NKE", "V",
        "GME", "AMC", "HOOD", "DKNG", "PLUG", "LCID", "NKLA", "FUBO", "OPEN", "CLOV"
    ]

# ================= POLYGON DATA (24/7 SUPPORT) =================
data_cache = {}
halt_status = {} # Track halt status for each symbol

def get_data(symbol, timespan="minute", multiplier=1, days_back=5):
    cache_key = f"{symbol}_{timespan}_{multiplier}_{days_back}"
    now_utc = datetime.now(pytz.utc)
    
    if cache_key in data_cache:
        cached_time, df = data_cache[cache_key]
        if (now_utc - cached_time).total_seconds() < 60: # Faster refresh for Halts
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
        
        data_cache[cache_key] = (now_utc, df)
        return df
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None

# ================= HALT RADAR =================
def check_halt(symbol, df):
    if df is None or len(df) < 5:
        return
    
    current_time = df.index[-1]
    last_price = df["close"].iloc[-1]
    prev_price = df["close"].iloc[-2]
    
    # If the last data point is older than 5 minutes during market hours, it might be halted
    # Or if volume is 0 and price is unchanged for several minutes
    time_diff = (datetime.now(pytz.utc) - current_time).total_seconds()
    
    is_currently_halted = time_diff > 300 # 5 minutes of no data
    
    if symbol not in halt_status:
        halt_status[symbol] = False
        
    if is_currently_halted and not halt_status[symbol]:
        halt_status[symbol] = True
        direction = "UP 📈" if last_price > prev_price else "DOWN 📉"
        msg = f"⚠️ <b>TRADING HALT DETECTED</b> ⚠️\n\n📊 <b>Ticker:</b> #{symbol}\n💰 <b>Halt Price:</b> ${last_price:.2f}\n⚡ <b>Direction:</b> {direction}\n⏰ <b>Time:</b> {datetime.now(pytz.timezone('US/Eastern')).strftime('%H:%M:%S EST')}\n\n<i>Waiting for resume...</i>"
        send(msg)
        
    elif not is_currently_halted and halt_status[symbol]:
        halt_status[symbol] = False
        msg = f"✅ <b>TRADING RESUMED</b> ✅\n\n📊 <b>Ticker:</b> #{symbol}\n💰 <b>Current Price:</b> ${last_price:.2f}\n⏰ <b>Time:</b> {datetime.now(pytz.timezone('US/Eastern')).strftime('%H:%M:%S EST')}\n\n🚀 <i>Watch for explosive movement!</i>"
        send(msg)

# ================= INDICATORS =================
def calculate_indicators(df):
    if len(df) < 100:
        return df
    
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    df["atr"] = np.max(ranges, axis=1).rolling(14).mean()
    
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df["rsi"] = 100 - (100 / (1 + (gain / loss)))
    
    df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
    
    df["bb_mid"] = df["close"].rolling(window=20).mean()
    bb_std = df["close"].rolling(window=20).std()
    df["bb_upper"] = df["bb_mid"] + (bb_std * 2)
    df["bb_lower"] = df["bb_mid"] - (bb_std * 2)
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]
    
    df["obv"] = (np.sign(df["close"].diff()) * df["volume"]).fillna(0).cumsum()
    df["obv_ema"] = df["obv"].ewm(span=20, adjust=False).mean()
    
    df["vol_sma_20"] = df["volume"].rolling(window=20).mean()
    
    return df

# ================= SCORING ENGINE =================
def score_setup(df):
    score = 0
    reasons = []
    current = df.iloc[-1]
    
    if current["obv"] > current["obv_ema"]:
        score += 20
        reasons.append("Smart Money Accumulation (OBV)")
    
    if current["bb_width"] < df["bb_width"].rolling(50).min().iloc[-1] * 1.2:
        score += 25
        reasons.append("Volatility Squeeze (Potential Explosion)")
    
    if current["close"] > current["ema_21"] and current["ema_9"] > current["ema_21"]:
        score += 15
        reasons.append("Bullish Trend Alignment")
    
    if 40 < current["rsi"] < 65:
        score += 10
        reasons.append("Healthy Momentum (RSI)")
    
    if current["volume"] > current["vol_sma_20"] * 2.5:
        score += 20
        reasons.append("Institutional Volume Spike")
    
    if current["close"] < 10 and current["volume"] > 1000000:
        score += 10
        reasons.append("Active Penny Stock Alert")
        
    return score, reasons

# ================= DYNAMIC TARGETS & RISK =================
def calculate_trade_params(price, atr_val):
    if atr_val == 0 or np.isnan(atr_val):
        return None
    sl_dist = atr_val * 2.0
    sl = price - sl_dist
    t1 = price + (sl_dist * 1.5)
    t2 = price + (sl_dist * 3.0)
    t3 = price + (sl_dist * 5.0)
    risk_amount = 100
    risk_per_share = price - sl
    shares = int(risk_amount / risk_per_share) if risk_per_share > 0 else 0
    return {"entry": price, "sl": sl, "t1": t1, "t2": t2, "t3": t3, "shares": shares}

# ================= ANALYSIS PIPELINE =================
def analyze(symbol):
    df = get_data(symbol)
    if df is None or len(df) < 100:
        return None
    
    # Check for Halts first
    check_halt(symbol, df)
    
    df = calculate_indicators(df)
    df = df.dropna()
    
    score, reasons = score_setup(df)
    if score < 75:
        return None
    
    current = df.iloc[-1]
    params = calculate_trade_params(current["close"], current["atr"])
    if not params:
        return None
        
    return {
        "symbol": symbol,
        "score": score,
        "price": current["close"],
        "reasons": reasons,
        "params": params,
        "time": datetime.now(pytz.timezone("US/Eastern")).strftime("%H:%M:%S EST")
    }

# ================= MAIN ENGINE (24/7) =================
def run_engine():
    startup_msg = "🚀 <b>AI Institutional Ultra Pro v7.5 Started</b>\n\n✅ 24/7 Scanning (Pre/Post Market)\n✅ Accumulation Detection (OBV)\n✅ Squeeze/Explosion Alerts\n✅ Penny Stock Filter\n✅ Halt Radar Active"
    send(startup_msg)
    seen_signals = deque(maxlen=100)
    
    while True:
        try:
            universe = get_universe()
            now_utc = datetime.now(pytz.utc)
            
            for symbol in universe:
                if any(s["symbol"] == symbol and (now_utc - s["timestamp"]).total_seconds() < 14400 for s in seen_signals):
                    continue
                
                r = analyze(symbol)
                if r:
                    seen_signals.append({"symbol": r["symbol"], "timestamp": now_utc})
                    p = r["params"]
                    reasons_str = "\n".join([f"🔹 {reason}" for reason in r["reasons"]])
                    
                    msg = f"""
🚨 <b>AI ULTRA PRO v7.5 SIGNAL</b> 🚨

📊 <b>Ticker:</b> #{r['symbol']}
⭐ <b>Score:</b> {r['score']}/100
⏰ <b>Time:</b> {r['time']}

💰 <b>Entry:</b> ${p['entry']:.2f}

🎯 <b>Dynamic Targets:</b>
T1: ${p['t1']:.2f} (Safe)
T2: ${p['t2']:.2f} (Explosive)
T3: ${p['t3']:.2f} (Moon)

🛑 <b>Stop Loss:</b> ${p['sl']:.2f}

📦 <b>Size:</b> {p['shares']} shares
🧠 <b>Analysis:</b>
{reasons_str}
"""
                    send(msg)
                    time.sleep(12)
            
            time.sleep(60) # Faster scan for Halts
        except Exception as e:
            logger.error(f"Error in engine: {e}")
            time.sleep(60)

if __name__ == "__main__":
    engine_thread = threading.Thread(target=run_engine, daemon=True)
    engine_thread.start()
    logger.info("Starting Telegram Polling...")
    try:
        bot.infinity_polling()
    except Exception as e:
        logger.error(f"Polling error: {e}")
        time.sleep(10)

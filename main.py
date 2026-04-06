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
import xml.etree.ElementTree as ET

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
    bot.reply_to(message, "🚀 <b>AI Institutional Ultra Pro v8.0 (GOD MODE) Active!</b>\n\n"
                          "✅ <b>NASDAQ Official Halt Radar:</b> Active\n"
                          "✅ <b>Market Crash Protection:</b> Active\n"
                          "✅ <b>24/7 Penny Stock Scanner:</b> Active\n"
                          "✅ <b>Smart Money Accumulation:</b> Active")

# ================= UNIVERSE =================
def get_universe():
    return [
        "AAPL", "MSFT", "NVDA", "AMD", "META", "TSLA", "AMZN", "GOOGL", "PLTR", "SOFI",
        "BABA", "NIO", "RIOT", "MARA", "COIN", "UBER", "NFLX", "PYPL", "SQ", "CRM",
        "GME", "AMC", "HOOD", "DKNG", "PLUG", "LCID", "NKLA", "FUBO", "OPEN", "CLOV",
        "SPY", "QQQ" # For Market Protection
    ]

# ================= NASDAQ HALT RADAR (OFFICIAL) =================
processed_halts = set()

def check_nasdaq_halts():
    url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
    ns = {'ndaq': 'http://www.nasdaqtrader.com/'}
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return
            
        root = ET.fromstring(response.content)
        for item in root.findall('.//item'):
            symbol = item.find('ndaq:IssueSymbol', ns).text
            halt_time = item.find('ndaq:HaltTime', ns).text
            reason = item.find('ndaq:ReasonCode', ns).text
            resumption_time = item.find('ndaq:ResumptionTradeTime', ns).text
            
            halt_id = f"{symbol}_{halt_time}_{reason}"
            
            if halt_id not in processed_halts:
                processed_halts.add(halt_id)
                
                if not resumption_time or resumption_time.strip() == "":
                    # It's a new Halt
                    msg = (f"⚠️ <b>NASDAQ OFFICIAL HALT</b> ⚠️\n\n"
                           f"📊 <b>Ticker:</b> #{symbol}\n"
                           f"🕒 <b>Halt Time:</b> {halt_time}\n"
                           f"🛑 <b>Reason:</b> {reason}\n"
                           f"⏰ <b>Detected:</b> {datetime.now(pytz.timezone('US/Eastern')).strftime('%H:%M:%S EST')}\n\n"
                           f"<i>Monitoring for resumption...</i>")
                    send(msg)
                else:
                    # It's a Resumption
                    msg = (f"✅ <b>TRADING RESUMED (NASDAQ)</b> ✅\n\n"
                           f"📊 <b>Ticker:</b> #{symbol}\n"
                           f"🕒 <b>Resume Time:</b> {resumption_time}\n"
                           f"🛑 <b>Original Reason:</b> {reason}\n"
                           f"🚀 <i>Watch for high volatility!</i>")
                    send(msg)
                    
    except Exception as e:
        logger.error(f"Error checking NASDAQ halts: {e}")

# ================= MARKET CRASH PROTECTION =================
def check_market_crash():
    # Check SPY for sudden drops
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute/{ (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') }/{ datetime.now().strftime('%Y-%m-%d') }?apiKey={POLYGON_API}"
    try:
        r = requests.get(url, timeout=10).json()
        if "results" in r and len(r["results"]) > 5:
            df = pd.DataFrame(r["results"])
            drop = (df['c'].iloc[-1] - df['c'].iloc[-5]) / df['c'].iloc[-5] * 100
            if drop < -1.5: # 1.5% drop in 5 minutes
                msg = (f"🚨 <b>MARKET CRASH WARNING</b> 🚨\n\n"
                       f"📉 <b>SPY Drop:</b> {drop:.2f}% in 5 mins\n"
                       f"⚠️ <b>Action:</b> Consider closing all long positions immediately!\n"
                       f"🛡️ <i>Safety First Mode Active.</i>")
                send(msg)
    except Exception as e:
        logger.error(f"Error checking market crash: {e}")

# ================= POLYGON DATA & INDICATORS =================
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
        if "results" not in r or len(r["results"]) == 0: return None
        df = pd.DataFrame(r["results"]).rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "t": "timestamp"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms', utc=True)
        df = df.set_index("timestamp")
        data_cache[cache_key] = (now_utc, df)
        return df
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None

def calculate_indicators(df):
    if len(df) < 100: return df
    df["atr"] = (df["high"] - df["low"]).rolling(14).mean()
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df["rsi"] = 100 - (100 / (1 + (gain / loss)))
    df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["bb_mid"] = df["close"].rolling(window=20).mean()
    bb_std = df["close"].rolling(window=20).std()
    df["bb_width"] = (bb_std * 4) / df["bb_mid"]
    df["obv"] = (np.sign(df["close"].diff()) * df["volume"]).fillna(0).cumsum()
    df["obv_ema"] = df["obv"].ewm(span=20, adjust=False).mean()
    df["vol_sma_20"] = df["volume"].rolling(window=20).mean()
    return df

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
    if current["close"] > current["ema_21"] and df["ema_9"].iloc[-1] > df["ema_21"].iloc[-1]:
        score += 15
        reasons.append("Bullish Trend Alignment")
    if current["volume"] > current["vol_sma_20"] * 2.5:
        score += 20
        reasons.append("Institutional Volume Spike")
    if current["close"] < 10 and current["volume"] > 1000000:
        score += 10
        reasons.append("Active Penny Stock Alert")
    return score, reasons

def calculate_trade_params(price, atr_val):
    if atr_val == 0 or np.isnan(atr_val): return None
    sl_dist = atr_val * 2.0
    return {"entry": price, "sl": price - sl_dist, "t1": price + (sl_dist * 1.5), "t2": price + (sl_dist * 3.0), "t3": price + (sl_dist * 5.0)}

def get_news(symbol):
    url = f"https://api.polygon.io/v2/reference/news?ticker={symbol}&limit=1&apiKey={POLYGON_API}"
    try:
        r = requests.get(url, timeout=10).json()
        if "results" in r and len(r["results"]) > 0:
            news = r["results"][0]
            return f"📰 <b>Latest News:</b> {news['title']}\n🔗 <a href='{news['article_url']}'>Read More</a>"
    except:
        pass
    return "📰 <b>News:</b> No recent news found."

# ================= MAIN ENGINE =================
def run_engine():
    send("🚀 <b>AI Institutional Ultra Pro v8.0 (GOD MODE) Started</b>\n\n✅ Official NASDAQ Halt Radar\n✅ Market Crash Protection\n✅ 24/7 Scanning Active")
    seen_signals = deque(maxlen=100)
    
    while True:
        try:
            # 1. Check NASDAQ Halts (Every 1 minute)
            check_nasdaq_halts()
            
            # 2. Check Market Crash (Every 5 minutes)
            check_market_crash()
            
            # 3. Scan Universe
            universe = get_universe()
            now_utc = datetime.now(pytz.utc)
            for symbol in universe:
                if symbol in ["SPY", "QQQ"]: continue
                if any(s["symbol"] == symbol and (now_utc - s["timestamp"]).total_seconds() < 14400 for s in seen_signals):
                    continue
                
                df = get_data(symbol)
                if df is None or len(df) < 100: continue
                df = calculate_indicators(df)
                score, reasons = score_setup(df)
                
                if score >= 75:
                    seen_signals.append({"symbol": symbol, "timestamp": now_utc})
                    p = calculate_trade_params(df["close"].iloc[-1], df["atr"].iloc[-1])
                    if p:
                        reasons_str = "\n".join([f"🔹 {reason}" for reason in reasons])
                        news_str = get_news(symbol)
                        msg = f"🚨 <b>AI ULTRA PRO v8.0 SIGNAL</b> 🚨\n\n📊 <b>Ticker:</b> #{symbol}\n⭐ <b>Score:</b> {score}/100\n\n💰 <b>Entry:</b> ${p['entry']:.2f}\n🎯 <b>Targets:</b>\nT1: ${p['t1']:.2f}\nT2: ${p['t2']:.2f}\nT3: ${p['t3']:.2f}\n🛑 <b>Stop Loss:</b> ${p['sl']:.2f}\n\n🧠 <b>Analysis:</b>\n{reasons_str}\n\n{news_str}"
                        send(msg)
                        time.sleep(12)
            
            time.sleep(60)
        except Exception as e:
            logger.error(f"Error in engine: {e}")
            time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=run_engine, daemon=True).start()
    logger.info("Starting Telegram Polling...")
    try:
        bot.infinity_polling()
    except Exception as e:
        logger.error(f"Polling error: {e}")
        time.sleep(10)
        

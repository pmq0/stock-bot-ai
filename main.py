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
from concurrent.futures import ThreadPoolExecutor

# ================= CONFIG =================
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
POLYGON_API = os.getenv("POLYGON_API")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

bot = telebot.TeleBot(TOKEN)

data_cache = {}
seen_signals = deque(maxlen=500)
dynamic_universe = []

api_lock = threading.Lock()
last_call = 0

# ================= RATE LIMIT =================
def rate_limit():
    global last_call
    with api_lock:
        now = time.time()
        wait = 12.5 - (now - last_call)
        if wait > 0:
            time.sleep(wait)
        last_call = time.time()

# ================= TELEGRAM =================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except:
        pass

# ================= MARKET CHECK =================
def market_open():
    tz = pytz.timezone("US/Eastern")
    now = datetime.now(tz)
    if now.weekday() >= 5:
        return False
    return now.hour >= 4 and now.hour <= 20

# ================= UNIVERSE =================
def update_universe():
    global dynamic_universe
    logger.info("Updating universe...")

    rate_limit()
    date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&apiKey={POLYGON_API}"

    try:
        r = requests.get(url, timeout=15).json()

        if "results" not in r:
            return

        valid = []
        for x in r["results"]:
            try:
                price = x["c"]
                vol = x["v"]
                change = (x["c"] - x["o"]) / x["o"]

                if 1 <= price <= 10 and vol > 300000 and change > 0.02:
                    valid.append(x)
            except:
                continue

        valid = sorted(valid, key=lambda x: x["v"], reverse=True)

        dynamic_universe = [x["T"] for x in valid[:40]]  # تقليل الضغط

        if not dynamic_universe:
            dynamic_universe = ["TSLA", "NVDA", "AMD", "PLTR"]

        logger.info(f"Universe size: {len(dynamic_universe)}")

    except Exception as e:
        logger.error(f"Universe error: {e}")

# ================= DATA =================
def fetch(symbol):
    rate_limit()

    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/15/minute/{start}/{end}?adjusted=true&apiKey={POLYGON_API}"

    try:
        r = requests.get(url, timeout=15).json()

        if "results" not in r:
            return None

        df = pd.DataFrame(r["results"])
        df.rename(columns={"c":"close","h":"high","l":"low","o":"open","v":"volume"}, inplace=True)
        return df

    except:
        return None

# ================= INDICATORS (FIXED) =================
def indicators(df):
    df["ema9"] = df["close"].ewm(span=9).mean()
    df["ema21"] = df["close"].ewm(span=21).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()

    # RSI (fixed)
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / (loss + 1e-9)
    df["rsi"] = 100 - (100 / (1 + rs))

    # ATR (simple)
    df["tr"] = df["high"] - df["low"]
    df["atr"] = df["tr"].rolling(14).mean()

    # OBV (FIXED)
    obv = []
    prev = 0
    for i in range(len(df)):
        if i == 0:
            obv.append(0)
            continue
        if df["close"].iloc[i] > df["close"].iloc[i-1]:
            prev += df["volume"].iloc[i]
        elif df["close"].iloc[i] < df["close"].iloc[i-1]:
            prev -= df["volume"].iloc[i]
        obv.append(prev)

    df["obv"] = obv

    df["squeeze"] = df["close"].rolling(20).std() < (df["close"].rolling(20).mean() * 0.01)

    return df

# ================= SCORE =================
def score(df):
    c = df.iloc[-1]
    s = 0

    if c["ema9"] > c["ema21"] > c["ema50"]:
        s += 25

    if 45 < c["rsi"] < 75:
        s += 15

    if c["volume"] > df["volume"].rolling(20).mean().iloc[-1] * 1.3:
        s += 15

    if c["obv"] > df["obv"].rolling(10).mean().iloc[-1]:
        s += 10

    if c["squeeze"]:
        s += 15

    if c["close"] > df["high"].iloc[-10:].max():
        s += 15

    if c["close"] < 10:
        s += 5

    return s

# ================= SCAN =================
def scan(symbol):
    try:
        if symbol in [x["symbol"] for x in seen_signals]:
            return

        df = fetch(symbol)
        if df is None or len(df) < 40:
            return

        df = indicators(df)
        s = score(df)

        if s >= 75:
            price = df["close"].iloc[-1]
            atr = df["atr"].iloc[-1]
            if pd.isna(atr):
                atr = price * 0.02

            sl = price - atr * 2
            tp1 = price + atr * 1.5
            tp2 = price + atr * 3

            msg = f"""
🚀 <b>SIGNAL v10.5</b>

📊 {symbol}
⭐ Score: {s}/100

💰 Entry: {price:.2f}
🎯 TP1: {tp1:.2f}
🎯 TP2: {tp2:.2f}
🛑 SL: {sl:.2f}
"""
            send(msg)
            seen_signals.append({"symbol": symbol, "time": time.time()})

    except:
        pass

# ================= LOOP =================
def run():
    send("🚀 v10.5 Hybrid Engine Started")

    last_update = 0

    while True:
        try:
            if time.time() - last_update > 3600:
                update_universe()
                last_update = time.time()

            if not market_open():
                time.sleep(300)
                continue

            with ThreadPoolExecutor(max_workers=3) as ex:
                for s in dynamic_universe:
                    ex.submit(scan, s)

            time.sleep(60)

        except Exception as e:
            logger.error(e)
            time.sleep(60)

# ================= START =================
if __name__ == "__main__":
    threading.Thread(target=lambda: bot.infinity_polling(), daemon=True).start()
    run()

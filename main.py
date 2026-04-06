import yfinance as yf
import pandas as pd
import numpy as np
import requests
import os
import time

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ================= TELEGRAM =================
def send(msg):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg})
    except:
        pass


# ================= UNIVERSE =================
def get_stocks():
    return [
        "AAPL","TSLA","NVDA","AMD","AMZN","META","PLTR",
        "SOFI","NIO","RIOT","MARA","CLSK","LCID",
        "F","INTC","BABA","MSFT","GOOGL",
        "SPY","QQQ"
    ]


# ================= INDICATORS =================
def rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def macd(df):
    ema12 = df['Close'].ewm(span=12).mean()
    ema26 = df['Close'].ewm(span=26).mean()
    return ema12 - ema26


def vwap(df):
    return (df['Volume'] * (df['High'] + df['Low'] + df['Close']) / 3).cumsum() / df['Volume'].cumsum()


# ================= NEWS (LIGHTWEIGHT) =================
def get_news(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={symbol}"
        r = requests.get(url, timeout=5).json()
        news = r.get("news", [])
        if news:
            return news[0]["title"]
    except:
        pass
    return None


# ================= SCORE ENGINE =================
def score(price, volume, avg_volume, rsi_val, macd_val, vwap_val, breakout):
    score = 0

    if price < 10:
        score += 20
    if volume > avg_volume * 2:
        score += 20
    if rsi_val > 55:
        score += 15
    if macd_val > 0:
        score += 15
    if price > vwap_val:
        score += 10
    if breakout:
        score += 20

    return score


# ================= ANALYSIS =================
def analyze(symbol):
    try:
        df = yf.download(symbol, period="5d", interval="15m", progress=False)

        if df is None or df.empty:
            return None

        price = float(df['Close'].iloc[-1])
        volume = float(df['Volume'].iloc[-1])
        avg_volume = float(df['Volume'].mean())

        if price <= 0 or price > 10:
            return None

        df['RSI'] = rsi(df['Close'])
        rsi_val = float(df['RSI'].iloc[-1])

        macd_val = float(macd(df).iloc[-1])
        vwap_val = float(vwap(df).iloc[-1])

        resistance = float(df['High'].rolling(20).max().iloc[-1])
        breakout = price >= resistance * 0.98

        final_score = score(price, volume, avg_volume, rsi_val, macd_val, vwap_val, breakout)

        if final_score < 75:
            return None

        entry = price
        tp1 = round(entry * 1.08, 3)
        tp2 = round(entry * 1.15, 3)
        tp3 = round(entry * 1.25, 3)
        sl = round(entry * 0.94, 3)

        news = get_news(symbol)

        return {
            "symbol": symbol,
            "score": final_score,
            "entry": entry,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl": sl,
            "news": news
        }

    except:
        return None


# ================= MAIN LOOP =================
def run():
    send("🚀 النظام الاحترافي بدأ العمل - مراقبة ذكية للأسهم")

    seen = {}
    cooldown = 60 * 60  # ساعة

    while True:
        results = []

        for s in get_stocks():
            r = analyze(s)

            if not r:
                continue

            last_time = seen.get(r["symbol"], 0)
            if time.time() - last_time < cooldown:
                continue

            results.append(r)

        results = sorted(results, key=lambda x: x["score"], reverse=True)[:3]

        for r in results:
            seen[r["symbol"]] = time.time()

            msg = f"""
🚀 فرصة احترافية

📊 السهم: {r['symbol']}
⭐ القوة: {r['score']}/100

💰 دخول: {r['entry']}
🎯 TP1: {r['tp1']}
🎯 TP2: {r['tp2']}
🎯 TP3: {r['tp3']}
🛑 SL: {r['sl']}
"""

            if r["news"]:
                msg += f"\n📰 خبر: {r['news']}"

            send(msg)

        time.sleep(300)


if __name__ == "__main__":
    run()

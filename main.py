import yfinance as yf
import pandas as pd
import numpy as np
import requests
import os
import time

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ---------------- TELEGRAM ----------------
def send(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})


# ---------------- MARKET MOVERS (LIVE SCAN SIMPLE) ----------------
def get_market_stocks():
    # قائمة موسعة + قابلة للتطوير لاحقًا لـ API حقيقي
    return [
        "AAPL","TSLA","NVDA","AMD","AMZN","META","PLTR",
        "SOFI","NIO","RIOT","MARA","CLSK","F","LCID",
        "BABA","INTC","GOOGL","MSFT","SPY","QQQ"
    ]


# ---------------- RSI ----------------
def rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


# ---------------- SCORING SYSTEM ----------------
def score_stock(price, volume, avg_volume, rsi_val, breakout):
    score = 0

    if price < 10:
        score += 20
    if volume > avg_volume * 1.5:
        score += 25
    if rsi_val > 55:
        score += 25
    if breakout:
        score += 30

    return score


# ---------------- ANALYSIS ----------------
def analyze(symbol):
    try:
        df = yf.download(symbol, period="5d", interval="15m", progress=False)

        if df.empty:
            return None

        price = df['Close'].iloc[-1]
        volume = df['Volume'].iloc[-1]
        avg_volume = df['Volume'].mean()

        if price > 10:
            return None

        df['RSI'] = rsi(df['Close'])
        rsi_val = df['RSI'].iloc[-1]

        resistance = df['High'].rolling(20).max().iloc[-1]
        breakout = price >= resistance * 0.98

        score = score_stock(price, volume, avg_volume, rsi_val, breakout)

        if score < 70:
            return None

        entry = price
        tp1 = round(entry * 1.10, 3)
        tp2 = round(entry * 1.20, 3)
        tp3 = round(entry * 1.35, 3)
        sl = round(entry * 0.92, 3)

        return {
            "symbol": symbol,
            "score": score,
            "entry": entry,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl": sl
        }

    except:
        return None


# ---------------- SCANNER ----------------
def run():
    send("🚀 البوت المطوّر بدأ مسح السوق...")

    while True:
        stocks = get_market_stocks()
        results = []

        for s in stocks:
            data = analyze(s)
            if data:
                results.append(data)

        # ترتيب أفضل الفرص
        results = sorted(results, key=lambda x: x['score'], reverse=True)[:5]

        for r in results:
            msg = f"""
🚀 فرصة قوية محتملة

📊 السهم: {r['symbol']}
⭐ القوة: {r['score']}/100

💰 دخول: {r['entry']}
🎯 TP1: {r['tp1']}
🎯 TP2: {r['tp2']}
🎯 TP3: {r['tp3']}
🛑 SL: {r['sl']}

⚡ سكالبينج / مضاربة سريعة
"""
            send(msg)

        time.sleep(300)


if __name__ == "__main__":
    run()

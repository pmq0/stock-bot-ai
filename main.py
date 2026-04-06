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
    # أسهم سيولة عالية + penny/low price potential
    return [
        "AAPL","TSLA","NVDA","AMD","AMZN","META","PLTR",
        "SOFI","NIO","RIOT","MARA","CLSK","LCID",
        "F","INTC","BABA","MSFT","GOOGL",
        "SPY","QQQ"
    ]


# ================= RSI =================
def rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


# ================= SCORE ENGINE =================
def score(price, volume, avg_volume, rsi_val, breakout):
    score = 0

    if price < 10:
        score += 25
    if volume > avg_volume * 1.8:
        score += 25
    if rsi_val > 55:
        score += 20
    if breakout:
        score += 30

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

        # شرط السعر
        if price <= 0 or price > 10:
            return None

        # RSI
        df['RSI'] = rsi(df['Close'])
        rsi_val = float(df['RSI'].iloc[-1])

        # breakout
        resistance = float(df['High'].rolling(20).max().iloc[-1])
        breakout = price >= resistance * 0.98

        final_score = score(price, volume, avg_volume, rsi_val, breakout)

        # فلتر قوي (يقلل الإشارات الضعيفة)
        if final_score < 80:
            return None

        entry = price
        tp1 = round(entry * 1.10, 3)
        tp2 = round(entry * 1.22, 3)
        tp3 = round(entry * 1.35, 3)
        sl = round(entry * 0.92, 3)

        return {
            "symbol": symbol,
            "score": final_score,
            "entry": entry,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl": sl
        }

    except:
        return None


# ================= MAIN LOOP =================
def run():
    send("🚀 البوت شغال الآن - يبدأ صيد الفرص قبل الحركة")

    seen = set()

    while True:
        results = []

        for s in get_stocks():
            r = analyze(s)
            if r and r["symbol"] not in seen:
                results.append(r)

        # ترتيب أقوى الفرص
        results = sorted(results, key=lambda x: x["score"], reverse=True)[:3]

        for r in results:
            seen.add(r["symbol"])

            msg = f"""
🚀 فرصة مضاربة قوية

📊 السهم: {r['symbol']}
⭐ القوة: {r['score']}/100

💰 دخول: {r['entry']}
🎯 TP1: {r['tp1']}
🎯 TP2: {r['tp2']}
🎯 TP3: {r['tp3']}
🛑 SL: {r['sl']}

⚡ سكالبينج / حركة سريعة
"""
            send(msg)

        time.sleep(300)


if __name__ == "__main__":
    run()

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import os

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ---------------- TELEGRAM ----------------
def send(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})


# ---------------- STOCK LIST (LIVE SCAN) ----------------
def get_movers():
    # قائمة بسيطة قابلة للتطوير (تقدر نطورها لاحقًا لـ API حقيقي)
    return [
        "AAPL","TSLA","NVDA","AMD","AMZN","META","PLTR",
        "NIO","SOFI","F","AMD","INTC","GOOGL","MSFT",
        "SPY","QQQ","BABA","RIOT","MARA","CLSK"
    ]


# ---------------- INDICATORS ----------------
def rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def analyze(symbol):
    try:
        df = yf.download(symbol, period="5d", interval="15m")
        if df.empty:
            return None

        price = df['Close'].iloc[-1]
        volume = df['Volume'].iloc[-1]
        avg_volume = df['Volume'].mean()

        if price > 10:
            return None

        # Indicators
        df['RSI'] = rsi(df['Close'])
        rsi_val = df['RSI'].iloc[-1]

        resistance = df['High'].rolling(20).max().iloc[-1]

        breakout = price >= resistance * 0.98
        volume_spike = volume > avg_volume * 1.5
        momentum = rsi_val > 55

        if breakout and volume_spike and momentum:
            entry = price
            tp1 = round(entry * 1.12, 3)
            tp2 = round(entry * 1.25, 3)
            sl = round(entry * 0.92, 3)

            return {
                "symbol": symbol,
                "entry": entry,
                "tp1": tp1,
                "tp2": tp2,
                "sl": sl
            }

    except:
        return None

    return None


# ---------------- SCANNER LOOP ----------------
def run():
    send("🚀 البوت بدأ المسح قبل الافتتاح...")

    while True:
        stocks = get_movers()

        for s in stocks:
            signal = analyze(s)

            if signal:
                msg = f"""
🚀 فرصة مضاربة محتملة

📊 السهم: {signal['symbol']}
💰 دخول: {signal['entry']}
🎯 TP1: {signal['tp1']}
🎯 TP2: {signal['tp2']}
🛑 SL: {signal['sl']}

⚡ سكالبينج سريع
"""
                send(msg)

        time.sleep(300)  # كل 5 دقائق


if __name__ == "__main__":
    run()

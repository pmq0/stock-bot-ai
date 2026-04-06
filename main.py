import yfinance as yf
import pandas as pd
import numpy as np
import requests
import os
import time
import threading
import telebot

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

bot = telebot.TeleBot(TOKEN)

# ================= TELEGRAM =================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )
    except:
        pass


@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "🚀 UTIS شغال: ذكاء سوق + أخبار + صيد فرص قبل الانفجار")


# ================= MARKET UNIVERSE (ناسداك + بيني ستوكس) =================
def get_universe():
    return [
        # Large Caps
        "AAPL","TSLA","NVDA","AMD","AMZN","META","MSFT","GOOGL","SPY","QQQ",

        # Growth / Momentum
        "PLTR","SOFI","HOOD","RIVN","DKNG","BABA","INTC","F",

        # Penny Stocks / High volatility
        "NIO","RIOT","MARA","CLSK","LCID","WKHS","SNDL","OCGN","TRKA"
    ]


# ================= INDICATORS =================
def rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def vwap(df):
    return (df['Volume'] * (df['High'] + df['Low'] + df['Close']) / 3).cumsum() / df['Volume'].cumsum()


# ================= NEWS INTELLIGENCE =================
def get_news_score(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={symbol}"
        r = requests.get(url, timeout=5).json()
        news = r.get("news", [])

        if not news:
            return 0, None

        title = news[0]["title"].lower()

        positive_words = ["surge", "up", "beats", "profit", "upgrade", "rise"]
        negative_words = ["drop", "lawsuit", "loss", "investigation", "down"]

        score = 0
        for w in positive_words:
            if w in title:
                score += 10
        for w in negative_words:
            if w in title:
                score -= 10

        return score, title

    except:
        return 0, None


# ================= ACCUMULATION =================
def is_accumulation(df):
    vol_now = df['Volume'].iloc[-5:].mean()
    vol_prev = df['Volume'].iloc[-20:].mean()

    price_range = df['Close'].iloc[-10:].max() - df['Close'].iloc[-10:].min()
    price = df['Close'].iloc[-1]

    return vol_now > vol_prev * 1.3 and price_range < price * 0.04


# ================= MICRO BREAKOUT =================
def is_micro_breakout(df):
    resistance = df['High'].rolling(20).max().iloc[-2]
    price = df['Close'].iloc[-1]
    volume = df['Volume'].iloc[-1]
    avg_vol = df['Volume'].mean()

    return price > resistance * 0.99 and volume > avg_vol * 2


# ================= SCORE ENGINE =================
def score(df, news_score):
    price = df['Close'].iloc[-1]
    volume = df['Volume'].iloc[-1]
    avg_vol = df['Volume'].mean()

    rsi_val = rsi(df['Close']).iloc[-1]
    vwap_val = vwap(df).iloc[-1]

    acc = is_accumulation(df)
    brk = is_micro_breakout(df)

    score = 0

    if price < 10:
        score += 15
    if volume > avg_vol * 1.8:
        score += 20
    if 45 < rsi_val < 75:
        score += 15
    if price > vwap_val:
        score += 10
    if acc:
        score += 25
    if brk:
        score += 20

    score += news_score

    return score, acc, brk


# ================= ANALYZE =================
def analyze(symbol):
    try:
        df = yf.download(symbol, period="5d", interval="15m", progress=False)

        if df is None or df.empty:
            return None

        news_score, news = get_news_score(symbol)

        sc, acc, brk = score(df, news_score)

        if sc < 85:
            return None

        price = df['Close'].iloc[-1]

        entry = price
        tp1 = round(entry * 1.10, 3)
        tp2 = round(entry * 1.20, 3)
        tp3 = round(entry * 1.35, 3)
        sl = round(entry * 0.93, 3)

        return {
            "symbol": symbol,
            "score": sc,
            "price": price,
            "entry": entry,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl": sl,
            "acc": acc,
            "brk": brk,
            "news": news
        }

    except:
        return None


# ================= SCANNER =================
def run():
    send("🚀 UTIS بدأ - ذكاء سوق + أخبار + تجميع + بيني ستوكس + انفجار قبل حدوثه")

    seen = {}
    cooldown = 60 * 60

    while True:
        results = []

        for symbol in get_universe():
            r = analyze(symbol)

            if not r:
                continue

            if time.time() - seen.get(r["symbol"], 0) < cooldown:
                continue

            results.append(r)

        results = sorted(results, key=lambda x: x["score"], reverse=True)[:10]

        for r in results:
            seen[r["symbol"]] = time.time()

            msg = f"""
🔥 فرصة قوية جدًا (UTIS)

📊 السهم: {r['symbol']}
⭐ القوة: {r['score']}/100

💰 دخول: {r['entry']}
🎯 TP1: {r['tp1']}
🎯 TP2: {r['tp2']}
🎯 TP3: {r['tp3']}
🛑 SL: {r['sl']}

📦 تجميع: {"نعم" if r['acc'] else "لا"}
🚀 انفجار: {"نعم" if r['brk'] else "لا"}

📰 خبر: {r['news']}
"""

            send(msg)

        time.sleep(300)


# ================= START =================
if __name__ == "__main__":
    threading.Thread(target=run).start()
    bot.polling()

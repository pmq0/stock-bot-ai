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
    bot.send_message(message.chat.id, "🚀 FMAS شغال: سوق كامل + تجميع + انفجار + penny stocks")


# ================= MARKET UNIVERSE (واسع + قابل للتوسعة) =================
def get_universe():
    return [
        # Large Caps
        "AAPL","TSLA","NVDA","AMD","AMZN","META","MSFT","GOOGL","SPY","QQQ",

        # Growth
        "PLTR","SOFI","HOOD","RIVN","DKNG","BABA","INTC","F","UBER","LYFT",

        # Penny / high volatility
        "NIO","RIOT","MARA","CLSK","LCID","SNDL","OCGN","TRKA","WKHS","BBIG",

        # More momentum
        "NFLX","PYPL","DIS","CRM","SNAP","TWTR"
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


# ================= ACCUMULATION =================
def accumulation(df):
    vol_now = df['Volume'].iloc[-5:].mean()
    vol_prev = df['Volume'].iloc[-20:].mean()

    price_range = df['Close'].iloc[-10:].max() - df['Close'].iloc[-10:].min()
    price = df['Close'].iloc[-1]

    return vol_now > vol_prev * 1.25 and price_range < price * 0.04


# ================= BREAKOUT =================
def breakout(df):
    resistance = df['High'].rolling(20).max().iloc[-2]
    price = df['Close'].iloc[-1]

    return price > resistance * 0.99


# ================= NEWS (خفيف) =================
def news_score(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={symbol}"
        r = requests.get(url, timeout=5).json()
        news = r.get("news", [])

        if not news:
            return 0, None

        title = news[0]["title"].lower()

        score = 0
        if any(w in title for w in ["surge","rise","beats","upgrade"]):
            score += 10
        if any(w in title for w in ["drop","lawsuit","loss"]):
            score -= 10

        return score, title
    except:
        return 0, None


# ================= SCORE ENGINE =================
def score(df, nscore):
    price = df['Close'].iloc[-1]
    vol = df['Volume'].iloc[-1]
    avg_vol = df['Volume'].mean()

    rsi_val = rsi(df['Close']).iloc[-1]
    vwap_val = vwap(df).iloc[-1]

    acc = accumulation(df)
    brk = breakout(df)

    score = 0

    if price < 10:
        score += 20
    if vol > avg_vol * 1.8:
        score += 20
    if 45 < rsi_val < 75:
        score += 15
    if price > vwap_val:
        score += 10
    if acc:
        score += 25
    if brk:
        score += 20

    score += nscore

    return score, acc, brk


# ================= ANALYZE =================
def analyze(symbol):
    try:
        df = yf.download(symbol, period="5d", interval="15m", progress=False)

        if df is None or df.empty:
            return None

        nscore, news = news_score(symbol)
        sc, acc, brk = score(df, nscore)

        if sc < 85:
            return None

        price = df['Close'].iloc[-1]

        return {
            "symbol": symbol,
            "score": sc,
            "price": price,
            "acc": acc,
            "brk": brk,
            "news": news
        }

    except:
        return None


# ================= SCANNER (BATCHED FOR RAILWAY) =================
def run():
    send("🚀 FMAS بدأ - سوق كامل + تجميع + انفجار + penny stocks")

    seen = {}
    cooldown = 60 * 60

    while True:
        universe = get_universe()

        results = []

        # 🔥 batching عشان Railway ما يعلق
        batch_size = 10

        for i in range(0, len(universe), batch_size):
            batch = universe[i:i+batch_size]

            for s in batch:
                r = analyze(s)

                if not r:
                    continue

                if time.time() - seen.get(r["symbol"], 0) < cooldown:
                    continue

                results.append(r)

            time.sleep(1)  # حماية السيرفر

        # Top 10 فقط
        results = sorted(results, key=lambda x: x["score"], reverse=True)[:10]

        for r in results:
            seen[r["symbol"]] = time.time()

            msg = f"""
🔥 FMAS - فرصة سوق كاملة

📊 {r['symbol']}
⭐ القوة: {r['score']}/100
💰 السعر: {r['price']}

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

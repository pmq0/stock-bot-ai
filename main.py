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
    bot.send_message(message.chat.id, "🚀 Market Scanner شغال - يصيد التجميع والانفجار الآن")


# ================= MARKET UNIVERSE (كبداية قوية) =================
def get_market():
    # هذا “Mini Universe” (نقدر نوسعه لاحقًا لـ NASDAQ كامل)
    return [
        "AAPL","TSLA","NVDA","AMD","AMZN","META","PLTR","SOFI",
        "NIO","RIOT","MARA","CLSK","LCID","F","INTC","BABA",
        "MSFT","GOOGL","SPY","QQQ","DKNG","HOOD","RIVN"
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


# ================= ACCUMULATION DETECTION =================
def is_accumulation(df):
    recent_vol = df['Volume'].iloc[-5:].mean()
    past_vol = df['Volume'].iloc[-20:].mean()

    price_range = df['Close'].iloc[-10:].max() - df['Close'].iloc[-10:].min()
    last_price = df['Close'].iloc[-1]

    # ضغط سعر + زيادة حجم = تجميع
    if recent_vol > past_vol * 1.2 and price_range < last_price * 0.04:
        return True

    return False


# ================= BREAKOUT =================
def is_breakout(df):
    resistance = df['High'].rolling(20).max().iloc[-2]
    price = df['Close'].iloc[-1]

    return price > resistance * 0.98


# ================= SCORE ENGINE =================
def score(df):
    price = df['Close'].iloc[-1]
    volume = df['Volume'].iloc[-1]
    avg_volume = df['Volume'].mean()

    rsi_val = rsi(df['Close']).iloc[-1]
    vwap_val = vwap(df).iloc[-1]

    acc = is_accumulation(df)
    brk = is_breakout(df)

    score = 0

    if price < 15:
        score += 15
    if volume > avg_volume * 1.5:
        score += 20
    if 45 < rsi_val < 70:
        score += 15
    if price > vwap_val:
        score += 10
    if acc:
        score += 25
    if brk:
        score += 15

    return score, acc, brk


# ================= ANALYZE SINGLE STOCK =================
def analyze(symbol):
    try:
        df = yf.download(symbol, period="5d", interval="15m", progress=False)

        if df is None or df.empty:
            return None

        sc, acc, brk = score(df)

        if sc < 75:
            return None

        price = df['Close'].iloc[-1]

        return {
            "symbol": symbol,
            "score": sc,
            "price": price,
            "acc": acc,
            "brk": brk
        }

    except:
        return None


# ================= SCANNER LOOP =================
def run_scanner():
    send("🚀 Scanner بدأ - يبحث عن تجميع + انفجار في السوق كامل")

    seen = {}
    cooldown = 60 * 60

    while True:
        results = []

        for symbol in get_market():
            r = analyze(symbol)

            if not r:
                continue

            if time.time() - seen.get(r["symbol"], 0) < cooldown:
                continue

            results.append(r)

        # ترتيب أقوى الفرص
        results = sorted(results, key=lambda x: x["score"], reverse=True)[:5]

        for r in results:
            seen[r["symbol"]] = time.time()

            msg = f"""
🔥 فرصة تجميع قوية / انفجار قريب

📊 {r['symbol']}
⭐ القوة: {r['score']}/100
💰 السعر: {r['price']}

📦 تجميع: {"نعم" if r['acc'] else "لا"}
🚀 اختراق: {"نعم" if r['brk'] else "لا"}
"""

            send(msg)

        time.sleep(300)


# ================= START =================
if __name__ == "__main__":
    threading.Thread(target=run_scanner).start()
    bot.polling()

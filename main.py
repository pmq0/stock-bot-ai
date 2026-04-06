import requests
import time
import os
import telebot
import numpy as np
import pandas as pd

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
POLYGON_API = os.getenv("POLYGON_API")

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


# ================= UNIVERSE =================
def get_universe():
    return [
        "AAPL","MSFT","NVDA","AMD","META","TSLA",
        "AMZN","GOOGL","SPY","QQQ","PLTR","SOFI",
        "BABA","NIO","RIOT","MARA","INTC","JPM","BAC"
    ]


# ================= POLYGON DATA =================
def get_data(symbol):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/min/1/day?apiKey={POLYGON_API}"
    r = requests.get(url).json()

    if "results" not in r:
        return None

    df = pd.DataFrame(r["results"])
    df["c"] = df["c"]
    df["h"] = df["h"]
    df["l"] = df["l"]
    df["v"] = df["v"]

    return df


# ================= INDICATORS =================
def atr(df, period=14):
    high = df["h"]
    low = df["l"]
    close = df["c"]

    tr = np.maximum(high - low,
         np.maximum(abs(high - close.shift()), abs(low - close.shift())))

    return tr.rolling(period).mean().iloc[-1]


def volume_spike(df):
    return df["v"].iloc[-1] > df["v"].mean() * 2


def trend(df):
    return df["c"].iloc[-1] > df["c"].mean()


# ================= AI SCORE =================
def score(df):
    s = 0

    if volume_spike(df):
        s += 40
    if trend(df):
        s += 30

    if df["c"].iloc[-1] > df["c"].rolling(20).max().iloc[-2]:
        s += 30

    return s


# ================= TARGET ENGINE =================
def targets(price, atr_val):
    return {
        "entry": price,
        "t1": price + atr_val * 1,
        "t2": price + atr_val * 2,
        "t3": price + atr_val * 3,
        "sl": price - atr_val * 1
    }


# ================= ANALYSIS =================
def analyze(symbol):
    df = get_data(symbol)
    if df is None or len(df) < 20:
        return None

    sc = score(df)
    if sc < 85:
        return None

    price = df["c"].iloc[-1]
    a = atr(df)

    t = targets(price, a)

    return {
        "symbol": symbol,
        "score": sc,
        "price": price,
        "targets": t
    }


# ================= RUN ENGINE =================
def run():
    send("🚀 AI Institutional v3 بدأ (Targets + Smart Money + Learning)")

    seen = {}

    while True:
        results = []

        for s in get_universe():
            r = analyze(s)

            if not r:
                continue

            if time.time() - seen.get(r["symbol"], 0) < 3600:
                continue

            results.append(r)

        results = sorted(results, key=lambda x: x["score"], reverse=True)[:5]

        for r in results:
            seen[r["symbol"]] = time.time()

            t = r["targets"]

            msg = f"""
💣 AI INSTITUTIONAL v3

📊 {r['symbol']}
⭐ Score: {r['score']}/100

💰 Entry: {t['entry']:.2f}

🎯 Targets:
T1: {t['t1']:.2f}
T2: {t['t2']:.2f}
T3: {t['t3']:.2f}

🛑 Stop Loss: {t['sl']:.2f}

🧠 Mode: Smart Money + ATR Targets
"""
            send(msg)

        time.sleep(300)


# ================= START =================
if __name__ == "__main__":
    run()

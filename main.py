import yfinance as yf
import asyncio
import os
from telegram import Bot

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

bot = Bot(token=TOKEN)

# 💣 Market Universe (واسع + مضاربي)
MARKET = [
    # Big movers
    "AAPL","TSLA","NVDA","AMZN","META","MSFT",

    # Momentum
    "GME","AMC","BB","PLTR","SOFI","RIOT","MARA",

    # Penny / high volatility
    "FFIE","NKLA","MULN","SNDL","ATER","BBIG","CEI","COSM","XELA",

    # Biotech pumps
    "VXRT","INO","OCGN","TNXP"
]

tracked = {}


# 🧠 حساب قوة الصفقة
def score_trade(volume_spike, momentum, breakout, trend):
    score = 0

    if volume_spike: score += 35
    if momentum: score += 25
    if breakout: score += 25
    if trend: score += 15

    return score


def analyze(symbol):
    try:
        df = yf.download(symbol, period="2d", interval="5m", progress=False)

        if df.empty or len(df) < 40:
            return None

        close = df["Close"]
        volume = df["Volume"]

        price = close.iloc[-1]

        # فلتر أساسي
        if price < 0.2 or price > 20:
            return None

        avg_vol = volume[:-1].mean()
        curr_vol = volume.iloc[-1]

        resistance = close.tail(25).max()
        support = close.tail(25).min()

        volume_spike = curr_vol > avg_vol * 3
        breakout = price >= resistance * 0.985
        momentum = close.iloc[-1] > close.iloc[-2] > close.iloc[-3]
        trend = close.iloc[-1] > close.mean()

        score = score_trade(volume_spike, momentum, breakout, trend)

        if score >= 70:
            return {
                "symbol": symbol,
                "price": price,
                "resistance": resistance,
                "support": support,
                "score": score
            }

    except:
        return None


def calc_targets(price, resistance, support):
    entry = resistance * 1.01

    tp1 = entry * 1.06
    tp2 = entry * 1.12
    tp3 = entry * 1.25

    sl = support * 0.98

    return entry, tp1, tp2, tp3, sl


async def scanner():
    print("🔥 FINAL PRO SCANNER RUNNING...")

    while True:
        for symbol in MARKET:
            data = analyze(symbol)

            if not data:
                continue

            entry, tp1, tp2, tp3, sl = calc_targets(
                data["price"],
                data["resistance"],
                data["support"]
            )

            prev = tracked.get(symbol)

            # 🚀 فرصة جديدة
            if not prev:
                tracked[symbol] = {"last": data["price"]}

                msg = f"""
🚀 HIGH PROBABILITY BREAKOUT

📊 {symbol}
💰 Price: {data['price']:.2f}

📊 Score: {data['score']}/100 🔥

🎯 Entry: {entry:.2f}

🎯 TP1: {tp1:.2f}
🎯 TP2: {tp2:.2f}
🎯 TP3: {tp3:.2f}

🛑 SL: {sl:.2f}

⚡ WAIT CONFIRMATION BREAKOUT
"""
                await bot.send_message(chat_id=CHAT_ID, text=msg)

            # 🔁 متابعة الزخم
            else:
                last = prev["last"]

                if data["price"] > last * 1.07:

                    tracked[symbol]["last"] = data["price"]

                    msg = f"""
⚡ MOMENTUM CONTINUATION

📊 {symbol}
💰 New Price: {data['price']:.2f}

🔥 Strong continuation detected
📈 Second entry zone forming

📊 Score still active: {data['score']}/100
"""
                    await bot.send_message(chat_id=CHAT_ID, text=msg)

        await asyncio.sleep(40)


asyncio.run(scanner())

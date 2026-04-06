import yfinance as yf
import asyncio
import os
from telegram import Bot

# 🔥 بياناتك من Railway
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

bot = Bot(token=TOKEN)

# 💣 أسهم مضاربية
MARKET = [
    "CDT","FFIE","NKLA","MARA","RIOT","LCID","SOFI",
    "PLTR","BBIG","GME","AMC","MULN","SNDL"
]

# 🧠 تخزين حالة الأسهم
tracked = {}


def analyze(symbol):
    try:
        df = yf.download(symbol, period="2d", interval="5m", progress=False)

        if df.empty or len(df) < 30:
            return None

        close = df["Close"]
        volume = df["Volume"]

        price = close.iloc[-1]

        # فلتر السعر
        if price > 10:
            return None

        avg_vol = volume[:-1].mean()
        curr_vol = volume.iloc[-1]

        resistance = close.tail(20).max()

        volume_spike = curr_vol > avg_vol * 2.5
        near_breakout = price >= resistance * 0.98
        momentum = close.iloc[-1] > close.iloc[-2] > close.iloc[-3]

        trend = close.iloc[-1] > close.mean()

        if volume_spike and near_breakout and momentum and trend:
            return {
                "price": price,
                "resistance": resistance
            }

    except:
        return None


def calc_targets(price, resistance):
    entry = resistance * 1.01

    tp1 = entry * 1.05
    tp2 = entry * 1.10
    tp3 = entry * 1.20

    sl = entry * 0.97

    return entry, tp1, tp2, tp3, sl


async def scanner():
    print("🔥 BOT RUNNING...")

    while True:
        for symbol in MARKET:
            data = analyze(symbol)

            if not data:
                continue

            price = data["price"]
            resistance = data["resistance"]

            entry, tp1, tp2, tp3, sl = calc_targets(price, resistance)

            # 🧠 هل السهم متابع؟
            prev = tracked.get(symbol)

            # 🔥 دخول جديد
            if not prev:
                tracked[symbol] = {
                    "entry": entry,
                    "last_price": price
                }

                msg = f"""
🚀 فرصة جديدة!

📊 {symbol}
💰 السعر: {price:.2f}

🎯 دخول: {entry:.2f}

🎯 TP1: {tp1:.2f}
🎯 TP2: {tp2:.2f}
🎯 TP3: {tp3:.2f}

🛑 SL: {sl:.2f}
"""
                await bot.send_message(chat_id=CHAT_ID, text=msg)

            else:
                # 🔁 دخول ثاني (إذا طلع السهم ورجع)
                last_price = prev["last_price"]

                if price > last_price * 1.05:
                    tracked[symbol]["last_price"] = price

                    msg = f"""
⚡ فرصة ثانية لنفس السهم!

📊 {symbol}
💰 السعر الجديد: {price:.2f}

🔥 السهم مستمر بالصعود!

🎯 دخول جديد: {price:.2f}
"""
                    await bot.send_message(chat_id=CHAT_ID, text=msg)

        await asyncio.sleep(60)


asyncio.run(scanner())

import os
import telebot
import yfinance as yf
import threading
import time
import logging
import pandas as pd

# =========================
# 🔐 TOKEN
# =========================
TOKEN = os.environ.get("TOKEN")

if not TOKEN:
    raise Exception("TOKEN missing")

bot = telebot.TeleBot(TOKEN)

# =========================
# ⚙️ إعدادات
# =========================
CHAT_ID = None
watchlist = {"AAPL", "TSLA", "NVDA", "AMD"}
last_alert = {}

logging.basicConfig(level=logging.INFO)

# =========================
# 📊 RSI
# =========================
def rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

# =========================
# 📊 تحليل متقدم
# =========================
def analyze(symbol):
    try:
        df = yf.download(symbol, period="1d", interval="5m", progress=False)

        if df is None or df.empty or len(df) < 30:
            return None

        close = df["Close"].dropna()

        price = float(close.iloc[-1])
        old_price = float(close.iloc[-6])
        change = ((price - old_price) / old_price) * 100

        # 📊 RSI
        rsi_val = rsi(close).iloc[-1]

        # 📊 SMA
        sma = close.tail(10).mean()

        # 📊 Breakout
        resistance = close.tail(20).max()
        breakout = price >= resistance

        # 📊 MACD (خفيف)
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()

        trend_up = macd.iloc[-1] > signal.iloc[-1]

        return price, change, rsi_val, trend_up, breakout, sma

    except Exception as e:
        logging.info(f"Error {symbol}: {e}")
        return None

# =========================
# 🔥 Scanner V3
# =========================
def scanner():
    global CHAT_ID

    while True:
        try:
            if CHAT_ID:

                for s in list(watchlist):

                    data = analyze(s)

                    if not data:
                        continue

                    price, change, rsi_val, trend_up, breakout, sma = data

                    # 💣 فلترة قوية جدًا
                    if abs(change) < 1.5:
                        continue

                    if rsi_val > 75 or rsi_val < 25:
                        continue

                    if not trend_up:
                        continue

                    if price < sma:
                        continue

                    if not breakout and change < 3:
                        continue

                    # ⛔ منع التكرار
                    now = time.time()
                    if s in last_alert and now - last_alert[s] < 600:
                        continue

                    last_alert[s] = now

                    msg = f"""
🚀 GOD MODE V3

📊 {s}
💰 السعر: {round(price,2)}
📈 التغير: {round(change,2)}%
📊 RSI: {round(rsi_val,2)}
📊 Trend: {"UP" if trend_up else "DOWN"}
🔥 Breakout: {"YES" if breakout else "NO"}
"""

                    bot.send_message(CHAT_ID, msg)

            time.sleep(60)

        except Exception as e:
            logging.info(f"Scanner crash: {e}")
            time.sleep(10)

# =========================
# ▶️ start
# =========================
@bot.message_handler(commands=['start'])
def start(message):
    global CHAT_ID
    CHAT_ID = message.chat.id
    bot.reply_to(message, "🔥 GOD MODE V3 شغال")

# =========================
# ➕ add
# =========================
@bot.message_handler(commands=['add'])
def add(message):
    try:
        symbol = message.text.split()[1].upper()
        watchlist.add(symbol)
        bot.reply_to(message, f"✅ أضيف {symbol}")
    except:
        bot.reply_to(message, "استخدم /add TSLA")

# =========================
# ➖ remove
# =========================
@bot.message_handler(commands=['remove'])
def remove(message):
    try:
        symbol = message.text.split()[1].upper()
        watchlist.discard(symbol)
        bot.reply_to(message, f"❌ حذف {symbol}")
    except:
        bot.reply_to(message, "استخدم /remove TSLA")

# =========================
# 📋 list
# =========================
@bot.message_handler(commands=['list'])
def list_cmd(message):
    bot.reply_to(message, "📊 الأسهم:\n" + "\n".join(watchlist))

# =========================
# 🚀 تشغيل
# =========================
threading.Thread(target=scanner, daemon=True).start()

while True:
    try:
        print("GOD MODE V3 running...")
        bot.infinity_polling(skip_pending=True)
    except Exception as e:
        print("Restarting...", e)
        time.sleep(5)

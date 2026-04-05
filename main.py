import os
import telebot
import yfinance as yf
import threading
import time
import logging

# =========================
# 🔐 TOKEN من ENV
# =========================
TOKEN = os.environ.get("TOKEN")

if not TOKEN:
    raise Exception("❌ TOKEN is missing in ENV variables")

bot = telebot.TeleBot(TOKEN)

# =========================
# ⚙️ إعدادات
# =========================
CHAT_ID = None
watchlist = {"AAPL", "TSLA", "NVDA", "AMD"}

logging.basicConfig(level=logging.INFO)

# =========================
# 📊 تحليل السهم (FIXED 100%)
# =========================
def analyze(symbol):
    try:
        df = yf.download(symbol, period="1d", interval="5m", progress=False)

        if df is None or df.empty or len(df) < 10:
            return None

        close = df["Close"].dropna()

        price = float(close.iloc[-1])
        old_price = float(close.iloc[-5])

        change = ((price - old_price) / old_price) * 100

        return round(price, 2), round(change, 2)

    except Exception as e:
        logging.info(f"Error analyzing {symbol}: {e}")
        return None

# =========================
# 🔥 Scanner
# =========================
def scanner():
    global CHAT_ID

    while True:
        try:
            if CHAT_ID:
                for s in list(watchlist):
                    result = analyze(s)

                    if result:
                        price, change = result

                        if change > 2:
                            msg = f"""
🚀 فرصة محتملة

📊 {s}
💰 السعر: {price}
📈 التغير: {change}%
"""
                            bot.send_message(CHAT_ID, msg)

            time.sleep(60)

        except Exception as e:
            logging.info(f"Scanner error: {e}")
            time.sleep(10)

# =========================
# ▶️ start
# =========================
@bot.message_handler(commands=['start'])
def start(message):
    global CHAT_ID
    CHAT_ID = message.chat.id
    bot.reply_to(message, "🔥 البوت شغال")

# =========================
# ➕ add
# =========================
@bot.message_handler(commands=['add'])
def add(message):
    try:
        symbol = message.text.split()[1].upper()
        watchlist.add(symbol)
        bot.reply_to(message, f"✅ تمت إضافة {symbol}")
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
        bot.reply_to(message, f"❌ تم حذف {symbol}")
    except:
        bot.reply_to(message, "استخدم /remove TSLA")

# =========================
# 📋 list
# =========================
@bot.message_handler(commands=['list'])
def list_cmd(message):
    bot.reply_to(message, "📊 القائمة:\n" + "\n".join(watchlist))

# =========================
# 🚀 تشغيل السكّانر
# =========================
threading.Thread(target=scanner, daemon=True).start()

# =========================
# 🔁 تشغيل البوت (Stable)
# =========================
while True:
    try:
        print("Bot running...")
        bot.infinity_polling(skip_pending=True)
    except Exception as e:
        print("Restarting bot...", e)
        time.sleep(5)

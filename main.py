import telebot
import yfinance as yf
import threading
import time
import logging

# =========================
# 🔧 إعدادات
# =========================
TOKEN = "PUT_YOUR_TOKEN_HERE"
bot = telebot.TeleBot(TOKEN)

CHAT_ID = None
watchlist = set(["AAPL", "TSLA", "NVDA", "AMD"])

logging.basicConfig(level=logging.INFO)

# =========================
# 📊 تحليل السهم
# =========================
def analyze(symbol):
    try:
        df = yf.download(symbol, period="1d", interval="5m")

        if df is None or df.empty or len(df) < 10:
            return None

        close = df["Close"]

        price = float(close.iloc[-1])
        old_price = float(close.iloc[-5])

        change = ((price - old_price) / old_price) * 100

        return round(price, 2), round(change, 2)

    except Exception as e:
        logging.info(f"Error analyzing {symbol}: {e}")
        return None

# =========================
# 🔥 سكّانر
# =========================
def scanner():
    global CHAT_ID

    while True:
        try:
            if CHAT_ID:
                for s in list(watchlist):
                    try:
                        result = analyze(s)

                        if not result:
                            continue

                        price, change = result

                        # فلترة الفرص
                        if change > 2:

                            msg = f"""
🚀 فرصة محتملة

📊 {s}
💰 السعر: {price}
📈 التغير: {change}%
"""

                            bot.send_message(CHAT_ID, msg)

                    except Exception as e:
                        logging.info(f"Error scanning {s}: {e}")
                        continue

            time.sleep(60)

        except Exception as e:
            logging.info(f"Scanner crash: {e}")
            time.sleep(10)

# =========================
# ▶️ تشغيل
# =========================
@bot.message_handler(commands=['start'])
def start(message):
    global CHAT_ID
    CHAT_ID = message.chat.id

    bot.reply_to(message, "🔥 البوت شغال\nاكتب /help")

# =========================
# 📘 help
# =========================
@bot.message_handler(commands=['help'])
def help_cmd(message):
    bot.reply_to(message, """
📘 الأوامر:

/add SYMBOL → إضافة سهم
/remove SYMBOL → حذف سهم
/list → عرض القائمة
""")

# =========================
# ➕ إضافة سهم
# =========================
@bot.message_handler(commands=['add'])
def add(message):
    try:
        s = message.text.split(" ")[1].upper()
        watchlist.add(s)
        bot.reply_to(message, f"✅ تمت إضافة {s}")
    except:
        bot.reply_to(message, "استخدم /add TSLA")

# =========================
# ➖ حذف سهم
# =========================
@bot.message_handler(commands=['remove'])
def remove(message):
    try:
        s = message.text.split(" ")[1].upper()
        watchlist.discard(s)
        bot.reply_to(message, f"❌ تم حذف {s}")
    except:
        bot.reply_to(message, "استخدم /remove TSLA")

# =========================
# 📋 عرض القائمة
# =========================
@bot.message_handler(commands=['list'])
def list_cmd(message):
    if not watchlist:
        bot.reply_to(message, "القائمة فاضية")
    else:
        bot.reply_to(message, "📊 القائمة:\n" + "\n".join(watchlist))

# =========================
# 🚀 تشغيل السكّانر
# =========================
threading.Thread(target=scanner, daemon=True).start()

# =========================
# 🔁 تشغيل البوت (حماية من الكراش)
# =========================
while True:
    try:
        print("Bot running...")
        bot.infinity_polling(timeout=60, long_polling_timeout=60)
    except Exception as e:
        print("Bot crashed, restarting:", e)
        time.sleep(5)

import telebot
import yfinance as yf
import pandas as pd
import time
import threading

TOKEN = "PUT_YOUR_BOT_TOKEN_HERE"
bot = telebot.TeleBot(TOKEN)

CHAT_ID = None

# 📊 الأسهم الافتراضية
WATCHLIST = set(["AAPL", "TSLA", "NVDA", "AMZN", "META", "AMD"])

# 🔔 منع تكرار الإشعارات
last_alerts = {}

# =========================
# 📈 تحليل السهم
# =========================
def analyze_stock(symbol):
    try:
        data = yf.download(symbol, period="5d", interval="15m")
        if data.empty:
            return None

        close = data["Close"]
        volume = data["Volume"]

        change = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5] * 100
        vol_spike = volume.iloc[-1] > volume.mean() * 1.5

        score = 50
        if change > 2:
            score += 20
        if change > 5:
            score += 20
        if vol_spike:
            score += 15

        return {
            "symbol": symbol,
            "change": round(change, 2),
            "score": score
        }

    except:
        return None


# =========================
# 🔥 المراقبة التلقائية
# =========================
def scanner():
    global CHAT_ID

    while True:
        if CHAT_ID:
            for s in list(WATCHLIST):
                res = analyze_stock(s)

                if res and res["score"] >= 80:
                    if last_alerts.get(s) != "sent":

                        msg = f"""
🚨 BREAKOUT ALERT 🚨

📊 السهم: {s}
📈 التغير: {res['change']}%
⚡ القوة: {res['score']}/100

💡 فرصة مضاربة محتملة
"""

                        bot.send_message(CHAT_ID, msg)
                        last_alerts[s] = "sent"

        time.sleep(60)


# =========================
# 👋 بدء البوت
# =========================
@bot.message_handler(commands=['start'])
def start(message):
    global CHAT_ID
    CHAT_ID = message.chat.id

    bot.send_message(message.chat.id,
        "🔥 تم تشغيل بوت الأسهم\n\nاكتب /help لمعرفة الأوامر"
    )


# =========================
# 📘 المساعدة
# =========================
@bot.message_handler(commands=['help'])
def help_cmd(message):
    bot.send_message(message.chat.id, """
📘 أوامر البوت:

/start - تشغيل البوت
/help - شرح الأوامر

/add SYMBOL - إضافة سهم للمراقبة
مثال: /add MTEN

/remove SYMBOL - حذف سهم
مثال: /remove TSLA

/analyze SYMBOL - تحليل سريع
مثال: /analyze AAPL

💡 البوت يرسل إشعارات تلقائية عند:
- Breakout قوي 💥
- حركة سعر مفاجئة ⚡
- زيادة حجم تداول 📊
""")


# =========================
# ➕ إضافة سهم
# =========================
@bot.message_handler(commands=['add'])
def add_stock(message):
    try:
        symbol = message.text.split(" ")[1].upper()
        WATCHLIST.add(symbol)

        bot.send_message(message.chat.id, f"✅ تم إضافة {symbol} للمراقبة")
    except:
        bot.send_message(message.chat.id, "استخدم: /add MTEN")


# =========================
# ➖ حذف سهم
# =========================
@bot.message_handler(commands=['remove'])
def remove_stock(message):
    try:
        symbol = message.text.split(" ")[1].upper()
        WATCHLIST.discard(symbol)

        bot.send_message(message.chat.id, f"❌ تم حذف {symbol}")
    except:
        bot.send_message(message.chat.id, "استخدم: /remove MTEN")


# =========================
# 📊 تحليل يدوي
# =========================
@bot.message_handler(commands=['analyze'])
def analyze_cmd(message):
    try:
        symbol = message.text.split(" ")[1].upper()
        res = analyze_stock(symbol)

        if not res:
            bot.send_message(message.chat.id, "ما قدرت أحلل السهم")
            return

        bot.send_message(message.chat.id, f"""
📊 تحليل {symbol}

📈 التغير: {res['change']}%
⚡ القوة: {res['score']}/100
""")

    except:
        bot.send_message(message.chat.id, "استخدم: /analyze AAPL")


# =========================
# 🚀 تشغيل المراقبة
# =========================
threading.Thread(target=scanner, daemon=True).start()

print("Bot is running...")
bot.polling()

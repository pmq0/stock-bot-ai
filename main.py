import telebot
import yfinance as yf
import time
import threading
import pytz
import logging
from datetime import datetime

# =========================
# 🔧 إعدادات أساسية
# =========================
TOKEN = "PUT_YOUR_BOT_TOKEN_HERE"
bot = telebot.TeleBot(TOKEN)

CHAT_ID = None

WATCHLIST = set(["AAPL", "TSLA", "NVDA", "AMZN", "META", "AMD", "SPY"])
last_alerts = {}

logging.basicConfig(level=logging.INFO)

# =========================
# 🕐 تحديد جلسة السوق
# =========================
def get_session():
    try:
        tz = pytz.timezone("US/Eastern")
        now = datetime.now(tz)
        hour = now.hour

        if 4 <= hour < 9:
            return "PRE"
        elif 9 <= hour < 16:
            return "MARKET"
        else:
            return "AFTER"
    except:
        return "UNKNOWN"


# =========================
# 📊 تحليل السهم (محمي ضد الكراش)
# =========================
def analyze(symbol):
    try:
        data = yf.download(symbol, period="1d", interval="5m", prepost=True)

        if data is None or data.empty or len(data) < 20:
            return None

        close = data["Close"]
        volume = data["Volume"]

        # 🔥 حركة سعر آمنة
        change = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5] * 100

        # 📊 حجم التداول
        vol_spike = volume.iloc[-1] > volume.mean() * 1.8

        score = 50

        if change > 1:
            score += 15
        if change > 3:
            score += 20
        if change > 7:
            score += 25

        if vol_spike:
            score += 20

        session = get_session()

        return {
            "symbol": symbol,
            "change": round(change, 2),
            "score": score,
            "session": session
        }

    except Exception as e:
        logging.info(f"Error analyzing {symbol}: {e}")
        return None


# =========================
# 🔥 المراقبة المستمرة (محمي)
# =========================
def scanner():
    global CHAT_ID

    while True:
        try:
            if CHAT_ID:

                for s in list(WATCHLIST):
                    try:
                        res = analyze(s)

                        if not res:
                            continue

                        if res["score"] >= 75:

                            key = f"{s}_{res['session']}"

                            if last_alerts.get(key) != "sent":

                                msg = f"""
🚨 STOCK ALERT 🚨

📊 السهم: {s}
⏰ الجلسة: {res['session']}

📈 التغير: {res['change']}%
⚡ القوة: {res['score']}/100

💡 فرصة محتملة (Momentum / Scalping)
"""

                                bot.send_message(CHAT_ID, msg)
                                last_alerts[key] = "sent"

                    except:
                        continue

            time.sleep(60)

        except Exception as e:
            logging.info(f"Scanner error: {e}")
            time.sleep(10)


# =========================
# ▶️ Start
# =========================
@bot.message_handler(commands=['start'])
def start(message):
    global CHAT_ID
    CHAT_ID = message.chat.id

    bot.send_message(message.chat.id,
        "🔥 Bot Started Successfully\nاكتب /help"
    )


# =========================
# 📘 Help
# =========================
@bot.message_handler(commands=['help'])
def help_cmd(message):
    bot.send_message(message.chat.id, """
📘 الأوامر:

/add SYMBOL → إضافة سهم
/remove SYMBOL → حذف سهم
/analyze SYMBOL → تحليل سريع
/start → تشغيل البوت

🔥 النظام:
- Pre Market
- Market
- After Hours

📊 إشارات:
- Momentum ⚡
- Breakout 💥
- Volume Spike 📊
""")


# =========================
# ➕ إضافة سهم
# =========================
@bot.message_handler(commands=['add'])
def add(message):
    try:
        s = message.text.split(" ")[1].upper()
        WATCHLIST.add(s)
        bot.send_message(message.chat.id, f"✅ Added {s}")
    except:
        bot.send_message(message.chat.id, "Use /add TSLA")


# =========================
# ➖ حذف سهم
# =========================
@bot.message_handler(commands=['remove'])
def remove(message):
    try:
        s = message.text.split(" ")[1].upper()
        WATCHLIST.discard(s)
        bot.send_message(message.chat.id, f"❌ Removed {s}")
    except:
        bot.send_message(message.chat.id, "Use /remove TSLA")


# =========================
# 📊 تحليل يدوي
# =========================
@bot.message_handler(commands=['analyze'])
def analyze_cmd(message):
    try:
        s = message.text.split(" ")[1].upper()
        res = analyze(s)

        if not res:
            bot.send_message(message.chat.id, "No data available")
            return

        bot.send_message(message.chat.id, f"""
📊 {s}

📈 Change: {res['change']}%
⚡ Score: {res['score']}/100
⏰ Session: {res['session']}
""")

    except:
        bot.send_message(message.chat.id, "Use /analyze AAPL")


# =========================
# 🚀 تشغيل السكّانر
# =========================
threading.Thread(target=scanner, daemon=True).start()

print("Bot Running...")
bot.infinity_polling()

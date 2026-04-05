import telebot
import yfinance as yf
import threading
import time
import datetime

TOKEN = "PUT_YOUR_BOT_TOKEN_HERE"
bot = telebot.TeleBot(TOKEN)

# قائمة متابعة للإشعارات
watchlist = []

# ---------------- HELP ----------------
@bot.message_handler(commands=['help'])
def help_message(message):
    bot.reply_to(message, """
📊 أوامر البوت:

/stock SYMBOL
تحليل سهم (مثال: /stock AAPL)

/add SYMBOL
إضافة سهم للإشعارات

/list
عرض الأسهم المتابعة

/remove SYMBOL
حذف سهم من القائمة

/help
عرض الأوامر
""")

# ---------------- STOCK ANALYSIS ----------------
@bot.message_handler(commands=['stock'])
def stock_analysis(message):
    try:
        symbol = message.text.split()[1].upper()

        data = yf.download(symbol, period="5d", interval="1h")

        if data.empty:
            bot.reply_to(message, "ما قدرت أجيب بيانات السهم ❌")
            return

        price = float(data['Close'].iloc[-1])
        high = float(data['High'].max())
        low = float(data['Low'].min())

        change = ((price - data['Close'].iloc[0]) / data['Close'].iloc[0]) * 100

        signal = "🟢 فرصة صعود" if change > 2 else "🔴 ضعيف" if change < -2 else "🟡 مراقبة"

        bot.reply_to(message,
            f"""
📊 تحليل: {symbol}

💰 السعر الحالي: {price:.2f}
📈 أعلى سعر: {high:.2f}
📉 أقل سعر: {low:.2f}

📊 التغير: {change:.2f}%

📌 التقييم: {signal}
""")

    except Exception as e:
        bot.reply_to(message, f"خطأ في التحليل: {e}")

# ---------------- ADD WATCH ----------------
@bot.message_handler(commands=['add'])
def add_stock(message):
    try:
        symbol = message.text.split()[1].upper()
        if symbol not in watchlist:
            watchlist.append(symbol)
        bot.reply_to(message, f"تمت إضافة {symbol} 🔔")
    except:
        bot.reply_to(message, "استخدم /add AAPL")

# ---------------- LIST ----------------
@bot.message_handler(commands=['list'])
def list_stocks(message):
    if not watchlist:
        bot.reply_to(message, "القائمة فاضية")
    else:
        bot.reply_to(message, "📌

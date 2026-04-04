import telebot
import os
import yfinance as yf

TOKEN = os.getenv("TOKEN")
bot = telebot.TeleBot(TOKEN)


@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "🔥 بوت الأسهم شغال")


def analyze_stock(symbol):
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="5d", interval="1h")

        if hist.empty:
            return None

        last_price = hist['Close'].iloc[-1]
        prev_price = hist['Close'].iloc[-2]

        change = ((last_price - prev_price) / prev_price) * 100

        signal = "🟢 BUY" if change > 1 else "🔴 WAIT"

        return f"""
📊 {symbol}
السعر: {round(last_price,2)}
التغير: {round(change,2)}%
الإشارة: {signal}
"""
    except:
        return None


@bot.message_handler(commands=['scan'])
def scan(message):
    bot.reply_to(message, "📊 جاري تحليل السوق الأمريكي...")

    stocks = ["AAPL", "TSLA", "NVDA", "AMZN", "META"]

    results = ""

    for s in stocks:
        data = analyze_stock(s)
        if data:
            results += data + "\n----------------\n"

    if results == "":
        results = "❌ ما قدرت أجيب بيانات"

    bot.send_message(message.chat.id, results)


bot.polling()

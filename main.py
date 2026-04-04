import telebot
import os
import yfinance as yf
import pandas as pd
import pandas_ta as ta

TOKEN = os.getenv("8319051258:AAGhYleYook5uVSqTYYc-sEVVgPEV1TYzvI")
bot = telebot.TeleBot(TOKEN)

# قائمة أسهم (نبدأ أمريكي)
WATCHLIST = ["AAPL", "TSLA", "NVDA", "AMZN", "META", "AMD", "PLTR"]

def analyze_stock(symbol):
    df = yf.download(symbol, period="5d", interval="1h")

    if df.empty:
        return None

    df["rsi"] = ta.rsi(df["Close"], length=14)
    df["volume_ma"] = df["Volume"].rolling(10).mean()

    last = df.iloc[-1]

    price = last["Close"]
    rsi = last["rsi"]
    volume = last["Volume"]
    vol_ma = last["volume_ma"]

    signal = "🟡 مراقبة"

    # شروط بسيطة ذكية
    if rsi < 35 and volume > vol_ma:
        signal = "🟢 احتمال صعود قوي (BUY WATCH)"

    if rsi > 70:
        signal = "🔴 تشبع شراء (خطر)"

    entry = round(price, 2)
    target = round(price * 1.05, 2)
    stop = round(price * 0.97, 2)

    return f"""
📊 {symbol}

السعر: {entry}
RSI: {round(rsi,2)}

الإشارة: {signal}

🎯 الهدف: {target}
⛔ وقف الخسارة: {stop}
"""

@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "🔥 بوت الأسهم الذكي شغال\nاكتب /scan لتحليل السوق")

@bot.message_handler(commands=['scan'])
def scan(message):
    bot.reply_to(message, "جاري تحليل الأسهم 🔍...")

    results = ""

    for stock in WATCHLIST:
        res = analyze_stock(stock)
        if res:
            results += res + "\n----------------\n"

    bot.send_message(message.chat.id, results[:4000])

bot.polling()

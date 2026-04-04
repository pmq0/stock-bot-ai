import os
import time
import telebot
import yfinance as yf
import numpy as np

TOKEN = os.getenv("TOKEN")
bot = telebot.TeleBot(TOKEN)

WATCHLIST = ["AAPL", "TSLA", "NVDA", "AMZN", "META", "AMD", "SPY"]

def rsi(data, period=14):
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_stock(symbol):
    data = yf.download(symbol, period="5d", interval="15m")

    if data.empty:
        return None

    close = data["Close"]
    volume = data["Volume"]

    last_price = close.iloc[-1]
    r = rsi(close).iloc[-1]

    vol_spike = volume.iloc[-1] > volume.mean() * 1.5

    signal = "WAIT"
    action = ""

    # 🔥 شراء قوي (زخم + تشبع بيع)
    if r < 30 and vol_spike:
        signal = "🟢 BUY ZONE"
        action = "📈 دخول مضاربة سريع"

    # 🔴 بيع / خروج
    elif r > 70:
        signal = "🔴 SELL ZONE"
        action = "📉 خروج أو جني أرباح"

    # ⚡ انفجار محتمل
    elif vol_spike and 45 < r < 65:
        signal = "⚡ BREAKOUT WATCH"
        action = "🔥 احتمال انفجار قريب"

    return f"""
📊 {symbol}
💰 السعر: {round(last_price, 2)}
📈 RSI: {round(r, 2)}
📦 Volume Spike: {vol_spike}

🚦 الإشارة: {signal}
📌 القرار: {action}
----------------
"""

def scan_market(chat_id):
    bot.send_message(chat_id, "📊 بدء تحليل السوق الأمريكي...")

    results = ""

    for stock in WATCHLIST:
        try:
            res = analyze_stock(stock)
            if res:
                results += res
        except:
            continue

    bot.send_message(chat_id, results if results else "ما فيه بيانات الآن")

@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "🔥 بوت الأسهم البرو شغال الآن")

@bot.message_handler(commands=['scan'])
def scan(message):
    scan_market(message.chat.id)

# 🔥 تشغيل تلقائي كل 10 دقائق
def auto_loop():
    while True:
        time.sleep(600)  # 10 دقائق
        # هنا ممكن تضيف chat_id ثابت لو تبي إشعارات تلقائية

if __name__ == "__main__":
    import threading
    threading.Thread(target=auto_loop).start()
    bot.polling()

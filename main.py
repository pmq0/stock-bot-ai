import os
import telebot
import threading
from flask import Flask

# ========== TELEGRAM BOT ==========
TOKEN = os.getenv("BOT_TOKEN")
bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "البوت شغال 🚀")

@bot.message_handler(func=lambda m: True)
def echo(message):
    bot.reply_to(message, message.text)

# ========== FLASK SERVER (عشان Render) ==========
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!"

def run_bot():
    bot.infinity_polling(skip_pending=True)

# تشغيل البوت + السيرفر مع بعض
if __name__ == "__main__":
    threading.Thread(target=run_bot).start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

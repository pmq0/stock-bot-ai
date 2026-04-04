import telebot
import os

TOKEN = os.getenv("TOKEN")

bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "🔥 بوت الأسهم شغال")

@bot.message_handler(commands=['scan'])
def scan(message):
    bot.reply_to(message, "📊 جاري تحليل السوق الأمريكي...")

bot.polling()

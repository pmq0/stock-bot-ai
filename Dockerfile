FROM python:3.11-slim

# تحديث النظام وتثبيت المكتبات مباشرة
RUN pip install --no-cache-dir requests pyTelegramBotAPI numpy pandas pytz

WORKDIR /app

# نسخ ملف الكود الرئيسي
COPY main.py .

# تشغيل البوت
CMD ["python", "main.py"]

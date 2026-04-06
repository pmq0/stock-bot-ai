FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests pyTelegramBotAPI numpy pandas pytz
COPY main.py .
CMD ["python", "main.py"]

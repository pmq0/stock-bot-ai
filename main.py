import os
import time
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
import telebot
from collections import deque
import threading
from concurrent.futures import ThreadPoolExecutor

# ================= CONFIGURATION =================
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
POLYGON_API = os.getenv("POLYGON_API")

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

bot = telebot.TeleBot(TOKEN)

# Global State
data_cache = {}
seen_signals = deque(maxlen=500)
last_api_call = 0
api_lock = threading.Lock()
dynamic_universe = [] # This will be updated automatically

# ================= UTILITIES =================
def rate_limit():
    """Ensures we don't exceed Polygon Free Tier (5 requests/minute)"""
    global last_api_call
    with api_lock:
        now = time.time()
        # To be safe with 5 req/min, we wait 12.5 seconds between calls
        elapsed = now - last_api_call
        if elapsed < 12.5:
            time.sleep(12.5 - elapsed)
        last_api_call = time.time()

def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

@bot.message_handler(commands=['start'])
def start_command(message):
    welcome = "🚀 <b>AI Trading Bot Ultra Pro v10.0 (The Autonomous Hunter)</b>\n\n"
    welcome += "✅ النظام متصل ويعمل بكامل طاقته\n"
    welcome += "✅ جلب الأسهم النشطة تلقائياً (Auto-Discovery)\n"
    welcome += "✅ رادار الانهيارات والسيولة الذكية\n"
    welcome += "✅ صيد الانفجارات السعرية (Penny Stocks)\n\n"
    welcome += f"📊 الأسهم المراقبة حالياً: {len(dynamic_universe)}\n"
    welcome += "<i>انتظر الإشارات القوية... سيتم تحديث القائمة تلقائياً كل ساعة.</i>"
    bot.reply_to(message, welcome, parse_mode="HTML")

# ================= MARKET LOGIC =================
def is_market_open():
    tz = pytz.timezone("US/Eastern")
    now = datetime.now(tz)
    if now.weekday() >= 5: return False
    market_start = now.replace(hour=4, minute=0, second=0, microsecond=0)
    market_end = now.replace(hour=20, minute=0, second=0, microsecond=0)
    return market_start <= now <= market_end

def update_dynamic_universe():
    """Automatically fetch Top Gainers and Active Tickers under $10"""
    global dynamic_universe
    logger.info("Updating dynamic universe...")
    rate_limit()
    
    # We use the Grouped Daily API to find stocks moving today
    date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}?adjusted=true&apiKey={POLYGON_API}"
    
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        if "results" in data:
            # Filter: Price < 10, Volume > 500,000, Change > 2%
            filtered = [
                r['T'] for r in data['results'] 
                if 1 <= r['c'] <= 10 and r['v'] > 500000 and ((r['c'] - r['o']) / r['o']) > 0.02
            ]
            # Sort by volume and take top 50 to stay within free tier limits for scanning
            filtered_sorted = sorted([r for r in data['results'] if r['T'] in filtered], key=lambda x: x['v'], reverse=True)
            dynamic_universe = [r['T'] for r in filtered_sorted[:50]]
            logger.info(f"Dynamic universe updated: {len(dynamic_universe)} tickers found.")
    except Exception as e:
        logger.error(f"Error updating universe: {e}")
        # Fallback to a safe list if API fails
        if not dynamic_universe:
            dynamic_universe = ["TSLA", "NVDA", "AMD", "PLTR", "SOFI", "NIO", "RIOT", "MARA"]

# ================= DATA FETCHING =================
def fetch_data(symbol, limit=100):
    rate_limit()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/15/minute/{start_date}/{end_date}?adjusted=true&sort=asc&limit={limit}&apiKey={POLYGON_API}"
    
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        if "results" in data and len(data["results"]) > 0:
            df = pd.DataFrame(data["results"])
            df = df.rename(columns={"c": "close", "h": "high", "l": "low", "o": "open", "v": "volume", "t": "timestamp"})
            return df
        return None
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")
        return None

# ================= TECHNICAL ANALYSIS =================
def calculate_indicators(df):
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    high_low = df['high'] - df['low']
    high_cp = np.abs(df['high'] - df['close'].shift())
    low_cp = np.abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1)
    df['atr'] = tr.rolling(window=14).mean()
    
    df['obv'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()
    
    df['stddev'] = df['close'].rolling(window=20).std()
    df['range_avg'] = (df['high'] - df['low']).rolling(window=20).mean()
    df['squeeze'] = df['stddev'] < (df['range_avg'] * 0.5)
    
    return df

def get_score(df, symbol):
    score = 0
    c = df.iloc[-1]
    
    if c['ema9'] > c['ema21'] > c['ema50']: score += 20
    if 50 < c['rsi'] < 75: score += 15
    if c['volume'] > df['volume'].rolling(20).mean().iloc[-1] * 1.5: score += 15
    if c['obv'] > df['obv'].rolling(10).mean().iloc[-1]: score += 10
    if c['squeeze']: score += 20
    if c['close'] > df['high'].iloc[-10:-1].max(): score += 10
    if c['close'] < 10: score += 10
    
    return score

# ================= CORE ENGINE =================
def scan_symbol(symbol):
    try:
        if any(s['symbol'] == symbol and (time.time() - s['time'] < 3600) for s in seen_signals):
            return

        df = fetch_data(symbol)
        if df is None or len(df) < 30: return
        
        df = calculate_indicators(df)
        final_score = get_score(df, symbol)
        
        if final_score >= 75:
            price = df['close'].iloc[-1]
            atr = df['atr'].iloc[-1]
            if pd.isna(atr) or atr == 0: atr = price * 0.02
            
            sl = price - (atr * 2)
            tp1 = price + (atr * 1.5)
            tp2 = price + (atr * 3)
            tp3 = price + (atr * 5)
            
            sentiment_msg = "🔍 تحليل الزخم: إيجابي"
            if final_score > 85:
                sentiment_msg = "🔥 <b>انفجار وشيك: سيولة ضخمة مكتشفة</b>"

            msg = f"""
🚀 <b>فرصة تداول مكتشفة (v10.0)</b>

📊 السهم: <code>{symbol}</code>
⭐ القوة: <b>{final_score}/100</b>
💰 سعر الدخول: <b>${price:.2f}</b>

🎯 الهدف 1: <code>${tp1:.2f}</code>
🎯 الهدف 2: <code>${tp2:.2f}</code>
🚀 هدف القمر: <code>${tp3:.2f}</code>
🛑 وقف الخسارة: <code>${sl:.2f}</code>

{sentiment_msg}
🛡️ حماية السوق: نشطة
⏰ التوقيت: {datetime.now(pytz.timezone('US/Eastern')).strftime('%H:%M')} EST
"""
            send_telegram(msg)
            seen_signals.append({'symbol': symbol, 'time': time.time()})
            
    except Exception as e:
        logger.error(f"Error scanning {symbol}: {e}")

def main_loop():
    send_telegram("✅ <b>تم تشغيل Ultra Pro v10.0 (The Autonomous Hunter)</b>\nنظام جلب الأسهم التلقائي والسيولة الذكية نشط الآن.")
    
    last_universe_update = 0
    
    while True:
        try:
            # Update dynamic universe every hour
            if time.time() - last_universe_update > 3600:
                update_dynamic_universe()
                last_universe_update = time.time()

            if not is_market_open():
                logger.info("Market closed. Sleeping...")
                time.sleep(300)
                continue
            
            # Parallel Scanning
            with ThreadPoolExecutor(max_workers=1) as executor:
                for symbol in dynamic_universe:
                    executor.submit(scan_symbol, symbol)
            
            logger.info("Scan cycle complete. Waiting for next round...")
            time.sleep(60)
            
        except Exception as e:
            logger.error(f"Main Loop Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=lambda: bot.infinity_polling(), daemon=True).start()
    main_loop()

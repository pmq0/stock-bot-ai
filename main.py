from deep_translator import GoogleTranslator
import os
import time
import json
import logging
import threading
import io
import urllib.request
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

import pandas as pd
import numpy as np
import pytz
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask
import telebot
from tenacity import retry, stop_after_attempt, wait_exponential
from curl_cffi import requests
import ftplib

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
STATE_DIR = os.getenv("STATE_DIR", ".")

# Trading Parameters
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 15
SIGNAL_COOLDOWN = 3600
DAILY_LOSS_LIMIT = 300.0
STATE_FILE = os.path.join(STATE_DIR, "state_v32.json")

# Scanner Settings - الإعدادات الأسرع (آمنة)
SCAN_INTERVAL_SEC = 900    # 15 دقيقة بدلاً من 30
CHUNK_SIZE = 100           # 100 بدلاً من 50
FAST_FILTER_WORKERS = 5    # 5 بدلاً من 3
DEEP_ANALYSIS_WORKERS = 3
DELAY_BETWEEN_REQUESTS = 0.15  # نصف الطريق بين 0.1 و 0.2
BREAK_BETWEEN_CHUNKS = 5      # 5 بدلاً من 10
TRADE_MONITOR_INTERVAL = 60

TELEGRAM_DELAY = 1.0

# Strategy Parameters
TP_PCT = 1.06
SL_PCT = 0.97

# Fast Filter Thresholds
MIN_PRICE = 0.7     # أقل سعر
MAX_PRICE = 500    # أعلى سعر
MIN_VOLUME = 50000 # أقل حجم تداول (يستخدم كحد أدنى عام)

def fast_momentum_scanner():
    """دالة سريعة لصيد الأسهم التي تحقق قفزات كبيرة في فترة قصيرة"""
    phase = get_market_phase()
    # يعمل فقط في أول ساعة من التداول العادي (أفضل وقت للقفزات)
    if phase != "REGULAR" or now_est().hour >= 11:
        return
        
    with state_lock:
        tickers = list(state["tickers"])
        
    for symbol in tickers[:200]: # افحص أول 200 سهم فقط للسرعة
        try:
            # جلب بيانات اليوم الحالي (شموع 5 دقائق)
            df = safe_download(symbol, period="1d", interval="5m")
            if df.empty or len(df) < 5:
                continue
                
            price_now = df['close'].iloc[-1]
            price_open = df['open'].iloc[0]
            volume_avg = df['volume'].rolling(20).mean().iloc[-1]
            
            # 1. فلتر السعر والحجم
            if not (MOMENTUM_PRICE_MIN < price_now < MOMENTUM_PRICE_MAX):
                continue
            if df['volume'].iloc[-1] < MOMENTUM_VOL_MIN:
                continue
                
            # 2. الشرط الأهم: هل ارتفع السهم بنسبة كبيرة عن سعر افتتاحه؟
            gain_pct = (price_now - price_open) / price_open * 100
            if gain_pct < MOMENTUM_GAIN_PCT:
                continue
                
            # 3. وصلنا إلى هنا، معناته السهم في قفزة قوية! نحتاج نتأكد من عدم تكرار الإشارة.
            now = time.time()
            last_momentum = state["seen_signals"].get(f"mom_{symbol}", 0)
            if now - last_momentum > 3600: # إشارة وحدة كل ساعة لكل سهم
                # نحسب حجم عقد مناسب للمخاطرة
                size = calculate_position_size(price_now)
                # نرسل الإشارة
                msg = f"⚡ *MOMENTUM ALERT: {symbol}* ⚡\n💰 Price: ${price_now:.2f}\n📈 Gain: +{gain_pct:.1f}% from open!\n📊 Volume: {df['volume'].iloc[-1]:,}\n🎯 Consider an entry with a tight stop-loss!"
                send_telegram(msg)
                
                with state_lock:
                    state["seen_signals"][f"mom_{symbol}"] = now
                    save_state()
                break # نكتفي بإشارة واحدة في كل دورة
                
        except Exception as e:
            logger.error(f"Momentum scan error {symbol}: {e}")
        time.sleep(0.5) # نرتاح بين كل رمز

# Fast Momentum Scanner Settings (لصيد الأسهم السريعة)
MOMENTUM_SCAN_INTERVAL = 120   # كل دقيقتين
MOMENTUM_PRICE_MIN = 2.0       # سعر لا يقل عن 2 دولار (لتجنب pennies الخطيرة)
MOMENTUM_PRICE_MAX = 100.0     # سعر لا يزيد عن 100 دولار
MOMENTUM_VOL_MIN = 500000      # حجم لا يقل عن نصف مليون سهم
MOMENTUM_GAIN_PCT = 10.0       # ارتفاع 10% أو أكثر في أول 30 دقيقة

def fast_filter(symbol):
    try:
        df = safe_download(symbol, period="1d")
        if df.empty:
            return False
        price = df["close"].iloc[-1]
        volume = df["volume"].iloc[-1]
        
        # فلتر السعر أولاً
        if price < MIN_PRICE or price > MAX_PRICE:
            return False
        
        # فلتر الحجم حسب السعر
        if price < 5:
            if volume < 50000:  # Penny stocks
                return False
        elif price < 28:
            if volume < 100000:  # Mid stocks
                return False
        else:
            if volume < 300000:  # Higher stocks
                return False
        
        return True
        
    except:
        return False
# ================= MARKET PHASE SETTINGS =================
PHASE_SETTINGS = {
    "PRE": {"min_score": 80, "size_multiplier": 0.5, "vol_surge_mult": 2.5, "description": "🟡 Pre-Market"},
    "REGULAR": {"min_score": 70, "size_multiplier": 1.0, "vol_surge_mult": 2.0, "description": "🟢 Regular Hours"},
    "AFTER": {"min_score": 85, "size_multiplier": 0.3, "vol_surge_mult": 3.0, "description": "🔵 After-Hours"},
    "CLOSED": {"min_score": 999, "size_multiplier": 0, "vol_surge_mult": 0, "description": "⚫ Market Closed"}
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger()

app = Flask(__name__)
bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

# ================= STATE MANAGEMENT =================
state_lock = threading.RLock()
state = {
    "open_trades": {},
    "performance": {"wins": 0, "losses": 0, "total_pnl": 0.0},
    "seen_signals": {},
    "daily_loss": 0.0,
    "last_reset": None,
    "tickers": [],
    "last_ticker_update": None,
    "halted_stocks": {}
}

EASTERN_TZ = pytz.timezone("US/Eastern")

def now_est():
    return datetime.now(EASTERN_TZ)

def get_market_phase():
    now = now_est()
    hour = now.hour
    weekday = now.weekday()
    if weekday >= 5: return "CLOSED"
    if 4 <= hour < 9: return "PRE"
    elif 9 <= hour < 16: return "REGULAR"
    elif 16 <= hour < 20: return "AFTER"
    else: return "CLOSED"

def save_state():
    with state_lock:
        try:
            os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Save state error: {e}")

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            with state_lock:
                state.update(loaded)
            logger.info("State loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")

def reset_daily_loss_if_needed():
    today = now_est().date().isoformat()
    with state_lock:
        if state.get("last_reset") != today:
            state["daily_loss"] = 0.0
            state["last_reset"] = today
            save_state()

# ================= TELEGRAM HELPERS =================
_last_telegram_time = 0
_telegram_lock = threading.Lock()

def send_telegram(message, photo=None):
    global _last_telegram_time
    if bot and CHAT_ID:
        with _telegram_lock:
            now = time.time()
            elapsed = now - _last_telegram_time
            if elapsed < TELEGRAM_DELAY:
                time.sleep(TELEGRAM_DELAY - elapsed)
            try:
                if photo:
                    bot.send_photo(CHAT_ID, photo, caption=message, parse_mode='Markdown')
                else:
                    bot.send_message(CHAT_ID, message, parse_mode='Markdown')
                _last_telegram_time = time.time()
            except Exception as e:
                logger.error(f"Telegram error: {e}")

# ================= DATA FETCHER WITH CURL_CFFI =================
def safe_download(symbol, period="5d", interval="15m"):
    """جلب البيانات مباشرة باستخدام curl_cffi - بدون حظر"""
    try:
        days_map = {"5d": 5, "1d": 1, "10d": 10, "1mo": 30}
        days = days_map.get(period, 5)
        
        end_date = int(datetime.now().timestamp())
        start_date = int((datetime.now() - timedelta(days=days)).timestamp())
        
        interval_map = {"15m": "15m", "5m": "5m", "1d": "1d", "1h": "60m"}
        yf_interval = interval_map.get(interval, "15m")
        
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval={yf_interval}&period1={start_date}&period2={end_date}"
        
        # تأخير عشوائي لتجنب الضغط
        time.sleep(random.uniform(0.3, 0.7))
        
        # الطلب باستخدام curl_cffi (يقلد متصفح Chrome)
        response = requests.get(url, impersonate="chrome120", timeout=15)
        
        if response.status_code != 200:
            return pd.DataFrame()
        
        data = response.json()
        
        if 'chart' not in data or 'result' not in data['chart'] or not data['chart']['result']:
            return pd.DataFrame()
        
        result = data['chart']['result'][0]
        timestamps = result.get('timestamp', [])
        quote = result.get('indicators', {}).get('quote', [{}])[0]
        
        if not timestamps or not quote:
            return pd.DataFrame()
        
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(timestamps, unit='s'),
            'open': quote.get('open', []),
            'high': quote.get('high', []),
            'low': quote.get('low', []),
            'close': quote.get('close', []),
            'volume': quote.get('volume', [])
        })
        
        df = df.dropna()
        if df.empty:
            return pd.DataFrame()
        
        df.set_index('timestamp', inplace=True)
        return df
        
    except Exception as e:
        logger.error(f"Download error {symbol}: {e}")
        return pd.DataFrame()

def monitor_trading_halts():
    """مراقبة إيقافات التداول وإرسال تنبيه"""
    try:
        url = "https://www.nasdaqtrader.com/dynamic/TradeHalts.csv"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        
        response = requests.get(url, headers=headers, impersonate="chrome120", timeout=15)
        
        if response.status_code != 200:
            logger.warning(f"Failed to fetch halts data, status code: {response.status_code}")
            return
        
        import csv
        from io import StringIO
        
        content = response.text
        lines = content.split('\n')
        
        new_halts = []
        current_date = datetime.now(EASTERN_TZ).strftime('%m/%d/%Y')
        
        for line in lines:
            if not line.strip() or line.startswith('Halt') or line.startswith('---'):
                continue
            
            parts = line.split(',')
            if len(parts) < 5:
                continue
            
            halt_date = parts[0].strip('"')
            if halt_date != current_date:
                continue
            
            halt_time = parts[1].strip('"')
            symbol = parts[2].strip('"')
            name = parts[3].strip('"')
            market = parts[4].strip('"')
            reason = parts[5].strip('"') if len(parts) > 5 else ''
            
            with state_lock:
                if symbol not in state.get("halted_stocks", {}):
                    new_halts.append({
                        'symbol': symbol,
                        'name': name,
                        'time': halt_time,
                        'market': market,
                        'reason': reason
                    })
                    if "halted_stocks" not in state:
                        state["halted_stocks"] = {}
                    state["halted_stocks"][symbol] = {
                        'time': halt_time,
                        'date': halt_date
                    }
        
        save_state()
        
        for halt in new_halts:
            price_info = get_stop_price(halt['symbol'])
            
            if price_info:
                current_price = price_info['price']
                change_pct = price_info['change_pct']
                direction = "🟢 صعود ⬆️" if change_pct > 0 else "🔴 نزول ⬇️" if change_pct < 0 else "⚪ ثابت"
            else:
                current_price = "غير متوفر"
                change_pct = 0
                direction = "⚪ غير معروف"
            
            reason_text = ""
            emoji = "⚠️"
            if reason == 'M':
                reason_text = "تقلبات السوق (Volatility)"
                emoji = "🔴"
            elif reason == 'D':
                reason_text = "في انتظار خبر (News Pending)"
                emoji = "🟡"
            elif reason == 'H':
                reason_text = "إيقاف مؤقت (Trading Halt)"
                emoji = "🟠"
            else:
                reason_text = f"كود {reason}"
            
            msg = f"{emoji} *تنبيه: إيقاف تداول*\n"
            msg += f"📊 *{halt['symbol']}* - {halt['name']}\n"
            msg += f"⏰ الوقت: {halt['time']} EST\n"
            msg += f"🏛️ السوق: {halt['market']}\n"
            msg += f"📋 السبب: {reason_text}\n"
            msg += f"💰 السعر وقت الإيقاف: "
            if isinstance(current_price, float):
                msg += f"${current_price:.2f}\n"
                msg += f"📈 التغير: {change_pct:+.2f}%\n"
                msg += f"🔻 الاتجاه: {direction}\n"
            else:
                msg += f"{current_price}\n"
            msg += f"⚠️ لا تتداول هذا السهم حتى يرفع الإيقاف"
            
            send_telegram(msg)
            
    except Exception as e:
        logger.error(f"Halts monitor error: {e}")

def get_stop_price(symbol):
    """جلب سعر السهم وقت الإيقاف"""
    try:
        df = safe_download(symbol, period="1d", interval="5m")
        if df.empty:
            return None
        
        current_price = df['close'].iloc[-1]
        prev_close = df['close'].iloc[0] if len(df) > 0 else current_price
        change_pct = ((current_price - prev_close) / prev_close) * 100
        
        return {
            'price': current_price,
            'change_pct': change_pct
        }
    except:
        return None

# ================= FALLBACK UNIVERSE =================
MINIMAL_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD", "NFLX",
    "INTC", "PLTR", "SOFI", "NIO", "GME", "AMC", "RIOT", "MARA", "COIN"
]

def update_all_tickers():
    global state
    tickers = []
    try:
        logger.info("🔄 Fetching from NASDAQ FTP...")
        ftp = ftplib.FTP("ftp.nasdaqtrader.com")
        ftp.login()
        ftp.cwd("SymbolDirectory")
        
        r_nasdaq = io.BytesIO()
        ftp.retrbinary("RETR nasdaqlisted.txt", r_nasdaq.write)
        r_nasdaq.seek(0)
        df_nasdaq = pd.read_csv(r_nasdaq, sep="|")
        tickers.extend(df_nasdaq["Symbol"].dropna().tolist())
        
        r_other = io.BytesIO()
        ftp.retrbinary("RETR otherlisted.txt", r_other.write)
        r_other.seek(0)
        df_other = pd.read_csv(r_other, sep="|")
        tickers.extend(df_other["NASDAQ Symbol"].dropna().tolist())
        ftp.quit()
        
        clean = [t for t in tickers if str(t).isalpha() and 1 <= len(str(t)) <= 5]
        clean = list(dict.fromkeys(clean))
        
        if len(clean) > 100:
            with state_lock:
                state["tickers"] = clean
                state["last_ticker_update"] = datetime.now().isoformat()
                save_state()
            logger.info(f"✅ Universe updated: {len(clean)} stocks")
            send_telegram(f"📊 Universe updated: {len(clean)} stocks")
            return clean
    except Exception as e:
        logger.error(f"NASDAQ FTP failed: {e}")
    
    logger.warning(f"Using minimal universe ({len(MINIMAL_UNIVERSE)} symbols)")
    with state_lock:
        state["tickers"] = MINIMAL_UNIVERSE
        save_state()
    return MINIMAL_UNIVERSE

# ================= RISK MANAGEMENT =================
def calculate_position_size(price):
    stop_loss = price * (1 - SL_PCT)
    risk_per_share = price - stop_loss
    if risk_per_share <= 0:
        return 1
    risk_amount = CAPITAL * RISK_PER_TRADE
    size = int(risk_amount / risk_per_share)
    return max(1, min(size, 100))

# ================= INDICATORS =================
def compute_indicators(df):
    df = df.copy()
    df["ema9"] = df["close"].ewm(span=9).mean()
    df["ema21"] = df["close"].ewm(span=21).mean()
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-9)
    df["rsi"] = 100 - (100 / (1 + rs))
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["sma"] = df["close"].rolling(20).mean()
    df["std"] = df["close"].rolling(20).std()
    df["upper_band"] = df["sma"] + (df["std"] * 2)
    df["lower_band"] = df["sma"] - (df["std"] * 2)
    df["bandwidth"] = (df["upper_band"] - df["lower_band"]) / (df["sma"] + 1e-9)
    return df

def detect_accumulation(df):
    if len(df) < 60:
        return False, 0
    last_30 = df.tail(30)
    last_60 = df.tail(60)
    price_change = abs((df['close'].iloc[-1] - df['close'].iloc[-30]) / (df['close'].iloc[-30] + 1e-9))
    price_stable = price_change < 0.03
    volume_surge = last_30['volume'].mean() > last_60['volume'].mean() * 1.3
    price_range = (last_30['high'].max() - last_30['low'].min()) / (df['close'].iloc[-1] + 1e-9)
    tight_range = price_range < 0.05
    acc_score = 0
    if price_stable: acc_score += 30
    if volume_surge: acc_score += 40
    if tight_range: acc_score += 30
    return acc_score >= 60, acc_score

def detect_pre_breakout(df):
    if len(df) < 20:
        return False
    is_squeezing = df['bandwidth'].iloc[-1] < df['bandwidth'].iloc[-10] * 0.7
    approaching = df['close'].iloc[-1] > df['upper_band'].iloc[-1] * 0.97
    volume_surge = df['volume'].iloc[-3:].mean() > df['volume'].rolling(20).mean().iloc[-1] * 1.5
    return is_squeezing and approaching and volume_surge

def generate_chart(symbol, df, entry, tp, sl, is_accumulating=False, is_pre_breakout=False):
    try:
        df_plot = df.tail(60).copy()
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]})
        ax1.plot(df_plot.index, df_plot['close'], 'cyan', linewidth=1.5, label='Price')
        ax1.plot(df_plot.index, df_plot['ema9'], 'yellow', linewidth=1, alpha=0.7, label='EMA 9')
        ax1.plot(df_plot.index, df_plot['ema21'], 'orange', linewidth=1, alpha=0.7, label='EMA 21')
        ax1.fill_between(df_plot.index, df_plot['upper_band'], df_plot['lower_band'], alpha=0.1, color='gray', label='BB')
        ax1.axhline(y=entry, color='lime', linestyle='--', linewidth=1.5, label=f'Entry ${entry:.2f}')
        ax1.axhline(y=tp, color='green', linestyle='--', linewidth=1.5, label=f'TP ${tp:.2f}')
        ax1.axhline(y=sl, color='red', linestyle='--', linewidth=1.5, label=f'SL ${sl:.2f}')
        ax1.set_title(f'{symbol} - Signal', fontsize=14, color='white')
        ax1.grid(True, alpha=0.15)
        ax1.tick_params(colors='white')
        colors = ['green' if df_plot['close'].iloc[i] >= df_plot['open'].iloc[i] else 'red' for i in range(len(df_plot))]
        ax2.bar(df_plot.index, df_plot['volume'], color=colors, alpha=0.7)
        ax2.axhline(y=df_plot['vol_ma'].iloc[-1], color='blue', linestyle='--', linewidth=1, label='Avg Vol')
        ax2.set_ylabel('Volume', color='white')
        ax2.tick_params(colors='white')
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100, facecolor='#0d1117')
        buf.seek(0)
        plt.close(fig)
        return buf
    except Exception as e:
        logger.error(f"Chart error: {e}")
        return None

# ================= FAST FILTER =================
def fast_filter(symbol):
    try:
        df = safe_download(symbol, period="1d")
        if df.empty:
            return False
        price = df["close"].iloc[-1]
        volume = df["volume"].iloc[-1]
        return MIN_PRICE < price < MAX_PRICE and volume > MIN_VOLUME
    except:
        return False

# ================= TRADE MANAGEMENT =================
def open_trade(symbol, price, score, df, is_accumulating=False, is_pre_breakout=False, phase="REGULAR", settings=None):
    with state_lock:
        if state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT or len(state["open_trades"]) >= MAX_OPEN_TRADES:
            return
        size = int(calculate_position_size(price) * settings["size_multiplier"])
        tp, sl = price * TP_PCT, price * SL_PCT
        state["open_trades"][symbol] = {"entry": price, "tp": tp, "sl": sl, "size": size, "time": time.time(), "score": score, "phase": phase}
        save_state()
    chart = generate_chart(symbol, df, price, tp, sl, is_accumulating, is_pre_breakout)
    phase_emoji = "🟡" if phase == "PRE" else ("🔵" if phase == "AFTER" else "🟢")
    caption = f"{phase_emoji} *SIGNAL: {symbol}* ({phase})\n💰 Entry: ${price:.2f}\n🎯 TP: ${tp:.2f}\n🛑 SL: ${sl:.2f}\n📦 Size: {size}\n📊 Score: {score}/{settings['min_score']}"
    send_telegram(caption, photo=chart)

def close_trade(symbol, price, reason):
    with state_lock:
        if symbol not in state["open_trades"]:
            return
        trade = state["open_trades"].pop(symbol)
        pnl = (price - trade["entry"]) * trade["size"]
        pnl_pct = ((price - trade["entry"]) / trade["entry"]) * 100
        if pnl > 0:
            state["performance"]["wins"] += 1
        else:
            state["performance"]["losses"] += 1
            state["daily_loss"] = state.get("daily_loss", 0) + abs(pnl)
        state["performance"]["total_pnl"] += pnl
        save_state()
    emoji = "🎉" if pnl > 0 else "🛑"
    send_telegram(f"{emoji} *CLOSED: {symbol}*\n📝 {reason}\n💵 Exit: ${price:.2f}\n📈 PnL: ${pnl:+.2f} ({pnl_pct:+.2f}%)")

# ================= PROCESS SYMBOL =================
def process_symbol(symbol):
    try:
        reset_daily_loss_if_needed()
        phase = get_market_phase()
        settings = PHASE_SETTINGS.get(phase, PHASE_SETTINGS["CLOSED"])
        if phase == "CLOSED":
            return
        with state_lock:
            if symbol in state["open_trades"] or state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT:
                return
        
        df = safe_download(symbol, period="5d", interval="15m")
        if df.empty:
            return
        df.columns = [c.lower() for c in df.columns]
        df = compute_indicators(df)
        is_accumulating, acc_score = detect_accumulation(df)
        is_pre_breakout = detect_pre_breakout(df)
        
        last = df.iloc[-1]
        vol_surge = last['volume'] > df['vol_ma'].iloc[-1] * settings["vol_surge_mult"]
        price_break = last['close'] > df['high'].iloc[-20:-1].max()
        score = 0
        if vol_surge: score += 40
        if price_break: score += 40
        if 40 < last['rsi'] < 70: score += 10
        if last['ema9'] > last['ema21']: score += 10
        
        if score >= settings["min_score"]:
            now = time.time()
            last_seen = state["seen_signals"].get(symbol, 0)
            if now - last_seen > SIGNAL_COOLDOWN:
                open_trade(symbol, last['close'], score, df, is_accumulating, is_pre_breakout, phase, settings)
                with state_lock:
                    state["seen_signals"][symbol] = now
                    save_state()
    except Exception as e:
        pass

def update_trades():
    with state_lock:
        symbols = list(state["open_trades"].keys())
    for symbol in symbols:
        try:
            df = safe_download(symbol, period='1d', interval='5m')
            if df.empty:
                continue
            price = df['close'].iloc[-1]
            with state_lock:
                trade = state["open_trades"].get(symbol)
                if not trade:
                    continue
                if price >= trade["tp"]:
                    close_trade(symbol, price, "TAKE PROFIT")
                elif price <= trade["sl"]:
                    close_trade(symbol, price, "STOP LOSS")
        except:
            pass

# ================= SCANNER ENGINE =================
def background_scanner():
    update_all_tickers()
    while True:
        try:
            fast_momentum_scanner()
            phase = get_market_phase()
            if phase != "CLOSED":
                with state_lock:
                    tickers = list(state["tickers"])
                
                if not tickers:
                    update_all_tickers()
                    continue
                
                logger.info(f"🔍 Starting scan of {len(tickers)} symbols - Phase: {phase}")
                
                for i in range(0, len(tickers), CHUNK_SIZE):
                    chunk = tickers[i:i+CHUNK_SIZE]
                    
                    filtered = []
                    with ThreadPoolExecutor(max_workers=FAST_FILTER_WORKERS) as executor:
                        future_to_sym = {executor.submit(fast_filter, sym): sym for sym in chunk}
                        for future in as_completed(future_to_sym):
                            if future.result():
                                filtered.append(future_to_sym[future])
                            time.sleep(DELAY_BETWEEN_REQUESTS / FAST_FILTER_WORKERS)
                    
                    logger.info(f"📊 Chunk {i//CHUNK_SIZE + 1}: {len(filtered)}/{len(chunk)} passed")
                    
                    if filtered:
                        with ThreadPoolExecutor(max_workers=DEEP_ANALYSIS_WORKERS) as executor:
                            executor.map(process_symbol, filtered)
                    
                    time.sleep(BREAK_BETWEEN_CHUNKS)
            time.sleep(SCAN_INTERVAL_SEC)
        except Exception as e:
            logger.error(f"Scanner error: {e}")
            time.sleep(60)

def background_monitor():
    while True:
        try:
            if get_market_phase() != "CLOSED":
                update_trades()
                monitor_trading_halts() 
            time.sleep(TRADE_MONITOR_INTERVAL)
        except Exception as e:
            logger.error(f"Monitor error: {e}")
            time.sleep(30)

# ================= TELEGRAM HANDLERS =================
def run_telegram_bot():
    if not bot:
        return
    try:
        bot.remove_webhook()
    except:
        pass
    while True:
        try:
            logger.info("🤖 Starting Telegram bot...")
            bot.infinity_polling(timeout=30, long_polling_timeout=10)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(15)

if bot:
    @bot.message_handler(commands=['start'])
    def cmd_start(message):
        phase = get_market_phase()
        msg = f"👋 Trading Bot v32!\n📊 {len(state['tickers'])} stocks\n🕐 {PHASE_SETTINGS[phase]['description']}\n\n/status - Performance\n/positions - Trades\n/close SYMBOL\n/scan SYMBOL"
        send_telegram(msg)

    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        with state_lock:
            perf = state["performance"]
            total = perf["wins"] + perf["losses"]
            wr = (perf["wins"] / total * 100) if total > 0 else 0
            msg = f"📊 *Status*\n✅ Wins: {perf['wins']}\n❌ Losses: {perf['losses']}\n📈 WR: {wr:.1f}%\n💵 PnL: ${perf['total_pnl']:+.2f}\n📦 Open: {len(state['open_trades'])}\n🌐 Universe: {len(state['tickers'])}"
        send_telegram(msg)

    @bot.message_handler(commands=['positions'])
    def cmd_positions(message):
        with state_lock:
            trades = state["open_trades"]
            if not trades:
                send_telegram("📭 No open positions")
                return
            msg = "*Open Positions*\n"
            for sym, t in trades.items():
                msg += f"\n🔹 *{sym}* | ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f}"
            send_telegram(msg)

    @bot.message_handler(commands=['close'])
    def cmd_close(message):
        try:
            args = message.text.split()
            if len(args) != 2:
                send_telegram("Usage: /close <SYMBOL>")
                return
            symbol = args[1].upper()
            with state_lock:
                if symbol not in state["open_trades"]:
                    send_telegram(f"❌ {symbol} not open")
                    return
            df = safe_download(symbol, period='1d', interval='5m')
            if df.empty:
                send_telegram(f"❌ No price for {symbol}")
                return
            close_trade(symbol, df['close'].iloc[-1], "Manual Close")
            send_telegram(f"✅ {symbol} closed")
        except Exception as e:
            send_telegram(f"❌ Error: {e}")

    @bot.message_handler(commands=['scan'])
    def cmd_scan(message):
        try:
            args = message.text.split()
            if len(args) != 2:
                send_telegram("Usage: /scan <SYMBOL>")
                return
            symbol = args[1].upper()
            send_telegram(f"🔍 Analyzing {symbol}...")
            df = safe_download(symbol, period="10d", interval="15m")
            if df.empty:
                send_telegram(f"❌ No data for {symbol}")
                return
            df.columns = [c.lower() for c in df.columns]
            df = compute_indicators(df)
            last = df.iloc[-1]
            phase = get_market_phase()
            settings = PHASE_SETTINGS.get(phase, PHASE_SETTINGS["REGULAR"])
            vol_surge = last['volume'] > df['vol_ma'].iloc[-1] * 2.0
            price_break = last['close'] > df['high'].iloc[-20:-1].max()
            score = 0
            if vol_surge: score += 40
            if price_break: score += 40
            if 40 < last['rsi'] < 70: score += 10
            if last['ema9'] > last['ema21']: score += 10
            msg = f"📊 *{symbol}*\n💰 ${last['close']:.2f}\n📊 Vol: {int(last['volume']):,}\n⚡ RSI: {last['rsi']:.1f}\n🎯 Score: {score}/{settings['min_score']}"
            send_telegram(msg)
        except Exception as e:
            send_telegram(f"❌ Error: {e}")
            
@bot.message_handler(commands=['news'])
def cmd_news(message):
    try:
        args = message.text.split()
        if len(args) != 2:
            send_telegram("Usage: /news <SYMBOL>")
            return
        symbol = args[1].upper()
        
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={symbol}"
        response = requests.get(url, impersonate="chrome120", timeout=10)
        data = response.json()
        
        news = data.get('news', [])
        if not news:
            send_telegram(f"📰 No news found for {symbol}")
            return
        
        translator = GoogleTranslator(source='en', target='ar')
        msg = f"📰 *أخبار {symbol}*\n\n"
        
        # كلمات للتحليل
        positive_words = ['surge', 'gain', 'rise', 'up', 'growth', 'partnership', 'patent', 'expansion', 'profit', 'record', 'high', 'positive', 'opportunity', 'breakthrough', 'launch', 'award', 'contract', 'deal']
        negative_words = ['layoff', 'sell', 'drop', 'down', 'loss', 'reverse', 'investigation', 'lawsuit', 'cut', 'decline', 'fall', 'low', 'negative', 'risk', 'warning', 'sue', 'fine', 'penalty']
        
        positive_count = 0
        negative_count = 0
        neutral_count = 0
        
        for item in news[:5]:
            title = item.get('title', 'No title')
            link = item.get('link', '#')
            publisher = item.get('publisher', 'Unknown')
            
            # ترجمة العنوان
            try:
                title_ar = translator.translate(title)
            except:
                title_ar = title
            
            # تحليل المشاعر
            title_lower = title.lower()
            is_positive = any(word in title_lower for word in positive_words)
            is_negative = any(word in title_lower for word in negative_words)
            
            if is_positive and not is_negative:
                emoji = "🟢"
                positive_count += 1
            elif is_negative:
                emoji = "🔴"
                negative_count += 1
            else:
                emoji = "🟡"
                neutral_count += 1
            
            msg += f"{emoji} **{publisher}**\n"
            msg += f"  {title_ar}\n"
            msg += f"  [رابط الخبر]({link})\n\n"
        
        # إضافة التحليل النهائي
        msg += f"\n📊 *تحليل الأخبار:*\n"
        msg += f"🟢 إيجابي: {positive_count}\n"
        msg += f"🔴 سلبي: {negative_count}\n"
        msg += f"🟡 محايد: {neutral_count}\n\n"
        
        if positive_count > negative_count:
            msg += f"✅ *الخلاصة: أخبار إيجابية* 👍"
        elif negative_count > positive_count:
            msg += f"⚠️ *الخلاصة: أخبار سلبية - كن حذراً* 👎"
        else:
            msg += f"🟡 *الخلاصة: أخبار محايدة*"
        
        send_telegram(msg)
        
    except Exception as e:
        send_telegram(f"❌ خطأ: {e}")
# ================= MAIN =================
if __name__ == "__main__":
    load_state()
    
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    
    if bot:
        threading.Thread(target=run_telegram_bot, daemon=True).start()
    
    logger.info("=" * 50)
    logger.info("✅ Bot started successfully!")
    logger.info(f"📊 Universe: {len(state.get('tickers', []))} stocks")
    logger.info("=" * 50)
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)

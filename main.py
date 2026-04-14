from deep_translator import GoogleTranslator
import os
import time
import json
import logging
import threading
import io
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

import pandas as pd
import numpy as np
import pytz
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, jsonify
import telebot
from tenacity import retry, stop_after_attempt, wait_exponential
from curl_cffi import requests
import ftplib

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
AUTHORIZED_CHAT_ID = int(CHAT_ID) if CHAT_ID and CHAT_ID.lstrip("-").isdigit() else None
STATE_DIR = os.getenv("STATE_DIR", ".")

# Trading Parameters
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 15
DAILY_LOSS_LIMIT = 300.0
STATE_FILE = os.path.join(STATE_DIR, "state_v32.json")

# Scanner Settings
SCAN_INTERVAL_SEC = 900
CHUNK_SIZE = 100
FAST_FILTER_WORKERS = 3       # تقليل لتخفيف الضغط على السيرفر (كان 5)
DEEP_ANALYSIS_WORKERS = 3
DELAY_BETWEEN_REQUESTS = 0.5
BREAK_BETWEEN_CHUNKS = 5
TRADE_MONITOR_INTERVAL = 60
MAX_TICKERS_TO_SCAN = 3800

TELEGRAM_DELAY = 1.0

# فترة الحجب بين إشارات نفس السهم (ساعتين)
SIGNAL_COOLDOWN = 7200

# Strategy Parameters
TP_PCT = 1.06
SL_PCT = 0.97

# Fast Filter Thresholds
MIN_PRICE = 0.1
MAX_PRICE = 500
MIN_VOLUME = 50000

# Fast Momentum Scanner Settings
MOMENTUM_SCAN_INTERVAL = 120
MOMENTUM_PRICE_MIN = 2.0
MOMENTUM_PRICE_MAX = 100.0
MOMENTUM_VOL_MIN = 500000
MOMENTUM_GAIN_PCT = 10.0

def fast_momentum_scanner():
    """دالة سريعة لصيد الأسهم التي تحقق قفزات كبيرة في فترة قصيرة"""
    phase = get_market_phase()
    if phase != "REGULAR" or now_est().hour >= 11:
        return
        
    with state_lock:
        tickers = list(state["tickers"])
        
    for symbol in tickers[:200]:
        try:
            df = safe_download(symbol, period="1d", interval="5m")
            if df.empty or len(df) < 5:
                continue
                
            price_now = df['close'].iloc[-1]
            price_open = df['open'].iloc[0]
            volume_avg = df['volume'].rolling(20).mean().iloc[-1]
            
            if not (MOMENTUM_PRICE_MIN < price_now < MOMENTUM_PRICE_MAX):
                continue
            if df['volume'].iloc[-1] < MOMENTUM_VOL_MIN:
                continue
                
            gain_pct = (price_now - price_open) / price_open * 100
            if gain_pct < MOMENTUM_GAIN_PCT:
                continue
                
            now = time.time()
            last_momentum = state["seen_signals"].get(f"mom_{symbol}", 0)
            if now - last_momentum > 3600:
                msg = f"⚡ *MOMENTUM ALERT: {symbol}* ⚡\n💰 Price: ${price_now:.2f}\n📈 Gain: +{gain_pct:.1f}% from open!\n📊 Volume: {df['volume'].iloc[-1]:,}\n🎯 Consider an entry with a tight stop-loss!"
                send_telegram(msg)
                
                with state_lock:
                    state["seen_signals"][f"mom_{symbol}"] = now
                    save_state()
                break
                
        except Exception as e:
            logger.error(f"Momentum scan error {symbol}: {e}")
        time.sleep(0.5)

def fast_filter(symbol):
    """🔥 Rocket Scanner - فلتر قوي يصطاد الأسهم المتفجرة فقط"""
    try:
        df = safe_download(symbol, period="1d", interval="5m")
        if df.empty or len(df) < 5:
            return False

        price = df["close"].iloc[-1]
        volume = df["volume"].iloc[-1]
        avg_volume = df["volume"].mean()

        # فلتر السعر: بين 0.5 و 20 دولار (هذه هي الأسهم اللي تتحرك بسرعة)
        if price < 0.5 or price > 20:
            return False

        # فلتر حجم التداول: لازم يكون فوق 300 ألف
        if volume < 300000:
            return False

        # فلتر الانفجار: الحجم الحالي لازم يكون 1.5x فوق المتوسط
        if avg_volume > 0 and volume < avg_volume * 1.5:
            return False

        return True

    except Exception as e:
        logger.debug(f"fast_filter failed for {symbol}: {e}")
        return False

# ================= MARKET PHASE SETTINGS =================
PHASE_SETTINGS = {
    "PRE":     {"min_score": 85,  "size_multiplier": 0.5, "vol_surge_mult": 2.5, "description": "🟡 Pre-Market"},
    "REGULAR": {"min_score": 85,  "size_multiplier": 1.0, "vol_surge_mult": 2.0, "description": "🟢 Regular Hours"},
    "AFTER":   {"min_score": 90,  "size_multiplier": 0.3, "vol_surge_mult": 3.0, "description": "🔵 After-Hours"},
    "CLOSED":  {"min_score": 999, "size_multiplier": 0,   "vol_surge_mult": 0,   "description": "⚫ Market Closed"}
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger()

app = Flask(__name__)
bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

@app.route("/health", methods=["GET"])
def healthcheck():
    return jsonify({"status": "ok", "bot_enabled": bool(bot), "has_chat_id": bool(CHAT_ID)})

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
    "halted_stocks": {},
    "halt_counter": {},
    "seen_news": {},            # أخبار RSS اللي أُرسلت مسبقاً
    "pending_halts": {},        # أسهم موقوفة ننتظر رفع إيقافها
    "seen_edgar": {},           # ملفات SEC 8-K اللي أُرسلت مسبقاً
    "seen_gappers": {},         # أسهم Pre-Market Gappers اللي أُرسلت
    "short_interest": {}        # بيانات Short Interest المحفوظة
}

EASTERN_TZ = pytz.timezone("US/Eastern")

def now_est():
    return datetime.now(EASTERN_TZ)

def is_authorized_message(message):
    if AUTHORIZED_CHAT_ID is None:
        return True
    chat = getattr(message, "chat", None)
    return getattr(chat, "id", None) == AUTHORIZED_CHAT_ID

def ensure_authorized(message):
    if is_authorized_message(message):
        return True
    logger.warning(f"Unauthorized Telegram command attempt from chat_id={getattr(getattr(message, 'chat', None), 'id', None)}")
    return False

def get_market_phase():
    now = now_est()
    weekday = now.weekday()
    minutes = now.hour * 60 + now.minute
    if weekday >= 5:
        return "CLOSED"
    if 4 * 60 <= minutes < 9 * 60 + 30:
        return "PRE"
    if 9 * 60 + 30 <= minutes < 16 * 60:
        return "REGULAR"
    if 16 * 60 <= minutes < 20 * 60:
        return "AFTER"
    return "CLOSED"

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

def reset_halt_counter_if_needed():
    """تصفير عداد الإيقافات كل يوم"""
    today = now_est().date().isoformat()
    with state_lock:
        if state.get("last_halt_reset") != today:
            state["halt_counter"] = {}
            state["last_halt_reset"] = today
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

# ================= DATA FETCHER =================
def safe_download(symbol, period="5d", interval="15m"):
    """جلب البيانات مباشرة باستخدام curl_cffi"""
    try:
        days_map = {"5d": 5, "1d": 1, "10d": 10, "1mo": 30}
        days = days_map.get(period, 5)
        
        end_date = int(datetime.now().timestamp())
        start_date = int((datetime.now() - timedelta(days=days)).timestamp())
        
        interval_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1d": "1d", "1h": "60m"}
        yf_interval = interval_map.get(interval, "15m")
        
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval={yf_interval}&period1={start_date}&period2={end_date}"
        
        time.sleep(random.uniform(0.3, 0.7))
        
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
    except Exception as e:
        logger.warning(f"Failed to compute halt stop price for {symbol}: {e}")
        return None

def monitor_trading_halts():
    """مراقبة إيقافات التداول وإرسال تنبيه"""
    try:
        url = "https://www.nasdaqtrader.com/dynamic/TradeHalts.csv"
        
        response = requests.get(url, impersonate="chrome120", timeout=15)
        
        if response.status_code != 200:
            logger.warning(f"Failed to fetch halts data, status code: {response.status_code}")
            return
        
        content = response.text
        lines = [l for l in content.split('\n') if l.strip()]
        
        if not lines:
            return

        # أكواد الإيقاف الكاملة حسب NASDAQ
        HALT_CODES = {
            "T1":   ("🔴", "إيقاف - أخبار قيد الانتظار (News Pending)"),
            "T2":   ("🟡", "إيقاف - أخبار صدرت (News Released)"),
            "T5":   ("🟠", "إيقاف - توقف تداول سهم واحد (Single Stock Pause)"),
            "T6":   ("🔴", "إيقاف - نشاط سوق غير اعتيادي (Extraordinary Activity)"),
            "T8":   ("🟠", "إيقاف - صندوق ETF"),
            "T12":  ("🟡", "إيقاف - طلب معلومات إضافية من NASDAQ"),
            "H4":   ("🔴", "إيقاف - عدم امتثال (Non-compliance)"),
            "H9":   ("🔴", "إيقاف - ملفات غير محدّثة (Not Current)"),
            "H10":  ("🔴", "إيقاف - تعليق تداول من SEC"),
            "H11":  ("🔴", "إيقاف - مخاوف تنظيمية (Regulatory Concern)"),
            "O1":   ("🟠", "إيقاف تشغيلي (Operations Halt)"),
            "IPO1": ("🔵", "IPO - لم يبدأ التداول بعد"),
            "M1":   ("🟡", "إجراء شركة (Corporate Action)"),
            "M2":   ("⚪", "اقتباس غير متاح (Quotation Not Available)"),
            "LUDP": ("🔴", "توقف تداول - تذبذب (Volatility Pause)"),
            "LUDS": ("🔴", "توقف تداول - Straddle Condition"),
            "MWC1": ("🚨", "توقف السوق كله - المستوى 1 (Circuit Breaker L1)"),
            "MWC2": ("🚨", "توقف السوق كله - المستوى 2 (Circuit Breaker L2)"),
            "MWC3": ("🚨", "توقف السوق كله - المستوى 3 (Circuit Breaker L3)"),
            "MWC0": ("🚨", "توقف Circuit Breaker - ترحيل من يوم سابق"),
            "M":    ("🟠", "توقف تذبذب - سهم مدرج (Volatility Pause Listed)"),
            "D":    ("⚫", "حذف السهم من NASDAQ/CQS"),
        }

        new_halts = []
        current_date = datetime.now(EASTERN_TZ).strftime('%m/%d/%Y')

        # تخطي السطر الأول (header)
        for line in lines[1:]:
            try:
                # الأعمدة: HaltDate, HaltTime, Symbol, Name, Mkt, ReasonCode, PausePrice, ResumptionDate, ResumptionQuoteTime, ResumptionTradeTime
                parts = [p.strip().strip('"') for p in line.split(',')]
                if len(parts) < 4:
                    continue

                halt_date           = parts[0] if len(parts) > 0 else ''
                halt_time           = parts[1] if len(parts) > 1 else ''
                symbol              = parts[2] if len(parts) > 2 else ''
                name                = parts[3] if len(parts) > 3 else ''
                market              = parts[4] if len(parts) > 4 else ''
                reason              = parts[5] if len(parts) > 5 else ''
                pause_price         = parts[6] if len(parts) > 6 else ''
                resume_date         = parts[7] if len(parts) > 7 else ''
                resume_quote_time   = parts[8] if len(parts) > 8 else ''
                resume_trade_time   = parts[9] if len(parts) > 9 else ''

                if not symbol or not halt_date:
                    continue

                # فقط أحداث اليوم
                if halt_date != current_date:
                    continue

                # تجاهل أكواد الاستئناف (Resume codes)
                resume_codes = {"T3","T7","R4","R9","C3","C4","C9","C11","R1","R2","IPOQ","IPOE","MWCQ"}
                if reason.upper() in resume_codes:
                    continue

                with state_lock:
                    halt_key = f"{symbol}_{halt_time}"
                    if halt_key not in state.get("halted_stocks", {}):
                        new_halts.append({
                            'symbol':           symbol,
                            'name':             name,
                            'halt_time':        halt_time,
                            'market':           market,
                            'reason':           reason.upper(),
                            'pause_price':      pause_price,
                            'resume_date':      resume_date,
                            'resume_quote_time':resume_quote_time,
                            'resume_trade_time':resume_trade_time,
                        })
                        if "halted_stocks" not in state:
                            state["halted_stocks"] = {}
                        state["halted_stocks"][halt_key] = {
                            'time': halt_time,
                            'date': halt_date
                        }

            except Exception as parse_err:
                logger.warning(f"Halt line parse error: {parse_err}")
                continue

        save_state()

        for halt in new_halts:
            try:
                # جلب السعر الحالي
                price_info = get_stop_price(halt['symbol'])
                if price_info:
                    current_price = price_info['price']
                    change_pct    = price_info['change_pct']
                    direction     = "🟢 صعود ⬆️" if change_pct > 0 else "🔴 نزول ⬇️" if change_pct < 0 else "⚪ ثابت"
                else:
                    current_price = None
                    change_pct    = 0
                    direction     = "⚪ غير معروف"

                # عداد الإيقافات
                with state_lock:
                    if "halt_counter" not in state:
                        state["halt_counter"] = {}
                    state["halt_counter"][halt['symbol']] = state["halt_counter"].get(halt['symbol'], 0) + 1
                    halt_count = state["halt_counter"][halt['symbol']]

                # نص السبب والإيموجي
                emoji, reason_text = HALT_CODES.get(halt['reason'], ("⚠️", f"كود {halt['reason']}"))

                msg  = f"{emoji} *تنبيه: إيقاف تداول*\n"
                msg += f"━━━━━━━━━━━━━━━━\n"
                msg += f"📊 *{halt['symbol']}*"
                if halt['name']:
                    msg += f" — {halt['name']}"
                msg += f"\n"
                msg += f"🏛️ السوق: {halt['market']}\n"
                msg += f"⏰ وقت الإيقاف: *{halt['halt_time']} EST*\n"
                msg += f"📋 السبب: {reason_text}\n"

                # سعر الإيقاف (Pause Threshold Price)
                if halt['pause_price'] and halt['pause_price'] not in ('', 'N/A', '0', '0.0'):
                    msg += f"💲 سعر عتبة الإيقاف: ${halt['pause_price']}\n"

                # السعر الحالي
                if current_price is not None:
                    msg += f"💰 السعر الحالي: *${current_price:.2f}*\n"
                    msg += f"📈 التغير: {change_pct:+.2f}%  {direction}\n"

                # وقت الاستئناف
                has_resume = any([halt['resume_date'], halt['resume_quote_time'], halt['resume_trade_time']])
                if has_resume:
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"🔄 *معلومات الاستئناف:*\n"
                    if halt['resume_date']:
                        msg += f"📅 تاريخ الاستئناف: {halt['resume_date']}\n"
                    if halt['resume_quote_time']:
                        msg += f"📌 وقت استئناف الاقتباس: {halt['resume_quote_time']}\n"
                    if halt['resume_trade_time']:
                        msg += f"▶️ وقت استئناف التداول: {halt['resume_trade_time']}\n"

                msg += f"━━━━━━━━━━━━━━━━\n"
                msg += f"🔁 عدد إيقافات اليوم: {halt_count} مرة\n"
                msg += f"⚠️ لا تتداول حتى يُرفع الإيقاف"

                send_telegram(msg)

                # 🔥 أضف السهم لقائمة المراقبة بعد رفع الإيقاف
                BULLISH_HALT_CODES = {"T1", "T2", "LUDP", "T5", "T6", "M1"}
                if halt['reason'] in BULLISH_HALT_CODES:
                    with state_lock:
                        state.setdefault("pending_halts", {})[halt['symbol']] = {
                            "reason":        halt['reason'],
                            "timestamp":     time.time(),
                            "price_at_halt": current_price if current_price else 0,
                        }
                        save_state()

            except Exception as send_err:
                logger.error(f"Halt notification error {halt.get('symbol','?')}: {send_err}")

    except Exception as e:
        logger.error(f"Halts monitor error: {e}")

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
            clean = clean[:MAX_TICKERS_TO_SCAN]  # 🔥 تطبيق الحد الأقصى
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
    stop_loss = price * SL_PCT
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
    # VWAP — متوسط السعر المرجح بالحجم
    df["vwap"] = (df["close"] * df["volume"]).cumsum() / (df["volume"].cumsum() + 1e-9)
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

def detect_explosion(df):
    """⚡ Volume Explosion Detector - يكشف انفجار الحجم مع الزخم"""
    if len(df) < 20:
        return False, 0

    last_vol = df["volume"].iloc[-1]
    avg_vol = df["volume"].rolling(20).mean().iloc[-1]

    if avg_vol <= 0:
        return False, 0

    # انفجار: الحجم الحالي أكثر من 3x المتوسط
    spike = last_vol > avg_vol * 3

    # زخم: السعر الحالي أعلى من سعر 5 شمعات قبل
    momentum = df["close"].iloc[-1] > df["close"].iloc[-5]

    score = 0
    if spike:
        score += 60
    if momentum:
        score += 40

    return score >= 60, score

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

# ================= TRADE MANAGEMENT =================
def open_trade(symbol, price, score, df, is_accumulating=False, is_pre_breakout=False, phase="REGULAR", settings=None):
    with state_lock:
        if state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT or len(state["open_trades"]) >= MAX_OPEN_TRADES:
            return
        size = max(1, int(calculate_position_size(price) * settings["size_multiplier"]))
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

        # 🔥 فحص الانفجار أولاً - إذا ما في انفجار نوقف هنا
        explosion, explosion_score = detect_explosion(df)
        if not explosion:
            return

        last = df.iloc[-1]
        vol_surge = last['volume'] > df['vol_ma'].iloc[-1] * settings["vol_surge_mult"]
        price_break = last['close'] > df['high'].iloc[-20:-1].max()
        score = explosion_score
        if vol_surge: score += 20
        if price_break: score += 20
        if 40 < last['rsi'] < 70: score += 10
        if last['ema9'] > last['ema21']: score += 10

        # 🛡️ فلتر VWAP + RSI (حماية من الدخول الخاطئ)
        price_above_vwap = last['close'] > last['vwap']
        rsi_ok           = last['rsi'] > 50
        if not price_above_vwap or not rsi_ok:
            return

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
        except Exception as e:
            logger.warning(f"Trade update failed for {symbol}: {e}")

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
                
                # 🔥 تقليل الأسهم للحد الأقصى لتحسين الأداء وتجنب الحظر
                tickers = tickers[:MAX_TICKERS_TO_SCAN]
                
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
            reset_halt_counter_if_needed()
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
    except Exception as e:
        logger.debug(f"Webhook removal skipped: {e}")
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
        if not ensure_authorized(message):
            return
        phase = get_market_phase()
        msg = f"👋 Trading Bot v32!\n📊 {len(state['tickers'])} stocks\n🕐 {PHASE_SETTINGS[phase]['description']}\n\n/status - Performance\n/positions - Trades\n/close SYMBOL\n/scan SYMBOL"
        send_telegram(msg)

    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        if not ensure_authorized(message):
            return
        with state_lock:
            perf = state["performance"]
            total = perf["wins"] + perf["losses"]
            wr = (perf["wins"] / total * 100) if total > 0 else 0
            msg = f"📊 *Status*\n✅ Wins: {perf['wins']}\n❌ Losses: {perf['losses']}\n📈 WR: {wr:.1f}%\n💵 PnL: ${perf['total_pnl']:+.2f}\n📦 Open: {len(state['open_trades'])}\n🌐 Universe: {len(state['tickers'])}"
        send_telegram(msg)

    @bot.message_handler(commands=['positions'])
    def cmd_positions(message):
        if not ensure_authorized(message):
            return
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
        if not ensure_authorized(message):
            return
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
        if not ensure_authorized(message):
            return
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

if bot:
    @bot.message_handler(commands=['news'])
    def cmd_news(message):
        if not ensure_authorized(message):
            return
        try:
            args = message.text.split()
            if len(args) != 2:
                send_telegram("Usage: /news <SYMBOL>")
                return

            symbol = args[1].upper()
            url = f"https://query1.finance.yahoo.com/v1/finance/search?q={symbol}"
            response = requests.get(url, impersonate="chrome120", timeout=10)
            if response.status_code != 200:
                send_telegram(f"❌ تعذر جلب الأخبار لـ {symbol}")
                return

            data = response.json()
            news = data.get('news', [])
            if not news:
                send_telegram(f"📰 No news found for {symbol}")
                return

            translator = GoogleTranslator(source='en', target='ar')
            msg = f"📰 *أخبار {symbol}*\n\n"

            positive_words = ['surge', 'gain', 'rise', 'up', 'growth', 'partnership', 'patent', 'expansion', 'profit', 'record', 'high', 'positive', 'opportunity', 'breakthrough', 'launch', 'award', 'contract', 'deal']
            negative_words = ['layoff', 'sell', 'drop', 'down', 'loss', 'reverse', 'investigation', 'lawsuit', 'cut', 'decline', 'fall', 'low', 'negative', 'risk', 'warning', 'sue', 'fine', 'penalty']

            positive_count = 0
            negative_count = 0
            neutral_count = 0

            for item in news[:5]:
                title = item.get('title', 'No title')
                link = item.get('link', '#')
                publisher = item.get('publisher', 'Unknown')

                try:
                    title_ar = translator.translate(title)
                except Exception:
                    title_ar = title

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

            msg += f"\n📊 *تحليل الأخبار:*\n"
            msg += f"🟢 إيجابي: {positive_count}\n"
            msg += f"🔴 سلبي: {negative_count}\n"
            msg += f"🟡 محايد: {neutral_count}\n\n"

            if positive_count > negative_count:
                msg += "✅ *الخلاصة: أخبار إيجابية* 👍"
            elif negative_count > positive_count:
                msg += "⚠️ *الخلاصة: أخبار سلبية - كن حذراً* 👎"
            else:
                msg += "🟡 *الخلاصة: أخبار محايدة*"

            send_telegram(msg)

        except Exception as e:
            logger.error(f"/news command failed: {e}")
            send_telegram(f"❌ خطأ: {e}")

# ================= RSS NEWS SCANNER =================

# كلمات إيجابية قوية تسبب ارتفاع السهم
STRONG_POSITIVE = [
    "fda approval", "fda approved", "fda grants", "breakthrough",
    "partnership", "acquisition", "merger", "contract awarded",
    "record revenue", "record earnings", "beats estimates", "beats expectations",
    "raised guidance", "raises guidance", "buyout", "uplisting",
    "nasdaq listing", "nyse listing", "phase 3", "positive results",
    "exclusive deal", "major contract", "patent granted", "new drug",
    "clinical trial success", "positive data", "ipo"
]

NEGATIVE_WORDS = [
    "bankruptcy", "delisted", "sec investigation", "fraud", "lawsuit",
    "recall", "missed estimates", "lowers guidance", "chapter 11",
    "going concern", "default", "suspended"
]

RSS_SOURCES = [
    # BusinessWire — إعلانات رسمية من الشركات (الأسرع والأدق)
    "https://feed.businesswire.com/rss/home/?rss=G1&_gl=1",
    # PR Newswire — مصدر ثاني للإعلانات الرسمية
    "https://www.prnewswire.com/rss/news-releases-list.rss",
    # GlobeNewswire
    "https://www.globenewswire.com/RssFeed/subjectcode/15-Banking%20and%20Financial%20Services",
]

def fetch_rss(url):
    """جلب وتحليل RSS Feed"""
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            content = resp.read()
        root = ET.fromstring(content)
        items = []
        for item in root.iter('item'):
            title = item.findtext('title', '').strip()
            link  = item.findtext('link', '').strip()
            pub   = item.findtext('pubDate', '').strip()
            desc  = item.findtext('description', '').strip()
            items.append({'title': title, 'link': link, 'pubDate': pub, 'desc': desc})
        return items
    except Exception as e:
        logger.warning(f"RSS fetch error {url}: {e}")
        return []

def extract_symbols_from_text(text):
    """استخراج رموز الأسهم من نص الخبر"""
    import re
    # رموز مثل (NASDAQ: MGRX) أو (NYSE: AAPL)
    pattern = r'\b(?:NASDAQ|NYSE|AMEX|OTC):\s*([A-Z]{1,5})\b'
    found = re.findall(pattern, text.upper())
    # أي كلمة كبيرة بين قوسين مثل (MGRX)
    pattern2 = r'\(([A-Z]{1,5})\)'
    found2 = re.findall(pattern2, text.upper())
    all_symbols = list(set(found + found2))
    # فلتر الكلمات الشائعة التي ليست رموز أسهم
    exclude = {'THE','AND','FOR','INC','LLC','LTD','CEO','CFO','IPO','FDA','SEC','NYSE','USD','ETF'}
    return [s for s in all_symbols if s not in exclude and 1 <= len(s) <= 5]

def analyze_news_sentiment(title, desc=""):
    """تحليل إيجابية / سلبية الخبر"""
    text = (title + " " + desc).lower()

    is_strong_positive = any(kw in text for kw in STRONG_POSITIVE)
    is_negative        = any(kw in text for kw in NEGATIVE_WORDS)

    if is_negative:
        return "negative", 0
    if is_strong_positive:
        # نحسب النقاط حسب عدد الكلمات الإيجابية
        score = sum(20 for kw in STRONG_POSITIVE if kw in text)
        return "positive", min(score, 100)
    return "neutral", 0

def rss_news_scanner():
    """
    يفحص RSS كل دقيقتين:
    - يجيب الأخبار الجديدة
    - يستخرج رمز السهم
    - لو الخبر إيجابي قوي → يفحص السعر والحجم → يرسل إشارة
    """
    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(300)
                continue

            for rss_url in RSS_SOURCES:
                items = fetch_rss(rss_url)
                for item in items:
                    title   = item['title']
                    link    = item['link']
                    pub     = item['pubDate']

                    # تجنب إرسال نفس الخبر مرتين
                    news_id = title[:80]
                    with state_lock:
                        if news_id in state.get("seen_news", {}):
                            continue
                        state.setdefault("seen_news", {})[news_id] = time.time()

                    # تحليل المشاعر
                    sentiment, score = analyze_news_sentiment(title, item.get('desc',''))
                    if sentiment != "positive" or score < 40:
                        continue

                    # استخراج رمز السهم
                    symbols = extract_symbols_from_text(title + " " + item.get('desc',''))
                    if not symbols:
                        continue

                    for symbol in symbols[:2]:  # أقصى سهمين من نفس الخبر
                        try:
                            df = safe_download(symbol, period="1d", interval="5m")
                            if df.empty or len(df) < 5:
                                continue

                            price      = df['close'].iloc[-1]
                            vol_now    = df['volume'].iloc[-1]
                            vol_avg    = df['volume'].mean()
                            price_open = df['open'].iloc[0]
                            gain_pct   = (price - price_open) / price_open * 100

                            # شروط الدخول: حجم انفجر + السهم لسه ما طار كثير
                            vol_spike = vol_avg > 0 and vol_now > vol_avg * 2
                            not_too_late = gain_pct < 25  # ما فات الوقت

                            if not vol_spike or not not_too_late:
                                continue

                            # حساب الدخول والهدف ووقف الخسارة
                            entry = price
                            tp    = round(entry * 1.10, 2)   # هدف +10%
                            sl    = round(entry * 0.95, 2)   # وقف -5%

                            # ترجمة عنوان الخبر
                            try:
                                translator = GoogleTranslator(source='en', target='ar')
                                title_ar = translator.translate(title[:200])
                            except:
                                title_ar = title

                            # تحديد نوع الخبر بالعربي
                            title_low = title.lower()
                            if "fda" in title_low:
                                news_type = "🧬 موافقة FDA"
                            elif "merger" in title_low or "acquisition" in title_low:
                                news_type = "🤝 استحواذ / دمج"
                            elif "contract" in title_low:
                                news_type = "📝 عقد جديد"
                            elif "partnership" in title_low:
                                news_type = "🤝 شراكة"
                            elif "earnings" in title_low or "revenue" in title_low:
                                news_type = "💰 نتائج مالية"
                            elif "ipo" in title_low or "listing" in title_low:
                                news_type = "🆕 إدراج جديد"
                            else:
                                news_type = "📰 خبر إيجابي"

                            now_time = now_est().strftime("%I:%M %p")

                            msg  = f"📰🚀 *إشارة خبر إيجابي*\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"📊 *{symbol}*\n"
                            msg += f"🏷️ النوع: {news_type}\n"
                            msg += f"📝 الخبر: {title_ar}\n"
                            msg += f"🔗 [رابط الخبر]({link})\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"⏰ وقت الدخول المقترح: *{now_time}*\n"
                            msg += f"💰 سعر الدخول: *${entry:.2f}*\n"
                            msg += f"🎯 الهدف: *${tp:.2f}* (+10%)\n"
                            msg += f"🛑 وقف الخسارة: *${sl:.2f}* (-5%)\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"📈 الارتفاع من الافتتاح: +{gain_pct:.1f}%\n"
                            msg += f"🔥 الحجم: {int(vol_now):,} ({vol_now/vol_avg:.1f}x المتوسط)\n"
                            msg += f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"

                            send_telegram(msg)
                            logger.info(f"📰 News signal sent: {symbol} — {title[:60]}")

                        except Exception as sym_err:
                            logger.warning(f"News signal error {symbol}: {sym_err}")

                time.sleep(2)  # delay بين المصادر

        except Exception as e:
            logger.error(f"RSS scanner error: {e}")

        time.sleep(120)  # كل دقيقتين


# ================= POST-HALT ENTRY SYSTEM =================

def post_halt_entry_monitor():
    """
    يراقب الأسهم الموقوفة وبمجرد رفع الإيقاف:
    - يفحص الحجم والسعر
    - لو الشروط مناسبة → يرسل إشارة دخول
    """
    # أكواد الإيقاف اللي تسبب ارتفاع بعد الرفع
    BULLISH_HALT_CODES = {"T1", "T2", "LUDP", "T5", "T6", "M1"}

    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(60)
                continue

            with state_lock:
                pending = dict(state.get("pending_halts", {}))

            for symbol, halt_info in list(pending.items()):
                try:
                    # تجاهل لو مضى أكثر من 30 دقيقة على الإيقاف
                    halt_age = time.time() - halt_info.get("timestamp", 0)
                    if halt_age > 1800:
                        with state_lock:
                            state["pending_halts"].pop(symbol, None)
                        continue

                    df = safe_download(symbol, period="1d", interval="1m")
                    if df.empty or len(df) < 3:
                        continue

                    price      = df['close'].iloc[-1]
                    vol_now    = df['volume'].iloc[-1]
                    vol_prev   = df['volume'].iloc[-3:-1].mean()
                    price_halt = halt_info.get("price_at_halt", price)

                    # هل رُفع الإيقاف؟ = في بيانات جديدة بعد وقت الإيقاف
                    last_candle_time = df.index[-1]
                    halt_time_ts     = halt_info.get("timestamp", 0)
                    resumed = last_candle_time.timestamp() > halt_time_ts + 60

                    if not resumed:
                        continue

                    # هل تم إرسال إشارة لهذا السهم مسبقاً؟
                    signal_key = f"posthalt_{symbol}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            state["pending_halts"].pop(symbol, None)
                            continue

                    # شروط الدخول بعد رفع الإيقاف
                    vol_spike     = vol_prev > 0 and vol_now > vol_prev * 2
                    price_up      = price > price_halt * 1.01
                    reason_code   = halt_info.get("reason", "")
                    bullish_halt  = reason_code in BULLISH_HALT_CODES

                    # 🛡️ فلتر VWAP + RSI
                    try:
                        df_ind = compute_indicators(df)
                        last   = df_ind.iloc[-1]
                        above_vwap = last['close'] > last['vwap']
                        rsi_ok     = last['rsi'] > 50
                    except:
                        above_vwap = True
                        rsi_ok     = True

                    if not (vol_spike and bullish_halt and above_vwap and rsi_ok):
                        continue

                    # حساب الدخول
                    entry    = price
                    tp       = round(entry * 1.10, 2)
                    sl       = round(entry * 0.95, 2)
                    now_time = now_est().strftime("%I:%M %p")

                    # نوع الإيقاف بالعربي
                    halt_desc = {
                        "T1":   "خبر قيد الانتظار",
                        "T2":   "خبر صدر",
                        "LUDP": "توقف تذبذب (Volatility)",
                        "T5":   "توقف تداول مؤقت",
                        "T6":   "نشاط غير اعتيادي",
                        "M1":   "إجراء شركة",
                    }.get(reason_code, reason_code)

                    change_from_halt = (price - price_halt) / price_halt * 100

                    msg  = f"🚀 *إشارة دخول بعد رفع إيقاف*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"📊 *{symbol}*\n"
                    msg += f"🔓 سبب الإيقاف: {halt_desc} ({reason_code})\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"⏰ وقت الدخول: *{now_time}*\n"
                    msg += f"💰 سعر الدخول: *${entry:.2f}*\n"
                    msg += f"🎯 الهدف: *${tp:.2f}* (+10%)\n"
                    msg += f"🛑 وقف الخسارة: *${sl:.2f}* (-5%)\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"📌 سعر وقت الإيقاف: ${price_halt:.2f}\n"
                    msg += f"📈 التغير بعد الرفع: {change_from_halt:+.1f}%\n"
                    msg += f"🔥 الحجم: {int(vol_now):,} ({vol_now/max(vol_prev,1):.1f}x)\n"
                    msg += f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"

                    send_telegram(msg)

                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        state["pending_halts"].pop(symbol, None)
                        save_state()

                    logger.info(f"🔓 Post-halt signal: {symbol} @ ${entry:.2f}")

                except Exception as sym_err:
                    logger.warning(f"Post-halt check error {symbol}: {sym_err}")

        except Exception as e:
            logger.error(f"Post-halt monitor error: {e}")

        time.sleep(30)  # يفحص كل 30 ثانية


# ================= SHORT INTEREST SCANNER =================

def fetch_short_interest():
    """
    يجلب قائمة Short Interest من NASDAQ مجاناً كل يوم.
    السهم اللي عنده short interest عالي + خبر/حجم = Short Squeeze محتمل.
    """
    try:
        # NASDAQ ينشر ملف Short Interest مجاناً
        url = "https://www.nasdaqtrader.com/dynamic/symdir/shortinterest/nasdaqshortinterest.txt"
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode('utf-8', errors='ignore')

        lines = content.strip().split('\n')
        short_data = {}

        for line in lines[1:]:  # تخطي الـ header
            parts = line.strip().split('|')
            if len(parts) < 5:
                continue
            symbol        = parts[0].strip()
            short_int_str = parts[3].strip().replace(',', '')
            days_str      = parts[4].strip()

            if not symbol or not symbol.isalpha():
                continue
            try:
                short_int  = int(short_int_str)
                days_cover = float(days_str) if days_str else 0
                if short_int > 500000:  # فقط أسهم فيها short interest حقيقي
                    short_data[symbol] = {
                        "short_interest": short_int,
                        "days_to_cover":  days_cover
                    }
            except:
                continue

        with state_lock:
            state["short_interest"] = short_data
            save_state()

        logger.info(f"📉 Short interest updated: {len(short_data)} stocks")
        return short_data

    except Exception as e:
        logger.error(f"Short interest fetch error: {e}")
        return {}


def short_squeeze_scanner():
    """
    يفحص الأسهم ذات Short Interest العالي ويبحث عن إشارات Squeeze:
    - Short Interest عالي (أكثر من 20% من الـ float)
    - حجم ينفجر فجأة
    - السعر يكسر مستوى مقاومة
    يشتغل كل 30 دقيقة
    """
    # جلب بيانات Short Interest مرة يومياً
    last_si_update = 0

    while True:
        try:
            if get_market_phase() not in ("REGULAR", "PRE"):
                time.sleep(300)
                continue

            # تحديث Short Interest كل 4 ساعات
            if time.time() - last_si_update > 14400:
                fetch_short_interest()
                last_si_update = time.time()

            with state_lock:
                short_data = dict(state.get("short_interest", {}))

            if not short_data:
                time.sleep(1800)
                continue

            # فرز الأسهم: الأعلى في Days to Cover أولاً (أكثر عرضة للـ Squeeze)
            sorted_symbols = sorted(
                short_data.keys(),
                key=lambda s: short_data[s].get("days_to_cover", 0),
                reverse=True
            )[:150]  # أعلى 150 سهم

            for symbol in sorted_symbols:
                try:
                    si_info = short_data[symbol]

                    # تجنب التكرار
                    signal_key = f"squeeze_{symbol}"
                    with state_lock:
                        last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                    if time.time() - last_sent < SIGNAL_COOLDOWN:
                        continue

                    df = safe_download(symbol, period="5d", interval="15m")
                    if df.empty or len(df) < 20:
                        continue

                    df.columns = [c.lower() for c in df.columns]
                    df = compute_indicators(df)
                    last = df.iloc[-1]

                    price       = last['close']
                    vol_now     = last['volume']
                    vol_avg     = df['vol_ma'].iloc[-1]
                    short_int   = si_info['short_interest']
                    days_cover  = si_info['days_to_cover']

                    # شروط الـ Short Squeeze
                    vol_spike     = vol_avg > 0 and vol_now > vol_avg * 2.5
                    above_vwap    = price > last['vwap']
                    rsi_ok        = last['rsi'] > 50
                    price_ok      = 0.5 < price < 50
                    high_si       = days_cover >= 3  # 3+ أيام لتغطية = ضغط عالي
                    ema_cross     = last['ema9'] > last['ema21']

                    if not (vol_spike and above_vwap and rsi_ok and price_ok and high_si):
                        continue

                    entry    = price
                    tp       = round(entry * 1.15, 2)   # هدف +15% لأن الـ Squeeze يكون قوي
                    sl       = round(entry * 0.95, 2)
                    now_time = now_est().strftime("%I:%M %p")

                    msg  = f"🔥 *إشارة Short Squeeze*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"📊 *{symbol}*\n"
                    msg += f"📉 Short Interest: {short_int:,} سهم\n"
                    msg += f"📅 أيام التغطية: {days_cover:.1f} يوم\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"⏰ وقت الدخول: *{now_time}*\n"
                    msg += f"💰 سعر الدخول: *${entry:.2f}*\n"
                    msg += f"🎯 الهدف: *${tp:.2f}* (+15%)\n"
                    msg += f"🛑 وقف الخسارة: *${sl:.2f}* (-5%)\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"🔥 الحجم: {int(vol_now):,} ({vol_now/max(vol_avg,1):.1f}x المتوسط)\n"
                    msg += f"📈 RSI: {last['rsi']:.1f} | فوق VWAP ✅\n"
                    msg += f"💡 السبب: Short Squeeze محتمل — البائعون على المكشوف مضطرون للشراء\n"
                    msg += f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"

                    send_telegram(msg)

                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()

                    logger.info(f"🔥 Squeeze signal: {symbol} | SI={short_int:,} | Days={days_cover}")
                    time.sleep(1)

                except Exception as sym_err:
                    logger.warning(f"Squeeze check error {symbol}: {sym_err}")

        except Exception as e:
            logger.error(f"Short squeeze scanner error: {e}")

        time.sleep(1800)  # كل 30 دقيقة


# ================= SEC EDGAR 8-K SCANNER =================

def edgar_8k_scanner():
    """
    يراقب ملفات 8-K على SEC EDGAR مجاناً وفورياً.
    8-K = حدث مهم: عقد جديد، استحواذ، موافقة FDA، تغيير إدارة...
    يشتغل كل 5 دقائق
    """
    # كلمات تدل على خبر إيجابي قوي في ملفات 8-K
    BULLISH_8K_KEYWORDS = [
        "fda approval", "fda approved", "fda grants",
        "definitive agreement", "merger agreement", "acquisition",
        "exclusive license", "license agreement",
        "record revenue", "record sales", "revenue increase",
        "contract award", "awarded contract", "major contract",
        "strategic partnership", "joint venture",
        "positive results", "positive data", "clinical trial",
        "patent granted", "patent issued",
        "nasdaq uplisting", "nyse uplisting",
        "going public", "ipo",
        "share repurchase", "buyback program",
        "special dividend", "increased dividend"
    ]

    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(300)
                continue

            # EDGAR RSS Feed للملفات الجديدة — مجاني وفوري
            url = "https://efts.sec.gov/LATEST/search-index?q=%228-K%22&dateRange=custom&startdt={}&enddt={}&forms=8-K".format(
                datetime.now().strftime("%Y-%m-%d"),
                datetime.now().strftime("%Y-%m-%d")
            )

            # نستخدم الـ RSS المباشر من EDGAR
            rss_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=20&search_text=&output=atom"

            req = urllib.request.Request(rss_url, headers={
                'User-Agent': 'TradingBot research@example.com'  # EDGAR يطلب User-Agent
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                content = resp.read()

            root = ET.fromstring(content)
            ns   = {'atom': 'http://www.w3.org/2005/Atom'}

            entries = root.findall('atom:entry', ns)

            for entry in entries:
                try:
                    title    = entry.findtext('atom:title',   '', ns).strip()
                    link_el  = entry.find('atom:link',        ns)
                    link     = link_el.get('href', '') if link_el is not None else ''
                    summary  = entry.findtext('atom:summary', '', ns).strip()
                    filing_id = entry.findtext('atom:id',     '', ns).strip()

                    # تجنب التكرار
                    with state_lock:
                        if filing_id in state.get("seen_edgar", {}):
                            continue
                        state.setdefault("seen_edgar", {})[filing_id] = time.time()

                    full_text = (title + " " + summary).lower()

                    # هل فيه كلمة إيجابية؟
                    matched_kw = next((kw for kw in BULLISH_8K_KEYWORDS if kw in full_text), None)
                    if not matched_kw:
                        continue

                    # استخرج رمز السهم
                    symbols = extract_symbols_from_text(title + " " + summary)
                    if not symbols:
                        continue

                    for symbol in symbols[:2]:
                        try:
                            signal_key = f"edgar_{symbol}"
                            with state_lock:
                                last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                            if time.time() - last_sent < SIGNAL_COOLDOWN:
                                continue

                            df = safe_download(symbol, period="1d", interval="5m")
                            if df.empty or len(df) < 5:
                                continue

                            df.columns = [c.lower() for c in df.columns]
                            df = compute_indicators(df)
                            last  = df.iloc[-1]
                            price = last['close']
                            vol   = last['volume']
                            vol_avg = df['vol_ma'].iloc[-1]

                            # شرط أساسي: حجم فوق المتوسط
                            if vol_avg > 0 and vol < vol_avg * 1.5:
                                continue

                            entry    = price
                            tp       = round(entry * 1.12, 2)   # هدف +12%
                            sl       = round(entry * 0.95, 2)
                            now_time = now_est().strftime("%I:%M %p")

                            # ترجمة العنوان
                            try:
                                translator = GoogleTranslator(source='en', target='ar')
                                title_ar   = translator.translate(title[:200])
                            except:
                                title_ar = title

                            msg  = f"📋 *إشارة ملف SEC 8-K*\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"📊 *{symbol}*\n"
                            msg += f"🔑 الكلمة المفتاحية: `{matched_kw}`\n"
                            msg += f"📝 {title_ar}\n"
                            msg += f"🔗 [رابط الملف]({link})\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"⏰ وقت الدخول: *{now_time}*\n"
                            msg += f"💰 سعر الدخول: *${entry:.2f}*\n"
                            msg += f"🎯 الهدف: *${tp:.2f}* (+12%)\n"
                            msg += f"🛑 وقف الخسارة: *${sl:.2f}* (-5%)\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"🔥 الحجم: {int(vol):,} ({vol/max(vol_avg,1):.1f}x)\n"
                            msg += f"💡 مصدر الخبر: SEC EDGAR (رسمي 100%)\n"
                            msg += f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"

                            send_telegram(msg)

                            with state_lock:
                                state.setdefault("seen_signals", {})[signal_key] = time.time()
                                save_state()

                            logger.info(f"📋 EDGAR signal: {symbol} — {matched_kw}")
                            time.sleep(1)

                        except Exception as sym_err:
                            logger.warning(f"EDGAR symbol error {symbol}: {sym_err}")

                except Exception as entry_err:
                    logger.warning(f"EDGAR entry error: {entry_err}")

        except Exception as e:
            logger.error(f"EDGAR scanner error: {e}")

        time.sleep(300)  # كل 5 دقائق


# ================= PRE-MARKET GAPPER SCANNER =================

def premarket_gapper_scanner():
    """
    كل يوم قبل الافتتاح (4 AM – 9:25 AM EST):
    يفحص الأسهم التي فتحت بفجوة كبيرة في ما قبل السوق.
    فجوة +5% أو أكثر = زخم قوي يستمر بعد الافتتاح غالباً.
    """
    MIN_GAP_PCT  = 5.0    # فجوة 5% على الأقل
    MIN_VOL      = 100000 # حجم قبل السوق لا يقل عن 100 ألف
    MAX_PRICE    = 20.0   # أسهم رخيصة تتحرك أسرع

    while True:
        try:
            now  = now_est()
            hour = now.hour
            wday = now.weekday()

            # يشتغل فقط أيام الأسبوع في فترة ما قبل السوق (4 AM – 9:25 AM)
            if wday >= 5 or not (4 <= hour < 9 or (hour == 9 and now.minute <= 25)):
                time.sleep(120)
                continue

            today_str = now.strftime("%Y-%m-%d")

            with state_lock:
                tickers = list(state.get("tickers", []))

            if not tickers:
                time.sleep(120)
                continue

            gappers = []

            # نفحص أول 500 سهم بس (Pre-Market بطيء)
            for symbol in tickers[:500]:
                try:
                    df = safe_download(symbol, period="2d", interval="5m")
                    if df.empty or len(df) < 5:
                        continue

                    df.columns = [c.lower() for c in df.columns]

                    # آخر سعر إغلاق أمس
                    yesterday_close = df['close'].iloc[-1]

                    # أول سعر اليوم (قبل السوق)
                    today_candles = df[df.index.date == now.date()]
                    if today_candles.empty:
                        continue

                    premarket_price = today_candles['close'].iloc[-1]
                    premarket_vol   = today_candles['volume'].sum()
                    price           = premarket_price

                    if price > MAX_PRICE:
                        continue
                    if premarket_vol < MIN_VOL:
                        continue

                    gap_pct = (premarket_price - yesterday_close) / yesterday_close * 100
                    if gap_pct < MIN_GAP_PCT:
                        continue

                    gappers.append({
                        "symbol":          symbol,
                        "gap_pct":         gap_pct,
                        "price":           premarket_price,
                        "prev_close":      yesterday_close,
                        "premarket_vol":   premarket_vol,
                    })

                except:
                    continue
                time.sleep(0.3)

            # فرز: الأعلى فجوة أولاً
            gappers.sort(key=lambda x: x['gap_pct'], reverse=True)

            for g in gappers[:5]:  # أقوى 5 أسهم فقط
                symbol = g['symbol']

                # تجنب التكرار لنفس اليوم
                gap_key = f"gap_{symbol}_{today_str}"
                with state_lock:
                    if gap_key in state.get("seen_gappers", {}):
                        continue
                    state.setdefault("seen_gappers", {})[gap_key] = time.time()

                entry    = g['price']
                tp       = round(entry * 1.10, 2)
                sl       = round(entry * 0.95, 2)
                open_time = "9:30 AM"  # وقت الافتتاح الرسمي

                msg  = f"🌅 *Pre-Market Gapper*\n"
                msg += f"━━━━━━━━━━━━━━━━\n"
                msg += f"📊 *{symbol}*\n"
                msg += f"📈 الفجوة: *+{g['gap_pct']:.1f}%* قبل السوق\n"
                msg += f"━━━━━━━━━━━━━━━━\n"
                msg += f"⏰ الدخول المقترح عند: *{open_time} EST*\n"
                msg += f"💰 سعر الدخول: *~${entry:.2f}*\n"
                msg += f"🎯 الهدف: *${tp:.2f}* (+10%)\n"
                msg += f"🛑 وقف الخسارة: *${sl:.2f}* (-5%)\n"
                msg += f"━━━━━━━━━━━━━━━━\n"
                msg += f"📌 إغلاق أمس: ${g['prev_close']:.2f}\n"
                msg += f"🔥 حجم ما قبل السوق: {int(g['premarket_vol']):,}\n"
                msg += f"💡 استراتيجية: ادخل عند أول شمعة خضراء بعد 9:30\n"
                msg += f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"

                send_telegram(msg)
                save_state()
                logger.info(f"🌅 Gapper: {symbol} gap={g['gap_pct']:.1f}%")
                time.sleep(2)

        except Exception as e:
            logger.error(f"Gapper scanner error: {e}")

        time.sleep(600)  # كل 10 دقائق


# ================= MAIN =================
if __name__ == "__main__":
    load_state()

    threading.Thread(target=background_scanner,       daemon=True).start()
    threading.Thread(target=background_monitor,       daemon=True).start()
    threading.Thread(target=rss_news_scanner,         daemon=True).start()
    threading.Thread(target=post_halt_entry_monitor,  daemon=True).start()
    threading.Thread(target=short_squeeze_scanner,    daemon=True).start()
    threading.Thread(target=edgar_8k_scanner,         daemon=True).start()
    threading.Thread(target=premarket_gapper_scanner, daemon=True).start()

    if bot:
        threading.Thread(target=run_telegram_bot, daemon=True).start()
    
    logger.info("=" * 50)
    logger.info("✅ Bot started successfully!")
    logger.info(f"📊 Universe: {len(state.get('tickers', []))} stocks")
    logger.info("=" * 50)
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)

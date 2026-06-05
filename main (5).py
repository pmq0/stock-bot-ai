import os
import yfinance as yf
import time
import json
import logging
import threading
import io
import urllib.request
import xml.etree.ElementTree as ET
import re
import feedparser
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
from openai import OpenAI

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger()

# ================= CONSTANTS & CONFIGURATION =================
EASTERN_TZ = pytz.timezone("US/Eastern")
SAUDI_TZ   = pytz.timezone("Asia/Riyadh")  # توقيت السعودية UTC+3
CACHE_TTL = {
    "1m": 15,
    "5m": 60,
    "15m": 180,
    "1d": 900,
}
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
AUTHORIZED_CHAT_ID = None
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5
DAILY_LOSS_LIMIT = 150.0
STATE_FILE = "state_penny_hunter.json"

# Strategy Parameters
BASE_TP_PCT = 1.06
HIGH_RVOL_TP_PCT = 1.09
EXTREME_RVOL_TP_PCT = 1.12
SL_PCT = 0.97
TRAIL_TO_BREAKEVEN_TRIGGER = 1.04
TRAIL_TO_LOCK_PROFIT_TRIGGER = 1.10
TRAIL_LOCK_PROFIT_SL_PCT = 1.05
NEAR_TARGET_ALERT_RATIO = 0.80
MIDDAY_MIN_SCORE = 70
DAILY_REPORT_HOUR = 16
DAILY_REPORT_MINUTE = 15

# === Penny Hunter Specific - EXPLOSIVE RUNNERS SETTINGS ===
PENNY_MIN_PRICE = 0.3
PENNY_MAX_PRICE = 10.0
PENNY_MIN_GAIN = 25.0
PENNY_MIN_RVOL = 4.0

# Scanner General Settings
MIN_PRICE = 0.2
MAX_PRICE = 30.0
MIN_VOLUME = 10000
MAX_TICKERS_TO_SCAN = 1200
CHUNK_SIZE = 100
MIN_RVOL_BY_PHASE = {"PRE": 2.0, "REGULAR": 1.5, "AFTER": 2.5}
TELEGRAM_DELAY = 1.0
TRADE_MONITOR_INTERVAL = 30
SCAN_INTERVAL_SEC = 900  # 15 دقيقة
BREAK_BETWEEN_CHUNKS = 5
DEEP_ANALYSIS_WORKERS = 4
SIGNAL_COOLDOWN = 3600
MAX_DAILY_SIGNALS = 60        # الحد الأقصى للرسائل يومياً (لتجنب الإزعاج)

# Fast Momentum Scanner Settings
MOMENTUM_SCAN_INTERVAL = 120
MOMENTUM_PRICE_MIN = 0.5
MOMENTUM_PRICE_MAX = 30.0
MOMENTUM_VOL_MIN = 100000
MOMENTUM_GAIN_PCT = 10.0

# Real-Time Support & Resistance Scanner Settings
SR_SCAN_INTERVAL_SEC = 60          # فحص لحظي كل دقيقة تقريباً
SR_MAX_TICKERS = 60               # نركز على أكثر الأسهم نشاطاً لتخفيف الضغط
SR_PRICE_MIN = 0.3
SR_PRICE_MAX = 30.0
SR_MIN_LAST_VOLUME = 50000         # حجم آخر شمعة 1m/5m كحد أدنى للسهم النشط
SR_MIN_RVOL = 1.8                  # حجم نسبي للتأكد أن الحركة حقيقية
SR_PROXIMITY_PCT = 1.0             # تنبيه عند الاقتراب من الدعم/المقاومة ضمن 1%
SR_BREAKOUT_BUFFER_PCT = 0.35      # هامش اختراق فوق المقاومة لتقليل الإشارات الكاذبة
SR_LEVEL_COOLDOWN = 900            # 15 دقيقة بين تنبيهات نفس السهم
SR_MIN_TOUCHES = 2                 # أقل عدد لمسات لاعتماد المستوى

# ================= LOCKS & CACHE =================
_cache = {}
_cache_lock = threading.RLock()
state_lock = threading.RLock()

# ================= OPENAI INIT =================
try:
    client = OpenAI()
    OPENAI_AVAILABLE = True
    logger.info("OpenAI client initialized successfully")
except Exception as e:
    logger.warning(f"OpenAI not available (no API key): {e}")
    client = None
    OPENAI_AVAILABLE = False

# ================= REMOVED: unreliable volatility swing scanner and unused broker integration =================
# تم حذف الماسح القديم غير الموثوق والتكامل غير المستخدم مع الوسيط.

def fast_momentum_scanner():
    """دالة سريعة لصيد الأسهم التي تحقق قفزات كبيرة في فترة قصيرة"""
    # تم حذف ماسح التذبذب القديم غير الموثوق.
    
    phase = get_market_phase()
    if phase != "REGULAR" or now_est().hour >= 11:
        return
        
    with state_lock:
        tickers = list(state.get("tickers", []))
        
    for symbol in tickers[:200]:
        try:
            df = cached_download(symbol, period="1d", interval="5m")
            if df.empty or len(df) < 5:
                continue
                
            price_now = df['close'].iloc[-1]
            price_open = df['open'].iloc[0]
            
            if not (MOMENTUM_PRICE_MIN < price_now < MOMENTUM_PRICE_MAX):
                continue
                
            gain_pct = (price_now - price_open) / price_open * 100
            if gain_pct < MOMENTUM_GAIN_PCT:
                continue
                
            now = time.time()
            last_momentum = state["seen_signals"].get(f"mom_{symbol}", 0)
            if now - last_momentum > 3600:
                msg = f"⚡ *MOMENTUM ALERT: {symbol}* ⚡\n💰 Price: ${price_now:.2f}\n📈 Gain: +{gain_pct:.1f}% from open!\n🎯 Consider an entry!"
                send_telegram(msg)
                with state_lock:
                    state["seen_signals"][f"mom_{symbol}"] = now
                    save_state()
                break
        except:
            continue
        time.sleep(0.5)

# ================= MARKET PHASE SETTINGS =================
PHASE_SETTINGS = {
    "PRE":     {"min_score": 55, "size_multiplier": 0.5, "vol_surge_mult": 2.0, "description": "🟡 Pre-Market"},
    "REGULAR": {"min_score": 60, "size_multiplier": 0.8, "vol_surge_mult": 1.5, "description": "🟢 Regular Hours"},
    "AFTER":   {"min_score": 65, "size_multiplier": 0.3, "vol_surge_mult": 2.5, "description": "🔵 After-Hours"},
    "CLOSED":  {"min_score": 999, "size_multiplier": 0,   "vol_surge_mult": 0,   "description": "⚫ Market Closed"}
}

app = Flask(__name__)
bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

@app.route("/health", methods=["GET"])
def healthcheck():
    return jsonify({"status": "ok", "bot_enabled": bool(bot), "has_chat_id": bool(CHAT_ID)})

# ================= STATE MANAGEMENT =================
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
    "seen_news": {},
    "pending_halts": {},
    "seen_edgar": {},
    "seen_gappers": {},
    "short_interest": {},
    "seen_gaps": {},
    "seen_sectors": {},
    "seen_catalyst": {},
    "elite_candidates": [],
    "daily_reports": {},
    "last_daily_report_sent": None,
    "seen_support_resistance": {}
}

def now_est():
    return datetime.now(EASTERN_TZ)

def now_saudi():
    return datetime.now(SAUDI_TZ)

def saudi_time_str():
    return now_saudi().strftime("%I:%M %p")

def get_trade_levels(price, rvol=1.0):
    tp_pct = BASE_TP_PCT
    if rvol > 5:
        tp_pct = EXTREME_RVOL_TP_PCT
    elif rvol > 3:
        tp_pct = HIGH_RVOL_TP_PCT
    return price * tp_pct, price * SL_PCT, tp_pct

def ensure_state_schema():
    with state_lock:
        state.setdefault("open_trades", {})
        state.setdefault("performance", {"wins": 0, "losses": 0, "total_pnl": 0.0})
        state.setdefault("seen_signals", {})
        state.setdefault("daily_reports", {})
        state.setdefault("last_daily_report_sent", None)
        state.setdefault("elite_candidates", [])
        state.setdefault("seen_support_resistance", {})

def save_state():
    with state_lock:
        try:
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
            ensure_state_schema()
            logger.info("State loaded successfully")
        except Exception as e:
            logger.error(f"Load state error: {e}")

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

# عداد الرسائل اليومي
_daily_signal_count = 0
_daily_signal_date  = ""
_daily_signal_lock  = threading.Lock()

def send_telegram(message, photo=None):
    global _last_telegram_time, _daily_signal_count, _daily_signal_date
    # ── حد يومي: لا تُرسل أكثر من MAX_DAILY_SIGNALS رسالة يومياً ──
    with _daily_signal_lock:
        today_str = now_est().strftime("%Y-%m-%d")
        if _daily_signal_date != today_str:
            _daily_signal_date  = today_str
            _daily_signal_count = 0
        # لا تحسب رسائل التقارير اليومية والأوامر ضمن الحد
        is_signal = any(k in message for k in [
            "إشارة", "فرصة", "دخول", "حركة", "Pre-Market",
            "آخر ساعتين", "Momentum", "Short Squeeze", "Form 4"
        ])
        if is_signal:
            if _daily_signal_count >= MAX_DAILY_SIGNALS:
                logger.info(f"Daily signal limit reached ({MAX_DAILY_SIGNALS}), skipping.")
                return
            _daily_signal_count += 1
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
def get_stop_price(symbol):
    """جلب سعر السهم وقت الإيقاف"""
    try:
        df = cached_download(symbol, period="1d", interval="5m")
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
    """مراقبة إيقافات التداول وإرسال تنبيه (باستخدام API الرسمي)"""
    try:
        # الرابط الصحيح لـ API Nasdaq (JSON-RPC)
        url = "https://www.nasdaqtrader.com/RPCHandler.axd"
        
        headers = {
            "Accept-Language": "en-US,en;q=0.8",
            "Connection": "keep-alive",
            "Referer": "https://www.nasdaqtrader.com/trader.aspx?id=TradeHalts",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        # البيانات الصحيحة لطلب JSON-RPC
        post_data = {
            "id": 1,
            "method": "BL_TradeHalt.GetTradeHalts",
            "params": "[]",
            "version": "1.1"
        }
        
        response = requests.post(url, data=json.dumps(post_data), headers=headers, timeout=15)
        
        if response.status_code != 200:
            logger.warning(f"Failed to fetch halts API, status code: {response.status_code}")
            return
        
        data = response.json()
        result_html = data.get('result')
        
        if not result_html:
            logger.warning("No result data in halts API response")
            return
        
        # استخدام BeautifulSoup لتحليل جدول HTML الناتج
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(result_html, 'html.parser')
        table = soup.find('table')
        
        if not table:
            logger.warning("No table found in halts data")
            return
        
        rows = table.find_all('tr')
        if len(rows) < 2:
            return
        
        new_halts = []
        current_date = datetime.now(EASTERN_TZ).strftime('%m/%d/%Y')
        
        for row in rows[1:]:
            cols = row.find_all('td')
            if len(cols) < 5:
                continue
            
            halt_date = cols[0].get_text(strip=True)
            if halt_date != current_date:
                continue
            
            halt_time = cols[1].get_text(strip=True)
            symbol = cols[2].get_text(strip=True)
            name = cols[3].get_text(strip=True)
            market = cols[4].get_text(strip=True)
            reason = cols[5].get_text(strip=True) if len(cols) > 5 else ''
            pause_price = cols[6].get_text(strip=True) if len(cols) > 6 else ''
            resume_date = cols[7].get_text(strip=True) if len(cols) > 7 else ''
            resume_trade_time = cols[9].get_text(strip=True) if len(cols) > 9 else ''
            
            if not symbol:
                continue
            
            with state_lock:
                halt_key = f"{symbol}_{halt_time}"
                if halt_key not in state.get("halted_stocks", {}):
                    new_halts.append({
                        'symbol': symbol,
                        'name': name,
                        'halt_time': halt_time,
                        'market': market,
                        'reason': reason.upper(),
                        'pause_price': pause_price,
                        'resume_date': resume_date,
                        'resume_trade_time': resume_trade_time,
                    })
                    if "halted_stocks" not in state:
                        state["halted_stocks"] = {}
                    state["halted_stocks"][halt_key] = {
                        'time': halt_time,
                        'date': halt_date
                    }
        
        save_state()
        
        # أكواد الإيقاف الكاملة حسب NASDAQ
        HALT_CODES = {
            "T1": ("🔴", "إيقاف - أخبار قيد الانتظار (News Pending)"),
            "T2": ("🟡", "إيقاف - أخبار صدرت (News Released)"),
            "T5": ("🟠", "إيقاف - توقف تداول سهم واحد (Single Stock Pause)"),
            "T6": ("🔴", "إيقاف - نشاط سوق غير اعتيادي (Extraordinary Activity)"),
            "T8": ("🟠", "إيقاف - صندوق ETF"),
            "T12": ("🟡", "إيقاف - طلب معلومات إضافية من NASDAQ"),
            "H4": ("🔴", "إيقاف - عدم امتثال (Non-compliance)"),
            "H9": ("🔴", "إيقاف - ملفات غير محدّثة (Not Current)"),
            "H10": ("🔴", "إيقاف - تعليق تداول من SEC"),
            "H11": ("🔴", "إيقاف - مخاوف تنظيمية (Regulatory Concern)"),
            "O1": ("🟠", "إيقاف تشغيلي (Operations Halt)"),
            "IPO1": ("🔵", "IPO - لم يبدأ التداول بعد"),
            "M1": ("🟡", "إجراء شركة (Corporate Action)"),
            "M2": ("⚪", "اقتباس غير متاح (Quotation Not Available)"),
            "LUDP": ("🔴", "توقف تداول - تذبذب (Volatility Pause)"),
            "LUDS": ("🔴", "توقف تداول - Straddle Condition"),
            "MWC1": ("🚨", "توقف السوق كله - المستوى 1 (Circuit Breaker L1)"),
            "MWC2": ("🚨", "توقف السوق كله - المستوى 2 (Circuit Breaker L2)"),
            "MWC3": ("🚨", "توقف السوق كله - المستوى 3 (Circuit Breaker L3)"),
            "MWC0": ("🚨", "توقف Circuit Breaker - ترحيل من يوم سابق"),
            "M": ("🟠", "توقف تذبذب - سهم مدرج (Volatility Pause Listed)"),
            "D": ("⚫", "حذف السهم من NASDAQ/CQS")
        }
        
        for halt in new_halts:
            try:
                price_info = get_stop_price(halt['symbol'])
                if price_info:
                    current_price = price_info['price']
                    change_pct = price_info['change_pct']
                    direction = "🟢 صعود ⬆️" if change_pct > 0 else "🔴 نزول ⬇️" if change_pct < 0 else "⚪ ثابت"
                else:
                    current_price = None
                    change_pct = 0
                    direction = "⚪ غير معروف"
                
                with state_lock:
                    if "halt_counter" not in state:
                        state["halt_counter"] = {}
                    state["halt_counter"][halt['symbol']] = state["halt_counter"].get(halt['symbol'], 0) + 1
                    halt_count = state["halt_counter"][halt['symbol']]
                
                emoji, reason_text = HALT_CODES.get(halt['reason'], ("⚠️", f"كود {halt['reason']}"))
                
                msg = f"{emoji} *تنبيه: إيقاف تداول*\n"
                msg += f"━━━━━━━━━━━━━━━━\n"
                msg += f"📊 *{halt['symbol']}*"
                if halt['name']:
                    msg += f" — {halt['name']}"
                msg += f"\n"
                msg += f"🏛️ السوق: {halt['market']}\n"
                msg += f"⏰ وقت الإيقاف: *{halt['halt_time']} EST*  |  *{saudi_time_str()} 🇸🇦*\n"
                msg += f"📋 السبب: {reason_text}\n"
                
                if halt['pause_price'] and halt['pause_price'] not in ('', 'N/A', '0', '0.0'):
                    msg += f"💲 سعر عتبة الإيقاف: ${halt['pause_price']}\n"
                
                if current_price is not None:
                    msg += f"💰 السعر الحالي: *${current_price:.2f}*\n"
                    msg += f"📈 التغير: {change_pct:+.2f}%  {direction}\n"
                
                if halt['resume_trade_time']:
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"🔄 *معلومات الاستئناف:*\n"
                    if halt['resume_date']:
                        msg += f"📅 تاريخ الاستئناف: {halt['resume_date']}\n"
                    if halt['resume_trade_time']:
                        msg += f"▶️ وقت استئناف التداول: {halt['resume_trade_time']}\n"
                
                msg += f"━━━━━━━━━━━━━━━━\n"
                msg += f"🔁 عدد إيقافات اليوم: {halt_count} مرة\n"
                msg += f"⚠️ لا تتداول حتى يُرفع الإيقاف"
                
                send_telegram(msg)
                
                BULLISH_HALT_CODES = {"T1", "T2", "LUDP", "T5", "T6", "M1"}
                if halt['reason'] in BULLISH_HALT_CODES:
                    with state_lock:
                        state.setdefault("pending_halts", {})[halt['symbol']] = {
                            "reason": halt['reason'],
                            "timestamp": time.time(),
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
    
    tickers = set()
    
    # ===== قائمة جاهزة من GitHub (موثوقة ومجربة) =====
    sources = [
        "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_tickers.txt",
        "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_tickers.txt",
        "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_tickers.txt",
    ]
    
    for url in sources:
        try:
            resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            if resp.status_code == 200:
                for line in resp.text.split('\n'):
                    sym = line.strip().upper()
                    if sym and sym.isalpha() and 2 <= len(sym) <= 5:
                        tickers.add(sym)
                logger.info(f"✅ Loaded from {url.split('/')[-1]}")
        except Exception as e:
            logger.warning(f"Failed {url}: {e}")
    
    if len(tickers) > 1000:
        final_list = sorted(list(tickers))[:MAX_TICKERS_TO_SCAN]
        with state_lock:
            state["tickers"] = final_list
            save_state()
        logger.info(f"✅ Universe updated: {len(final_list)} stocks")
        return final_list
    
    # Backup نهائي: قائمة كبيرة تشمل أشهر الأسهم
    backup = [
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD", "NFLX",
        "INTC", "PLTR", "SOFI", "NIO", "GME", "AMC", "RIOT", "MARA", "COIN",
        "MSTR", "ROKU", "PYPL", "UBER", "SNAP", "ZM", "BA", "DIS", "JPM", "WMT",
        "KO", "PEP", "MCD", "NKE", "SBUX", "T", "VZ", "SPCE", "RBLX", "U",
        "DASH", "ABNB", "CRWD", "PANW", "SNOW", "DOCU", "TEAM", "NET", "HOOD", "DKNG"
    ]
    with state_lock:
        state["tickers"] = backup
        save_state()
    logger.info(f"⚠️ Using backup universe: {len(backup)} stocks")
    return backup
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
def calculate_atr(df, period=14):
    """حساب Average True Range - يقيس تقلب السهم"""
    try:
        df = df.copy()
        df['high_low'] = df['high'] - df['low']
        df['high_close'] = abs(df['high'] - df['close'].shift(1))
        df['low_close'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
        atr = df['tr'].rolling(window=period).mean().iloc[-1]
        return round(float(atr), 2)
    except:
        return 0.0

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

    # انفجار: الحجم الحالي أكثر من 1.5x المتوسط (تم خفضه لضمان التقاط الأسهم ذات السيولة المنخفضة مثل MGRT)
    spike = last_vol > avg_vol * 1.5

    # زخم قوي: السعر الحالي أعلى من أعلى سعر في آخر 3 شمعات (أكثر سرعة)
    momentum = df["close"].iloc[-1] >= df["high"].iloc[-4:-1].max()

    score = 0
    if spike:
        score += 50
    if momentum:
        score += 50
    
    # بونص إضافي للانفجارات العنيفة جداً (مثل ONFO)
    if last_vol > avg_vol * 5:
        score += 20

    return score >= 50, score


def detect_silent_accumulation(df):
    """
    🕵️ يكتشف إذا كان هناك حجم عالٍ يدخل السهم دون أن يتحرك سعره (تجميع صامت).
    حجم أكبر بـ 3 أضعاف المتوسط مع حركة سعر أقل من 2%.
    """
    try:
        if len(df) < 20:
            return False, 0
        avg_vol = df['volume'].rolling(20).mean().iloc[-1]
        current_vol = df['volume'].iloc[-1]
        # حركة السعر في آخر 3 شمعات
        price_change = abs((df['close'].iloc[-1] - df['close'].iloc[-3]) / (df['close'].iloc[-3] + 1e-9))

        if avg_vol > 0 and current_vol > avg_vol * 3 and price_change < 0.02:
            return True, round(current_vol / avg_vol, 2)
        return False, 0
    except:
        return False, 0


def detect_whale_accumulation(df):
    """
    🐋 كاشف السيولة المخفية (Whale Accumulation).
    يبحث عن زيادة تدريجية في الحجم (آخر 5 شمعات) مع ثبات سعري شديد.
    هذا يدل على دخول هادئ قبل الانفجار.
    """
    try:
        if len(df) < 10:
            return False
        
        last_5 = df.tail(5)
        # هل الحجم في ازدياد تدريجي؟
        vol_increasing = last_5['volume'].iloc[0] < last_5['volume'].iloc[-1]
        # هل السعر ثابت جداً (تذبذب أقل من 1%)؟
        price_range = (last_5['high'].max() - last_5['low'].min()) / last_5['close'].iloc[-1]
        price_tight = price_range < 0.01
        
        # حجم التداول التراكمي في آخر 5 شمعات أكبر من المتوسط
        avg_vol = df['volume'].rolling(20).mean().iloc[-1]
        high_vol_activity = last_5['volume'].mean() > avg_vol * 1.5
        
        return vol_increasing and price_tight and high_vol_activity
    except:
        return False


def detect_bb_squeeze(df):
    """
    🌀 كاشف ضغط الانفجار (Bollinger Band Squeeze).
    عندما يضيق الباند بشكل تاريخي، فهذا يعني أن الانفجار قادم.
    """
    try:
        if len(df) < 30:
            return False
        
        # عرض الباند الحالي مقارنة بمتوسط آخر 30 شمعة
        current_bandwidth = df['bandwidth'].iloc[-1]
        avg_bandwidth = df['bandwidth'].rolling(30).mean().iloc[-1]
        
        # ضيق بنسبة 30% أقل من المتوسط
        is_squeezed = current_bandwidth < avg_bandwidth * 0.7
        
        # ميل RSI للصعود رغم ضيق السعر (Divergence بسيط)
        rsi_bullish = df['rsi'].iloc[-1] > df['rsi'].iloc[-3]
        
        return is_squeezed and rsi_bullish
    except:
        return False


def detect_momentum_start(symbol):
    """
    🚀 يفحص شموع الدقيقة الواحدة (1-minute). 
    إذا وجد 3 شمعات خضراء متتالية بحجم متزايد، فهذه بداية اندفاع (Raw Momentum).
    """
    try:
        df_1min = cached_download(symbol, period="1d", interval="1m")
        if df_1min.empty or len(df_1min) < 3:
            return False
        
        df_1min.columns = [c.lower() for c in df_1min.columns]
        last_3 = df_1min.tail(3)
        
        # هل الشمعات خضراء؟ (نسمح بشمعة واحدة متعادلة إذا كان الزخم قوياً)
        green_count = (last_3['close'] >= last_3['open']).sum()
        # الزخم: السعر في آخر شمعة أعلى بكثير من الأولى
        price_jump = (last_3['close'].iloc[-1] - last_3['open'].iloc[0]) / (last_3['open'].iloc[0] + 1e-9) > 0.02
        # الحجم في آخر شمعة دقيقة أكبر من المتوسط
        high_vol = last_3['volume'].iloc[-1] > last_3['volume'].mean()
        
        return green_count >= 2 and (price_jump or high_vol)
    except:
        return False


def detect_psychological_break(price):
    """
    🧠 يكتشف كسر المقاومة النفسية (أرقام صحيحة مثل $0.3, $0.5, $1, $2, $5, $10, $20, $50).
    الاختراق فوق هذه الأرقام غالباً ما يتبعه انفجار سعري.
    """
    psych_levels = [0.3, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500]
    for level in psych_levels:
        # إذا كان السعر الحالي فوق المستوى بـ 1% كحد أقصى (اختراق طازج)
        if level <= price <= level * 1.01:
            return True, level
    return False, 0


def detect_sudden_gap(df):
    """
    ⚡ يكتشف فجوة سعرية مفاجئة (Sudden Gap) في أي اتجاه (صعود أو نزول).
    إذا قفز السعر أكثر من 1.5% في شمعة واحدة مع حجم عالي.
    """
    try:
        if len(df) < 2:
            return False, 0, 0
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        # فجوة صاعدة (gap up)
        gap_up_pct = (last['low'] - prev['high']) / (prev['high'] + 1e-9) * 100
        
        # فجوة هابطة (gap down)
        gap_down_pct = (prev['low'] - last['high']) / (prev['low'] + 1e-9) * 100
        
        avg_vol = df['volume'].rolling(20).mean().iloc[-1]
        vol_spike = last['volume'] > avg_vol * 2 if avg_vol > 0 else False
        
        if gap_up_pct > 1.5 and vol_spike:
            return True, round(gap_up_pct, 2), "UP"
        
        if gap_down_pct > 1.5 and vol_spike:
            return True, round(gap_down_pct, 2), "DOWN"
        
        return False, 0, 0
    except:
        return False, 0, 0


def detect_v_shape_recovery(df):
    """
    🏹 يكتشف ارتداد V-Shape السريع.
    شمعة حمراء قوية يليها ارتداد أخضر سريع بابتلاع سعري وحجم متزايد.
    """
    try:
        if len(df) < 5:
            return False
        
        last = df.iloc[-1]   # الشمعة الحالية (يجب أن تكون خضراء)
        prev = df.iloc[-2]   # الشمعة السابقة (يجب أن تكون حمراء قوية)
        
        # شرط الشمعة السابقة: حمراء وبانخفاض > 1%
        is_red_drop = prev['close'] < prev['open'] and (prev['open'] - prev['close']) / prev['open'] > 0.01
        
        # شرط الشمعة الحالية: خضراء وتبتلع 50% على الأقل من الشمعة الحمراء
        is_green_recovery = last['close'] > last['open'] and last['close'] > (prev['open'] + prev['close']) / 2
        
        # شرط الحجم: حجم الارتداد أكبر من حجم الهبوط
        volume_ok = last['volume'] > prev['volume'] * 1.1
        
        return is_red_drop and is_green_recovery and volume_ok
    except:
        return False

def scan_vshape_opportunities():
    """خيط منفصل للبحث عن فرص الارتداد السريع V-Shape لضمان عدم فواتها"""
    while True:
        try:
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(600)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))
            
            if not tickers:
                time.sleep(60)
                continue
                
            # فحص سريع لأهم 300 سهم نشط
            for symbol in tickers[:300]:
                try:
                    df = cached_download(symbol, period="1d", interval="5m")
                    if df.empty or len(df) < 5:
                        continue
                    
                    if detect_v_shape_recovery(df):
                        # إذا وجد ارتداد، نمرره لـ process_symbol للتحليل العميق والفتح
                        process_symbol(symbol)
                        
                except Exception as e:
                    continue
                time.sleep(0.5)
                
        except Exception as e:
            logger.error(f"V-Shape scanner error: {e}")
        time.sleep(300)


def rvol_spike_scanner():
    """يراقب الأسهم اللي RVOL حقق قفزة استثنائية (أكثر من 5x)"""
    while True:
        try:
            phase = get_market_phase()
            if phase not in ("REGULAR", "PRE"):
                time.sleep(120)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))
            
            if not tickers:
                time.sleep(60)
                continue
            
            for symbol in tickers[:300]:
                try:
                    df = cached_download(symbol, period="5d", interval="15m")
                    if df.empty or len(df) < 20:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    rvol = calculate_rvol(df, phase=get_market_phase())
                    
                    if rvol >= 8:
                        last = df.iloc[-1]
                        if last['close'] < 0.5:
                            continue
                            
                        signal_key = f"rvol_{symbol}"
                        with state_lock:
                            last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                        if time.time() - last_sent > 3600:  # مرة كل ساعة

                            # ✅ فلتر البيانات الخاطئة
                            vol = last['volume']
                            if vol is None or vol == 0 or vol < 1000:
                                continue  # حجم صفر = بيانات فاضية، تجاهل
                            if len(df) >= 5 and abs(last['close'] - df['close'].iloc[-5]) < 0.0001:
                                continue  # سعر ثابت = بيانات مجمدة، تجاهل

                            # ✅ لازم يتحرك فعلاً (+1% على الأقل)
                            if len(df) >= 5:
                                price_change = (last['close'] - df['close'].iloc[-5]) / df['close'].iloc[-5] * 100
                                if abs(price_change) < 1.0:
                                    continue  # تحرك أقل من 1% = ليس spike حقيقي
                            else:
                                price_change = 0.0

                            msg = f"🔥 *RVOL SPIKE: {symbol}*\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"📊 RVOL: *{rvol:.1f}x* (نشاط استثنائي!)\n"
                            msg += f"💰 السعر: ${last['close']:.2f}\n"
                            msg += f"📈 التغير: {price_change:+.1f}% (5 شمعات)\n"
                            msg += f"🔥 الحجم: {int(vol):,}\n"
                            msg += f"💡 ضع هذا السهم على قائمة المراقبة فوراً!"
                            send_telegram(msg)
                            with state_lock:
                                state.setdefault("seen_signals", {})[signal_key] = time.time()
                                save_state()
                except:
                    continue
                time.sleep(0.5)
        except:
            continue
        time.sleep(300)

# ================================================================
# 🚀 POWER RUNNER SCANNER — مصمم لصيد الأسهم المتفجرة مثل HTCO
# ================================================================
def power_runner_scanner():
    """
    يصطاد الأسهم اللي تتحرك حركة قوية اليوم بدون أي API خارجي.
    
    شروط الإشارة (لازم كلها):
    1) السعر بين 0.5$ و 30$ (نطاق المضاربين القوي).
    2) صعد +12% أو أكثر من إغلاق أمس.
    3) حجم اليوم تجاوز 2.5x من متوسط حجم 5 أيام.
    4) آخر 3 شموع 5دقيقة كلها صاعدة (Higher Closes).
    5) السعر فوق EMA9 على فريم 5 دقائق.
    6) RSI على 5m بين 55 و 80 (قوي لكن ليس مرهق).
    7) ما زال تحت قمة اليوم بنسبة معقولة (في مساحة للحركة).
    """
    while True:
        try:
            phase = get_market_phase()
            if phase not in ("REGULAR", "PRE"):
                time.sleep(180)
                continue

            with state_lock:
                tickers = list(state.get("tickers", []))[:400]

            if not tickers:
                time.sleep(60)
                continue

            today_key = now_est().date().isoformat()

            for symbol in tickers:
                try:
                    # تجاهل الإشارات المكررة لنفس السهم في نفس اليوم
                    signal_key = f"runner_{symbol}_{today_key}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue

                    # 1) داتا يومية لحساب الربح من إغلاق أمس
                    df_d = cached_download(symbol, period="10d", interval="1d")
                    if df_d.empty or len(df_d) < 6:
                        continue
                    df_d.columns = [c.lower() for c in df_d.columns]

                    prev_close = float(df_d['close'].iloc[-2])
                    today_open = float(df_d['open'].iloc[-1])
                    today_close = float(df_d['close'].iloc[-1])
                    today_high = float(df_d['high'].iloc[-1])
                    today_vol = float(df_d['volume'].iloc[-1])

                    if prev_close <= 0:
                        continue
                    price = today_close

                    # شرط 1: نطاق السعر
                    if price < 0.5 or price > 30:
                        continue

                    # شرط 2: قفزة +12% أو أكثر
                    gain_pct = (price - prev_close) / prev_close * 100
                    if gain_pct < 12:
                        continue

                    # شرط 3: حجم اليوم > 3x من متوسط 5 أيام
                    avg_vol_5d = float(df_d['volume'].iloc[-6:-1].mean())
                    if avg_vol_5d <= 0:
                        continue
                    vol_ratio = today_vol / avg_vol_5d
                    if vol_ratio < 3.0:
                        continue

                    # 2) داتا 5 دقائق للتحقق من قوة الزخم اللحظي
                    df_5m = cached_download(symbol, period="2d", interval="5m")
                    if df_5m.empty or len(df_5m) < 15:
                        continue
                    df_5m.columns = [c.lower() for c in df_5m.columns]

                    closes = df_5m['close'].astype(float)
                    last3 = closes.iloc[-3:].values
                    # شرط 4: آخر 3 شموع صاعدة
                    if not (last3[0] < last3[1] < last3[2]):
                        continue

                    # شرط 5: السعر فوق EMA9 على 5m
                    ema9 = closes.ewm(span=9, adjust=False).mean()
                    if float(closes.iloc[-1]) < float(ema9.iloc[-1]):
                        continue

                    # شرط 6: RSI 5m > 40 (أكثر مرونة للبيني ستوكس)
                    delta = closes.diff()
                    gain = delta.where(delta > 0, 0).rolling(14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                    rs = gain / loss.replace(0, 1e-10)
                    rsi5 = float((100 - (100 / (1 + rs))).iloc[-1])
                    if rsi5 < 40:
                        continue

                    # شرط 7: تحت قمة اليوم بنسبة معقولة (في مساحة)
                    distance_from_high = (today_high - price) / today_high * 100
                    # نقبل إذا قريب من القمة (اختراق جديد) أو ابتعد قليلاً (سحب صحي)
                    if distance_from_high > 8:
                        continue

                    # حساب الـ Stop Loss والـ Targets
                    sl = round(price * 0.93, 2)  # وقف -7%
                    t1 = round(price * 1.08, 2)  # هدف +8%
                    t2 = round(price * 1.15, 2)  # هدف +15%
                    t3 = round(price * 1.25, 2)  # هدف +25%

                    msg = (
                        f"🚀 *POWER RUNNER: {symbol}*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💎 *سهم متفجر بقوة اليوم!*\n\n"
                        f"💰 السعر: *${price:.2f}*\n"
                        f"📈 الربح اليوم: *+{gain_pct:.1f}%*\n"
                        f"🔥 الحجم: *{vol_ratio:.1f}x* من المعدل\n"
                        f"📊 RSI 5m: {rsi5:.0f} (قوي)\n"
                        f"⛰️ قمة اليوم: ${today_high:.2f}\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🎯 الأهداف:\n"
                        f"  • T1: ${t1} (+8%)\n"
                        f"  • T2: ${t2} (+15%)\n"
                        f"  • T3: ${t3} (+25%)\n"
                        f"🛡️ وقف الخسارة: ${sl} (-7%)\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💡 *أسلوب HTCO* — زخم + حجم + اتجاه صاعد\n"
                        f"⚠️ الأسهم المنخفضة السعر تتحرك بسرعة، استخدم وقف الخسارة"
                    )
                    send_telegram(msg)

                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()

                    logger.info(f"🚀 POWER RUNNER signal: {symbol} +{gain_pct:.1f}% vol={vol_ratio:.1f}x")
                except Exception as e:
                    logger.debug(f"Power runner check failed for {symbol}: {e}")
                    continue
                time.sleep(0.4)

        except Exception as e:
            logger.error(f"Power Runner scanner error: {e}")
        time.sleep(240)  # إعادة فحص كل 4 دقائق


def elite_3_summary_loop():
    """خيط يرسل ملخص لأفضل 3 صفقات كل 30 دقيقة"""
    while True:
        try:
            # ننتظر 30 دقيقة بين كل ملخص
            time.sleep(1800)
            
            phase = get_market_phase()
            if phase == "CLOSED":
                continue
                
            with state_lock:
                candidates = list(state.get("elite_candidates", []))
                # تصفير القائمة بعد جلبها للملخص القادم
                state["elite_candidates"] = []
                save_state()
                
            if not candidates:
                continue
                
            # ترتيب حسب السكور (الأعلى أولاً)
            candidates.sort(key=lambda x: x['score'], reverse=True)
            
            # اختيار أفضل 3
            top_3 = candidates[:3]
            
            msg = "🏆 *TOP 3 ELITE PICKS* 🏆\n"
            msg += "_أفضل الفرص المكتشفة حالياً بناءً على تحليل 11 ماسحاً_\n"
            msg += "━━━━━━━━━━━━━━━━\n\n"
            
            medals = ["🥇", "🥈", "🥉"]
            for i, c in enumerate(top_3):
                medal = medals[i]
                msg += f"{medal} *المركز {'الأول' if i==0 else ('الثاني' if i==1 else 'الثالث')}: ${c['symbol']}*\n"
                msg += f"🔥 *السكور:* *{c['score']}/100*\n"
                msg += f"📊 *الحالة:* {c['reason']}\n"
                msg += f"💰 *السعر:* ${c['price']:.2f}\n"
                msg += f"🛡️ *الترند:* {'📈 Above SMA50' if c['bullish'] else '📉 Below SMA50'}\n\n"
                
            msg += "━━━━━━━━━━━━━━━━\n"
            if top_3:
                msg += f"💡 *نصيحة المحلل الآلي:* سهم *${top_3[0]['symbol']}* يمتلك أعلى احتمالية نجاح بناءً على دمج المؤشرات.\n"
            msg += "🚀 *تداول بحذر وإدارة مخاطر صارمة!*"
            
            send_telegram(msg)
            
        except Exception as e:
            logger.error(f"Elite 3 loop error: {e}")
            time.sleep(60)


# ================= RVOL & LOW FLOAT =================
def _classify_session(ts):
    """يميز جلسة كل شمعة (PRE / REGULAR / AFTER) باستخدام توقيت نيويورك."""
    try:
        if ts.tzinfo is None:
            ts = EASTERN_TZ.localize(ts)
        else:
            ts = ts.astimezone(EASTERN_TZ)
        minutes = ts.hour * 60 + ts.minute
        if 4 * 60 <= minutes < 9 * 60 + 30:
            return "PRE"
        if 9 * 60 + 30 <= minutes < 16 * 60:
            return "REGULAR"
        if 16 * 60 <= minutes < 20 * 60:
            return "AFTER"
        return "CLOSED"
    except Exception:
        return "REGULAR"


def calculate_rvol(df, phase=None):
    """
    حساب الحجم النسبي (Relative Volume) — نسخة محسّنة تدعم Pre/After Market.

    - في الجلسة العادية: مقارنة الشمعة الحالية بمتوسط آخر 20 شمعة عادية.
    - في Pre-Market: مقارنة مجموع حجم Pre اليوم بمتوسط مجموع حجم Pre لآخر 5 أيام.
    - في After-Hours: نفس المنطق لكن لشموع After.
    - دائماً نتجاهل أحجام الصفر / NaN حتى لا تطلع 0.0x.
    """
    try:
        if df is None or df.empty:
            return 1.0

        if phase is None:
            try:
                phase = _classify_session(df.index[-1])
            except Exception:
                phase = "REGULAR"

        # نضمن وجود معلومة الجلسة لكل صف
        if not isinstance(df.index, pd.DatetimeIndex):
            return _simple_rvol(df)

        try:
            sessions = df.index.map(_classify_session)
        except Exception:
            return _simple_rvol(df)

        df_local = df.copy()
        df_local['_session'] = sessions
        df_local['_date'] = df_local.index.date

        # نتعامل فقط مع الأحجام > 0
        df_local = df_local[df_local['volume'] > 0]
        if df_local.empty:
            return 1.0

        if phase == "PRE":
            grouped = (
                df_local[df_local['_session'] == "PRE"]
                .groupby('_date')['volume'].sum()
            )
            if grouped.empty:
                return _simple_rvol(df)
            today_vol = float(grouped.iloc[-1])
            history = grouped.iloc[:-1].tail(5)
            avg = float(history.mean()) if len(history) else 0.0
            if avg <= 0 or pd.isna(avg):
                return 1.0
            return round(today_vol / avg, 2)

        if phase == "AFTER":
            grouped = (
                df_local[df_local['_session'] == "AFTER"]
                .groupby('_date')['volume'].sum()
            )
            if grouped.empty:
                return _simple_rvol(df)
            today_vol = float(grouped.iloc[-1])
            history = grouped.iloc[:-1].tail(5)
            avg = float(history.mean()) if len(history) else 0.0
            if avg <= 0 or pd.isna(avg):
                return 1.0
            return round(today_vol / avg, 2)

        # REGULAR — نقارن آخر شمعة عادية بمتوسط آخر 20 شمعة عادية (بدون أصفار)
        regular = df_local[df_local['_session'] == "REGULAR"]
        if regular.empty:
            return _simple_rvol(df)
        current_vol = float(regular['volume'].iloc[-1])
        prev = regular['volume'].iloc[-21:-1] if len(regular) > 1 else regular['volume']
        avg_vol = float(prev.mean()) if len(prev) else 0.0
        if avg_vol <= 0 or pd.isna(avg_vol):
            return 1.0
        return round(current_vol / avg_vol, 2)

    except Exception as e:
        logger.debug(f"calculate_rvol error: {e}")
        return 1.0


def _simple_rvol(df):
    """احتياطي بسيط: متوسط متحرك مع تجاهل الأصفار."""
    try:
        vols = df['volume'][df['volume'] > 0]
        if vols.empty:
            return 1.0
        current = float(vols.iloc[-1])
        avg = float(vols.tail(20).mean())
        if avg <= 0 or pd.isna(avg):
            return 1.0
        return round(current / avg, 2)
    except Exception:
        return 1.0
        
def calculate_mfi(df, period=14):
    """
    Money Flow Index - يقيس تدفق المال داخل السهم
    MFI > 80 = Overbought (ممكن يصحح)
    MFI < 20 = Oversold (فرصة شراء)
    MFI فوق 50 مع صعود = تأكيد قوي
    """
    try:
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        money_flow = typical_price * df['volume']
        
        positive_flow = []
        negative_flow = []
        
        for i in range(1, len(typical_price)):
            if typical_price.iloc[i] > typical_price.iloc[i-1]:
                positive_flow.append(money_flow.iloc[i])
                negative_flow.append(0)
            else:
                positive_flow.append(0)
                negative_flow.append(money_flow.iloc[i])
        
        positive_flow = pd.Series(positive_flow).rolling(window=period).sum()
        negative_flow = pd.Series(negative_flow).rolling(window=period).sum()
        
        money_ratio = positive_flow / (negative_flow + 1e-9)
        mfi = 100 - (100 / (1 + money_ratio))
        
        return mfi.iloc[-1] if len(mfi) > 0 else 50
    except:
        return 50


def vwap_deviation(df):
    """يحسب نسبة انحراف السعر عن VWAP"""
    try:
        last = df.iloc[-1]
        deviation = (last['close'] - last['vwap']) / last['vwap'] * 100
        return round(deviation, 2)
    except:
        return 0


def get_time_score():
    """يضيف نقاط إضافية حسب الوقت من اليوم"""
    now = now_est()
    hour = now.hour
    minute = now.minute
    
    # 9:30 - 10:00 (فجوات الصباح)
    if hour == 9 and minute >= 30:
        return 15
    # 10:30 - 11:00 (ارتدادات منتصف الجلسة)
    elif hour == 10 and minute >= 30:
        return 10
    # 14:30 - 15:00 (تحضير للإغلاق)
    elif hour == 14 and minute >= 30:
        return 10
    # 15:30 - 16:00 (Power Hour)
    elif hour == 15 and minute >= 30:
        return 20
    return 0


def detect_early_bottom(df):
    """يكتشف إذا كان السهم في قاع محتمل (شمعة دوجي + RSI منخفض + حجم منخفض)"""
    try:
        if len(df) < 10:
            return False, 0
        
        last = df.iloc[-1]
        
        # شمعة دوجي (فرق بين الافتتاح والإغلاق أقل من 10% من المدى)
        body = abs(last['close'] - last['open'])
        range_ = last['high'] - last['low']
        is_doji = body < (range_ * 0.1) if range_ > 0 else False
        
        # RSI منخفض (أقل من 35)
        rsi_low = last['rsi'] < 35
        
        # حجم منخفض مقارنة بالمتوسط
        avg_vol = df['volume'].rolling(20).mean().iloc[-1]
        low_volume = last['volume'] < avg_vol * 0.7
        
        if is_doji and rsi_low and low_volume:
            return True, last['rsi']
        return False, 0
    except:
        return False, 0


def correlation_spinoff(leader_symbol):
    """إذا انفجر سهم قائد، افحص الأسهم التابعة تلقائياً"""
    CORRELATIONS = {
        "NVDA": ["AMD", "INTC", "MRVL", "AVGO"],
        "TSLA": ["RIVN", "LCID", "NIO"],
        "GME": ["AMC", "BB", "KOSS"],
        "MSTR": ["COIN", "RIOT", "MARA"],
    }
    
    followers = CORRELATIONS.get(leader_symbol, [])
    for follower in followers:
        # نشغل الفحص في خيط منفصل عشان ما نعطل العملية الحالية
        threading.Thread(target=process_symbol, args=(follower,), daemon=True).start()


def is_low_float(price, volume):
    """
    تقدير إذا كان السهم Low Float بدون API خارجي.
    المنطق: Low Float غالباً = سعر أقل من $10 + حجم يومي أقل من 2M.
    هذه الأسهم تنفجر أسرع لأن الأسهم المتداولة قليلة.
    """
    try:
        return price < 10.0 and volume < 2_000_000
    except:
        return False


# ================= DAILY TREND & PDH =================
def get_daily_metrics(symbol):
    """
    جلب بيانات اليومي (60 يوم) لحساب SMA 50 وقمة أمس (PDH).
    SMA 50: يحدد إذا كان السهم في ترند صاعد عام.
    PDH: يحدد إذا كان السهم اخترق أعلى سعر وصل له أمس.
    """
    try:
        df_daily = cached_download(symbol, period="60d", interval="1d")
        if df_daily.empty or len(df_daily) < 2:
            return None
        
        df_daily.columns = [c.lower() for c in df_daily.columns]
        last_close = df_daily['close'].iloc[-1]
        
        # حساب SMA 50
        sma50 = df_daily['close'].rolling(50).mean().iloc[-1] if len(df_daily) >= 50 else None
        is_bullish = last_close > sma50 if sma50 else True
        
        # حساب PDH (أعلى سعر أمس)
        yesterday_high = df_daily['high'].iloc[-2]
        is_breakout_pdh = last_close > yesterday_high
        
        return {
            "is_bullish": is_bullish,
            "is_breakout_pdh": is_breakout_pdh,
            "sma50": sma50,
            "pdh": yesterday_high
        }
    except Exception as e:
        logger.error(f"Daily metrics error for {symbol}: {e}")
        return None


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
def open_trade(symbol, price, score, df, is_accumulating=False, is_pre_breakout=False, phase="REGULAR", settings=None, rvol=1.0, low_float=False, is_bullish=True, is_pdh=True, is_silent_acc=False, is_raw_mom=False, psych_level=0, gap_pct=0, is_v_shape=False, is_whale_acc=False, is_bb_squeeze=False, effective_min_score=None):
    tp, sl, tp_pct = get_trade_levels(price, rvol)
    target_move_pct = (tp_pct - 1) * 100
    effective_min_score = effective_min_score or settings["min_score"]

    with state_lock:
        if state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT or len(state["open_trades"]) >= MAX_OPEN_TRADES:
            return
        size = max(1, int(calculate_position_size(price) * settings["size_multiplier"]))
        
        state["open_trades"][symbol] = {
            "entry": price,
            "tp": tp,
            "sl": sl,
            "size": size,
            "time": time.time(),
            "score": score,
            "phase": phase,
            "rvol": rvol,
            "near_target_alert_sent": False,
            "breakeven_moved": False,
            "profit_lock_moved": False,
            "effective_min_score": effective_min_score
        }
        save_state()
    update_daily_report_on_open(symbol, price, score)

    chart = generate_chart(symbol, df, price, tp, sl, is_accumulating, is_pre_breakout)
    phase_emoji = "🟡" if phase == "PRE" else ("🔵" if phase == "AFTER" else "🟢")

    # بناء البادجات والرموز
    rvol_bar = "🔥" if rvol >= 5 else ("⚡" if rvol >= 3 else ("📊" if rvol >= 2 else "•"))
    lf_tag   = " | 🎯 LOW FLOAT" if low_float else ""
    pdh_tag  = "✅ PDH Breakout" if is_pdh else "⏳ Below PDH"
    trend_tag = "📈 Above SMA50" if is_bullish else "📉 Below SMA50"

    # ميزات Pump & Dump والتحليل النفسي الجديدة
    pump_tag = ""
    if is_silent_acc: pump_tag += "\n🕵️ *مرحلة تجميع مشبوهة (Silent)*"
    if is_raw_mom:   pump_tag += "\n🚀 *بداية اندفاع (Raw Momentum)*"
    if psych_level > 0: pump_tag += f"\n🧠 *كسر حاجز نفسي (${psych_level})*"
    if gap_pct > 0: pump_tag += f"\n⚡ *فجوة سعرية مفاجئة (+{gap_pct}%)*"
    if is_v_shape:   pump_tag += "\n🏹 *ارتداد V-Shape سريع*"
    if is_whale_acc: pump_tag += "\n🐋 *تنبيه استباقي: سيولة مخفية (Whale Flow)*"
    if is_bb_squeeze: pump_tag += "\n🌀 *تنبيه استباقي: ضغط انفجار (BB Squeeze)*"

    caption  = (
        f"{phase_emoji} *SIGNAL: {symbol}* ({phase}){lf_tag}\n"
        f"💰 Entry: ${price:.2f}\n"
        f"🎯 TP: ${tp:.2f} (+{target_move_pct:.0f}%)\n"
        f"🛑 SL: ${sl:.2f}\n"
        f"📦 Size: {size}\n"
        f"📊 Score: {score}/{effective_min_score}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{rvol_bar} RVOL: {rvol:.1f}x\n"
        f"🛡️ Trend: {trend_tag}\n"
        f"🚀 Breakout: {pdh_tag}"
        f"{pump_tag}"
    )
    send_telegram(caption, photo=chart)

    # 🔗 Correlation Spinoff
    correlation_spinoff(symbol)

    # 🏆 Add to Elite 3 Candidates
    reason_list = []
    if rvol >= 3: reason_list.append(f"انفجار RVOL ({rvol:.1f}x)")
    if is_silent_acc: reason_list.append("تجميع صامت")
    if is_raw_mom: reason_list.append("بداية اندفاع")
    if psych_level > 0: reason_list.append(f"كسر حاجز نفسي ${psych_level}")
    if is_v_shape: reason_list.append("ارتداد V-Shape")
    if is_pdh: reason_list.append("اختراق قمة أمس")
    if is_whale_acc: reason_list.append("سيولة مخفية")
    if is_bb_squeeze: reason_list.append("ضغط انفجار")
    if not is_bullish: reason_list.append("تحت SMA50 مع خصم جودة")
    if 0.03 * price <= calculate_atr(df) <= 0.20 * price: reason_list.append("تقلب صحي (ATR)")

    reason_str = " + ".join(reason_list) if reason_list else "اختراق فني قوي"

    with state_lock:
        state.setdefault("elite_candidates", []).append({
            "symbol": symbol,
            "score": score,
            "price": price,
            "reason": reason_str,
            "bullish": is_bullish,
            "time": time.time()
        })
        save_state()

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
    update_daily_report_on_close(symbol, pnl, pnl_pct)
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

        df = cached_download(symbol, period="5d", interval="15m")
        if df.empty:
            return
        df.columns = [c.lower() for c in df.columns]
        df = compute_indicators(df)
        
        # تم تعطيل فلتر SMA200 للسماح بصيد أسهم البيني المتفجرة حتى لو كانت تحت المتوسط الطويل
        # df['sma200'] = df['close'].rolling(200).mean()
        # if df['close'].iloc[-1] < df['sma200'].iloc[-1]:
        #     return  # تحت المتوسط الطويل → تجاهل
            
        is_accumulating, acc_score = detect_accumulation(df)
        is_pre_breakout = detect_pre_breakout(df)

        # 🕵️ فحص التجميع الصامت (Silent Accumulation)
        is_silent_acc, acc_ratio = detect_silent_accumulation(df)
        
        # 🔥 فحص الانفجار - إذا ما في انفجار ولا تجميع صامت نوقف هنا
        explosion, explosion_score = detect_explosion(df)
        
        # ✅ تم إزالة البوابة القاسية - نسمح بفحص كل الأسهم
        # if not explosion and not is_silent_acc:
        #     return

        last = df.iloc[-1]

        # 🔴 فلتر الأسعار - فقط أسهم تحت $30 (لصيد البيني ستوكس)
        price = last['close']
        if price > 30.0:
            logger.info(f"Skipping {symbol}: Price ${price:.2f} > $30 limit")
            return

        # للأسهم الرخيصة تحت $5، نشترط RVOL أعلى وزخم
        if price < 5.0:
            rvol_check = calculate_rvol(df, phase=phase)
            if rvol_check < 4.0:
                logger.info(f"Skipping {symbol}: Penny stock with weak RVOL ({rvol_check:.1f}x)")
                return

        # 🚀 فحص بداية الزخم (Raw Momentum) على فريم الدقيقة
        is_raw_momentum = detect_momentum_start(symbol) if explosion else False
        
        # 🧠 فحص الحواجز النفسية والفجوات المفاجئة
        is_psych_break, psych_level = detect_psychological_break(last['close'])
        is_sudden_gap, gap_val, gap_direction = detect_sudden_gap(df)
        
        # 🏹 فحص ارتداد V-Shape
        is_v_shape = detect_v_shape_recovery(df)
        
        # 📉 فحص القاع المبكر (Early Bottom)
        is_bottom, bottom_rsi = detect_early_bottom(df)

        # 🐋 فحص السيولة المخفية وضغط الانفجار (Pre-Pump)
        is_whale_acc = detect_whale_accumulation(df)
        is_bb_squeeze = detect_bb_squeeze(df)
        
        # تجاهل الفجوات الهابطة (DOWN) في عملية التقييم
        if is_sudden_gap and gap_direction == "DOWN":
            is_sudden_gap = False
            gap_val = 0
        daily = get_daily_metrics(symbol)
        is_bullish = daily['is_bullish'] if daily else True
        is_breakout_pdh = daily['is_breakout_pdh'] if daily else True
        
        # 🛡️ جودة الاتجاه: بدل الرفض الكامل تحت SMA50 نخصم من السكور فقط
        trend_penalty = 20 if not is_bullish else 0

        # خفضنا شرط الانفجار للسماح بالدخول المبكر
        vol_surge = last['volume'] > df['vol_ma'].iloc[-1] * settings["vol_surge_mult"]
        # شرط السعر أصبح أكثر مرونة (اختراق قمة آخر 10 شموع بدلاً من 20)
        price_break = last['close'] > df['high'].iloc[-10:-1].max()
        
        score = explosion_score
        if vol_surge: score += 25
        if price_break: score += 25
        if 40 < last['rsi'] < 70: score += 10
        if last['ema9'] > last['ema21']: score += 10

        # 📊 RVOL - الحجم النسبي (يدعم Pre/After Market)
        rvol = calculate_rvol(df, phase=phase)

        # ✅ ضمان قيمة منطقية حتى لا يطلع 0.0x في الإشارات
        if rvol is None or pd.isna(rvol) or rvol <= 0:
            rvol = 1.0

        if rvol >= 5:    score += 15
        elif rvol >= 3:  score += 10
        elif rvol >= 2:  score += 5
        
        # 🎯 Low Float
        low_float = is_low_float(last['close'], last['volume'])
        if low_float:
            score += 10
            
        # 🕵️ بونص التجميع الصامت
        if is_silent_acc:
            score += 20
            
        # 🚀 بونص بداية الزخم
        if is_raw_momentum:
            score += 15

        # 📊 ATR
        atr = calculate_atr(df)
        price = last['close']
        if atr > price * 0.3:
            logger.info(f"Skipping {symbol}: Extreme volatility (ATR {atr} > 30% of price {price})")
            return
        if 0.03 * price <= atr <= 0.20 * price:
            score += 5
            
        # 🧠 بونص الحاجز النفسي
        if is_psych_break:
            score += 10
            
        # ⚡ بونص الفجوة المفاجئة
        if is_sudden_gap:
            score += 15
            
        # 🏹 بونص ارتداد V-Shape
        if is_v_shape:
            score += 20
            
        # 📉 بونص القاع المبكر
        if is_bottom:
            score += 15
            
        # 🐋 بونص السيولة المخفية وضغط الانفجار
        if is_whale_acc:
            score += 25
        if is_bb_squeeze:
            score += 15
            
        # 💰 بونص التدفق المالي (MFI)
        mfi = calculate_mfi(df)
        if mfi > 60:
            score += 10
        elif mfi < 30:
            score -= 5
            
        # 📏 بونص الانحراف عن VWAP
        vwap_dev = vwap_deviation(df)
        if vwap_dev > 2:
            score += 5
            
        # ⏰ بونص توقيت الجلسة
        score += get_time_score()

        score -= trend_penalty
        effective_min_score = get_effective_min_score(phase, settings)

        # ✅ حماية الجودة: لا ترسل إشارات بـ RVOL ضعيف جداً (دلالة على بيانات غير ناضجة)
        min_rvol_required = MIN_RVOL_BY_PHASE.get(phase, 1.0)
        if rvol < min_rvol_required:
            logger.info(f"Skip {symbol}: RVOL {rvol:.2f}x < {min_rvol_required}x ({phase})")
            return

        if score >= effective_min_score:
            now = time.time()
            last_seen = state["seen_signals"].get(symbol, 0)
            if now - last_seen > SIGNAL_COOLDOWN:
                open_trade(symbol, last['close'], score, df, is_accumulating, is_pre_breakout, phase, settings,
                           rvol=rvol, low_float=low_float, is_bullish=is_bullish, is_pdh=is_breakout_pdh,
                           is_silent_acc=is_silent_acc, is_raw_mom=is_raw_momentum,
                           psych_level=psych_level, gap_pct=gap_val, is_v_shape=is_v_shape,
                           is_whale_acc=is_whale_acc, is_bb_squeeze=is_bb_squeeze,
                           effective_min_score=effective_min_score)
                with state_lock:
                    state["seen_signals"][symbol] = now
                    save_state()
        elif score >= 72:
            now = time.time()
            signal_key = f"watch_{symbol}_{int(now/3600)}"
            hour_key   = f"watch_count_{int(now/3600)}"
            with state_lock:
                # حد أقصى 3 تنبيهات مراقبة كل ساعة
                watch_count = state.get("seen_signals", {}).get(hour_key, 0)
                if signal_key not in state.get("seen_signals", {}) and watch_count < 3:

                    # بناء أسباب التنبيه بشكل واضح
                    reasons = []
                    if explosion:          reasons.append(f"⚡ انفجار حجم (score={explosion_score})")
                    if vol_surge:          reasons.append(f"📊 حجم أعلى من المتوسط ×{settings['vol_surge_mult']}")
                    if price_break:        reasons.append("🔺 اختراق قمة آخر 10 شموع")
                    if is_silent_acc:      reasons.append(f"🕵️ تجميع صامت (×{acc_ratio:.1f} متوسط)")
                    if is_raw_momentum:    reasons.append("🚀 بداية زخم (1m)")
                    if is_psych_break:     reasons.append(f"🧠 كسر حاجز نفسي ${psych_level}")
                    if is_sudden_gap:      reasons.append(f"⚡ فجوة صاعدة +{gap_val:.1f}%")
                    if is_v_shape:         reasons.append("🏹 ارتداد V-Shape")
                    if is_whale_acc:       reasons.append("🐋 سيولة مخفية (Whale)")
                    if is_bb_squeeze:      reasons.append("🌀 ضغط بولنجر (Squeeze)")
                    if is_bottom:          reasons.append(f"📉 قاع مبكر (RSI={bottom_rsi:.0f})")
                    if rvol >= 3:          reasons.append(f"🔥 RVOL {rvol:.1f}x")
                    if low_float:          reasons.append("🎯 Low Float")
                    if is_breakout_pdh:    reasons.append("📈 اختراق قمة أمس (PDH)")
                    if not is_bullish:     reasons.append("⚠️ تحت SMA50 (ترند ضعيف)")

                    # لو ما في أسباب واضحة — لا ترسل
                    if not reasons:
                        pass
                    else:
                        reasons_text = "\n".join(f"  {r}" for r in reasons)
                        price_val    = last['close']
                        atr_val      = calculate_atr(df)
                        tp_est       = round(price_val * BASE_TP_PCT, 2)
                        sl_est       = round(price_val * SL_PCT, 2)

                        msg  = f"👀 *مراقبة عالية الجودة: {symbol}*\n"
                        msg += f"━━━━━━━━━━━━━━━━\n"
                        msg += f"💰 السعر: *${price_val:.2f}*\n"
                        msg += f"📊 السكور: *{score}/100* (الحد: {effective_min_score})\n"
                        msg += f"📈 RVOL: {rvol:.1f}x | ATR: ${atr_val:.2f}\n"
                        msg += f"━━━━━━━━━━━━━━━━\n"
                        msg += f"🔍 *أسباب التنبيه:*\n{reasons_text}\n"
                        msg += f"━━━━━━━━━━━━━━━━\n"
                        msg += f"🎯 هدف مقدّر: ${tp_est} | وقف: ${sl_est}\n"
                        msg += f"⚠️ *لم يصل للحد — راقبه ولا تدخل إلا بتأكيد*"

                        send_telegram(msg)
                        state.setdefault("seen_signals", {})[signal_key] = now
                        state["seen_signals"][hour_key] = watch_count + 1
                        save_state()
    except Exception as e:
        pass

def update_trades():
    with state_lock:
        symbols = list(state["open_trades"].keys())

    for symbol in symbols:
        try:
            df = cached_download(symbol, period='1d', interval='5m')
            if df.empty:
                continue
            price = df['close'].iloc[-1]
            near_target_message = None
            trailing_message = None
            close_reason = None

            with state_lock:
                trade = state["open_trades"].get(symbol)
                if not trade:
                    continue

                entry = trade["entry"]
                current_sl = trade["sl"]
                tp = trade["tp"]
                near_target_price = entry + ((tp - entry) * NEAR_TARGET_ALERT_RATIO)

                # تنبيه قرب الهدف مرة واحدة فقط
                if price >= near_target_price and not trade.get("near_target_alert_sent", False):
                    trade["near_target_alert_sent"] = True
                    save_state()
                    near_target_message = (
                        f"⚠️ *قارب الهدف: {symbol}*\n"
                        f"💰 السعر الحالي: ${price:.2f}\n"
                        f"🎯 الهدف: ${tp:.2f}\n"
                        f"📏 وصل إلى {NEAR_TARGET_ALERT_RATIO * 100:.0f}% من المسافة للهدف"
                    )

                # إذا ارتفع السهم 4%، حرك الوقف إلى نقطة الدخول (Breakeven)
                if price >= entry * TRAIL_TO_BREAKEVEN_TRIGGER and current_sl < entry:
                    trade["sl"] = entry
                    trade["breakeven_moved"] = True
                    current_sl = trade["sl"]
                    save_state()
                    trailing_message = f"🔄 *Trailing Stop Updated: {symbol}*\n💰 SL moved to ${entry:.2f} (breakeven)"

                # إذا ارتفع 10%، حرك الوقف إلى +5%
                elif price >= entry * TRAIL_TO_LOCK_PROFIT_TRIGGER and current_sl < entry * TRAIL_LOCK_PROFIT_SL_PCT:
                    trade["sl"] = entry * TRAIL_LOCK_PROFIT_SL_PCT
                    trade["profit_lock_moved"] = True
                    save_state()
                    trailing_message = (
                        f"🔄 *Trailing Stop Updated: {symbol}*\n"
                        f"💰 SL moved to ${entry * TRAIL_LOCK_PROFIT_SL_PCT:.2f} (+5%)"
                    )

                if price >= trade["tp"]:
                    close_reason = "TAKE PROFIT"
                elif price <= trade["sl"]:
                    close_reason = "STOP LOSS"

            if near_target_message:
                send_telegram(near_target_message)
            if trailing_message:
                send_telegram(trailing_message)
            if close_reason:
                close_trade(symbol, price, close_reason)
                    
        except Exception as e:
            logger.warning(f"Trade update failed for {symbol}: {e}")

# ================= SCANNER ENGINE =================
def background_scanner():
    time.sleep(10)
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
                
                tickers = tickers[:MAX_TICKERS_TO_SCAN]
                
                logger.info(f"🔍 Starting scan of {len(tickers)} symbols - Phase: {phase}")
                
                for i in range(0, len(tickers), CHUNK_SIZE):
                    chunk = tickers[i:i+CHUNK_SIZE]
                    
                    filtered = []
                    with ThreadPoolExecutor(max_workers=min(len(chunk), 6)) as executor:
                        future_to_sym = {}
                        for sym in chunk:
                            future_to_sym[executor.submit(fast_filter, sym)] = sym
                            time.sleep(0.05)
                            
                        for future in as_completed(future_to_sym):
                            try:
                                if future.result(timeout=8):
                                    filtered.append(future_to_sym[future])
                            except:
                                pass
                    
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
        time.sleep(2)
    except Exception as e:
        logger.debug(f"Webhook removal skipped: {e}")
    logger.info("🤖 Starting Telegram bot polling...")
    try:
        bot.polling(non_stop=True, interval=1, timeout=20)
    except Exception as e:
        logger.error(f"Telebot polling error: {e}")

# (Final functions like fast_filter and cached_download were missing in previous read, adding them now)

def fast_filter(symbol):
    try:
        # تأخير عشوائي بين الطلبات لمنع الحظر
        time.sleep(random.uniform(1.5, 3.0))
        df = cached_download(symbol, period="5d", interval="15m")
        if df.empty or len(df) < 5:
            return False
        df.columns = [c.lower() for c in df.columns]
        last = df.iloc[-1]
        
        # فلتر أساسي سريع
        if not (MIN_PRICE <= last['close'] <= MAX_PRICE):
            return False
        if last['volume'] < MIN_VOLUME:
            return False
            
        return True
    except:
        return False

def fetch_from_yahoo(symbol, period="5d", interval="15m", timeout=10):
    try:
        # تأخير عشوائي قبل كل طلب
        time.sleep(random.uniform(1.5, 3.0))
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval, auto_adjust=True)
        if df.empty:
            return pd.DataFrame()
        df.columns = [c.lower() for c in df.columns]
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize("UTC").tz_convert(EASTERN_TZ)
        else:
            df.index = df.index.tz_convert(EASTERN_TZ)
        return df
    except Exception as e:
        logger.error(f"yfinance error {symbol}: {e}")
        return pd.DataFrame()

def cached_download(symbol, period="5d", interval="15m", timeout=10, force_refresh=False):
    cache_key = f"{symbol}_{period}_{interval}"
    if not force_refresh:
        with _cache_lock:
            if cache_key in _cache:
                cached_data, timestamp = _cache[cache_key]
                ttl = CACHE_TTL.get(interval, 60)
                if time.time() - timestamp < ttl:
                    return cached_data.copy() if not cached_data.empty else pd.DataFrame()
    df = fetch_from_yahoo(symbol, period, interval, timeout)
    with _cache_lock:
        _cache[cache_key] = (df, time.time())
    return df.copy() if not df.empty else pd.DataFrame()

# ================= REAL-TIME SUPPORT & RESISTANCE SCANNER =================
def _safe_float(value, default=0.0):
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default

def calculate_sr_levels(df, window=20):
    """حساب مستويات الدعم والمقاومة بناءً على القمم والقيعان المحلية."""
    try:
        df = df.copy()
        highs = df['high'].rolling(window=window, center=True).max()
        lows = df['low'].rolling(window=window, center=True).min()
        
        resistance_levels = []
        support_levels = []
        
        for i in range(window, len(df) - window):
            if df['high'].iloc[i] == highs.iloc[i]:
                price = _safe_float(df['high'].iloc[i])
                found = False
                for r in resistance_levels:
                    if abs(r['price'] - price) / price < 0.005:
                        r['touches'] += 1
                        found = True
                        break
                if not found:
                    resistance_levels.append({'price': price, 'touches': 1})
                    
            if df['low'].iloc[i] == lows.iloc[i]:
                price = _safe_float(df['low'].iloc[i])
                found = False
                for s in support_levels:
                    if abs(s['price'] - price) / price < 0.005:
                        s['touches'] += 1
                        found = True
                        break
                if not found:
                    support_levels.append({'price': price, 'touches': 1})
        
        resistance_levels.sort(key=lambda x: x['touches'], reverse=True)
        support_levels.sort(key=lambda x: x['touches'], reverse=True)
        
        return support_levels, resistance_levels
    except Exception as e:
        logger.error(f"SR calculation error: {e}")
        return [], []

def sr_scanner_loop():
    """ماسح الدعم والمقاومة اللحظي المتطور."""
    while True:
        try:
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(300)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))[:SR_MAX_TICKERS]
            
            for symbol in tickers:
                try:
                    df = cached_download(symbol, period="2d", interval="5m")
                    if df.empty or len(df) < 30:
                        continue
                    
                    price = _safe_float(df['close'].iloc[-1])
                    if not (SR_PRICE_MIN <= price <= SR_PRICE_MAX):
                        continue
                        
                    supports, resistances = calculate_sr_levels(df)
                    
                    near_support = None
                    for s in supports:
                        if s['touches'] >= SR_MIN_TOUCHES:
                            dist = (price - s['price']) / s['price'] * 100
                            if 0 <= dist <= SR_PROXIMITY_PCT:
                                near_support = s
                                break
                                
                    near_resistance = None
                    for r in resistances:
                        if r['touches'] >= SR_MIN_TOUCHES:
                            dist = (r['price'] - price) / r['price'] * 100
                            if 0 <= dist <= SR_PROXIMITY_PCT:
                                near_resistance = r
                                break
                    
                    if near_support or near_resistance:
                        now = time.time()
                        last_sent = state.get("seen_support_resistance", {}).get(symbol, 0)
                        if now - last_sent > SR_LEVEL_COOLDOWN:
                            msg = f"📍 *منطقة دعم/مقاومة مهمة: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                            msg += f"💰 السعر الحالي: ${price:.4f}\n"
                            if near_support:
                                msg += f"🟢 الدعم الأقرب: ${near_support['price']:.4f} | لمسات: {near_support['touches']} | بعد: {(price-near_support['price'])/near_support['price']*100:.2f}%\n"
                            if near_resistance:
                                msg += f"🔴 المقاومة الأقرب: ${near_resistance['price']:.4f} | لمسات: {near_resistance['touches']} | بعد: {(near_resistance['price']-price)/near_resistance['price']*100:.2f}%\n"
                            
                            rvol = calculate_rvol(df, phase=phase)
                            vwap_val = _safe_float(compute_indicators(df)['vwap'].iloc[-1])
                            msg += f"📊 RVOL آخر شمعة: {rvol:.1f}x | الفريم: 5m\n"
                            msg += f"📏 VWAP: ${vwap_val:.4f} ({'فوق' if price > vwap_val else 'تحت'} VWAP)\n"
                            msg += "━━━━━━━━━━━━━━━━\n🧭 الحالة: السعر داخل منطقة مراقبة لحظية\n"
                            msg += "⚠️ مراقبة فنية فقط وليست توصية شراء أو بيع. انتظر تأكيد الحجم والثبات فوق/تحت المستوى."
                            
                            send_telegram(msg)
                            with state_lock:
                                state.setdefault("seen_support_resistance", {})[symbol] = now
                                save_state()
                except:
                    continue
                time.sleep(0.5)
            time.sleep(SR_SCAN_INTERVAL_SEC)
        except Exception as e:
            logger.error(f"SR Scanner Loop error: {e}")
            time.sleep(60)

# ================= BOT COMMANDS =================
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    if not ensure_authorized(message): return
    help_text = (
        "🚀 *PENNY HUNTER BOT — دليل الأوامر*\n"
        "━━━━━━━━━━━━━━━━\n"
        "🔍 *أوامر الفحص:*\n"
        "• /sr SYMBOL - فحص الدعم والمقاومة اللحظي لسهم معين\n"
        "• /price SYMBOL - جلب السعر اللحظي والتحليل السريع\n\n"
        "📊 *أوامر المحفظة:*\n"
        "• /trades - عرض الصفقات المفتوحة حالياً\n"
        "• /stats - إحصائيات الأداء اليومي والPnL\n\n"
        "⚙️ *أوامر الإدارة:*\n"
        "• /reset - تصفير الحالة والبدء من جديد\n"
        "• /stop SYMBOL - إغلاق صفقة معينة يدوياً\n"
        "━━━━━━━━━━━━━━━━\n"
        "💡 البوت يعمل تلقائياً ويصطاد الفرص بناءً على 11 ماسحاً فنيًا."
    )
    bot.reply_to(message, help_text, parse_mode='Markdown')

@bot.message_handler(commands=['sr'])
def manual_sr_check(message):
    if not ensure_authorized(message): return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "⚠️ يرجى إدخال رمز السهم. مثال: `/sr AAPL`", parse_mode='Markdown')
            return
        symbol = parts[1].upper()
        df = cached_download(symbol, period="2d", interval="5m", force_refresh=True)
        if df.empty:
            bot.reply_to(message, f"❌ تعذر جلب بيانات السهم {symbol}")
            return
        
        price = _safe_float(df['close'].iloc[-1])
        supports, resistances = calculate_sr_levels(df)
        
        msg = f"🔍 *تقرير الدعم والمقاومة: {symbol}*\n━━━━━━━━━━━━━━━━\n"
        msg += f"💰 السعر الحالي: *${price:.4f}*\n\n"
        
        msg += "🟢 *مستويات الدعم (القيعان):*\n"
        for s in supports[:3]:
            msg += f"• ${s['price']:.4f} ({s['touches']} لمسات)\n"
            
        msg += "\n🔴 *مستويات المقاومة (القمم):*\n"
        for r in resistances[:3]:
            msg += f"• ${r['price']:.4f} ({r['touches']} لمسات)\n"
            
        rvol = calculate_rvol(df)
        msg += f"\n━━━━━━━━━━━━━━━━\n📊 RVOL: {rvol:.1f}x\n💡 التقرير بناءً على فريم 5 دقائق لآخر يومين."
        bot.reply_to(message, msg, parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"❌ خطأ: {str(e)}")

# ================= MAIN ENTRY =================
if __name__ == "__main__":
    load_state()
    ensure_state_schema()
    
    # تشغيل الخيوط الخلفية
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    threading.Thread(target=sr_scanner_loop, daemon=True).start()
    threading.Thread(target=power_runner_scanner, daemon=True).start()
    threading.Thread(target=elite_3_summary_loop, daemon=True).start()
    threading.Thread(target=scan_vshape_opportunities, daemon=True).start()
    threading.Thread(target=rvol_spike_scanner, daemon=True).start()
    threading.Thread(target=daily_report_scheduler, daemon=True).start()
    
    # تشغيل Flask في خيط منفصل
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False), daemon=True).start()
    
    # تشغيل بوت تلغرام في الخيط الرئيسي
    run_telegram_bot()

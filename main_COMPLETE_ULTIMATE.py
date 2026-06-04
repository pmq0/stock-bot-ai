from deep_translator import GoogleTranslator
import os
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
import matplotlib.pyplot as plt
from flask import Flask, jsonify
import telebot
from tenacity import retry, stop_after_attempt, wait_exponential
from curl_cffi import requests
import ftplib
from openai import OpenAI
from webull import webull
        from bs4 import BeautifulSoup
import requests
from datetime import datetime
from flask import Flask, request
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import yfinance as yf
from collections import deque
    import re
import re  # 🔥 أضف هذا أيضاً
from bs4 import BeautifulSoup  # 🔥 هذا السطر الجديد
from flask import Flask
        import csv
        from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from webull import webull  # إضافة مكتبة Webull
    import uuid
        import yfinance as yf
    import feedparser  # مكتبة لقراءة RSS feeds
from __future__ import annotations
from typing import Optional
from curl_cffi import requests as curl_requests
import pandas_ta as ta

matplotlib.use('Agg')

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger()

# ================= CONSTANTS & CONFIGURATION =================
EASTERN_TZ = pytz.timezone("US/Eastern")
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

# ================= WEBULL SETUP =================
def setup_webull():
    """Initializes and logs into the Webull account."""
    wb = webull()
    email = os.getenv('WEBULL_EMAIL')
    password = os.getenv('WEBULL_PASSWORD')
    if not email or not password:
        logger.error("Webull email or password not found in environment variables.")
        return None
    try:
        logger.info(f"Attempting to log in to Webull with email: {email}...")
        # استخدام الطريقة الأكثر توافقاً لتجنب خطأ التنسيق
        wb.login(email, password)
        accounts = wb.get_accounts()
        if accounts:
            logger.info(f"Webull login successful. First account ID: {accounts[0].get('accountId')}")
        return wb
    except Exception as e:
        logger.error(f"Webull login failed: {e}")
        return None

def place_order_with_webull(wb, symbol, action, quantity, order_type='MKT', price=None):
    if wb is None: return None
    try:
        logger.info(f"Webull Order: {action} {quantity} {symbol} ({order_type})")
    except Exception as e:
        logger.error(f"Failed to place order: {e}")
    return None

        logger.info(f"Webull Order: {action} {quantity} {symbol} ({order_type})")
    except Exception as e:
        logger.error(f"Failed to place order: {e}")
    return None

def fast_momentum_scanner():
    """دالة سريعة لصيد الأسهم التي تحقق قفزات كبيرة في فترة قصيرة"""
    phase = get_market_phase()
    if phase != "REGULAR" or now_est().hour >= 11:
        return
        
    with state_lock:
        tickers = list(state["tickers"])
        
    for symbol in tickers[:200]:
        try:
            df = cached_download(symbol, period="1d", interval="5m")
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
    """فلتر يمرر كل الأسهم - للاختبار فقط"""
    return True
        
# ================= MARKET PHASE SETTINGS =================
PHASE_SETTINGS = {
    "PRE":     {"min_score": 55, "size_multiplier": 0.5, "vol_surge_mult": 2.0, "description": "🟡 Pre-Market"},
    "REGULAR": {"min_score": 60, "size_multiplier": 0.8, "vol_surge_mult": 1.5, "description": "🟢 Regular Hours"},
    "AFTER":   {"min_score": 65, "size_multiplier": 0.3, "vol_surge_mult": 2.5, "description": "🔵 After-Hours"},
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
    "short_interest": {},       # بيانات Short Interest المحفوظة
    "seen_gaps": {},            # تنبيهات Gap Up اللي أُرسلت
    "seen_sectors": {},          # تقارير Sector Strength اللي أُرسلت
    "seen_catalyst": {},         # أخبار Catalyst اللي أُرسلت
    "elite_candidates": [],      # المرشحين لنظام النخبة Top 3
    "daily_reports": {},         # إحصاءات يومية للملخص التلقائي
    "last_daily_report_sent": None
}

EASTERN_TZ = pytz.timezone("US/Eastern")

def now_est():
    return datetime.now(EASTERN_TZ)

def get_trade_levels(price, rvol=1.0):
    """تحديد الهدف ووقف الخسارة بشكل ديناميكي حسب RVOL."""
    tp_pct = BASE_TP_PCT
    if rvol > 5:
        tp_pct = EXTREME_RVOL_TP_PCT
    elif rvol > 3:
        tp_pct = HIGH_RVOL_TP_PCT
    return price * tp_pct, price * SL_PCT, tp_pct

def get_effective_min_score(phase, settings):
    """تشديد الحد الأدنى للسكور في ساعات الهدوء خلال الجلسة العادية."""
    min_score = settings["min_score"]
    now = now_est()
    if phase == "REGULAR" and 11 <= now.hour < 14:
        min_score = max(min_score, MIDDAY_MIN_SCORE)
    return min_score

def get_today_key():
    return now_est().date().isoformat()

def ensure_state_schema():
    """ضمان وجود المفاتيح الجديدة حتى مع ملفات الحالة القديمة."""
    with state_lock:
        state.setdefault("open_trades", {})
        state.setdefault("performance", {"wins": 0, "losses": 0, "total_pnl": 0.0})
        state.setdefault("seen_signals", {})
        state.setdefault("daily_reports", {})
        state.setdefault("last_daily_report_sent", None)
        state.setdefault("elite_candidates", [])

def update_daily_report_on_open(symbol, entry, score):
    today = get_today_key()
    with state_lock:
        daily = state.setdefault("daily_reports", {}).setdefault(today, {
            "signals": 0,
            "closed": 0,
            "wins": 0,
            "losses": 0,
            "total_pnl": 0.0,
            "best_trade": None,
            "worst_trade": None,
            "symbols": []
        })
        daily["signals"] += 1
        daily.setdefault("symbols", []).append({
            "symbol": symbol,
            "entry": round(float(entry), 4),
            "score": int(score),
            "time": time.time()
        })
        save_state()

def update_daily_report_on_close(symbol, pnl, pnl_pct):
    today = get_today_key()
    trade_snapshot = {
        "symbol": symbol,
        "pnl": round(float(pnl), 2),
        "pnl_pct": round(float(pnl_pct), 2)
    }
    with state_lock:
        daily = state.setdefault("daily_reports", {}).setdefault(today, {
            "signals": 0,
            "closed": 0,
            "wins": 0,
            "losses": 0,
            "total_pnl": 0.0,
            "best_trade": None,
            "worst_trade": None,
            "symbols": []
        })
        daily["closed"] += 1
        daily["total_pnl"] += pnl
        if pnl > 0:
            daily["wins"] += 1
        else:
            daily["losses"] += 1

        best_trade = daily.get("best_trade")
        worst_trade = daily.get("worst_trade")
        if best_trade is None or pnl > best_trade.get("pnl", float("-inf")):
            daily["best_trade"] = trade_snapshot
        if worst_trade is None or pnl < worst_trade.get("pnl", float("inf")):
            daily["worst_trade"] = trade_snapshot
        save_state()

def build_daily_report_message(report_date=None):
    report_date = report_date or get_today_key()
    with state_lock:
        daily = state.get("daily_reports", {}).get(report_date, {})

    signals = daily.get("signals", 0)
    closed = daily.get("closed", 0)
    wins = daily.get("wins", 0)
    losses = daily.get("losses", 0)
    total_pnl = daily.get("total_pnl", 0.0)
    win_rate = (wins / closed * 100) if closed else 0.0
    best_trade = daily.get("best_trade")
    worst_trade = daily.get("worst_trade")

    best_line = "لا يوجد" if not best_trade else f"{best_trade['symbol']} | ${best_trade['pnl']:+.2f} ({best_trade['pnl_pct']:+.2f}%)"
    worst_line = "لا يوجد" if not worst_trade else f"{worst_trade['symbol']} | ${worst_trade['pnl']:+.2f} ({worst_trade['pnl_pct']:+.2f}%)"

    return (
        f"📘 *Daily Report — {report_date}*\n"
        f"📡 Signals: {signals}\n"
        f"✅ Wins: {wins}\n"
        f"❌ Losses: {losses}\n"
        f"🎯 Win Rate: {win_rate:.1f}%\n"
        f"💵 Total PnL: ${total_pnl:+.2f}\n"
        f"🏆 Best Trade: {best_line}\n"
        f"📉 Worst Trade: {worst_line}"
    )

def daily_report_scheduler():
    """يرسل تقريراً يومياً واحداً عند 4:15 PM بتوقيت نيويورك."""
    while True:
        try:
            now = now_est()
            today = now.date().isoformat()
            if now.weekday() < 5 and now.hour == DAILY_REPORT_HOUR and now.minute >= DAILY_REPORT_MINUTE:
                with state_lock:
                    last_sent = state.get("last_daily_report_sent")
                if last_sent != today:
                    send_telegram(build_daily_report_message(today))
                    with state_lock:
                        state["last_daily_report_sent"] = today
                        save_state()
            time.sleep(30)
        except Exception as e:
            logger.error(f"Daily report scheduler error: {e}")
            time.sleep(60)

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
            ensure_state_schema()
            logger.info("State loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
    else:
        ensure_state_schema()

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
                msg += f"⏰ وقت الإيقاف: *{halt['halt_time']} EST*\n"
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
    🧠 يكتشف كسر المقاومة النفسية (أرقام صحيحة مثل $0.3, $0.5, $1, $5, $10, $50).
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
        except Exception as e:
            logger.error(f"RVOL scanner error: {e}")
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
        
        # 📊 تحليل البيانات اليومية (SMA 50 & PDH)
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
    time.sleep(10)  # انتظر 10 ثواني عشان الجامعة تتحمل
    update_all_tickers()
    while True:
        # ... الباقي نفس ما هو
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
                    # رفع عدد العمال مع إضافة تأخير بسيط جداً لمنع الحظر (Stealth Mode)
                    with ThreadPoolExecutor(max_workers=min(len(chunk), 15)) as executor:
                        future_to_sym = {}
                        for sym in chunk:
                            future_to_sym[executor.submit(fast_filter, sym)] = sym
                            time.sleep(0.05) # تأخير 50 ملي ثانية بين إرسال الطلبات لتجنب الـ Rate Limit
                            
                        for future in as_completed(future_to_sym):
                            try:
                                if future.result(timeout=8):
                                    filtered.append(future_to_sym[future])
                            except:
                                pass # تجاهل الأسهم التي تتأخر في الاستجابة
                    
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
        msg = f"👋 Trading Bot v33!\n📊 {len(state['tickers'])} stocks\n🕐 {PHASE_SETTINGS[phase]['description']}\n\n/status - Performance\n/positions - Trades\n/close SYMBOL\n/scan SYMBOL\n/b SYMBOL QUANTITY - شراء يدوي (مثال: /b SMX 50)\n/s SYMBOL - بيع يدوي (مثال: /s SMX)"
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
            df = cached_download(symbol, period='1d', interval='5m')
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
            df = cached_download(symbol, period="10d", interval="15m")
            if df.empty:
                send_telegram(f"❌ No data for {symbol}")
                return
            df.columns = [c.lower() for c in df.columns]
            df = compute_indicators(df)
            last = df.iloc[-1]
            phase = get_market_phase()
            settings = PHASE_SETTINGS.get(phase, PHASE_SETTINGS["REGULAR"])
            effective_min_score = get_effective_min_score(phase, settings)
            vol_surge = last['volume'] > df['vol_ma'].iloc[-1] * 2.0
            price_break = last['close'] > df['high'].iloc[-20:-1].max()
            score = 0
            if vol_surge: score += 40
            if price_break: score += 40
            if 40 < last['rsi'] < 70: score += 10
            if last['ema9'] > last['ema21']: score += 10
            msg = f"📊 *{symbol}*\n💰 ${last['close']:.2f}\n📊 Vol: {int(last['volume']):,}\n⚡ RSI: {last['rsi']:.1f}\n🎯 Score: {score}/{effective_min_score}"
            send_telegram(msg)
        except Exception as e:
            send_telegram(f"❌ Error: {e}")

if bot:
    @bot.message_handler(commands=['b'])
    def cmd_buy(message):
        """شراء يدوي: /b SYMBOL QUANTITY — مثال: /b SMX 50"""
        if not ensure_authorized(message):
            return
        try:
            args = message.text.split()
            if len(args) != 3:
                send_telegram("⚠️ الاستخدام الصحيح:\n`/b SYMBOL QUANTITY`\nمثال: `/b SMX 50`")
                return

            symbol   = args[1].upper().strip()
            quantity = int(args[2])

            if quantity <= 0:
                send_telegram("❌ الكمية يجب أن تكون أكبر من صفر")
                return

            # فحص إذا كان السهم مفتوح مسبقاً
            with state_lock:
                if symbol in state["open_trades"]:
                    send_telegram(f"⚠️ *{symbol}* مفتوح مسبقاً!\nأغلقه أولاً بـ `/close {symbol}`")
                    return

                # فحص حد الصفقات المفتوحة
                if len(state["open_trades"]) >= MAX_OPEN_TRADES:
                    send_telegram(f"❌ وصلت الحد الأقصى للصفقات المفتوحة ({MAX_OPEN_TRADES})")
                    return

                # فحص حد الخسارة اليومية
                if state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT:
                    send_telegram(f"❌ تجاوزت حد الخسارة اليومي (${DAILY_LOSS_LIMIT})")
                    return

            send_telegram(f"🔍 جاري جلب سعر *{symbol}*...")

            # جلب السعر الحالي
            df = cached_download(symbol, period="1d", interval="5m")
            if df.empty or len(df) < 2:
                send_telegram(f"❌ لم أستطع جلب بيانات *{symbol}* — تحقق من رمز السهم")
                return

            df.columns = [c.lower() for c in df.columns]
            price = float(df['close'].iloc[-1])

            if price <= 0:
                send_telegram(f"❌ السعر غير صالح لـ *{symbol}*")
                return

            # حساب TP و SL
            phase    = get_market_phase()
            settings = PHASE_SETTINGS.get(phase, PHASE_SETTINGS["REGULAR"])
            rvol     = 1.0
            try:
                df15 = cached_download(symbol, period="5d", interval="15m")
                if not df15.empty:
                    df15.columns = [c.lower() for c in df15.columns]
                    rvol = calculate_rvol(df15, phase=phase)
            except Exception:
                pass

            tp, sl, tp_pct = get_trade_levels(price, rvol)
            target_move_pct = (tp_pct - 1) * 100

            # تسجيل الصفقة في الحالة
            with state_lock:
                state["open_trades"][symbol] = {
                    "entry":               price,
                    "tp":                  tp,
                    "sl":                  sl,
                    "size":                quantity,
                    "time":                time.time(),
                    "score":               0,           # شراء يدوي = لا سكور
                    "phase":               phase,
                    "rvol":                rvol,
                    "near_target_alert_sent": False,
                    "breakeven_moved":     False,
                    "profit_lock_moved":   False,
                    "effective_min_score": settings["min_score"],
                    "manual":              True          # علامة أن هذه صفقة يدوية
                }
                save_state()

            update_daily_report_on_open(symbol, price, 0)

            phase_emoji = "🟡" if phase == "PRE" else ("🔵" if phase == "AFTER" else "🟢")
            total_value = price * quantity

            msg  = f"{phase_emoji} *شراء يدوي: {symbol}*\n"
            msg += f"━━━━━━━━━━━━━━━━\n"
            msg += f"💰 سعر الدخول: *${price:.2f}*\n"
            msg += f"📦 الكمية:      *{quantity} سهم*\n"
            msg += f"💵 القيمة:      *${total_value:,.2f}*\n"
            msg += f"━━━━━━━━━━━━━━━━\n"
            msg += f"🎯 الهدف (TP):  *${tp:.2f}* (+{target_move_pct:.0f}%)\n"
            msg += f"🛑 وقف الخسارة: *${sl:.2f}* (-3%)\n"
            msg += f"━━━━━━━━━━━━━━━━\n"
            msg += f"✅ تم تسجيل الصفقة — البوت يراقب تلقائياً"

            send_telegram(msg)
            logger.info(f"✅ Manual buy: {symbol} x{quantity} @ ${price:.2f}")

        except ValueError:
            send_telegram("❌ الكمية يجب أن تكون رقماً صحيحاً\nمثال: `/b SMX 50`")
        except Exception as e:
            logger.error(f"/b command error: {e}")
            send_telegram(f"❌ خطأ غير متوقع: {e}")

if bot:
    @bot.message_handler(commands=['s'])
    def cmd_sell(message):
        """بيع يدوي: /s SYMBOL — مثال: /s SMX"""
        if not ensure_authorized(message):
            return
        try:
            args = message.text.split()
            if len(args) != 2:
                send_telegram("⚠️ الاستخدام الصحيح:\n`/s SYMBOL`\nمثال: `/s SMX`")
                return

            symbol = args[1].upper().strip()

            with state_lock:
                if symbol not in state["open_trades"]:
                    send_telegram(f"❌ *{symbol}* غير موجود في الصفقات المفتوحة\nتحقق من `/positions`")
                    return
                trade = state["open_trades"][symbol]

            send_telegram(f"🔍 جاري جلب سعر *{symbol}* للبيع...")

            df = cached_download(symbol, period='1d', interval='5m')
            if df.empty or len(df) < 1:
                send_telegram(f"❌ تعذر جلب السعر الحالي لـ *{symbol}*")
                return

            df.columns = [c.lower() for c in df.columns]
            exit_price = float(df['close'].iloc[-1])
            entry_price = trade["entry"]
            size        = trade["size"]
            pnl         = (exit_price - entry_price) * size
            pnl_pct     = ((exit_price - entry_price) / entry_price) * 100

            # إغلاق الصفقة عبر دالة close_trade الموجودة
            close_trade(symbol, exit_price, "Manual Sell /s")

            emoji = "🎉" if pnl >= 0 else "🛑"
            result_word = "ربح" if pnl >= 0 else "خسارة"

            msg  = f"{emoji} *بيع يدوي: {symbol}*\n"
            msg += f"━━━━━━━━━━━━━━━━\n"
            msg += f"💰 سعر الدخول:  *${entry_price:.2f}*\n"
            msg += f"💵 سعر البيع:   *${exit_price:.2f}*\n"
            msg += f"📦 الكمية:       *{size} سهم*\n"
            msg += f"━━━━━━━━━━━━━━━━\n"
            msg += f"📈 النتيجة: *{result_word}*\n"
            msg += f"💰 PnL: *${pnl:+.2f}* ({pnl_pct:+.2f}%)\n"
            msg += f"━━━━━━━━━━━━━━━━\n"
            msg += f"✅ تم إغلاق الصفقة بنجاح"

            send_telegram(msg)
            logger.info(f"✅ Manual sell: {symbol} x{size} @ ${exit_price:.2f} | PnL: ${pnl:+.2f}")

        except Exception as e:
            logger.error(f"/s command error: {e}")
            send_telegram(f"❌ خطأ غير متوقع: {e}")

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

# ================= NEWS CATALYST SCANNER =================

CATALYST_FEEDS = [
    "https://www.businesswire.com/rss/home/?rss=G1",
    "https://www.prnewswire.com/rss/news-releases-list.rss",
]

CATALYST_KEYWORDS = [
    "acquisition", "merger", "partnership", "fda approval",
    "contract", "financing", "pivot to", "breakthrough"
]

def fetch_news_catalyst():
    """
    يفحص مصادر الأخبار الكبرى (BusinessWire, PRNewswire) عن كلمات مفتاحية قوية (Catalysts).
    يرسل تنبيه فوري لو الخبر يخص سهم معين.
    """
    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(300)
                continue
            
            for feed_url in CATALYST_FEEDS:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries[:10]:
                    title = entry.get('title', '').lower()
                    link = entry.get('link', '')
                    
                    matched = [kw for kw in CATALYST_KEYWORDS if kw in title]
                    if not matched:
                        continue
                    
                    # استخراج رمز السهم من العنوان (غالباً يكون بين قوسين مثل (AAPL))
                    pattern = r'\(([A-Z]{1,5})\)'
                    symbols = re.findall(pattern, title.upper())
                    if not symbols:
                        continue
                    
                    symbol = symbols[0]
                    
                    # تجنب التكرار
                    news_id = f"cat_{symbol}_{title[:50]}"
                    with state_lock:
                        if news_id in state.get("seen_catalyst", {}):
                            continue
                        state.setdefault("seen_catalyst", {})[news_id] = time.time()
                        save_state()
                    
                    msg = f"📰🚨 *خبر عاجل: {symbol}*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"🔑 الكلمات المكتشفة: {', '.join(matched)}\n"
                    msg += f"📝 العنوان: {title[:150]}...\n"
                    msg += f"🔗 [المصدر]({link})\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"💡 ضع السهم على قائمة المراقبة فوراً!"
                    send_telegram(msg)
                    
            time.sleep(180)
        except Exception as e:
            logger.error(f"Catalyst news error: {e}")
            time.sleep(60)

# ================= RSS NEWS SCANNER (ENHANCED - NO API) =================

# ✅ كلمات مفتاحية قوية مع أوزان مختلفة
STRONG_POSITIVE = [
    "fda approval", "fda approved", "fda grants", "breakthrough",
    "partnership", "acquisition", "merger", "contract awarded",
    "record revenue", "record earnings", "beats estimates", "beats expectations",
    "raised guidance", "raises guidance", "buyout", "uplisting",
    "nasdaq listing", "nyse listing", "phase 3", "positive results",
    "exclusive deal", "major contract", "patent granted", "new drug",
    "clinical trial success", "positive data", "ipo",
    "takeover", "tender offer", "preliminary results", "topline results",
    "breakthrough therapy", "fast track", "orphan drug", "emergency use authorization"
]

NEGATIVE_WORDS = [
    "bankruptcy", "delisted", "sec investigation", "fraud", "lawsuit",
    "recall", "missed estimates", "lowers guidance", "chapter 11",
    "going concern", "default", "suspended"
]

# ✅ 8 مصادر RSS بدل 3
RSS_SOURCES = [
    "https://feed.businesswire.com/rss/home/?rss=G1",
    "https://www.prnewswire.com/rss/news-releases-list.rss",
    "https://www.globenewswire.com/RssFeed/subjectcode/15-Banking%20and%20Financial%20Services",
    "https://www.benzinga.com/feed",
    "https://www.marketwatch.com/newsviewer/rssfeed.aspx",
    "https://seekingalpha.com/feed.xml",
    "https://www.investors.com/feed/",
    "https://www.nasdaq.com/feed/rssoutbound",
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
    pattern = r'\b(?:NASDAQ|NYSE|AMEX|OTC):\s*([A-Z]{1,5})\b'
    found = re.findall(pattern, text.upper())
    pattern2 = r'\(([A-Z]{1,5})\)'
    found2 = re.findall(pattern2, text.upper())
    all_symbols = list(set(found + found2))
    exclude = {'THE','AND','FOR','INC','LLC','LTD','CEO','CFO','IPO','FDA','SEC','NYSE','USD','ETF'}
    return [s for s in all_symbols if s not in exclude and 1 <= len(s) <= 5]

def ai_analyze_news(title, desc=""):
    """استخدام الذكاء الاصطناعي لتحليل الخبر (يتوقف بهدوء إذا لم يوجد مفتاح)"""
    if not OPENAI_AVAILABLE or client is None:
        return analyze_news_sentiment_basic(title, desc)
    
    try:
        prompt = f"""
        Analyze this stock market news and provide a JSON response.
        Title: {title}
        Description: {desc}
        
        Required JSON format:
        {{
            "sentiment": "positive" | "negative" | "neutral",
            "impact_score": 0-100,
            "category": "FDA" | "Merger" | "Contract" | "Earnings" | "Partnership" | "General",
            "is_catalyst": true | false,
            "reason_ar": "سبب التقييم باللغة العربية باختصار"
        }}
        """
        
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "You are an expert stock market analyst specializing in penny stocks and momentum catalysts."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            timeout=10
        )
        
        result = json.loads(response.choices[0].message.content)
        return result
    except Exception as e:
        logger.error(f"AI News Analysis Error: {e}")
        return analyze_news_sentiment_basic(title, desc)

def analyze_news_sentiment_basic(title, desc=""):
    """التحليل التقليدي (Fallback) في حال فشل الذكاء الاصطناعي"""
    text = (title + " " + desc).lower()
    for kw in NEGATIVE_WORDS:
        if kw in text: return {"sentiment": "negative", "impact_score": 0, "category": "General", "is_catalyst": False, "reason_ar": "خبر سلبي"}
    
    score = 0
    category = "General"
    for kw in STRONG_POSITIVE:
        if kw in text:
            if kw in ["fda approval", "fda approved"]: 
                score += 40; category = "FDA"
            elif kw in ["acquisition", "merger", "buyout"]: 
                score += 40; category = "Merger"
            elif kw in ["partnership", "contract awarded"]: 
                score += 30; category = "Contract"
            else: 
                score += 15
                
    sentiment = "positive" if score > 0 else "neutral"
    return {
        "sentiment": sentiment,
        "impact_score": min(score, 100),
        "category": category,
        "is_catalyst": score >= 30,
        "reason_ar": "تحليل تقليدي للكلمات المفتاحية"
    }

def analyze_news_sentiment(title, desc=""):
    """هذه الدالة الآن تستخدم الذكاء الاصطناعي"""
    res = ai_analyze_news(title, desc)
    return res.get("sentiment", "neutral"), res.get("impact_score", 0)

def rss_news_scanner():
    """يقرأ 8 مصادر RSS ويصيد الأخبار الإيجابية القوية على الأسهم تحت $30"""
    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(300)
                continue

            for rss_url in RSS_SOURCES:
                items = fetch_rss(rss_url)
                for item in items:
                    title = item['title']
                    link  = item['link']

                    news_id = title[:80]
                    with state_lock:
                        if news_id in state.get("seen_news", {}):
                            continue
                        state.setdefault("seen_news", {})[news_id] = time.time()

                    ai_res = ai_analyze_news(title, item.get('desc', ''))
                    sentiment = ai_res.get("sentiment", "neutral")
                    impact_score = ai_res.get("impact_score", 0)
                    
                    if sentiment != "positive" or impact_score < 70:
                        continue

                    symbols = extract_symbols_from_text(title + " " + item.get('desc', ''))
                    if not symbols:
                        continue

                    for symbol in symbols[:2]:
                        try:
                            df = cached_download(symbol, period="1d", interval="5m")
                            if df.empty or len(df) < 5:
                                continue

                            price = df['close'].iloc[-1]
                            if price < 0.5 or price > 30:
                                continue

                            vol_now  = df['volume'].iloc[-1]
                            vol_avg  = df['volume'].mean()
                            gain_pct = (price - df['open'].iloc[0]) / df['open'].iloc[0] * 100

                            # فلاتر إضافية للأخبار
                            vol_spike    = vol_avg > 0 and vol_now > vol_avg * 1.5
                            not_too_late = gain_pct < 30

                            if not vol_spike or not not_too_late:
                                continue

                            entry = price
                            tp = round(entry * 1.12, 2)
                            sl = round(entry * 0.94, 2)

                            reason_ar = ai_res.get("reason_ar", "خبر إيجابي تم تحليله بالذكاء الاصطناعي")
                            news_type = ai_res.get("category", "General")
                            
                            type_map = {
                                "FDA": "🧬 موافقة FDA",
                                "Merger": "🤝 استحواذ / دمج",
                                "Contract": "📝 عقد جديد",
                                "Partnership": "🤝 شراكة",
                                "Earnings": "💰 نتائج مالية",
                                "General": "📰 خبر إيجابي"
                            }
                            news_emoji = type_map.get(news_type, "📰 خبر إيجابي")

                            now_time = now_est().strftime("%I:%M %p")

                            msg = (
                                f"🤖 *AI NEWS ANALYSIS: {symbol}*\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"🔥 قوة الخبر: *{impact_score}/100*\n"
                                f"🏷️ النوع: {news_emoji}\n"
                                f"🧠 تحليل AI: {reason_ar}\n"
                                f"📝 العنوان: {title[:150]}\n"
                                f"🔗 [رابط الخبر]({link})\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"💰 السعر الحالي: *${entry:.2f}*\n"
                                f"🎯 الهدف المتوقع: *${tp:.2f}* (+12%)\n"
                                f"🛑 وقف الخسارة: *${sl:.2f}* (-6%)\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"🔥 الحجم: {int(vol_now):,} ({vol_now/vol_avg:.1f}x)\n"
                                f"📈 التغير اليوم: {gain_pct:+.1f}%\n"
                                f"⚠️ *تحليل الذكاء الاصطناعي هو أداة مساعدة فقط*"
                            )
                            send_telegram(msg)
                            logger.info(f"📰 News signal sent: {symbol} — {title[:60]}")

                        except Exception as sym_err:
                            logger.warning(f"News signal error {symbol}: {sym_err}")

                time.sleep(2)

        except Exception as e:
            logger.error(f"RSS scanner error: {e}")

        time.sleep(120)

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

                    df = cached_download(symbol, period="1d", interval="1m")
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
    نظام مزدوج للـ Short Squeeze:
    1) يفحص قائمة NASDAQ Short Interest (بيانات رسمية)
    2) يراقب أسهم معروفة بإمكانية Squeeze فورياً
    يشتغل كل 30 دقيقة
    """
    last_si_update = 0

    # أسهم معروفة بإمكانية Short Squeeze
    KNOWN_SQUEEZE_STOCKS = ["GME", "AMC", "BB", "KOSS", "EXPR", "NOK", "BBBY", "CLOV", "WISH"]

    while True:
        try:
            if get_market_phase() not in ("REGULAR", "PRE"):
                time.sleep(300)
                continue

            # ===== الجزء 1: Short Interest من NASDAQ =====
            if time.time() - last_si_update > 14400:
                fetch_short_interest()
                last_si_update = time.time()

            with state_lock:
                short_data = dict(state.get("short_interest", {}))

            sorted_symbols = sorted(
                short_data.keys(),
                key=lambda s: short_data[s].get("days_to_cover", 0),
                reverse=True
            )[:150]

            all_candidates = list(dict.fromkeys(sorted_symbols + KNOWN_SQUEEZE_STOCKS))

            for symbol in all_candidates:
                try:
                    signal_key = f"squeeze_{symbol}"
                    with state_lock:
                        last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                    if time.time() - last_sent < SIGNAL_COOLDOWN:
                        continue

                    df = cached_download(symbol, period="5d", interval="15m")
                    if df.empty or len(df) < 20:
                        continue

                    df.columns = [c.lower() for c in df.columns]
                    df = compute_indicators(df)
                    last = df.iloc[-1]

                    price    = last['close']
                    vol_now  = last['volume']
                    vol_avg  = df['vol_ma'].iloc[-1]

                    # شروط الـ Squeeze
                    vol_spike  = vol_avg > 0 and vol_now > vol_avg * 2.5
                    above_vwap = price > last['vwap']
                    rsi_ok     = last['rsi'] > 50
                    price_ok   = 0.5 < price < 50
                    price_up   = price > df['close'].iloc[-5] * 1.05

                    # نقاط SI إذا في بيانات
                    si_info    = short_data.get(symbol, {})
                    days_cover = si_info.get("days_to_cover", 0)
                    short_int  = si_info.get("short_interest", 0)
                    high_si    = days_cover >= 3

                    if not (vol_spike and above_vwap and rsi_ok and price_ok and price_up):
                        continue

                    entry    = price
                    tp       = round(entry * 1.15, 2)
                    sl       = round(entry * 0.95, 2)
                    now_time = now_est().strftime("%I:%M %p")

                    msg  = f"🔥 *إشارة Short Squeeze*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"📊 *{symbol}*\n"
                    if short_int > 0:
                        msg += f"📉 Short Interest: {short_int:,} سهم\n"
                        msg += f"📅 أيام التغطية: {days_cover:.1f} يوم\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"⏰ وقت الدخول: *{now_time}*\n"
                    msg += f"💰 سعر الدخول: *${entry:.2f}*\n"
                    msg += f"🎯 الهدف: *${tp:.2f}* (+15%)\n"
                    msg += f"🛑 وقف الخسارة: *${sl:.2f}* (-5%)\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"📈 الارتفاع: +{(price/df['close'].iloc[-5]-1)*100:.1f}% (5 شمعات)\n"
                    msg += f"🔥 الحجم: {int(vol_now):,} ({vol_now/max(vol_avg,1):.1f}x المتوسط)\n"
                    msg += f"📊 RSI: {last['rsi']:.1f} | فوق VWAP ✅\n"
                    msg += f"💡 البائعون على المكشوف مضطرون للشراء\n"
                    msg += f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"

                    send_telegram(msg)

                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()

                    logger.info(f"🔥 Squeeze: {symbol} | Days={days_cover:.1f}")
                    time.sleep(1)

                except Exception as sym_err:
                    logger.warning(f"Squeeze check error {symbol}: {sym_err}")

        except Exception as e:
            logger.error(f"Short squeeze scanner error: {e}")

        time.sleep(1800)

# ================= PRE-MARKET GAP UP SCANNER =================

def premarket_gap_scanner():
    """
    يراقب الأسهم اللي تفتح بفجوة كبيرة قبل الافتتاح.
    فجوة +3% مع حجم 1.5x = زخم قوي عند 9:30.
    يشتغل كل 10 دقائق في فترة PRE فقط.
    """
    while True:
        try:
            if get_market_phase() != "PRE":
                time.sleep(120)
                continue

            with state_lock:
                tickers = list(state.get("tickers", []))

            if not tickers:
                time.sleep(120)
                continue

            today = now_est().strftime("%Y-%m-%d")

            for symbol in tickers[:300]:
                try:
                    df = cached_download(symbol, period="2d", interval="5m")
                    if df.empty or len(df) < 5:
                        continue

                    df.columns = [c.lower() for c in df.columns]

                    # أمس = آخر شمعة من اليوم السابق
                    yesterday_candles = df[df.index.date < now_est().date()]
                    today_candles     = df[df.index.date == now_est().date()]

                    if yesterday_candles.empty or today_candles.empty:
                        continue

                    yesterday_close = yesterday_candles['close'].iloc[-1]
                    current         = today_candles['close'].iloc[-1]
                    volume          = today_candles['volume'].sum()
                    avg_volume      = df['volume'].mean()

                    if yesterday_close <= 0 or avg_volume <= 0:
                        continue

                    gap_pct = (current - yesterday_close) / yesterday_close * 100

                    if gap_pct < 3.0 or current > 20:
                        continue
                    if volume < avg_volume * 1.5:
                        continue

                    gap_key = f"gap_{symbol}_{today}"
                    with state_lock:
                        if gap_key in state.get("seen_gaps", {}):
                            continue
                        state.setdefault("seen_gaps", {})[gap_key] = time.time()

                    entry    = current
                    tp       = round(entry * 1.10, 2)
                    sl       = round(entry * 0.95, 2)

                    msg  = f"🌅 *PRE-MARKET GAP UP: {symbol}*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"💰 السعر الحالي: *${current:.2f}*\n"
                    msg += f"📌 إغلاق أمس: ${yesterday_close:.2f}\n"
                    msg += f"📈 الفجوة: *+{gap_pct:.1f}%*\n"
                    msg += f"🔥 الحجم: {int(volume):,} ({volume/avg_volume:.1f}x)\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"⏰ الدخول المقترح: *9:30 AM EST*\n"
                    msg += f"🎯 الهدف: *${tp:.2f}* (+10%)\n"
                    msg += f"🛑 وقف الخسارة: *${sl:.2f}* (-5%)\n"
                    msg += f"💡 ادخل عند أول شمعة خضراء بعد 9:30\n"
                    msg += f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"

                    send_telegram(msg)
                    logger.info(f"🌅 Gap Up: {symbol} +{gap_pct:.1f}%")

                except Exception as sym_err:
                    logger.warning(f"Gap scanner error {symbol}: {sym_err}")
                time.sleep(0.3)

        except Exception as e:
            logger.error(f"Premarket gap scanner error: {e}")

        time.sleep(600)

# ================= CORRELATION SCANNER =================

def correlation_scanner():
    """
    يتتبع أسهم مرتبطة بأسهم قيادية.
    إذا القائد طلع بقوة (+3%) → الأسهم المرتبطة فيها فرصة.
    يشتغل كل 15 دقيقة في ساعات التداول.
    """
    # خريطة الارتباطات: القائد → الأتباع
    CORRELATIONS = {
        "NVDA": ["AMD", "INTC", "MRVL", "AVGO", "SMH"],
        "TSLA": ["RIVN", "LCID", "NIO", "F", "GM"],
        "GME":  ["AMC", "BB", "KOSS", "EXPR"],
        "AAPL": ["MSFT", "GOOGL", "META"],
        "MSTR": ["COIN", "RIOT", "MARA", "HUT"],
        "NVAX": ["MRNA", "BNTX", "PFE"],
    }

    while True:
        try:
            if get_market_phase() != "REGULAR":
                time.sleep(300)
                continue

            for leader, followers in CORRELATIONS.items():
                try:
                    df_leader = cached_download(leader, period="1d", interval="5m")
                    if df_leader.empty or len(df_leader) < 5:
                        continue

                    df_leader.columns = [c.lower() for c in df_leader.columns]
                    leader_change = (df_leader['close'].iloc[-1] - df_leader['close'].iloc[-5]) / df_leader['close'].iloc[-5] * 100

                    if leader_change < 3.0:
                        continue

                    logger.info(f"🔄 Leader {leader} +{leader_change:.1f}% — checking followers")

                    for follower in followers:
                        try:
                            corr_key = f"corr_{follower}"
                            with state_lock:
                                last_sent = state.get("seen_signals", {}).get(corr_key, 0)
                            if time.time() - last_sent < SIGNAL_COOLDOWN:
                                continue

                            df_f = cached_download(follower, period="1d", interval="5m")
                            if df_f.empty or len(df_f) < 5:
                                continue

                            df_f.columns = [c.lower() for c in df_f.columns]
                            df_f = compute_indicators(df_f)
                            last_f = df_f.iloc[-1]

                            price   = last_f['close']
                            vol_now = last_f['volume']
                            vol_avg = df_f['vol_ma'].iloc[-1]

                            # فلتر: الحجم منتبه + السعر فوق VWAP
                            if vol_avg <= 0 or vol_now < vol_avg * 1.5:
                                continue
                            if price <= last_f['vwap']:
                                continue

                            follower_change = (price - df_f['close'].iloc[-5]) / df_f['close'].iloc[-5] * 100

                            entry    = price
                            tp       = round(entry * 1.10, 2)
                            sl       = round(entry * 0.95, 2)
                            now_time = now_est().strftime("%I:%M %p")

                            msg  = f"🔄 *إشارة Correlation: {follower}*\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"📊 القائد *{leader}*: +{leader_change:.1f}%\n"
                            msg += f"📊 *{follower}*: {follower_change:+.1f}% (لسا ما لحق)\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"⏰ وقت الدخول: *{now_time}*\n"
                            msg += f"💰 سعر الدخول: *${entry:.2f}*\n"
                            msg += f"🎯 الهدف: *${tp:.2f}* (+10%)\n"
                            msg += f"🛑 وقف الخسارة: *${sl:.2f}* (-5%)\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"🔥 الحجم: {int(vol_now):,} ({vol_now/max(vol_avg,1):.1f}x)\n"
                            msg += f"💡 متابعة حركة {leader}\n"
                            msg += f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"

                            send_telegram(msg)

                            with state_lock:
                                state.setdefault("seen_signals", {})[corr_key] = time.time()
                                save_state()

                            logger.info(f"🔄 Correlation: {follower} follows {leader}")
                            time.sleep(1)

                        except Exception as f_err:
                            logger.warning(f"Correlation follower error {follower}: {f_err}")

                except Exception as l_err:
                    logger.warning(f"Correlation leader error {leader}: {l_err}")
                time.sleep(1)

        except Exception as e:
            logger.error(f"Correlation scanner error: {e}")

        time.sleep(900)  # كل 15 دقيقة

# ================= SECTOR STRENGTH SCANNER =================

def sector_strength_scanner():
    """
    يكتشف أقوى قطاع في السوق كل ساعة.
    لو قطاع يتحرك بقوة → ركز عليه.
    يشتغل كل ساعة في ساعات التداول.
    """
    SECTORS = {
        "SEMI 🖥️":   ["NVDA", "AMD", "INTC", "TSM", "AVGO"],
        "TECH 💻":   ["AAPL", "MSFT", "GOOGL", "META", "ORCL"],
        "EV 🚗":     ["TSLA", "RIVN", "LCID", "NIO", "F"],
        "MEME 🎮":   ["GME", "AMC", "BB", "KOSS", "NOK"],
        "CRYPTO 🪙": ["MSTR", "COIN", "RIOT", "MARA", "HUT"],
        "BIOTECH 🧬":["MRNA", "BNTX", "NVAX", "SGEN", "BIIB"],
        "ENERGY ⚡": ["XOM", "CVX", "OXY", "SLB", "HAL"],
    }

    while True:
        try:
            if get_market_phase() != "REGULAR":
                time.sleep(300)
                continue

            sector_key = f"sector_{now_est().strftime('%Y-%m-%d_%H')}"
            with state_lock:
                if sector_key in state.get("seen_sectors", {}):
                    time.sleep(600)
                    continue

            sector_scores = {}
            sector_details = {}

            for sector, stocks in SECTORS.items():
                changes = []
                for symbol in stocks:
                    try:
                        df = cached_download(symbol, period="1d", interval="5m")
                        if df.empty or len(df) < 5:
                            continue
                        df.columns = [c.lower() for c in df.columns]
                        change = (df['close'].iloc[-1] - df['close'].iloc[-5]) / df['close'].iloc[-5] * 100
                        changes.append((symbol, change))
                    except:
                        continue

                if changes:
                    avg_change = sum(c for _, c in changes) / len(changes)
                    sector_scores[sector] = avg_change
                    sector_details[sector] = sorted(changes, key=lambda x: x[1], reverse=True)

            if not sector_scores:
                time.sleep(1800)
                continue

            # ترتيب القطاعات
            sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
            best_sector, best_score = sorted_sectors[0]
            worst_sector, worst_score = sorted_sectors[-1]

            # أرسل فقط لو في تحرك حقيقي
            if best_score < 0.5:
                time.sleep(1800)
                continue

            with state_lock:
                state.setdefault("seen_sectors", {})[sector_key] = time.time()
                save_state()

            now_time = now_est().strftime("%I:%M %p")

            msg  = f"📊 *تقرير قوة القطاعات — {now_time}*\n"
            msg += f"━━━━━━━━━━━━━━━━\n"
            msg += f"🏆 *أقوى قطاع: {best_sector}*\n"
            msg += f"📈 المتوسط: +{best_score:.1f}%\n"

            # أفضل سهمين في القطاع الأقوى
            if best_sector in sector_details:
                top_stocks = sector_details[best_sector][:2]
                for sym, chg in top_stocks:
                    msg += f"  • *{sym}*: {chg:+.1f}%\n"

            msg += f"━━━━━━━━━━━━━━━━\n"

            # باقي القطاعات
            for sector, score in sorted_sectors[1:]:
                arrow = "📈" if score > 0 else "📉"
                msg += f"{arrow} {sector}: {score:+.1f}%\n"

            msg += f"━━━━━━━━━━━━━━━━\n"
            msg += f"💡 ركز على أسهم قطاع *{best_sector}*"

            send_telegram(msg)
            logger.info(f"📊 Sector report: {best_sector} leads +{best_score:.1f}%")

        except Exception as e:
            logger.error(f"Sector strength scanner error: {e}")

        time.sleep(3600)  # كل ساعة

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

                            df = cached_download(symbol, period="1d", interval="5m")
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
                    df = cached_download(symbol, period="2d", interval="5m")
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

def clean_old_signals():
    """تحذف الإشارات والأخبار الأقدم من 24 ساعة لتقليل استهلاك الذاكرة"""
    expiry = 86400  # 24 ساعة
    now = time.time()
    with state_lock:
        # تنظيف الإشارات
        old_signals_count = len(state.get("seen_signals", {}))
        state["seen_signals"] = {k: v for k, v in state.get("seen_signals", {}).items() if isinstance(v, (int, float)) and now - v < expiry}
        
        # تنظيف الأخبار
        old_news_count = len(state.get("seen_news", {}))
        state["seen_news"] = {k: v for k, v in state.get("seen_news", {}).items() if isinstance(v, (int, float)) and now - v < expiry}
        
        # تنظيف الكاتالست
        old_catalyst_count = len(state.get("seen_catalyst", {}))
        state["seen_catalyst"] = {k: v for k, v in state.get("seen_catalyst", {}).items() if isinstance(v, (int, float)) and now - v < expiry}
        
        # تنظيف ملفات SEC
        old_edgar_count = len(state.get("seen_edgar", {}))
        state["seen_edgar"] = {k: v for k, v in state.get("seen_edgar", {}).items() if isinstance(v, (int, float)) and now - v < expiry}
        
        logger.info(f"🧹 Memory Cleanup: signals={old_signals_count - len(state['seen_signals'])}, news={old_news_count - len(state['seen_news'])}")
        save_state()

def cleaner_loop():
    """دورة تنظيف دورية كل ساعة"""
    while True:
        try:
            clean_old_signals()
        except Exception as e:
            logger.error(f"Cleaner loop error: {e}")
        time.sleep(3600)

# ================= MAIN =================

# ================= YAHOO DATA FETCHER =================
def fetch_from_yahoo(symbol, period="5d", interval="15m", timeout=10):
    """تجلب البيانات مباشرة من Yahoo Finance — مع دعم بيانات Pre/Post Market"""
    try:
        # ✅ زيادة الفترات لضمان وجود بيانات كافية لحساب RVOL على فترات قصيرة
        days_map = {"1d": 5, "2d": 5, "5d": 7, "10d": 14, "30d": 30, "60d": 60, "1mo": 30}
        days = days_map.get(period, 7)

        end_date = int(datetime.now().timestamp())
        start_date = int((datetime.now() - timedelta(days=days)).timestamp())

        interval_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1d": "1d", "1h": "60m"}
        yf_interval = interval_map.get(interval, "15m")

        # ✅ includePrePost=true لجلب بيانات قبل/بعد السوق (يصلح مشكلة RVOL=0.0)
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            f"?interval={yf_interval}&period1={start_date}&period2={end_date}"
            f"&includePrePost=true&events=history"
        )

        time.sleep(random.uniform(0.1, 0.3))
        response = requests.get(url, impersonate="chrome120", timeout=timeout)

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
            'timestamp': pd.to_datetime(timestamps, unit='s', utc=True),
            'open': quote.get('open', []),
            'high': quote.get('high', []),
            'low': quote.get('low', []),
            'close': quote.get('close', []),
            'volume': quote.get('volume', [])
        })

        # ✅ تنظيف ذكي: نحذف الصفوف اللي فيها سعر مفقود فقط، ونصفّر الحجم المفقود
        df = df.dropna(subset=['close'])
        df['volume'] = df['volume'].fillna(0)
        if df.empty:
            return pd.DataFrame()

        # ✅ تحويل الوقت لتوقيت نيويورك ليتم استخدامه في تمييز الجلسات
        df['timestamp'] = df['timestamp'].dt.tz_convert(EASTERN_TZ)
        df.set_index('timestamp', inplace=True)
        return df

    except Exception as e:
        logger.error(f"Yahoo fetch error {symbol}: {e}")
        return pd.DataFrame()

def cached_download(symbol, period="5d", interval="15m", timeout=10, force_refresh=False):
    """نفس safe_download ولكن مع طبقة تخزين مؤقت (Cache)"""
    cache_key = f"{symbol}_{period}_{interval}"
    
    if not force_refresh:
        with _cache_lock:
            if cache_key in _cache:
                cached_data, timestamp = _cache[cache_key]
                ttl = CACHE_TTL.get(interval, 60)
                if time.time() - timestamp < ttl:
                    logger.debug(f"Cache HIT: {symbol} ({interval})")
                    return cached_data.copy() if not cached_data.empty else pd.DataFrame()
    
    logger.debug(f"Cache MISS: {symbol} ({interval})")
    df = fetch_from_yahoo(symbol, period, interval, timeout)
    
    with _cache_lock:
        _cache[cache_key] = (df, time.time())
    
    return df.copy() if not df.empty else pd.DataFrame()

def clear_old_cache():
    """تنظيف الـ Cache من البيانات القديمة"""
    now = time.time()
    with _cache_lock:
        old_keys = []
        for key, (_, timestamp) in _cache.items():
            parts = key.split('_')
            interval = parts[2] if len(parts) > 2 else "15m"
            ttl = CACHE_TTL.get(interval, 60)
            if now - timestamp > ttl * 2:
                old_keys.append(key)
        
        for key in old_keys:
            del _cache[key]
        
        if old_keys:
            logger.debug(f"Cache cleaned: removed {len(old_keys)} expired entries")

def cache_cleaner_loop():
    """دورة تنظيف الـ Cache كل 5 دقائق"""
    while True:
        time.sleep(300)
        clear_old_cache()

def ultra_fast_penny_scanner():
    """ماسح خفيف جداً - يفحص فقط السعر + الحجم + التغير (نسخة DeepSeek المحسنة والنهائية)"""
    while True:
        try:
            phase = get_market_phase()
            if phase not in ("REGULAR", "PRE"):
                time.sleep(60)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))[:800]
            
            for symbol in tickers:
                try:
                    df = cached_download(symbol, period="5d", interval="5m")  # 5d لضمان جلب بيانات أمس
                    if df.empty or len(df) < 20:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    price = df['close'].iloc[-1]
                    
                    if price < PENNY_MIN_PRICE or price > PENNY_MAX_PRICE:
                        continue
                    
                    # حساب إغلاق أمس بشكل صحيح
                    yesterday = df[df.index.date < now_est().date()]
                    if yesterday.empty or len(yesterday) < 5:
                        continue
                    prev_close = yesterday['close'].iloc[-1]
                    
                    gain = (price - prev_close) / prev_close * 100
                    if gain < PENNY_MIN_GAIN:
                        continue
                    
                    vol = df['volume'].iloc[-1]
                    avg_vol = yesterday['volume'].mean()
                    
                    # فلاتر أمان إضافية (نسخة DeepSeek)
                    if avg_vol <= 0 or vol < 20000:  # استبعاد السيولة الضعيفة جداً
                        continue
                    if vol < max(avg_vol, 1) * PENNY_MIN_RVOL:
                        continue
                    
                    signal_key = f"fast_{symbol}_{now_est().date()}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue
                    
                    tp1 = round(price * 1.25, 4)
                    tp2 = round(price * 1.50, 4)
                    sl = round(price * 0.92, 4)
                    
                    msg = f"⚡ *PENNY BREAKOUT: {symbol}*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"💰 السعر: *${price:.4f}*\n"
                    msg += f"📈 الصعود: *+{gain:.1f}%* من أمس\n"
                    msg += f"🔥 RVOL: *{vol/avg_vol:.1f}x*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"🎯 T1: ${tp1} (+25%)\n"
                    msg += f"🎯 T2: ${tp2} (+50%)\n"
                    msg += f"🛑 وقف: ${sl} (-8%)\n"
                    msg += f"⚠️ *هذا سهم عالي المخاطر*"
                    
                    send_telegram(msg)
                    with state_lock:
                        state["seen_signals"][signal_key] = time.time()
                        save_state()
                    time.sleep(3)
                except Exception as e:
                    logger.debug(f"Fast scan error {symbol}: {e}")
                    continue
                time.sleep(0.15)
        except Exception as e:
            logger.error(f"Fast scanner error: {e}")
        time.sleep(180)

def penny_gap_scanner():
    """يراقب الفجوات الصباحية للبيني ستوكس"""
    while True:
        try:
            now = now_est()
            if not (9 <= now.hour < 10) or now.minute > 45:
                time.sleep(60)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))[:800]
            
            for symbol in tickers:
                try:
                    df = cached_download(symbol, period="2d", interval="5m")
                    if df.empty or len(df) < 10:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    price = df['close'].iloc[-1]
                    if price < PENNY_MIN_PRICE or price > PENNY_MAX_PRICE:
                        continue
                    
                    yesterday_close = df['close'].iloc[-6]
                    if yesterday_close <= 0:
                        continue
                    
                    gap_pct = (price - yesterday_close) / yesterday_close * 100
                    if gap_pct < 8:
                        continue
                    
                    morning_vol = df['volume'].iloc[-5:].sum()
                    avg_vol = df['volume'].rolling(20).mean().iloc[-1]
                    if avg_vol > 0 and morning_vol < avg_vol * 2:
                        continue
                    
                    signal_key = f"gap_penny_{symbol}_{now.date()}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue
                    
                    tp = round(price * 1.20, 4)
                    sl = round(price * 0.94, 4)
                    
                    msg = f"🌅 *PENNY GAP UP: {symbol}*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"💰 السعر: *${price:.4f}*\n"
                    msg += f"📈 الفجوة: *+{gap_pct:.1f}%*\n"
                    msg += f"🎯 الهدف: ${tp} (+20%)\n"
                    msg += f"🛑 وقف: ${sl} (-6%)\n"
                    msg += f"💡 طريقة الدخول: انتظر أول شمعة 5 دقائق خضراء"
                    
                    send_telegram(msg)
                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()
                except:
                    continue
                time.sleep(0.3)
        except Exception as e:
            logger.error(f"Penny gap scanner error: {e}")
        time.sleep(180)

if __name__ == "__main__":
    global webull_client
    webull_client = setup_webull()
    load_state()
    ensure_state_schema()

    # ===== IMPORTANT: Load tickers FIRST =====
    update_all_tickers()  # <--- أضف هذا السطر هنا
    
    # أساسيات (إلزامية)
    threading.Thread(target=background_monitor, daemon=True).start()
    threading.Thread(target=cleaner_loop, daemon=True).start()
    threading.Thread(target=cache_cleaner_loop, daemon=True).start()

    # 🔥 ماسحات صيد البيني (كلها شغالة)
    threading.Thread(target=ultra_fast_penny_scanner, daemon=True).start()
    threading.Thread(target=penny_gap_scanner, daemon=True).start()
    threading.Thread(target=rvol_spike_scanner, daemon=True).start()
    threading.Thread(target=power_runner_scanner, daemon=True).start()
    threading.Thread(target=fast_momentum_scanner, daemon=True).start()
    
    # أخبار RSS (مفيدة للمحفزات)
    threading.Thread(target=rss_news_scanner, daemon=True).start()
    threading.Thread(target=fetch_news_catalyst, daemon=True).start()
    
    # تقارير
    threading.Thread(target=elite_3_summary_loop, daemon=True).start()
    threading.Thread(target=daily_report_scheduler, daemon=True).start()

    if bot:
        threading.Thread(target=run_telegram_bot, daemon=True).start()
    
    logger.info("=" * 50)
    logger.info("🚀 PENNY HUNTER BOT STARTED!")
    logger.info("📊 Target: $0.3 - $20 penny stocks")
    logger.info("=" * 50)
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
# ================= CACHED DATA FETCHER =================

# ================= MERGED FROM BLOCKS =================

# ================= CONFIGURATION =================
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
POLYGON_API = os.getenv("POLYGON_API")

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if not TOKEN or not CHAT_ID or not POLYGON_API:
    logger.error("Missing Environment Variables! Please set TOKEN, CHAT_ID, and POLYGON_API.")
    exit(1)

bot = telebot.TeleBot(TOKEN)

# ================= TELEGRAM =================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
        )
        logger.info("Telegram message sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

# ================= UNIVERSE =================
def get_universe():
    # Expanded universe of highly liquid US stocks
    return [
        "AAPL", "MSFT", "NVDA", "AMD", "META", "TSLA",
        "AMZN", "GOOGL", "SPY", "QQQ", "PLTR", "SOFI",
        "BABA", "NIO", "RIOT", "MARA", "INTC", "JPM", "BAC",
        "COIN", "UBER", "NFLX", "DIS", "PYPL", "SQ"
    ]

# ================= MARKET HOURS =================
def is_market_open():
    """Check if US stock market is currently open (9:30 AM - 4:00 PM EST, Mon-Fri)"""
    tz = pytz.timezone('US/Eastern')
    now = datetime.now(tz)
    
    # Check if weekend
    if now.weekday() >= 5:
        return False
        
    # Check time (9:30 AM to 4:00 PM)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= now <= market_close

# ================= POLYGON DATA =================
def get_data(symbol, days_back=10):
    """Fetch historical data from Polygon.io"""
    # Calculate dates
    tz = pytz.timezone('US/Eastern')
    end_date = datetime.now(tz).strftime('%Y-%m-%d')
    
    # We use a 15-minute timeframe for better intraday signals
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/15/minute/2023-01-01/{end_date}?adjusted=true&sort=asc&limit=500&apiKey={POLYGON_API}"
    
    try:
        r = requests.get(url).json()
        
        if "results" not in r or len(r["results"]) == 0:
            logger.warning(f"No data returned for {symbol}")
            return None
            
        df = pd.DataFrame(r["results"])
        # Rename columns to standard OHLCV
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "vw": "vwap"})
        
        return df
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None

# ================= ADVANCED INDICATORS =================
def calculate_indicators(df):
    """Calculate technical indicators for the dataframe"""
    # 1. ATR (Average True Range) - Volatility
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df['atr'] = true_range.rolling(14).mean()
    
    # 2. RSI (Relative Strength Index) - Momentum
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # 3. EMAs (Exponential Moving Averages) - Trend
    df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
    
    # 4. MACD (Moving Average Convergence Divergence)
    ema_12 = df['close'].ewm(span=12, adjust=False).mean()
    ema_26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema_12 - ema_26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']
    
    # 5. Bollinger Bands - Volatility & Mean Reversion
    df['bb_middle'] = df['close'].rolling(window=20).mean()
    bb_std = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
    df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
    
    # 6. Volume Analysis
    df['vol_sma'] = df['volume'].rolling(window=20).mean()
    
    return df

# ================= AI SCORING ENGINE =================
def score_setup(df):
    """
    Advanced scoring system based on multiple confluences.
    Max score is 100.
    """
    score = 0
    reasons = []
    
    current = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 1. Trend Alignment (Max 30 pts)
    if current['close'] > current['ema_50']:
        score += 10
        reasons.append("Above 50 EMA")
    
    if current['ema_9'] > current['ema_21']:
        score += 20
        reasons.append("Bullish EMA Cross (9 > 21)")
        
    # 2. Momentum (Max 25 pts)
    if 40 < current['rsi'] < 70:
        score += 10
        reasons.append("Healthy RSI")
    elif current['rsi'] > 70:
        score -= 10 # Overbought
        reasons.append("Overbought RSI (Warning)")
        
    if current['macd'] > current['macd_signal'] and current['macd_hist'] > 0:
        score += 15
        reasons.append("Bullish MACD")
        
    # 3. Volatility & Volume (Max 25 pts)
    if current['volume'] > current['vol_sma'] * 1.5:
        score += 15
        reasons.append("High Volume Spike")
        
    if current['close'] > current['vwap']:
        score += 10
        reasons.append("Above VWAP")
        
    # 4. Price Action / Breakout (Max 20 pts)
    recent_high = df['high'].iloc[-20:-1].max()
    if current['close'] > recent_high:
        score += 20
        reasons.append("20-Period Breakout")
    elif current['close'] > current['bb_middle'] and prev['close'] <= prev['bb_middle']:
        score += 15
        reasons.append("BB Middle Bounce")
        
    return score, reasons

# ================= RISK MANAGEMENT & TARGETS =================
def calculate_trade_params(price, atr_val):
    """Calculate dynamic targets and stop loss based on ATR"""
    # Risk 1 ATR, Reward 1.5, 2.5, 4 ATR
    sl_dist = atr_val * 1.5
    
    sl = price - sl_dist
    t1 = price + (atr_val * 1.5) # 1:1 RR
    t2 = price + (atr_val * 3.0) # 1:2 RR
    t3 = price + (atr_val * 5.0) # Runner
    
    # Calculate Position Size (Assuming $10,000 account, 1% risk per trade = $100 risk)
    account_risk = 100
    risk_per_share = price - sl
    suggested_shares = int(account_risk / risk_per_share) if risk_per_share > 0 else 0
    
    return {
        "entry": price,
        "sl": sl,
        "t1": t1,
        "t2": t2,
        "t3": t3,
        "shares": suggested_shares,
        "risk_reward": round((t2 - price) / (price - sl), 2) if price > sl else 0
    }

# ================= ANALYSIS PIPELINE =================
def analyze(symbol):
    logger.info(f"Analyzing {symbol}...")
    df = get_data(symbol)
    
    if df is None or len(df) < 50:
        return None
        
    df = calculate_indicators(df)
    
    # Drop NaNs created by rolling windows
    df = df.dropna()
    if len(df) < 5:
        return None
        
    score, reasons = score_setup(df)
    
    # Only return high probability setups
    if score < 75:
        return None
        
    current = df.iloc[-1]
    price = current['close']
    atr_val = current['atr']
    
    trade_params = calculate_trade_params(price, atr_val)
    
    return {
        "symbol": symbol,
        "score": score,
        "price": price,
        "reasons": reasons,
        "params": trade_params,
        "time": datetime.now().strftime("%H:%M:%S EST")
    }

# ================= MAIN ENGINE =================
def run():
    startup_msg = "🚀 <b>AI Institutional Pro v4.0 Started</b>\n\n"
    startup_msg += "✅ Advanced Indicators (RSI, MACD, VWAP)\n"
    startup_msg += "✅ Dynamic ATR Risk Management\n"
    startup_msg += "✅ Market Hours Filtering\n"
    startup_msg += "✅ Railway Deployment Ready"
    send(startup_msg)
    logger.info("Bot started successfully.")

    seen_signals = {}

    while True:
        try:
            # Check if market is open (Optional: comment out for 24/7 crypto)
            # if not is_market_open():
            #     logger.info("Market is closed. Waiting...")
            #     time.sleep(300)
            #     continue

            results = []
            universe = get_universe()
            
            for symbol in universe:
                r = analyze(symbol)
                
                if not r:
                    continue
                    
                # Prevent spamming the same signal within 2 hours (7200 seconds)
                if time.time() - seen_signals.get(r["symbol"], 0) < 7200:
                    logger.info(f"Skipping {symbol} - Signal already sent recently.")
                    continue
                    
                results.append(r)
                
                # Respect Polygon API rate limits (5 calls / minute for free tier)
                time.sleep(12)

            # Sort by highest score
            results = sorted(results, key=lambda x: x["score"], reverse=True)[:3]

            for r in results:
                seen_signals[r["symbol"]] = time.time()
                p = r["params"]
                
                reasons_str = "\n".join([f"🔹 {reason}" for reason in r["reasons"]])
                
                msg = f"""
🚨 <b>AI PRO SIGNAL DETECTED</b> 🚨

📊 <b>Ticker:</b> #{r['symbol']}
⭐ <b>Score:</b> {r['score']}/100
⏰ <b>Time:</b> {r['time']}

💰 <b>Entry Price:</b> ${p['entry']:.2f}

🎯 <b>Targets:</b>
T1: ${p['t1']:.2f} (Safe)
T2: ${p['t2']:.2f} (Main)
T3: ${p['t3']:.2f} (Runner)

🛑 <b>Stop Loss:</b> ${p['sl']:.2f}

⚖️ <b>Risk/Reward:</b> 1:{p['risk_reward']}
📦 <b>Suggested Size:</b> {p['shares']} shares ($100 Risk)

🧠 <b>Confluence Factors:</b>
{reasons_str}
"""
                send(msg)
                logger.info(f"Signal sent for {r['symbol']}")

            logger.info("Cycle complete. Waiting for next scan...")
            time.sleep(300) # Wait 5 minutes before next scan
            
        except Exception as e:
            logger.error(f"Critical error in main loop: {e}")
            time.sleep(60) # Wait a bit before retrying on error

# ================= ENTRY POINT =================
if __name__ == "__main__":
    run()

# ================= MERGED FROM BLOCKS =================

matplotlib.use('Agg')

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("⚠️ Missing TELEGRAM_TOKEN or CHAT_ID")

CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5
DAILY_LOSS_LIMIT = 200.0

# --- FIX: Scoring thresholds separated for penny vs regular ---
MIN_SCORE_REGULAR = 75
MIN_SCORE_PENNY = 80          # Penny stocks require higher confidence
SIGNAL_COOLDOWN = 3600
STATE_FILE = "state.json"
MAX_SCAN_SYMBOLS = 150

plt.style.use('dark_background')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

app = Flask(__name__)
bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
executor = ThreadPoolExecutor(max_workers=3)

# ================= STATE =================
state_lock = threading.RLock()

# --- FIX: cache_lock added to protect data_cache ---
cache_lock = threading.RLock()
data_cache = {}

state = {
    "open_trades": {},
    "performance": {},
    "weights": {
        "trend":     20,
        "rsi":       15,
        "volume":    15,
        "obv":       10,
        "breakout":  15,
        "macd":      10,   # NEW
        "bb_pos":    10,   # NEW: Bollinger Band position
        "adx":        5,   # NEW: trend strength
    },
    "seen_signals":       {},
    "halted_alerts":      {},
    "last_accumulation":  {},
    "daily_loss":         0.0,
    "last_reset":         None,
    "backtest_results":   {},   # NEW
    "equity_curve":       [],   # NEW
}

def fetch_yfinance_data(symbol: str, period: str = '5d', interval: str = '15m') -> pd.DataFrame | None:
    """Fetch OHLCV data with retry logic."""
    for attempt in range(3):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval, auto_adjust=True)
            if df.empty:
                return None
            df = df.rename(columns={
                'Open': 'open', 'High': 'high',
                'Low': 'low', 'Close': 'close', 'Volume': 'volume'
            })
            df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
            df.dropna(inplace=True)
            return df
        except Exception as e:
            if attempt == 2:
                logger.warning(f"fetch_yfinance_data failed for {symbol}: {e}")
            time.sleep(0.5 * (attempt + 1))
    return None

def fetch_yfinance_quick(symbol: str) -> dict | None:
    """Lightweight fetch for pre-filter."""
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period='5d')
        if data.empty or len(data) < 2:
            return None
        return {
            'price':      float(data['Close'].iloc[-1]),
            'volume':     float(data['Volume'].iloc[-1]),
            'avg_volume': float(data['Volume'].mean()),
            'change_pct': float((data['Close'].iloc[-1] - data['Close'].iloc[-2]) / data['Close'].iloc[-2] * 100),
        }
    except Exception:
        return None

# ================= INDICATORS =================
def score_signal(df: pd.DataFrame) -> tuple[int, dict]:
    """
    Returns (total_score 0-100, breakdown dict).
    Each component is independent so we can inspect what fired.
    """
    if df is None or len(df) < 50:
        return 0, {}

    c   = df.iloc[-1]
    c_5 = df.iloc[-5] if len(df) > 5 else c

    with state_lock:
        w = state["weights"].copy()

    breakdown = {}
    score = 0

    # 1. Trend: EMA alignment (9 > 21 > 50)
    if c["ema9"] > c["ema21"] > c["ema50"]:
        breakdown["trend"] = w["trend"]
        score += w["trend"]
    elif c["ema9"] > c["ema21"]:
        breakdown["trend"] = w["trend"] // 2
        score += w["trend"] // 2

    # 2. RSI: momentum zone 52–68 (avoid overbought)
    if 52 < c["rsi"] < 68:
        breakdown["rsi"] = w["rsi"]
        score += w["rsi"]
    elif 48 < c["rsi"] <= 52:
        breakdown["rsi"] = w["rsi"] // 3
        score += w["rsi"] // 3

    # 3. Volume surge
    if c["volume"] > c["vol_ma"] * 1.5:
        breakdown["volume"] = w["volume"]
        score += w["volume"]
    elif c["volume"] > c["vol_ma"] * 1.2:
        breakdown["volume"] = w["volume"] // 2
        score += w["volume"] // 2

    # 4. OBV rising vs its own MA
    if c["obv"] > c["obv_ma"]:
        breakdown["obv"] = w["obv"]
        score += w["obv"]

    # 5. Breakout: close above 20-period high
    recent_high = df["high"].iloc[-20:-1].max()
    if c["close"] > recent_high:
        breakdown["breakout"] = w["breakout"]
        score += w["breakout"]

    # 6. MACD bullish crossover
    prev_hist = df["macd_hist"].iloc[-2]
    if c["macd_hist"] > 0 and prev_hist <= 0:        # fresh cross
        breakdown["macd"] = w["macd"]
        score += w["macd"]
    elif c["macd_hist"] > 0 and c["macd"] > 0:       # already above
        breakdown["macd"] = w["macd"] // 2
        score += w["macd"] // 2

    # 7. Bollinger Band position (0.5–0.8 = healthy uptrend, not overextended)
    if 0.5 < c["bb_pct"] < 0.8:
        breakdown["bb_pos"] = w["bb_pos"]
        score += w["bb_pos"]

    # 8. ADX: trend strength > 25
    if c["adx"] > 25:
        breakdown["adx"] = w["adx"]
        score += w["adx"]
    elif c["adx"] > 20:
        breakdown["adx"] = w["adx"] // 2
        score += w["adx"] // 2

    # Penalty: RSI overbought
    if c["rsi"] > 75:
        score = int(score * 0.7)
        breakdown["penalty_overbought"] = True

    # Penalty: price below EMA50 (weak trend)
    if c["close"] < c["ema50"]:
        score = int(score * 0.8)
        breakdown["penalty_below_ema50"] = True

    return min(100, max(0, score)), breakdown

# ================= PENNY STOCK =================
def is_penny_stock(price: float) -> bool:
    return price < 5.0

# ================= ACCUMULATION =================
def pre_breakout_detection(df: pd.DataFrame) -> bool:
    if len(df) < 20:
        return False
    is_squeezing  = df['bandwidth'].iloc[-1] < df['bandwidth'].iloc[-10] * 0.7
    approaching   = df['close'].iloc[-1] > df['upper_band'].iloc[-1] * 0.97
    volume_surge  = df['volume'].iloc[-3:].mean() > df['volume'].rolling(20).mean().iloc[-1] * 1.5
    return is_squeezing and approaching and volume_surge

# ================= BACKTESTING (NEW) =================
def simple_backtest(symbol: str, df: pd.DataFrame) -> dict:
    """
    Walk-forward backtest on historical data.
    Returns summary stats to validate the strategy before going live.
    """
    if df is None or len(df) < 80:
        return {}

    trades      = []
    in_trade    = False
    entry_price = 0.0
    entry_idx   = 0
    tp_mult     = 2.0
    sl_mult     = 2.0

    for i in range(50, len(df)):
        window = df.iloc[:i]
        score_val, _ = score_signal(window)
        price = df['close'].iloc[i]
        atr   = df['atr'].iloc[i]
        if pd.isna(atr) or atr == 0:
            continue

        if not in_trade and score_val >= MIN_SCORE_REGULAR:
            entry_price = price
            tp_price    = price + atr * tp_mult
            sl_price    = price - atr * sl_mult
            in_trade    = True
            entry_idx   = i

        elif in_trade:
            if price >= tp_price:
                trades.append({"pnl": tp_price - entry_price, "bars": i - entry_idx, "result": "win"})
                in_trade = False
            elif price <= sl_price:
                trades.append({"pnl": sl_price - entry_price, "bars": i - entry_idx, "result": "loss"})
                in_trade = False
            # Max hold: 20 bars
            elif i - entry_idx >= 20:
                trades.append({"pnl": price - entry_price, "bars": 20, "result": "timeout"})
                in_trade = False

    if not trades:
        return {"total_trades": 0, "win_rate": 0, "avg_pnl": 0}

    wins     = sum(1 for t in trades if t["result"] == "win")
    total    = len(trades)
    avg_pnl  = sum(t["pnl"] for t in trades) / total
    win_rate = wins / total * 100

    return {
        "symbol":       symbol,
        "total_trades": total,
        "wins":         wins,
        "losses":       total - wins,
        "win_rate":     round(win_rate, 1),
        "avg_pnl":      round(avg_pnl, 4),
        "profitable":   avg_pnl > 0,
    }

# ================= TRADING HALTS =================
def check_trading_halt(symbol: str) -> tuple:
    try:
        rss_url  = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
        response = requests.get(rss_url, timeout=10)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            for item in root.findall('.//item'):
                title       = item.find('title').text or ''
                description = item.find('description').text or ''
                if symbol in title or symbol in description:
                    price_match  = re.search(r'PauseThresholdPrice:\s*\$?([0-9]+\.[0-9]+)', description)
                    halt_price   = price_match.group(1) if price_match else "N/A"
                    time_match   = re.search(r'(\d{1,2}:\d{2}:\d{2})\s*ET', description)
                    halt_time    = time_match.group(1) if time_match else "N/A"
                    reason_match = re.search(r'Reason:\s*(.+?)(?:\.|$)', description)
                    reason       = reason_match.group(1) if reason_match else "Trading Pause"
                    return True, datetime.now().strftime('%Y-%m-%d'), halt_time, reason, halt_price
    except Exception as e:
        logger.warning(f"Halt check error: {e}")
    return False, None, None, None, None

# ================= SIGNAL CONTROL =================
def is_seen(symbol: str) -> bool:
    now = time.time()
    with state_lock:
        ts = state["seen_signals"].get(symbol)
        return ts is not None and now - ts < SIGNAL_COOLDOWN

def mark_seen(symbol: str):
    with state_lock:
        state["seen_signals"][symbol] = time.time()
        save_state()

def is_halt_alerted(symbol: str) -> bool:
    now = time.time()
    with state_lock:
        ts = state["halted_alerts"].get(symbol)
        return ts is not None and now - ts < 3600

def mark_halt_alerted(symbol: str):
    with state_lock:
        state["halted_alerts"][symbol] = time.time()
        save_state()

def should_send_accumulation_alert(symbol: str, new_score: int) -> bool:
    with state_lock:
        last = state["last_accumulation"].get(symbol)
        if not last:
            return True
        if abs(new_score - last.get("score", 0)) >= 10:
            return True
        if time.time() - last.get("time", 0) > 4 * 3600:
            return True
    return False

def update_accumulation_alert(symbol: str, new_score: int):
    with state_lock:
        state["last_accumulation"][symbol] = {"score": new_score, "time": time.time()}
        save_state()

# ================= MARKET HOURS =================
def get_market_session() -> tuple[str, str]:
    tz   = pytz.timezone("US/Eastern")
    now  = datetime.now(tz)
    hour = now.hour
    if 9 <= hour < 16:
        return "REGULAR HOURS", "🟢 تداول نشط"
    elif 4 <= hour < 9:
        return "PRE-MARKET", "🟡 تحليل فقط"
    else:
        return "AFTER-HOURS", "🟡 تحليل فقط"

# ================= RISK MANAGEMENT =================
def can_trade(price: float, atr: float) -> tuple[bool, int]:
    reset_daily_loss_if_needed()
    with state_lock:
        if len(state["open_trades"]) >= MAX_OPEN_TRADES:
            return False, 0
        if state["daily_loss"] >= DAILY_LOSS_LIMIT:
            return False, 0

    risk_amount   = CAPITAL * RISK_PER_TRADE
    stop_distance = atr * 2 if atr > 0 else price * 0.02
    size          = int(risk_amount / stop_distance)

    if size < 1:
        return False, 0

    # Cap at 20% of capital per trade
    if size * price > CAPITAL * 0.2:
        size = int(CAPITAL * 0.2 / price)

    return (size >= 1), max(1, size)

# ================= CHART =================
def send_chart(symbol: str, df: pd.DataFrame, entry: float, tp: float, sl: float,
               is_penny: bool, is_accumulating: bool, session: str, breakdown: dict):
    if not bot:
        return
    buf = generate_chart(symbol, df, entry, tp, sl, is_penny, is_accumulating, session, breakdown)
    if buf is None:
        return
    caption = f"📊 *{symbol}*\n💰 Entry: ${entry:.2f}  🎯 TP: ${tp:.2f}  🛑 SL: ${sl:.2f}"
    if is_penny:
        caption += "\n🔥 *Penny Stock*"
    if is_accumulating:
        caption += "\n📦 *Accumulation Detected*"
    try:
        bot.send_photo(chat_id=CHAT_ID, photo=buf, caption=caption, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"send_photo error: {e}")

# ================= TRADE MANAGEMENT =================
def update_positions():
    """Check open trades against cached prices."""
    with cache_lock:
        cache_snapshot = dict(data_cache)

    for symbol, trade in list(state["open_trades"].items()):
        df = cache_snapshot.get(symbol)
        if df is None:
            continue
        price = df["close"].iloc[-1]
        if price >= trade["tp"] or price <= trade["sl"]:
            close_trade(symbol, price)

# ================= CACHE BUILD (THREAD-SAFE) =================
def build_cache(symbols: list, period: str = '5d', interval: str = '15m'):
    """Fetch and compute indicators for all symbols, then swap cache atomically."""
    new_cache = {}
    for symbol in symbols:
        df = fetch_yfinance_data(symbol, period, interval)
        if df is not None and len(df) >= 50:
            new_cache[symbol] = compute_indicators(df)
        time.sleep(0.2)

    with cache_lock:
        data_cache.clear()
        data_cache.update(new_cache)

    logger.info(f"Cache refreshed: {len(data_cache)} symbols")

# ================= SCAN =================
def quick_filter_stocks(symbols: list) -> list:
    promising = []
    for i, symbol in enumerate(symbols):
        try:
            quick = fetch_yfinance_quick(symbol)
            if quick:
                price    = quick['price']
                volume   = quick['volume']
                avg_vol  = quick['avg_volume']
                change   = quick['change_pct']
                # Filter: price range, volume activity, positive momentum
                if (1.0 < price < 200 and
                        volume > 150_000 and
                        volume > avg_vol * 1.1 and
                        change > -3):
                    promising.append(symbol)
            if (i + 1) % 50 == 0:
                logger.info(f"Quick filter: {i+1}/{len(symbols)}")
        except Exception:
            pass
        time.sleep(0.1)
    logger.info(f"Quick filter: {len(promising)} / {len(symbols)} passed")
    return promising[:100]

def deep_analysis(symbols: list) -> list:
    results = []
    for symbol in symbols:
        if is_seen(symbol):
            continue
        with cache_lock:
            df = data_cache.get(symbol)

        if df is None:
            df = fetch_yfinance_data(symbol, '5d', '15m')
            if df is None or len(df) < 50:
                continue
            df = compute_indicators(df)
            with cache_lock:
                data_cache[symbol] = df

        score_val, breakdown = score_signal(df)
        if score_val >= MIN_SCORE_REGULAR - 10:
            results.append((symbol, score_val, breakdown, df))
        time.sleep(0.05)

    return sorted(results, key=lambda x: x[1], reverse=True)

def scan_large_universe():
    session, _ = get_market_session()
    logger.info(f"🔄 Scan started ({len(dynamic_universe)} symbols) [{session}]")

    # Step 1: quick filter
    promising = quick_filter_stocks(dynamic_universe)
    if not promising:
        logger.warning("No promising stocks from quick filter")
        return

    # Step 2: build cache for filtered list
    build_cache(promising)

    # Step 3: deep analysis
    top_signals = deep_analysis(promising)
    logger.info(f"Top signals: {len(top_signals)}")

    # Step 4: process top 5
    for symbol, score_val, breakdown, df in top_signals[:5]:
        price     = df["close"].iloc[-1]
        atr       = df["atr"].iloc[-1]
        is_penny  = is_penny_stock(price)
        threshold = MIN_SCORE_PENNY if is_penny else MIN_SCORE_REGULAR

        if score_val < threshold or is_seen(symbol):
            continue
        if pd.isna(atr) or atr <= 0:
            atr = price * 0.02

        # Halt check
        halted, *halt_info = check_trading_halt(symbol)
        if halted and not is_halt_alerted(symbol):
            send_telegram(f"🚨 *TRADING HALT*: {symbol} — {halt_info[2]}")
            mark_halt_alerted(symbol)
            continue

        is_acc, acc_score = detect_accumulation(df)
        ok, size          = can_trade(price, atr)

        if ok:
            open_trade(symbol, price, atr, score_val, size, df, is_penny, is_acc, breakdown)
            mark_seen(symbol)

# ================= TELEGRAM =================
def generate_analysis_message(symbol: str) -> str:
    df = fetch_yfinance_data(symbol, '10d', '15m')
    if df is None or len(df) < 50:
        return f"⚠️ No sufficient data for {symbol}"

    df        = compute_indicators(df)
    session, _= get_market_session()
    score_val, breakdown = score_signal(df)
    price     = df["close"].iloc[-1]
    atr       = df["atr"].iloc[-1]
    volume    = df["volume"].iloc[-1]
    avg_vol   = df["vol_ma"].iloc[-1]
    rsi       = df["rsi"].iloc[-1]
    adx       = df["adx"].iloc[-1]
    is_penny  = is_penny_stock(price)
    is_acc, acc_score    = detect_accumulation(df)
    pre_bo    = pre_breakout_detection(df)

    # Backtest summary
    bt = simple_backtest(symbol, df)
    bt_line = ""
    if bt.get("total_trades", 0) > 0:
        bt_line = (f"🔬 Backtest: {bt['total_trades']} trades | "
                   f"WR {bt['win_rate']}% | Avg PnL ${bt['avg_pnl']:.3f}\n")

    breakdown_str = " | ".join(f"{k}:{v}" for k, v in breakdown.items()
                               if isinstance(v, (int, float)))

    msg  = f"📊 *{symbol} Analysis* ({session})\n"
    msg += f"💰 Price:   ${price:.2f}\n"
    msg += f"📊 Volume:  {volume:,.0f}  (avg {avg_vol:,.0f})\n"
    msg += f"🎢 ATR:     ${atr:.2f}\n"
    msg += f"⚡ RSI:     {rsi:.1f}\n"
    msg += f"💪 ADX:     {adx:.1f}\n"
    msg += f"🧠 Score:   {score_val}/100\n"
    msg += f"📐 Detail:  {breakdown_str}\n"
    msg += bt_line
    if is_penny:     msg += "🔥 *Penny Stock*\n"
    if is_acc:       msg += f"📦 *Accumulation* ({acc_score}%)\n"
    if pre_bo:       msg += "⚡ *Pre-Breakout Detected*\n"
    return msg

# ================= COMMANDS =================
@bot.message_handler(commands=['start'])
def cmd_start(message):
    session, session_msg = get_market_session()
    welcome = (
        "🚀 *AI Trading Bot v19*\n\n"
        f"*Session:* {session}\n{session_msg}\n\n"
        "*Commands:*\n"
        "/scan \\<symbol\\> — Technical analysis + backtest\n"
        "/status — Bot overview\n"
        "/positions — Open trades\n"
        "/performance — Win/loss stats\n"
        "/backtest \\<symbol\\> — Run backtest only\n"
        "/close \\<symbol\\> — Manually close trade\n"
    )
    bot.reply_to(message, welcome, parse_mode='Markdown')

@bot.message_handler(commands=['scan'])
def cmd_scan(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "⚠️ Usage: /scan <SYMBOL>")
        return
    symbol = parts[1].upper().strip()
    msg_obj = bot.reply_to(message, f"🔍 Analyzing {symbol}…")

    def do_analysis():
        analysis = generate_analysis_message(symbol)
        try:
            bot.edit_message_text(chat_id=message.chat.id,
                                  message_id=msg_obj.message_id,
                                  text=analysis, parse_mode='Markdown')
        except Exception:
            bot.send_message(chat_id=message.chat.id, text=analysis, parse_mode='Markdown')

    future = executor.submit(do_analysis)
    try:
        future.result(timeout=25)
    except TimeoutError:
        bot.edit_message_text(chat_id=message.chat.id,
                              message_id=msg_obj.message_id,
                              text="⚠️ Timeout. Try again.")

@bot.message_handler(commands=['backtest'])
def cmd_backtest(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "⚠️ Usage: /backtest <SYMBOL>")
        return
    symbol  = parts[1].upper().strip()
    msg_obj = bot.reply_to(message, f"🔬 Running backtest for {symbol}…")

    def do_bt():
        df = fetch_yfinance_data(symbol, '60d', '1h')
        if df is None or len(df) < 80:
            bot.edit_message_text(chat_id=message.chat.id,
                                  message_id=msg_obj.message_id,
                                  text="⚠️ Not enough data for backtest (need 60d 1h).")
            return
        df  = compute_indicators(df)
        bt  = simple_backtest(symbol, df)
        if not bt or bt.get("total_trades", 0) == 0:
            text = f"⚠️ No trades triggered for {symbol} in backtest period."
        else:
            emoji = "✅" if bt["profitable"] else "❌"
            text  = (
                f"🔬 *Backtest: {symbol}*\n"
                f"📊 Trades:   {bt['total_trades']}\n"
                f"✅ Wins:     {bt['wins']}\n"
                f"❌ Losses:   {bt['losses']}\n"
                f"📈 Win Rate: {bt['win_rate']}%\n"
                f"💰 Avg PnL:  ${bt['avg_pnl']:.4f}\n"
                f"{emoji} Strategy: {'PROFITABLE' if bt['profitable'] else 'UNPROFITABLE'}"
            )
        bot.edit_message_text(chat_id=message.chat.id,
                              message_id=msg_obj.message_id,
                              text=text, parse_mode='Markdown')

    future = executor.submit(do_bt)
    try:
        future.result(timeout=60)
    except TimeoutError:
        bot.edit_message_text(chat_id=message.chat.id,
                              message_id=msg_obj.message_id,
                              text="⚠️ Backtest timed out.")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    reset_daily_loss_if_needed()
    session, session_msg = get_market_session()
    with state_lock:
        total_wins   = sum(p["wins"]   for p in state["performance"].values())
        total_losses = sum(p["losses"] for p in state["performance"].values())
        total        = total_wins + total_losses
        winrate      = (total_wins / total * 100) if total else 0
        total_pnl    = sum(p.get("total_pnl", 0) for p in state["performance"].values())
        open_count   = len(state["open_trades"])
        daily_loss   = state["daily_loss"]

    msg = (
        f"📊 *Bot Status* ({session})\n"
        f"{session_msg}\n\n"
        f"💰 Capital:     ${CAPITAL:,.0f}\n"
        f"📈 Open Trades: {open_count}/{MAX_OPEN_TRADES}\n"
        f"✅ Wins:        {total_wins}\n"
        f"❌ Losses:      {total_losses}\n"
        f"📈 Win Rate:    {winrate:.1f}%\n"
        f"💵 Total PnL:   ${total_pnl:+.2f}\n"
        f"📉 Daily Loss:  ${daily_loss:.2f} / ${DAILY_LOSS_LIMIT}\n"
        f"🌐 Universe:    {len(dynamic_universe)} stocks\n"
        f"📦 Cached:      {len(data_cache)} symbols"
    )
    bot.reply_to(message, msg, parse_mode='Markdown')

@bot.message_handler(commands=['positions'])
def cmd_positions(message):
    with state_lock:
        trades = dict(state["open_trades"])
    if not trades:
        bot.reply_to(message, "📭 No open positions")
        return
    lines = ["*Open Positions*\n"]
    for sym, t in trades.items():
        age = int((time.time() - t["time"]) / 60)
        lines.append(
            f"🔹 *{sym}*\n"
            f"   Entry ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f}\n"
            f"   Shares {t['size']} | Age {age}m | Score {t['score']}"
        )
    bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')

@bot.message_handler(commands=['performance'])
def cmd_performance(message):
    with state_lock:
        perf = dict(state["performance"])
    if not perf:
        bot.reply_to(message, "📭 No completed trades yet")
        return
    lines = ["*Per-Symbol Performance*\n"]
    for sym, p in sorted(perf.items(), key=lambda x: x[1].get("total_pnl", 0), reverse=True):
        total = p["wins"] + p["losses"]
        wr    = (p["wins"] / total * 100) if total else 0
        lines.append(
            f"*{sym}*: {p['wins']}W / {p['losses']}L "
            f"({wr:.1f}%) | PnL ${p.get('total_pnl', 0):+.2f}"
        )
    bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')

@bot.message_handler(commands=['close'])
def cmd_close(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "⚠️ Usage: /close <SYMBOL>")
        return
    symbol = parts[1].upper().strip()
    with state_lock:
        if symbol not in state["open_trades"]:
            bot.reply_to(message, f"📭 No open trade for {symbol}")
            return
    df = fetch_yfinance_data(symbol, '1d', '5m')
    if df is None or df.empty:
        bot.reply_to(message, f"⚠️ Cannot fetch current price for {symbol}")
        return
    price = df["close"].iloc[-1]
    close_trade(symbol, price)
    bot.reply_to(message, f"✅ Manually closed *{symbol}* at ${price:.2f}", parse_mode='Markdown')

# ================= BACKGROUND SCANNER =================
def home():
    return f"AI Trading Bot v19 | Cache: {len(data_cache)} | Trades: {len(state['open_trades'])}"

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = telebot.types.Update.de_json(request.data.decode("utf-8"))
        bot.process_new_updates([update])
        return "ok", 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return "error", 500

@app.route("/health", methods=['GET'])
def health():
    return json.dumps({
        "status":      "ok",
        "cache_size":  len(data_cache),
        "open_trades": len(state["open_trades"]),
        "daily_loss":  state["daily_loss"],
        "market_open": is_market_open(),
    })

# ================= ENTRY POINT =================
if __name__ == "__main__":
    load_state()
    reset_daily_loss_if_needed()
    threading.Thread(target=background_scanner, daemon=True).start()
    if bot:
        threading.Thread(
            target=lambda: bot.infinity_polling(timeout=10, long_polling_timeout=5),
            daemon=True
        ).start()
    send_telegram("✅ *AI Trading Bot v19 is LIVE!*\nImprovements: MACD + ADX + Backtest + Thread-safe cache")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

# ================= MERGED FROM BLOCKS =================

matplotlib.use('Agg')

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

def score_stock(df, vol_surge_mult=2.0):
    if df is None or len(df) < 20:
        return 0, {}
    last = df.iloc[-1]
    vol_surge = last['volume'] > df['vol_ma'].iloc[-1] * vol_surge_mult
    price_break = last['close'] > df['high'].iloc[-20:-1].max()
    score = 0
    if vol_surge: score += 40
    if price_break: score += 40
    if 40 < last['rsi'] < 70: score += 10
    if last['ema9'] > last['ema21']: score += 10
    return score, {"price": last['close']}

# ================= CHART GENERATION =================
def run_backtest(symbol):
    try:
        df = yf.download(symbol, period="60d", interval="1h", progress=False)
        if df.empty or len(df) < 50:
            return None
        df.columns = [c.lower() for c in df.columns]
        df = compute_indicators(df)
        trades = []
        in_trade = False
        entry_price = 0
        for i in range(50, len(df)):
            window = df.iloc[:i]
            last = window.iloc[-1]
            if not in_trade:
                score, _ = score_stock(window, 2.0)
                if score >= 70:
                    entry_price = last['close']
                    in_trade = True
            else:
                if last['close'] >= entry_price * TP_PCT:
                    trades.append({"pnl": (last['close'] - entry_price) / entry_price * 100, "result": "win"})
                    in_trade = False
                elif last['close'] <= entry_price * SL_PCT:
                    trades.append({"pnl": (last['close'] - entry_price) / entry_price * 100, "result": "loss"})
                    in_trade = False
        if not trades: return None
        wins = sum(1 for t in trades if t["result"] == "win")
        total_pnl = sum(t["pnl"] for t in trades)
        return {
            "total_trades": len(trades),
            "wins": wins,
            "losses": len(trades) - wins,
            "win_rate": round(wins / len(trades) * 100, 1),
            "avg_pnl": round(total_pnl / len(trades), 2),
            "total_pnl": round(total_pnl, 2)
        }
    except Exception as e:
        logger.error(f"Backtest error: {e}")
        return None

# ================= PROCESS SYMBOL =================
def peak_detector_for_sell(df):
    """🔴 كاشف الذروة للبيع - توصية للخروج بعد الصعود"""
    try:
        if len(df) < 5:
            return False
        
        last = df.iloc[-1]
        rsi = last['rsi']
        bb_width = last['bandwidth']
        prev_bb_width = df['bandwidth'].iloc[-2]
        
        # RSI متخم (> 75) والباند بدأ يضيق
        if rsi > 75 and bb_width < prev_bb_width:
            return True
        
        return False
    except:
        return False

def get_fallback_from_github():
    try:
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        response = urllib.request.urlopen(url, timeout=15)
        tickers = response.read().decode('utf-8').splitlines()
        clean = [t.strip().upper() for t in tickers if t.strip() and len(t.strip()) <= 5 and t.strip().isalpha()]
        clean = list(dict.fromkeys(clean))
        logger.info(f"GitHub fallback: {len(clean)} unique symbols")
        return clean[:2000]
    except Exception as e:
        logger.error(f"GitHub fallback error: {e}")
        return []

MINIMAL_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD", "NFLX",
    "INTC", "PLTR", "SOFI", "NIO", "GME", "AMC", "RIOT", "MARA", "COIN", "HOOD",
    "MSTR", "AFRM", "UPST", "SNDL", "TLRY", "ACB", "BYND", "PLUG", "FCEL", "SPCE"
]

def safe_yf_download(symbol, period, interval=None):
    if interval:
        return yf.Ticker(symbol).history(period=period, interval=interval)
    else:
        return yf.download(symbol, period=period, progress=False)

# ================= FAST FILTER =================
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

# ================= MERGED FROM BLOCKS =================

matplotlib.use('Agg')

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

# ================= MERGED FROM BLOCKS =================

matplotlib.use(\'Agg\')

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
STATE_DIR = os.getenv("STATE_DIR", ".")

# Trading Parameters
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02  # 2% risk per trade
MAX_OPEN_TRADES = 15
SIGNAL_COOLDOWN = 3600
DAILY_LOSS_LIMIT = 300.0
STATE_FILE = os.path.join(STATE_DIR, "state_v27.json")
SCAN_INTERVAL_SEC = 300  # 5 minutes
TRADE_MONITOR_INTERVAL = 60

# Strategy Parameters
TP_PCT = 1.05  # 5% Target
SL_PCT = 0.97  # 3% Stop Loss

# Fast Filter Thresholds
MIN_PRICE = 1.0
MAX_PRICE = 200.0
MIN_VOLUME = 200000

# ================= MARKET PHASE SETTINGS =================
PHASE_SETTINGS = {
    "PRE": {
        "min_score": 80,
        "size_multiplier": 0.5,
        "vol_surge_mult": 2.5,
        "description": "🟡 Pre-Market - High risk, low liquidity"
    },
    "REGULAR": {
        "min_score": 70,
        "size_multiplier": 1.0,
        "vol_surge_mult": 2.0,
        "description": "🟢 Regular Hours - Normal trading"
    },
    "AFTER": {
        "min_score": 85,
        "size_multiplier": 0.3,
        "vol_surge_mult": 3.0,
        "description": "🔵 After-Hours - Very high risk"
    },
    "CLOSED": {
        "min_score": 999,
        "size_multiplier": 0,
        "vol_surge_mult": 0,
        "description": "⚫ Market Closed"
    }
}

logging.basicConfig(
    level=logging.INFO,
    format=\'%(asctime)s - %(levelname)s - %(message)s\',
    handlers=[
        logging.FileHandler("bot_v27.log"),
        logging.StreamHandler()
    ]
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
    "last_accumulation": {},
    "daily_loss": 0.0,
    "last_reset": None,
    "tickers": [],
    "last_ticker_update": None
}

EASTERN_TZ = pytz.timezone("US/Eastern")

def run_engine():
    startup_msg = "🚀 <b>AI Institutional Ultra Pro v6.0 Started</b>\n\n✅ Advanced Indicators\n✅ Dynamic Risk Management\n✅ Market Hours Filtering\n✅ Railway Deployment Ready\n✅ Polling Active"
    send(startup_msg)
    seen_signals = deque(maxlen=100)
    while True:
        try:
            if not is_market_open():
                logger.info("Market is closed. Waiting...")
                time.sleep(300)
                continue
            universe = get_universe()
            now_utc = datetime.now(pytz.utc)
            for symbol in universe:
                # Fixed seen_signals logic using UTC comparison
                if any(s["symbol"] == symbol and (now_utc - s["timestamp"]).total_seconds() < 7200 for s in seen_signals):
                    continue
                r = analyze(symbol)
                if r:
                    seen_signals.append({"symbol": r["symbol"], "timestamp": now_utc})
                    p = r["params"]
                    reasons_str = "\n".join([f"🔹 {reason}" for reason in r["reasons"]])
                    msg = f"🚨 <b>AI ULTRA PRO SIGNAL</b> 🚨\n\n📊 <b>Ticker:</b> #{r['symbol']}\n⭐ <b>Score:</b> {r['score']}/100\n⏰ <b>Time:</b> {r['time']}\n\n💰 <b>Entry:</b> ${p['entry']:.2f}\n\n🎯 <b>Targets:</b>\nT1: ${p['t1']:.2f}\nT2: ${p['t2']:.2f}\nT3: ${p['t3']:.2f}\n\n🛑 <b>Stop Loss:</b> ${p['sl']:.2f}\n\n⚖️ <b>R/R:</b> 1:{p['risk_reward']}\n📦 <b>Size:</b> {p['shares']} shares\n\n🧠 <b>Confluence:</b>\n{reasons_str}"
                    send(msg)
                    time.sleep(12) # Respect Polygon Free Tier (5 calls/min)
            time.sleep(300)
        except Exception as e:
            logger.error(f"Error in engine: {e}")
            time.sleep(60)

if __name__ == "__main__":
    # Start the engine in a separate thread
    engine_thread = threading.Thread(target=run_engine, daemon=True)
    engine_thread.start()
    
    # Start Telegram Polling in the main thread
    logger.info("Starting Telegram Polling...")
    try:
        bot.infinity_polling()
    except Exception as e:
        logger.error(f"Polling error: {e}")
        time.sleep(10)

# ================= MERGED FROM BLOCKS =================

matplotlib.use('Agg')

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger()

# ================= OPENAI INIT (اختياري - لا يوقف البوت إذا لم يوجد مفتاح) =================
try:
    client = OpenAI()
    OPENAI_AVAILABLE = True
    logger.info("OpenAI client initialized successfully")
except Exception as e:
    logger.warning(f"OpenAI not available (no API key): {e}")
    client = None
    OPENAI_AVAILABLE = False

# ================= WEBULL SETUP =================
def volatility_swing_scanner():
    """يراقب الأسهم المتذبذبة جداً (مثل WOK) ويرسل تنبيهات شراء عند القاع وبيع عند القمة"""
    try:
        # قائمة الأسهم التي نراقب تذبذبها (يمكنك إضافة المزيد هنا)
        VOLATILE_WATCHLIST = ["WOK", "SMX", "AIXI", "SKK"]
        
        for symbol in VOLATILE_WATCHLIST:
            df = cached_download(symbol, period="1d", interval="5m")
            if df.empty or len(df) < 20:
                continue
            
            df.columns = [c.lower() for c in df.columns]
            # حساب RSI على فريم 5 دقائق
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss.replace(0, 0.001)
            rsi = 100 - (100 / (1 + rs))
            current_rsi = rsi.iloc[-1]
            price = df['close'].iloc[-1]
            
            signal_key = f"swing_{symbol}"
            with state_lock:
                last_action = state.get("seen_signals", {}).get(signal_key, "NONE")
            
            # 🟢 إشارة شراء (Dip): RSI تحت 30 (تشبع بيعي)
            if current_rsi < 30 and last_action != "BUY":
                msg = f"📉 *فرصة شراء (Buy the Dip): {symbol}*\n"
                msg += f"━━━━━━━━━━━━━━━━\n"
                msg += f"💰 السعر الحالي: ${price:.2f}\n"
                msg += f"📉 RSI: {current_rsi:.1f} (تشبع بيعي شديد)\n"
                msg += f"💡 السهم نزل كثير ومن المتوقع يرتد، فكر في الشراء الآن!"
                send_telegram(msg)
                with state_lock:
                    state.setdefault("seen_signals", {})[signal_key] = "BUY"
                    save_state()
                    
            # 🔴 إشارة بيع (Rip): RSI فوق 70 (تشبع شرائي)
            elif current_rsi > 70 and last_action != "SELL":
                msg = f"📈 *فرصة بيع (Sell the Rip): {symbol}*\n"
                msg += f"━━━━━━━━━━━━━━━━\n"
                msg += f"💰 السعر الحالي: ${price:.2f}\n"
                msg += f"📈 RSI: {current_rsi:.1f} (تشبع شرائي)\n"
                msg += f"💡 السهم ارتفع بقوة، فكر في جني الأرباح الآن!"
                send_telegram(msg)
                with state_lock:
                    state.setdefault("seen_signals", {})[signal_key] = "SELL"
                    save_state()
                    
    except Exception as e:
        logger.error(f"Volatility swing scanner error: {e}")

def check_halt(symbol, df):
    if df is None or len(df) < 5:
        return
    
    current_time = df.index[-1]
    last_price = df["close"].iloc[-1]
    prev_price = df["close"].iloc[-2]
    
    # If the last data point is older than 5 minutes during market hours, it might be halted
    # Or if volume is 0 and price is unchanged for several minutes
    time_diff = (datetime.now(pytz.utc) - current_time).total_seconds()
    
    is_currently_halted = time_diff > 300 # 5 minutes of no data
    
    if symbol not in halt_status:
        halt_status[symbol] = False
        
    if is_currently_halted and not halt_status[symbol]:
        halt_status[symbol] = True
        direction = "UP 📈" if last_price > prev_price else "DOWN 📉"
        msg = f"⚠️ <b>TRADING HALT DETECTED</b> ⚠️\n\n📊 <b>Ticker:</b> #{symbol}\n💰 <b>Halt Price:</b> ${last_price:.2f}\n⚡ <b>Direction:</b> {direction}\n⏰ <b>Time:</b> {datetime.now(pytz.timezone('US/Eastern')).strftime('%H:%M:%S EST')}\n\n<i>Waiting for resume...</i>"
        send(msg)
        
    elif not is_currently_halted and halt_status[symbol]:
        halt_status[symbol] = False
        msg = f"✅ <b>TRADING RESUMED</b> ✅\n\n📊 <b>Ticker:</b> #{symbol}\n💰 <b>Current Price:</b> ${last_price:.2f}\n⏰ <b>Time:</b> {datetime.now(pytz.timezone('US/Eastern')).strftime('%H:%M:%S EST')}\n\n🚀 <i>Watch for explosive movement!</i>"
        send(msg)

# ================= INDICATORS =================
def check_nasdaq_halts():
    url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
    ns = {'ndaq': 'http://www.nasdaqtrader.com/'}
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return
            
        root = ET.fromstring(response.content)
        for item in root.findall('.//item'):
            symbol = item.find('ndaq:IssueSymbol', ns).text
            halt_time = item.find('ndaq:HaltTime', ns).text
            reason = item.find('ndaq:ReasonCode', ns).text
            resumption_time = item.find('ndaq:ResumptionTradeTime', ns).text
            
            halt_id = f"{symbol}_{halt_time}_{reason}"
            
            if halt_id not in processed_halts:
                processed_halts.add(halt_id)
                
                if not resumption_time or resumption_time.strip() == "":
                    # It's a new Halt
                    msg = (f"⚠️ <b>NASDAQ OFFICIAL HALT</b> ⚠️\n\n"
                           f"📊 <b>Ticker:</b> #{symbol}\n"
                           f"🕒 <b>Halt Time:</b> {halt_time}\n"
                           f"🛑 <b>Reason:</b> {reason}\n"
                           f"⏰ <b>Detected:</b> {datetime.now(pytz.timezone('US/Eastern')).strftime('%H:%M:%S EST')}\n\n"
                           f"<i>Monitoring for resumption...</i>")
                    send(msg)
                else:
                    # It's a Resumption
                    msg = (f"✅ <b>TRADING RESUMED (NASDAQ)</b> ✅\n\n"
                           f"📊 <b>Ticker:</b> #{symbol}\n"
                           f"🕒 <b>Resume Time:</b> {resumption_time}\n"
                           f"🛑 <b>Original Reason:</b> {reason}\n"
                           f"🚀 <i>Watch for high volatility!</i>")
                    send(msg)
                    
    except Exception as e:
        logger.error(f"Error checking NASDAQ halts: {e}")

# ================= MARKET CRASH PROTECTION =================
def check_market_crash():
    # Check SPY for sudden drops
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute/{ (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') }/{ datetime.now().strftime('%Y-%m-%d') }?apiKey={POLYGON_API}"
    try:
        r = requests.get(url, timeout=10).json()
        if "results" in r and len(r["results"]) > 5:
            df = pd.DataFrame(r["results"])
            drop = (df['c'].iloc[-1] - df['c'].iloc[-5]) / df['c'].iloc[-5] * 100
            if drop < -1.5: # 1.5% drop in 5 minutes
                msg = (f"🚨 <b>MARKET CRASH WARNING</b> 🚨\n\n"
                       f"📉 <b>SPY Drop:</b> {drop:.2f}% in 5 mins\n"
                       f"⚠️ <b>Action:</b> Consider closing all long positions immediately!\n"
                       f"🛡️ <i>Safety First Mode Active.</i>")
                send(msg)
    except Exception as e:
        logger.error(f"Error checking market crash: {e}")

# ================= POLYGON DATA & INDICATORS =================
data_cache = {}

def get_news(symbol):
    url = f"https://api.polygon.io/v2/reference/news?ticker={symbol}&limit=1&apiKey={POLYGON_API}"
    try:
        r = requests.get(url, timeout=10).json()
        if "results" in r and len(r["results"]) > 0:
            news = r["results"][0]
            return f"📰 <b>Latest News:</b> {news['title']}\n🔗 <a href='{news['article_url']}'>Read More</a>"
    except:
        pass
    return "📰 <b>News:</b> No recent news found."

# ================= MAIN ENGINE =================
def get_market_sentiment():
    """Check SPY to see if the overall market is crashing"""
    try:
        df = fetch_data("SPY", limit=20)
        if df is not None:
            current_price = df['close'].iloc[-1]
            sma20 = df['close'].rolling(window=20).mean().iloc[-1]
            if current_price < sma20 * 0.98: # 2% below SMA20
                return "CRASH_PROTECTION"
    except:
        pass
    return "NORMAL"

# ================= DATA FETCHING =================
def explosive_pump_scanner():
    """
    🚀 ماسح الانفجارات المجنونة
    يصطاد الأسهم اللي تقفز 50-200% في 5-15 دقيقة
    معايير قاسية جداً - فقط الانفجارات الحقيقية
    """
    PUMP_MIN_RVOL = 5.0          # حجم نسبي 5x على الأقل
    PUMP_MIN_GAIN = 50.0         # قفزة 50% على الأقل
    PUMP_MAX_PRICE = 5.0         # أسهم رخيصة فقط (Penny Stocks)
    PUMP_MIN_VOL = 1000000       # حجم مطلق مليون على الأقل
    
    while True:
        try:
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(300)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))
            
            if not tickers:
                time.sleep(60)
                continue
            
            # فحص أول 500 سهم فقط (الأسهم النشطة)
            for symbol in tickers[:500]:
                try:
                    # جلب بيانات 5 دقائق (للسرعة)
                    df = safe_download(symbol, period="1d", interval="5m")
                    if df.empty or len(df) < 5:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    
                    # حساب المؤشرات
                    price_now = df['close'].iloc[-1]
                    price_open = df['open'].iloc[0]
                    
                    volume_now = df['volume'].iloc[-1]
                    volume_avg = df['volume'].rolling(20).mean().iloc[-1]
                    
                    # حساب القفزة
                    gain_pct = ((price_now - price_open) / price_open) * 100 if price_open > 0 else 0
                    rvol = (volume_now / volume_avg) if volume_avg > 0 else 1.0
                    
                    # فلاتر قاسية جداً
                    if price_now > PUMP_MAX_PRICE:
                        continue
                    if volume_now < PUMP_MIN_VOL:
                        continue
                    if rvol < PUMP_MIN_RVOL:
                        continue
                    if gain_pct < PUMP_MIN_GAIN:
                        continue
                    
                    # تجنب التكرار (مرة كل ساعة)
                    signal_key = f"pump_{symbol}"
                    with state_lock:
                        last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                    
                    if time.time() - last_sent < 3600:  # ساعة واحدة
                        continue
                    
                    # إرسال التنبيه
                    entry = price_now
                    tp = round(entry * 1.30, 2)  # هدف 30% للانفجار
                    sl = round(entry * 0.95, 2)  # وقف خسارة 5%
                    
                    msg = f"🚀 *EXPLOSIVE PUMP DETECTED!*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"📊 *{symbol}*\n"
                    msg += f"💰 السعر: *${price_now:.4f}*\n"
                    msg += f"📈 القفزة: *+{gain_pct:.1f}%* من الفتح!\n"
                    msg += f"🔥 RVOL: *{rvol:.1f}x* (نشاط ضخم!)\n"
                    msg += f"📊 الحجم: *{int(volume_now):,}*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"⏰ الدخول: *${entry:.4f}*\n"
                    msg += f"🎯 الهدف: *${tp:.4f}* (+30%)\n"
                    msg += f"🛑 الوقف: *${sl:.4f}* (-5%)\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"⚠️ *انفجار قوي جداً - ادخل بحذر وإدارة مخاطر صارمة!*"
                    
                    send_telegram(msg)
                    
                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()
                    
                    logger.info(f"🚀 PUMP: {symbol} +{gain_pct:.1f}% RVOL:{rvol:.1f}x")
                    
                except Exception as e:
                    continue
                
                time.sleep(0.3)
            
        except Exception as e:
            logger.error(f"Explosive pump scanner error: {e}")
        
        time.sleep(300)  # كل 5 دقائق

# ============= PRE-PUMP DETECTION SCANNER =============
# بوت مدمر يصطاد الفرص قبل الانفجار بـ 5-15 دقيقة
# معايير قاسية جداً - فقط الفرص الحقيقية

def detect_silent_accumulation_advanced(df):
    """
    🕵️ كشف التجميع الصامت المتقدم
    عندما يجمع الكبار الأسهم بدون ما يرفعوا السعر كثير
    """
    try:
        if len(df) < 20:
            return False, 0
        
        df.columns = [c.lower() for c in df.columns]
        
        # آخر 10 شمعات
        recent = df.tail(10)
        
        # الحجم يزيد لكن السعر ما يتحرك كثير
        vol_trend = recent['volume'].iloc[-1] > recent['volume'].mean() * 2
        price_range = (recent['high'].max() - recent['low'].min()) / recent['close'].mean() * 100
        
        # حجم عالي مع حركة سعرية صغيرة = تجميع
        if vol_trend and price_range < 2:
            acc_ratio = recent['volume'].iloc[-1] / recent['volume'].mean()
            return True, acc_ratio
        
        return False, 0
    except:
        return False, 0

def detect_squeeze_pressure(df):
    """
    🔫 كشف ضغط الانفجار (Bollinger Bands Squeeze)
    عندما تنضغط الفرقة البولينجر = انفجار قادم
    """
    try:
        if len(df) < 20:
            return False, 0
        
        df.columns = [c.lower() for c in df.columns]
        
        # حساب Bollinger Bands
        sma20 = df['close'].rolling(20).mean()
        std20 = df['close'].rolling(20).std()
        
        bb_upper = sma20 + (std20 * 2)
        bb_lower = sma20 - (std20 * 2)
        bb_width = bb_upper - bb_lower
        
        # آخر 5 شمعات
        recent_width = bb_width.tail(5)
        historical_width = bb_width.tail(20).head(15)
        
        # الفرقة تضيق (Squeeze)
        if recent_width.mean() < historical_width.mean() * 0.5:
            squeeze_strength = 1 - (recent_width.mean() / historical_width.mean())
            return True, squeeze_strength
        
        return False, 0
    except:
        return False, 0

def detect_volume_accumulation_pattern(df):
    """
    📊 كشف نمط تراكم الحجم
    حجم يزيد تدريجياً = تجهيز للانفجار
    """
    try:
        if len(df) < 15:
            return False, 0
        
        df.columns = [c.lower() for c in df.columns]
        
        # آخر 10 شمعات
        recent_vols = df['volume'].tail(10).values
        
        # هل الحجم يزيد تدريجياً؟
        increasing = True
        for i in range(1, len(recent_vols)):
            if recent_vols[i] < recent_vols[i-1] * 0.8:
                increasing = False
                break
        
        if increasing:
            # احسب نسبة الزيادة
            vol_increase = (recent_vols[-1] - recent_vols[0]) / recent_vols[0]
            return True, vol_increase
        
        return False, 0
    except:
        return False, 0

def detect_price_consolidation(df):
    """
    📍 كشف تجميع السعر (Consolidation)
    السعر يتحرك في نطاق ضيق = استعداد للقفز
    """
    try:
        if len(df) < 20:
            return False, 0
        
        df.columns = [c.lower() for c in df.columns]
        
        # آخر 15 شمعة
        recent = df.tail(15)
        
        high = recent['high'].max()
        low = recent['low'].min()
        close = recent['close'].iloc[-1]
        
        # النطاق الضيق
        range_pct = (high - low) / close * 100
        
        # إذا النطاق أقل من 3% = تجميع قوي
        if range_pct < 3:
            consolidation_strength = 1 - (range_pct / 3)
            return True, consolidation_strength
        
        return False, 0
    except:
        return False, 0

def detect_rsi_divergence(df):
    """
    🔄 كشف تباعد RSI
    السعر ينخفض لكن RSI يرتفع = إشارة شراء قوية
    """
    try:
        if len(df) < 30:
            return False
        
        df.columns = [c.lower() for c in df.columns]
        
        # حساب RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / (loss + 1e-9)
        rsi = 100 - (100 / (1 + rs))
        
        # آخر 5 شمعات
        recent_prices = df['close'].tail(5).values
        recent_rsi = rsi.tail(5).values
        
        # السعر ينخفض لكن RSI يرتفع
        price_down = recent_prices[-1] < recent_prices[0]
        rsi_up = recent_rsi[-1] > recent_rsi[0]
        
        if price_down and rsi_up:
            return True
        
        return False
    except:
        return False

def detect_macd_bullish_crossover(df):
    """
    📈 كشف تقاطع MACD الصعودي
    MACD يقطع فوق Signal Line = إشارة شراء
    """
    try:
        if len(df) < 30:
            return False
        
        df.columns = [c.lower() for c in df.columns]
        
        # حساب MACD
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        
        # آخر شمعتين
        macd_prev = macd.iloc[-2]
        signal_prev = signal.iloc[-2]
        macd_now = macd.iloc[-1]
        signal_now = signal.iloc[-1]
        
        # تقاطع صعودي
        if macd_prev < signal_prev and macd_now > signal_now:
            return True
        
        return False
    except:
        return False

def detect_volume_spike_before_move(df):
    """
    ⚡ كشف قفزة حجم قبل الحركة
    حجم ضخم مع سعر ثابت = انفجار قادم
    """
    try:
        if len(df) < 10:
            return False, 0
        
        df.columns = [c.lower() for c in df.columns]
        
        # آخر شمعة
        current_vol = df['volume'].iloc[-1]
        avg_vol = df['volume'].tail(20).mean()
        
        # آخر شمعة
        current_price = df['close'].iloc[-1]
        prev_price = df['close'].iloc[-2]
        
        # حجم عالي جداً مع حركة سعرية صغيرة
        vol_spike = current_vol > avg_vol * 3
        price_stable = abs(current_price - prev_price) / prev_price < 0.01
        
        if vol_spike and price_stable:
            spike_strength = current_vol / avg_vol
            return True, spike_strength
        
        return False, 0
    except:
        return False, 0

def pre_pump_detection_scanner():
    """
    🚀 ماسح الفرص قبل الانفجار
    يصطاد الأسهم قبل ما تنفجر بـ 5-15 دقيقة
    """
    while True:
        try:
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(300)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))
            
            if not tickers:
                time.sleep(60)
                continue
            
            # فحص أول 300 سهم (الأسهم الأكثر نشاطاً)
            for symbol in tickers[:300]:
                try:
                    # جلب بيانات 5 دقائق
                    df = safe_download(symbol, period="1d", interval="5m")
                    if df.empty or len(df) < 20:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    
                    # حساب الإشارات المبكرة
                    is_silent_acc, acc_ratio = detect_silent_accumulation_advanced(df)
                    is_squeeze, squeeze_str = detect_squeeze_pressure(df)
                    is_vol_acc, vol_acc_ratio = detect_volume_accumulation_pattern(df)
                    is_consolidation, consol_str = detect_price_consolidation(df)
                    is_rsi_div = detect_rsi_divergence(df)
                    is_macd_cross = detect_macd_bullish_crossover(df)
                    is_vol_spike, vol_spike_str = detect_volume_spike_before_move(df)
                    
                    # حساب النقاط
                    signals = 0
                    if is_silent_acc: signals += 2
                    if is_squeeze: signals += 2
                    if is_vol_acc: signals += 2
                    if is_consolidation: signals += 1
                    if is_rsi_div: signals += 1
                    if is_macd_cross: signals += 1
                    if is_vol_spike: signals += 2
                    
                    # يجب أن تكون 4 إشارات على الأقل
                    if signals < 4:
                        continue
                    
                    # تجنب التكرار (مرة كل 30 دقيقة)
                    signal_key = f"prepump_{symbol}"
                    with state_lock:
                        last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                    
                    if time.time() - last_sent < 1800:  # 30 دقيقة
                        continue
                    
                    # إرسال التنبيه
                    last = df.iloc[-1]
                    price = last['close']
                    entry = price
                    tp = round(entry * 1.25, 2)  # هدف 25% (قبل الانفجار)
                    sl = round(entry * 0.97, 2)   # وقف خسارة 3%
                    
                    signals_text = []
                    if is_silent_acc: signals_text.append(f"🕵️ تجميع صامت ({acc_ratio:.1f}x)")
                    if is_squeeze: signals_text.append(f"🔫 ضغط انفجار ({squeeze_str:.1%})")
                    if is_vol_acc: signals_text.append(f"📊 تراكم حجم (+{vol_acc_ratio:.0%})")
                    if is_consolidation: signals_text.append(f"📍 تجميع سعر ({consol_str:.1%})")
                    if is_rsi_div: signals_text.append("🔄 تباعد RSI")
                    if is_macd_cross: signals_text.append("📈 تقاطع MACD")
                    if is_vol_spike: signals_text.append(f"⚡ قفزة حجم ({vol_spike_str:.1f}x)")
                    
                    msg = f"🎯 *PRE-PUMP SIGNAL DETECTED!*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"📊 *{symbol}*\n"
                    msg += f"💰 السعر: *${price:.4f}*\n"
                    msg += f"🔥 الإشارات: {signals}/7\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += "\n".join(signals_text)
                    msg += f"\n━━━━━━━━━━━━━━━━\n"
                    msg += f"⏰ الدخول: *${entry:.4f}*\n"
                    msg += f"🎯 الهدف: *${tp:.4f}* (+25%)\n"
                    msg += f"🛑 الوقف: *${sl:.4f}* (-3%)\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"⚠️ *هذه فرصة قبل الانفجار - ادخل بسرعة!*"
                    
                    send_telegram(msg)
                    
                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()
                    
                    logger.info(f"🎯 PRE-PUMP: {symbol} signals:{signals}/7")
                    
                except Exception as e:
                    continue
                
                time.sleep(0.2)
            
        except Exception as e:
            logger.error(f"Pre-pump detection error: {e}")
        
        time.sleep(180)  # كل 3 دقائق

# ============= إضافة في MAIN =============
# threading.Thread(target=pre_pump_detection_scanner, daemon=True).start()

# ================= MERGED FROM BLOCKS =================

matplotlib.use('Agg')

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger()

# ================= OPENAI INIT (اختياري - لا يوقف البوت إذا لم يوجد مفتاح) =================
try:
    client = OpenAI()
    OPENAI_AVAILABLE = True
    logger.info("OpenAI client initialized successfully")
except Exception as e:
    logger.warning(f"OpenAI not available (no API key): {e}")
    client = None
    OPENAI_AVAILABLE = False

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
AUTHORIZED_CHAT_ID = None  # أو ضع ID معرفك الخاص إذا كنت تريد تقييد الأوامر لـ Chat محدد
# Trading Parameters - VERSION PENNY HUNTER
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5
DAILY_LOSS_LIMIT = 150.0
STATE_FILE = "state_penny_hunter.json"

# Scanner Settings
SCAN_INTERVAL_SEC = 900
CHUNK_SIZE = 100
FAST_FILTER_WORKERS = 2
DEEP_ANALYSIS_WORKERS = 2
DELAY_BETWEEN_REQUESTS = 0.7
BREAK_BETWEEN_CHUNKS = 10
TRADE_MONITOR_INTERVAL = 60
MAX_TICKERS_TO_SCAN = 1200

TELEGRAM_DELAY = 1.0
SIGNAL_COOLDOWN = 7200

# ✅ MIN RVOL - شديد للأسهم الرخيصة
MIN_RVOL_BY_PHASE = {
    "PRE":     3.0,
    "REGULAR": 2.5,
    "AFTER":   3.0,
    "CLOSED":  999,
}

# ================= CACHE SYSTEM SETTINGS =================
CACHE_TTL = {
    "1m": 15,
    "5m": 60,
    "15m": 180,
    "1d": 900,
}
_cache = {}
_cache_lock = threading.RLock()

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

# توافق عكسي مع أي أجزاء قديمة ما زالت تستخدم الاسم السابق
TP_PCT = BASE_TP_PCT

# Fast Filter Thresholds
MIN_PRICE = 0.2
MAX_PRICE = 30.0
MIN_VOLUME = 10000
MAX_TICKERS_TO_SCAN = 2000

# === Penny Hunter Specific ===
PENNY_MIN_PRICE = 0.3
PENNY_MAX_PRICE = 25.0
PENNY_MIN_GAIN = 12.0
PENNY_MIN_RVOL = 3.0

# Fast Momentum Scanner Settings
MOMENTUM_SCAN_INTERVAL = 120
MOMENTUM_PRICE_MIN = 0.5
MOMENTUM_PRICE_MAX = 30.0
MOMENTUM_VOL_MIN = 100000
MOMENTUM_GAIN_PCT = 10.0

# (بقية الكود الأصلي يتبع هنا...)
# ملاحظة للمحرر: سأقوم بدمج التعديل مع بقية الملف الأصلي
# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
AUTHORIZED_CHAT_ID = None  # أو ضع ID معرفك الخاص إذا كنت تريد تقييد الأوامر لـ Chat محدد
# Trading Parameters - VERSION PENNY HUNTER
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5
DAILY_LOSS_LIMIT = 150.0
STATE_FILE = "state_penny_hunter.json"

# Scanner Settings
SCAN_INTERVAL_SEC = 900
CHUNK_SIZE = 100
FAST_FILTER_WORKERS = 2
DEEP_ANALYSIS_WORKERS = 2
DELAY_BETWEEN_REQUESTS = 0.7
BREAK_BETWEEN_CHUNKS = 10
TRADE_MONITOR_INTERVAL = 60
MAX_TICKERS_TO_SCAN = 1200

TELEGRAM_DELAY = 1.0
SIGNAL_COOLDOWN = 7200

# ✅ MIN RVOL - شديد للأسهم الرخيصة
MIN_RVOL_BY_PHASE = {
    "PRE":     3.0,
    "REGULAR": 2.5,
    "AFTER":   3.0,
    "CLOSED":  999,
}

# ================= CACHE SYSTEM SETTINGS =================
CACHE_TTL = {
    "1m": 15,
    "5m": 60,
    "15m": 180,
    "1d": 900,
}
_cache = {}
_cache_lock = threading.RLock()

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

# توافق عكسي مع أي أجزاء قديمة ما زالت تستخدم الاسم السابق
TP_PCT = BASE_TP_PCT

# Fast Filter Thresholds
MIN_PRICE = 0.2
MAX_PRICE = 30.0
MIN_VOLUME = 10000
MAX_TICKERS_TO_SCAN = 2000

# === Penny Hunter Specific ===
PENNY_MIN_PRICE = 0.3
PENNY_MAX_PRICE = 25.0
PENNY_MIN_GAIN = 12.0
PENNY_MIN_RVOL = 3.0

# Fast Momentum Scanner Settings
MOMENTUM_SCAN_INTERVAL = 120
MOMENTUM_PRICE_MIN = 0.5
MOMENTUM_PRICE_MAX = 30.0
MOMENTUM_VOL_MIN = 100000
MOMENTUM_GAIN_PCT = 10.0

def detect_early_pump(df):
    """
    🚀 كشف البامب المبكر قبل الارتفاع الكبير
    يبحث عن:
    1. حجم متزايد بشكل مستمر
    2. تغير سعر إيجابي
    3. RSI قوي لكن ليس مرهق
    """
    try:
        if len(df) < 10:
            return False, 0
        
        last = df.iloc[-1]
        
        # 1. حجم متزايد
        vol_ma = df['volume'].rolling(20).mean().iloc[-1]
        vol_surge = last['volume'] > vol_ma * PUMP_SETTINGS["early_pump_volume_multiplier"]
        
        # 2. تغير سعر إيجابي في آخر 5 شمعات
        price_change_pct = (last['close'] - df['close'].iloc[-5]) / df['close'].iloc[-5]
        price_positive = price_change_pct > PUMP_SETTINGS["early_pump_price_change"]
        
        # 3. RSI قوي
        rsi_strong = last['rsi'] > PUMP_SETTINGS["early_pump_rsi_threshold"]
        
        # 4. الشموع الأخيرة صاعدة
        last_3_green = (df['close'].iloc[-3:] > df['open'].iloc[-3:]).sum() >= 2
        
        pump_detected = vol_surge and price_positive and rsi_strong and last_3_green
        pump_score = 0
        if vol_surge: pump_score += 25
        if price_positive: pump_score += 25
        if rsi_strong: pump_score += 25
        if last_3_green: pump_score += 25
        
        return pump_detected, pump_score
    except:
        return False, 0

def detect_peak(df):
    """
    📍 كشف القمة - نقطة البيع المثالية
    يبحث عن:
    1. RSI في منطقة الذروة (> 75)
    2. انعكاس الشمعة (close < open)
    3. انخفاض الحجم
    """
    try:
        if len(df) < 5:
            return False, 0
        
        last = df.iloc[-1]
        
        # 1. RSI في الذروة
        rsi_peak = last['rsi'] > PUMP_SETTINGS["peak_rsi_threshold"]
        
        # 2. شمعة انعكاسية
        reversal = last['close'] < last['open']
        
        # 3. الحجم ينخفض
        vol_ma = df['volume'].rolling(5).mean().iloc[-1]
        vol_drop = last['volume'] < vol_ma * PUMP_SETTINGS["peak_volume_drop_pct"]
        
        # 4. السعر فوق EMA9 و EMA21 (اتجاه صعودي)
        above_emas = last['close'] > last['ema9'] and last['ema9'] > last['ema21']
        
        peak_detected = rsi_peak and reversal and vol_drop
        peak_score = 0
        if rsi_peak: peak_score += 30
        if reversal: peak_score += 30
        if vol_drop: peak_score += 20
        if above_emas: peak_score += 20
        
        return peak_detected, peak_score
    except:
        return False, 0

def detect_halt(symbol):
    """
    🛑 كشف الوقف المؤقت (Trading Halt)
    """
    try:
        # محاولة تحميل البيانات - إذا فشلت = halt
        df = cached_download(symbol, period="1d", interval="15m", timeout=3)
        
        # إذا كانت البيانات قديمة جداً = halt
        if not df.empty:
            last_time = df.index[-1]
            now = datetime.now(pytz.timezone('US/Eastern'))
            time_diff = (now - last_time).total_seconds() / 60
            
            if time_diff > 30:  # ما تحديث البيانات لأكثر من 30 دقيقة
                return True
        
        return False
    except:
        return True  # إذا حدث خطأ = halt

# ================= TRADING FUNCTIONS =================

def open_pump_trade(symbol, price, pump_score, df, phase="REGULAR"):
    """فتح صفقة على البامب"""
    settings = PHASE_SETTINGS.get(phase, PHASE_SETTINGS["REGULAR"])
    
    # اختيار TP و SL بناءً على قوة البامب
    if pump_score >= 90:
        tp = price * PUMP_SETTINGS["aggressive_tp_pct"]
        sl = price * PUMP_SETTINGS["aggressive_sl_pct"]
    else:
        tp = price * PUMP_SETTINGS["entry_tp_pct"]
        sl = price * PUMP_SETTINGS["entry_sl_pct"]
    
    with state_lock:
        if symbol in state["open_trades"] or len(state["open_trades"]) >= MAX_OPEN_TRADES:
            return
        
        size = max(1, int((CAPITAL * RISK_PER_TRADE) / (price - sl)))
        
        state["open_trades"][symbol] = {
            "entry": price,
            "tp": tp,
            "sl": sl,
            "size": size,
            "time": time.time(),
            "score": pump_score,
            "phase": phase,
            "pump_type": "early_pump",
            "near_target_alert_sent": False,
            "halt_detected": False,
        }
        save_state()
    
    tp_pct = (tp - price) / price * 100
    msg = (
        f"🚀 *PUMP ALERT: {symbol}*\n"
        f"💰 Entry: ${price:.2f}\n"
        f"🎯 TP: ${tp:.2f} (+{tp_pct:.1f}%)\n"
        f"🛑 SL: ${sl:.2f}\n"
        f"📊 Pump Score: {pump_score}/100\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"⏰ ادخل معهم من الأول وبع في القمة!"
    )
    send_telegram(msg)

def close_pump_trade(symbol, price, reason):
    """إغلاق صفقة البامب"""
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
    msg = (
        f"{emoji} *CLOSED: {symbol}*\n"
        f"📝 {reason}\n"
        f"💵 Exit: ${price:.2f}\n"
        f"📈 PnL: ${pnl:+.2f} ({pnl_pct:+.2f}%)"
    )
    send_telegram(msg)

# ================= SCANNING FUNCTIONS =================

def pump_hunter_scanner():
    """
    🚀 الماسح الرئيسي لصيد البامب
    يعمل 24/7 على PRE و REGULAR و AFTER
    """
    while True:
        try:
            phase = get_market_phase()
            
            with state_lock:
                tickers = list(state.get("tickers", []))[:1000]  # فحص أول 1000 سهم
            
            if not tickers:
                time.sleep(60)
                continue
            
            logger.info(f"🔍 Pump Hunter Scan: {len(tickers)} stocks - Phase: {phase}")
            
            for symbol in tickers:
                try:
                    # تجاهل الأسهم المفتوحة بالفعل
                    with state_lock:
                        if symbol in state["open_trades"]:
                            continue
                    
                    # تحميل البيانات
                    df = cached_download(symbol, period="5d", interval="5m")
                    if df.empty or len(df) < 20:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    df = compute_indicators(df)
                    
                    last = df.iloc[-1]
                    price = last['close']
                    
                    # فلتر السعر
                    if price < MIN_PRICE or price > MAX_PRICE:
                        continue
                    
                    # كشف البامب المبكر
                    pump_detected, pump_score = detect_early_pump(df)
                    
                    if pump_detected and pump_score >= 75:
                        # تحقق من الـ cooldown
                        signal_key = f"pump_{symbol}"
                        with state_lock:
                            last_signal = state["seen_signals"].get(signal_key, 0)
                        
                        if time.time() - last_signal > SIGNAL_COOLDOWN:
                            open_pump_trade(symbol, price, pump_score, df, phase)
                            
                            with state_lock:
                                state["seen_signals"][signal_key] = time.time()
                                save_state()
                    
                    # كشف القمة للأسهم المفتوحة
                    peak_detected, peak_score = detect_peak(df)
                    if peak_detected and symbol in state["open_trades"]:
                        close_pump_trade(symbol, price, f"Peak detected (Score: {peak_score})")
                    
                    time.sleep(0.1)
                
                except Exception as e:
                    logger.debug(f"Pump scan error {symbol}: {e}")
            
            time.sleep(SCAN_INTERVAL_SEC)
        
        except Exception as e:
            logger.error(f"Pump hunter error: {e}")
            time.sleep(60)

def halt_monitor():
    """
    🛑 مراقب الوقفات - لا يتجاهل الـ Halts
    يراقب الأسهم المتوقفة ويعيد الدخول عند رفع الـ Halt
    """
    while True:
        try:
            with state_lock:
                open_trades = list(state["open_trades"].keys())
            
            for symbol in open_trades:
                try:
                    is_halted = detect_halt(symbol)
                    
                    with state_lock:
                        trade = state["open_trades"].get(symbol)
                        if not trade:
                            continue
                        
                        if is_halted and not trade.get("halt_detected"):
                            # تسجيل الـ halt
                            trade["halt_detected"] = True
                            trade["halt_time"] = time.time()
                            logger.info(f"🛑 HALT DETECTED: {symbol}")
                            send_telegram(f"🛑 *HALT: {symbol}*\nالسهم توقف مؤقتاً - في انتظار الرفع...")
                        
                        elif not is_halted and trade.get("halt_detected"):
                            # الـ halt تم رفعه
                            halt_duration = time.time() - trade.get("halt_time", time.time())
                            logger.info(f"✅ HALT LIFTED: {symbol} (Duration: {halt_duration:.0f}s)")
                            send_telegram(f"✅ *HALT LIFTED: {symbol}*\nمدة الوقف: {halt_duration:.0f} ثانية\n🚀 البوت جاهز للعودة!")
                            trade["halt_detected"] = False
                    
                    save_state()
                    time.sleep(0.5)
                
                except Exception as e:
                    logger.debug(f"Halt monitor error {symbol}: {e}")
            
            time.sleep(TRADE_MONITOR_INTERVAL)
        
        except Exception as e:
            logger.error(f"Halt monitor error: {e}")
            time.sleep(60)

def trade_monitor():
    """
    📊 مراقب الصفقات - إدارة TP و SL
    """
    while True:
        try:
            with state_lock:
                open_trades = dict(state["open_trades"])
            
            for symbol, trade in open_trades.items():
                try:
                    df = cached_download(symbol, period="1d", interval="5m")
                    if df.empty:
                        continue
                    
                    last = df.iloc[-1]
                    price = last['close']
                    
                    # فحص TP
                    if price >= trade["tp"]:
                        close_pump_trade(symbol, price, "Target Profit Hit! 🎯")
                    
                    # فحص SL
                    elif price <= trade["sl"]:
                        close_pump_trade(symbol, price, "Stop Loss Hit! 🛑")
                    
                    # تنبيه قريب من الهدف
                    elif not trade.get("near_target_alert_sent"):
                        near_target = trade["tp"] * 0.85
                        if price >= near_target:
                            msg = f"⚠️ *NEAR TARGET: {symbol}*\n💰 Price: ${price:.2f}\n🎯 TP: ${trade['tp']:.2f}"
                            send_telegram(msg)
                            
                            with state_lock:
                                if symbol in state["open_trades"]:
                                    state["open_trades"][symbol]["near_target_alert_sent"] = True
                                    save_state()
                    
                    time.sleep(0.1)
                
                except Exception as e:
                    logger.debug(f"Trade monitor error {symbol}: {e}")
            
            time.sleep(TRADE_MONITOR_INTERVAL)
        
        except Exception as e:
            logger.error(f"Trade monitor error: {e}")
            time.sleep(60)

# ================= INDICATOR FUNCTIONS =================

def calculate_rsi(prices, period=14):
    """حساب RSI"""
    try:
        deltas = np.diff(prices)
        seed = deltas[:period+1]
        up = seed[seed >= 0].sum() / period
        down = -seed[seed < 0].sum() / period
        rs = up / down if down != 0 else 0
        rsi = np.zeros_like(prices)
        rsi[:period] = 100. - 100. / (1. + rs)
        
        for i in range(period, len(prices)):
            delta = deltas[i-1]
            if delta > 0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = -delta
            
            up = (up * (period - 1) + upval) / period
            down = (down * (period - 1) + downval) / period
            rs = up / down if down != 0 else 0
            rsi[i] = 100. - 100. / (1. + rs)
        
        return rsi
    except:
        return np.zeros_like(prices)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def stats():
    with state_lock:
        return jsonify({
            "open_trades": len(state["open_trades"]),
            "performance": state["performance"],
            "daily_loss": state.get("daily_loss", 0)
        }), 200

# ================= TELEGRAM BOT COMMANDS =================

@bot.message_handler(commands=['start'])
def main():
    """تشغيل البوت"""
    logger.info("🚀 PUMP HUNTER BOT v2.0 Starting...")
    
    load_state()
    
    # تحميل قائمة الأسهم
    try:
        with open("tickers.txt", "r") as f:
            with state_lock:
                state["tickers"] = [line.strip().upper() for line in f if line.strip()]
    except:
        logger.warning("tickers.txt not found")
    
    # بدء الخيوط
    threads = [
        threading.Thread(target=pump_hunter_scanner, daemon=True),
        threading.Thread(target=halt_monitor, daemon=True),
        threading.Thread(target=trade_monitor, daemon=True),
    ]
    
    for thread in threads:
        thread.start()
    
    # بدء Flask
    app.run(host="0.0.0.0", port=8080, debug=False)

if __name__ == "__main__":
    main()

# ================= MERGED FROM BLOCKS =================

matplotlib.use('Agg')

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
AUTHORIZED_CHAT_ID = int(CHAT_ID) if CHAT_ID and CHAT_ID.lstrip("-").isdigit() else None
STATE_DIR = os.getenv("STATE_DIR", ".")

# Trading Parameters
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 50
DAILY_LOSS_LIMIT = 300.0
STATE_FILE = os.path.join(STATE_DIR, "state_v32.json")

# Scanner Settings
SCAN_INTERVAL_SEC = 540
CHUNK_SIZE = 100
FAST_FILTER_WORKERS = 3       # تقليل لتخفيف الضغط على السيرفر (كان 5)
DEEP_ANALYSIS_WORKERS = 3
DELAY_BETWEEN_REQUESTS = 0.5
BREAK_BETWEEN_CHUNKS = 5
TRADE_MONITOR_INTERVAL = 60
MAX_TICKERS_TO_SCAN = 4800

TELEGRAM_DELAY = 1.0

# فترة الحجب بين إشارات نفس السهم (ساعتين)
SIGNAL_COOLDOWN = 7200

# ✅ حد أدنى لـ RVOL بحسب الجلسة — لمنع إرسال إشارات بـ 0.0x
MIN_RVOL_BY_PHASE = {
    "PRE":     2.5,   # كان 1.5
    "REGULAR": 2.0,   # كان 1.2
    "AFTER":   2.5,   # كان 1.5
    "CLOSED":  999,
}

# ================= CACHE SYSTEM SETTINGS =================
CACHE_TTL = {
    "1m": 15,      # بيانات الدقيقة تخزن لمدة 15 ثانية فقط
    "5m": 60,      # بيانات 5 دقائق تخزن لمدة دقيقة
    "15m": 180,    # بيانات 15 دقيقة تخزن لمدة 3 دقائق
    "1d": 900,     # بيانات يومية تخزن لمدة 15 دقيقة
}
_cache = {}
_cache_lock = threading.RLock()

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

# توافق عكسي مع أي أجزاء قديمة ما زالت تستخدم الاسم السابق
TP_PCT = BASE_TP_PCT

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

def momentum_breaker_scanner():
    """🎯 يكتشف اختراق المقاومات اللحظي على شموع 1 دقيقة"""
    RESISTANCE_LEVELS = [0.5, 1, 1.5, 2, 2.5, 3, 5, 7, 10, 12, 15, 20, 25, 30, 50, 75, 100]
    
    while True:
        try:
            phase = get_market_phase()
            if phase not in ("REGULAR", "PRE"):
                time.sleep(60)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))
            
            for symbol in tickers[:200]:
                try:
                    df = safe_download(symbol, period="1d", interval="1m")
                    if df.empty or len(df) < 10:
                        continue
                    
                    df = df.copy()
                    df.columns = [c.lower() for c in df.columns]
                    
                    price = df['close'].iloc[-1]
                    prev_price = df['close'].iloc[-2]
                    volume = df['volume'].iloc[-1]
                    avg_volume = df['volume'].rolling(20).mean().iloc[-1]
                    
                    momentum_spike = (price - prev_price) / prev_price > 0.01
                    volume_blast = volume > avg_volume * 3
                    
                    near_resistance = False
                    for level in RESISTANCE_LEVELS:
                        if level * 0.99 <= price <= level * 1.01:
                            near_resistance = True
                            break
                    
                    if momentum_spike and near_resistance and volume_blast:
                        signal_key = f"breaker_{symbol}"
                        with state_lock:
                            last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                        
                        if time.time() - last_sent < SNIPER_COOLDOWN:
                            continue
                        
                        tp, sl, _ = get_trade_levels(price, volume/avg_volume if avg_volume > 0 else 1)
                        msg = f"⚡ *MOMENTUM BREAKER: {symbol}*\n💰 ${price:.4f}\n📈 +{(price-prev_price)/prev_price*100:.2f}%\n🎯 ${tp:.4f} | 🛑 ${sl:.4f}"
                        send_telegram(msg)
                        
                        with state_lock:
                            state.setdefault("seen_signals", {})[signal_key] = time.time()
                            save_state()
                        
                except:
                    continue
                time.sleep(0.2)
                
        except Exception as e:
            logger.error(f"Momentum breaker error: {e}")
        
        time.sleep(30)

def snap_scanner():
    """⚡ ماسح فوري - يشتغل كل 30 ثانية على شموع 1 دقيقة"""
    while True:
        try:
            phase = get_market_phase()
            if phase not in ("PRE", "REGULAR"):
                time.sleep(30)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))
            
            for symbol in tickers[:200]:
                try:
                    df = safe_download(symbol, period="1d", interval="1m")
                    if df.empty or len(df) < 5:
                        continue
                    
                    df = df.copy()
                    df.columns = [c.lower() for c in df.columns]
                    
                    price_now = df['close'].iloc[-1]
                    price_3min_ago = df['close'].iloc[-4] if len(df) >= 4 else price_now
                    gain_3min = (price_now - price_3min_ago) / price_3min_ago * 100 if price_3min_ago > 0 else 0
                    
                    volume_now = df['volume'].iloc[-1]
                    volume_avg = df['volume'].rolling(5).mean().iloc[-1]
                    rvol = volume_now / volume_avg if volume_avg > 0 else 1
                    
                    if gain_3min > 3 and rvol > 2:
                        signal_key = f"snap_{symbol}"
                        with state_lock:
                            if signal_key in state.get("seen_signals", {}):
                                continue
                            state.setdefault("seen_signals", {})[signal_key] = time.time()
                            save_state()
                        
                        tp, sl, _ = get_trade_levels(price_now, rvol)
                        msg = f"⚡ *SNAP SHOT: {symbol}*\n📈 +{gain_3min:.1f}% في 3 دقائق\n🔥 RVOL: {rvol:.1f}x\n💰 ${price_now:.2f}\n🎯 ${tp:.2f} | 🛑 ${sl:.2f}"
                        send_telegram(msg)
                        
                except:
                    continue
                
            time.sleep(30)
            
        except Exception as e:
            logger.error(f"Snap scanner error: {e}")
            time.sleep(30)

def pre_blast_scanner():
    """💣 يصطاد الأسهم قبل ثوانٍ من القفزة - صائد الوحوش"""
    while True:
        try:
            if get_market_phase() != "REGULAR":
                time.sleep(30)
                continue

            with state_lock:
                tickers = list(state.get("tickers", []))

            for symbol in tickers[:100]:
                try:
                    df = safe_download(symbol, period="1d", interval="1m")
                    if df.empty or len(df) < 30:
                        continue
                    
                    df = df.copy()
                    df.columns = [c.lower() for c in df.columns]

                    price = df['close'].iloc[-1]
                    vol_avg_10 = df['volume'].tail(10).mean()
                    vol_avg_30 = df['volume'].tail(30).mean()
                    vol_blast = vol_avg_10 > vol_avg_30 * 2

                    price_avg_10 = df['close'].tail(10).mean()
                    price_surge = price > price_avg_10 * 1.02

                    if vol_blast and price_surge:
                        signal_key = f"blast_{symbol}"
                        with state_lock:
                            last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                        
                        if time.time() - last_sent > SNIPER_COOLDOWN:
                            tp, sl, _ = get_trade_levels(price, vol_avg_10/vol_avg_30 if vol_avg_30 > 0 else 1)
                            msg = f"💣 *PRE-BLAST ALERT: {symbol}*\n💰 ${price:.4f}\n⚡ حجم مفاجئ! ({vol_avg_10/vol_avg_30:.1f}x)\n🎯 ${tp:.4f} | 🛑 ${sl:.4f}"
                            send_telegram(msg)
                            
                            with state_lock:
                                state.setdefault("seen_signals", {})[signal_key] = time.time()
                                save_state()

                except:
                    continue
                time.sleep(0.2)

        except Exception as e:
            logger.error(f"Pre-blast error: {e}")
        
        time.sleep(45)

# ================= MERGED FROM BLOCKS =================

matplotlib.use('Agg')

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
AUTHORIZED_CHAT_ID = int(CHAT_ID) if CHAT_ID and CHAT_ID.lstrip("-").isdigit() else None
STATE_DIR = os.getenv("STATE_DIR", ".")

# Trading Parameters
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5  # تقليل للتركيز
DAILY_LOSS_LIMIT = 300.0
STATE_FILE = os.path.join(STATE_DIR, "state_v32.json")

# Scanner Settings
SCAN_INTERVAL_SEC = 540
CHUNK_SIZE = 100
FAST_FILTER_WORKERS = 3       # تقليل لتخفيف الضغط على السيرفر (كان 5)
DEEP_ANALYSIS_WORKERS = 3
DELAY_BETWEEN_REQUESTS = 0.5
BREAK_BETWEEN_CHUNKS = 5
TRADE_MONITOR_INTERVAL = 60
MAX_TICKERS_TO_SCAN = 4800

TELEGRAM_DELAY = 1.0

# فترة الحجب بين إشارات نفس السهم (ساعتين)
SIGNAL_COOLDOWN = 7200
# فترة الحجب لصياد القنص (10 دقائق)
SNIPER_COOLDOWN = 600

# Strategy Parameters
BASE_TP_PCT = 1.10
HIGH_RVOL_TP_PCT = 1.15
EXTREME_RVOL_TP_PCT = 1.20
SL_PCT = 0.985
TRAIL_TO_BREAKEVEN_TRIGGER = 1.04
TRAIL_TO_LOCK_PROFIT_TRIGGER = 1.10
TRAIL_LOCK_PROFIT_SL_PCT = 1.05
NEAR_TARGET_ALERT_RATIO = 0.80
MIDDAY_MIN_SCORE = 70
DAILY_REPORT_HOUR = 16
DAILY_REPORT_MINUTE = 15

# توافق عكسي مع أي أجزاء قديمة ما زالت تستخدم الاسم السابق
TP_PCT = BASE_TP_PCT

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

def atr_trailing_stop_update(trade, price, atr):
    """🎯 يحرك الوقف بناءً على ATR"""
    entry = trade["entry"]
    current_sl = trade["sl"]
    
    if not trade.get("highest_price"):
        trade["highest_price"] = entry
    
    if price > trade["highest_price"]:
        trade["highest_price"] = price
    
    new_sl = trade["highest_price"] - (atr * 2)
    
    if new_sl > current_sl:
        trade["sl"] = new_sl
        return True, new_sl
    return False, current_sl

# ================= PARABOLIC SAR DETECTOR =================
def detect_parabolic_sar_buy(df):
    """🎯 يكتشف إشارة شراء Parabolic SAR"""
    try:
        if len(df) < 20:
            return False
        
        df.columns = [c.lower() for c in df.columns]
        
        low = df['low'].values
        sar_bullish = low[-1] > low[-2] and low[-2] > low[-3]
        
        return sar_bullish
    except:
        return False

# ================= FLOAT & SHORT INTEREST FILTER =================
def get_float_data(symbol):
    """📊 يجلب بيانات Float و Short Interest"""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        float_shares = info.get('floatShares', 0)
        short_percent = info.get('shortPercentOfFloat', 0)
        
        is_low_float = float_shares < 10_000_000 if float_shares else False
        is_high_short = short_percent > 0.10 if short_percent else False
        
        return {
            'is_low_float': is_low_float,
            'is_high_short': is_high_short,
            'float_shares': float_shares,
            'short_percent': short_percent
        }
    except:
        return None

# ================= TIME-BASED BREAKOUT BOOST =================
def get_time_based_boost():
    """⏰ يضيف نقاط إضافية حسب الوقت"""
    now = now_est()
    hour = now.hour
    minute = now.minute
    
    if hour == 9 and minute >= 30:
        return 20
    if hour == 10 and minute < 30:
        return 15
    if hour == 11 and minute >= 30:
        return 10
    if hour == 12 and minute < 30:
        return 10
    if hour == 15:
        return 20
    
    return 0

# ================= VOLUME PROFILE DETECTOR =================
def find_high_volume_nodes(df):
    """📊 يبحث عن مستويات الأسعار ذات الحجم العالي"""
    try:
        if len(df) < 50:
            return [], {}
        
        df.columns = [c.lower() for c in df.columns]
        
        price_min = df['low'].min()
        price_max = df['high'].max()
        step = (price_max - price_min) / 20
        
        volume_profile = {}
        for i in range(20):
            level_low = price_min + (i * step)
            level_high = price_min + ((i+1) * step)
            
            mask = (df['close'] >= level_low) & (df['close'] < level_high)
            volume_profile[f"${level_low:.2f}-${level_high:.2f}"] = df.loc[mask, 'volume'].sum()
        
        sorted_nodes = sorted(volume_profile.items(), key=lambda x: x[1], reverse=True)
        high_volume_nodes = sorted_nodes[:3]
        
        return high_volume_nodes, volume_profile
    except:
        return [], {}

# ================= IMPROVED PRE-PUMP DETECTION =================
def pre_pump_detection_scanner_improved():
    """🎉 ماسح الفرص قبل الانفجار - محسّن للسرعة"""
    while True:
        try:
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(30)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))
            
            if not tickers:
                time.sleep(30)
                continue
            
            for symbol in tickers[:150]:
                try:
                    df = safe_download(symbol, period="1d", interval="1m")
                    if df.empty or len(df) < 20:
                        continue
                    
                    df = df.copy()
                    df.columns = [c.lower() for c in df.columns]
                    df = compute_indicators(df)
                    
                    last = df.iloc[-1]
                    price = last['close']
                    
                    signals = 0
                    reasons = []
                    
                    if last['rsi'] < 30:
                        signals += 1
                        reasons.append("💶 RSI منخفض")
                    
                    if last['close'] > last['sma50']:
                        signals += 1
                        reasons.append("📈 فوق SMA50")
                    
                    vol_avg = df['volume'].tail(20).mean()
                    if last['volume'] > vol_avg * 2:
                        signals += 1
                        reasons.append("⚡ حجم مرتفع")
                    
                    if last['ema9'] > last['ema21']:
                        signals += 1
                        reasons.append("📈 EMA صعودي")
                    
                    rvol = last['volume'] / vol_avg if vol_avg > 0 else 1
                    if rvol > 2:
                        signals += 1
                        reasons.append(f"🔥 RVOL {rvol:.1f}x")
                    
                    if signals < 3:
                        continue
                    
                    signal_key = f"prepump_{symbol}"
                    with state_lock:
                        last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                    
                    if time.time() - last_sent < 600:
                        continue
                    
                    entry = price
                    tp, sl, _ = get_trade_levels(entry, rvol)
                    
                    msg = f"🎉 *PRE-PUMP SIGNAL!*\n📊 *{symbol}*\n💰 ${price:.4f}\n📊 الإشارات: {signals}/5\n" + "\n".join(reasons)
                    msg += f"\n🎯 ${entry:.4f} | 🎯 ${tp:.4f} | 🛑 ${sl:.4f}"
                    send_telegram(msg)
                    
                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()
                    
                except Exception as e:
                    continue
                
                time.sleep(0.15)
            
        except Exception as e:
            logger.error(f"Pre-pump scanner error: {e}")
        
        time.sleep(30)

# ================= PRE-BLAST SCANNER (السلاح السري) =================
def detect_catalyst_keyword(text):
    """يرجع أول كلمة مفتاحية قوية موجودة في الخبر"""
    text = (text or "").lower()
    for kw in STRONG_KEYWORDS:
        if kw in text:
            return kw
    return None

def extract_symbol_from_news(text):
    """يستخرج رمز السهم من نص الخبر بطريقة مرنة"""
    if not text:
        return None

    text_upper = text.upper()

    # مثل (NASDAQ: BIRD) أو (NYSE: AAPL)
    pattern = r'\((?:NASDAQ|NYSE|AMEX|OTC|NASDAQCM|NASDAQGM|NASDAQGS)\s*:\s*([A-Z]{1,5})\)'
    match = re.search(pattern, text_upper)
    if match:
        return match.group(1)

    # الاستفادة من الدالة العامة الموجودة بالفعل
    symbols = extract_symbols_from_text(text_upper)
    return symbols[0] if symbols else None

def ensure_state_defaults():
    defaults = {
        "open_trades": {},
        "performance": {},
        "weights": {
            "trend": 20,
            "rsi": 15,
            "volume": 15,
            "obv": 10,
            "breakout": 15,
            "macd": 10,
            "bb_pos": 10,
            "adx": 5,
        },
        "seen_signals": {},
        "halted_alerts": {},
        "last_accumulation": {},
        "daily_loss": 0.0,
        "last_reset": None,
        "backtest_results": {},
        "equity_curve": [],
    }
    with state_lock:
        for key, value in defaults.items():
            if key not in state:
                state[key] = value.copy() if isinstance(value, dict) else list(value) if isinstance(value, list) else value
        for key, value in defaults["weights"].items():
            state["weights"].setdefault(key, value)

def register_bot_handlers():
    if not bot:
        logger.warning("Telegram bot is disabled: handlers were not registered")
        return

    @bot.message_handler(commands=['start'])
    def cmd_start(message):
        session, session_msg = get_market_session()
        mode = "Webhook" if USE_WEBHOOK else "Polling"
        welcome = (
            "🚀 *AI Trading Bot v20*\n\n"
            f"*Session:* {session}\n{session_msg}\n"
            f"*Mode:* {mode}\n\n"
            "*Commands:*\n"
            "/scan \\<symbol\\> — Technical analysis + backtest\n"
            "/status — Bot overview\n"
            "/positions — Open trades\n"
            "/performance — Win/loss stats\n"
            "/backtest \\<symbol\\> — Run backtest only\n"
            "/close \\<symbol\\> — Manually close trade\n"
        )
        bot.reply_to(message, welcome, parse_mode='Markdown')

    @bot.message_handler(commands=['scan'])
    def cmd_scan(message):
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "⚠️ Usage: /scan <SYMBOL>")
            return
        symbol = parts[1].upper().strip()
        msg_obj = bot.reply_to(message, f"🔍 Analyzing {symbol}…")

        def do_analysis():
            analysis = generate_analysis_message(symbol)
            try:
                bot.edit_message_text(chat_id=message.chat.id,
                                      message_id=msg_obj.message_id,
                                      text=analysis, parse_mode='Markdown')
            except Exception:
                bot.send_message(chat_id=message.chat.id, text=analysis, parse_mode='Markdown')

        future = executor.submit(do_analysis)
        try:
            future.result(timeout=25)
        except TimeoutError:
            bot.edit_message_text(chat_id=message.chat.id,
                                  message_id=msg_obj.message_id,
                                  text="⚠️ Timeout. Try again.")

    @bot.message_handler(commands=['backtest'])
    def cmd_backtest(message):
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "⚠️ Usage: /backtest <SYMBOL>")
            return
        symbol = parts[1].upper().strip()
        msg_obj = bot.reply_to(message, f"🔬 Running backtest for {symbol}…")

        def do_bt():
            df = fetch_yfinance_data(symbol, '60d', '1h')
            if df is None or len(df) < 120:
                bot.edit_message_text(chat_id=message.chat.id,
                                      message_id=msg_obj.message_id,
                                      text="⚠️ Not enough data for backtest (need 60d 1h).")
                return
            df = compute_indicators(df)
            bt = simple_backtest(symbol, df)
            if not bt or bt.get("total_trades", 0) == 0:
                text_out = f"⚠️ No trades triggered for {symbol} in backtest period."
            else:
                emoji = "✅" if bt["profitable"] else "❌"
                text_out = (
                    f"🔬 *Backtest: {symbol}*\n"
                    f"📊 Trades:   {bt['total_trades']}\n"
                    f"✅ Wins:     {bt['wins']}\n"
                    f"❌ Losses:   {bt['losses']}\n"
                    f"📈 Win Rate: {bt['win_rate']}%\n"
                    f"💰 Avg PnL:  ${bt['avg_pnl']:.4f}\n"
                    f"🧮 Gross PnL:${bt['gross_pnl']:+.4f}\n"
                    f"{emoji} Strategy: {'PROFITABLE' if bt['profitable'] else 'UNPROFITABLE'}"
                )
            bot.edit_message_text(chat_id=message.chat.id,
                                  message_id=msg_obj.message_id,
                                  text=text_out, parse_mode='Markdown')

        future = executor.submit(do_bt)
        try:
            future.result(timeout=60)
        except TimeoutError:
            bot.edit_message_text(chat_id=message.chat.id,
                                  message_id=msg_obj.message_id,
                                  text="⚠️ Backtest timed out.")

    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        reset_daily_loss_if_needed()
        session, session_msg = get_market_session()
        with state_lock:
            total_wins = sum(p["wins"] for p in state["performance"].values())
            total_losses = sum(p["losses"] for p in state["performance"].values())
            total = total_wins + total_losses
            winrate = (total_wins / total * 100) if total else 0
            total_pnl = sum(p.get("total_pnl", 0) for p in state["performance"].values())
            open_count = len(state["open_trades"])
            daily_loss = state["daily_loss"]
            exposure = sum(float(t.get("entry", 0)) * int(t.get("size", 0)) for t in state["open_trades"].values())

        msg = (
            f"📊 *Bot Status* ({session})\n"
            f"{session_msg}\n\n"
            f"💰 Capital:     ${CAPITAL:,.0f}\n"
            f"📈 Open Trades: {open_count}/{MAX_OPEN_TRADES}\n"
            f"🧱 Exposure:    ${exposure:,.2f} / ${CAPITAL * MAX_TOTAL_EXPOSURE_PCT:,.2f}\n"
            f"✅ Wins:        {total_wins}\n"
            f"❌ Losses:      {total_losses}\n"
            f"📈 Win Rate:    {winrate:.1f}%\n"
            f"💵 Total PnL:   ${total_pnl:+.2f}\n"
            f"📉 Daily Loss:  ${daily_loss:.2f} / ${DAILY_LOSS_LIMIT}\n"
            f"🌐 Universe:    {len(dynamic_universe)} stocks\n"
            f"📦 Cached:      {len(data_cache)} symbols\n"
            f"🛰️ Mode:        {'Webhook' if USE_WEBHOOK else 'Polling'}"
        )
        bot.reply_to(message, msg, parse_mode='Markdown')

    @bot.message_handler(commands=['positions'])
    def cmd_positions(message):
        with state_lock:
            trades = dict(state["open_trades"])
        if not trades:
            bot.reply_to(message, "📭 No open positions")
            return
        lines = ["*Open Positions*\n"]
        for sym, t in trades.items():
            age = int((time.time() - t["time"]) / 60)
            lines.append(
                f"🔹 *{sym}*\n"
                f"   Entry ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f}\n"
                f"   Shares {t['size']} | Age {age}m | Score {t['score']}"
            )
        bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')

    @bot.message_handler(commands=['performance'])
    def cmd_performance(message):
        with state_lock:
            perf = dict(state["performance"])
        if not perf:
            bot.reply_to(message, "📭 No completed trades yet")
            return
        lines = ["*Per-Symbol Performance*\n"]
        for sym, p in sorted(perf.items(), key=lambda x: x[1].get("total_pnl", 0), reverse=True):
            total = p["wins"] + p["losses"]
            wr = (p["wins"] / total * 100) if total else 0
            lines.append(
                f"*{sym}*: {p['wins']}W / {p['losses']}L "
                f"({wr:.1f}%) | PnL ${p.get('total_pnl', 0):+.2f}"
            )
        bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')

    @bot.message_handler(commands=['close'])
    def cmd_close(message):
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "⚠️ Usage: /close <SYMBOL>")
            return
        symbol = parts[1].upper().strip()
        with state_lock:
            if symbol not in state["open_trades"]:
                bot.reply_to(message, f"📭 No open trade for {symbol}")
                return
        df = fetch_yfinance_data(symbol, '1d', '5m')
        if df is None or df.empty:
            bot.reply_to(message, f"⚠️ Cannot fetch current price for {symbol}")
            return
        price = float(df["close"].iloc[-1])
        close_trade(symbol, price, reason="manual_close")
        bot.reply_to(message, f"✅ Manually closed *{symbol}* at ${price:.2f}", parse_mode='Markdown')

register_bot_handlers()

# ================= BACKGROUND SCANNER =================
# ================= BACKGROUND SCANNER =================
# ================= BACKGROUND SCANNER =================
def get_dynamic_universe():
    """Fetches active stocks and top gainers for focused scanning."""
    tickers = set()
    try:
        # 1. Top Gainers from Yahoo Finance
        gainers = yf.Search("", max_results=20).stocks
        for s in gainers:
            tickers.add(s['symbol'])
        
        # 2. Most Active from Yahoo Finance
        # Note: yfinance doesn't have a direct 'most active' method, 
        # but we can use a predefined list of high-volume stocks as a base
        base_active = ["TSLA", "NVDA", "AMD", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NFLX", "PLTR", "GME", "AMC", "MULN", "NIO", "SOFI", "F", "BAC", "INTC", "T", "XOM"]
        tickers.update(base_active)
        
        # 3. Add Penny Stocks (Curated list or dynamic search)
        penny_base = ["SNDL", "ZOM", "CTRM", "IDEX", "NAKD", "GNUS", "SHIP", "TOPS"]
        tickers.update(penny_base)
        
        logger.info(f"Dynamic universe built with {len(tickers)} symbols.")
        return list(tickers)
    except Exception as e:
        logger.error(f"Universe discovery error: {e}")
        return ["AAPL", "TSLA", "NVDA", "AMD", "MSFT", "GME", "AMC", "NIO", "SOFI"]

# ================= DATA ENGINE =================
def scan_all_stocks():
    logger.info("Starting market scan...")
    tickers = get_dynamic_universe()
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(process_symbol, tickers)
    
    logger.info("Market scan completed.")

def background_loop():
    while True:
        try:
            if is_market_open():
                scan_all_stocks()
            else:
                logger.info("Market closed. Sleeping...")
            time.sleep(SCAN_INTERVAL_SEC)
        except Exception as e:
            logger.error(f"Loop error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    load_state()
    # Start background scanner
    threading.Thread(target=background_loop, daemon=True).start()
    # Start Flask for health checks
    app.run(host="0.0.0.0", port=5000)

# ================= MERGED FROM BLOCKS =================

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
STATE_DIR = os.getenv("STATE_DIR", ".")

# Trading Parameters
CAPITAL = 10000.0
MAX_OPEN_TRADES = 10
MIN_SCORE_REGULAR = 70
MIN_SCORE_PENNY = 75
SIGNAL_COOLDOWN = 3600
STATE_FILE = os.path.join(STATE_DIR, "state_v23.json")
SCAN_INTERVAL_SEC = 600  # 10 minutes for full market scan
TRADE_MONITOR_INTERVAL = 60 # 1 minute

# Strategy Parameters
TP_PCT = 1.05  # 5% Target
SL_PCT = 0.97  # 3% Stop Loss

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_v23.log"),
        logging.StreamHandler()
    ]
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
    "last_reset": None,
    "tickers": [],
    "last_ticker_update": None
}

EASTERN_TZ = pytz.timezone("US/Eastern")

def get_sentiment_score(df): # Placeholder for sentiment analysis
    # In a real scenario, this would fetch news/social media sentiment
    # and return a score (e.g., -10 to +10)
    # For demonstration, let's return a random positive score sometimes
    if np.random.rand() > 0.7: # 30% chance of positive sentiment
        return 5
    return 0

# ================= RISK MANAGEMENT & TARGETS =================
def is_overextended(df_daily: pd.DataFrame, max_3d_gain_pct: float = 35.0,
                    max_distance_from_sma20_pct: float = 25.0) -> tuple[bool, str]:
    """
    يرفض الأسهم في قمة موجة Pump (احتمال انعكاس عالي).

    Args:
        df_daily: داتا يومية (1d interval).
        max_3d_gain_pct: أعلى ربح مسموح في 3 أيام.
        max_distance_from_sma20_pct: أعلى بُعد عن SMA20.

    Returns:
        (is_extended, reason).
    """
    try:
        if df_daily is None or df_daily.empty or len(df_daily) < 20:
            return False, ""
        close = df_daily['close']
        last = float(close.iloc[-1])

        # 1) ربح آخر 3 أيام
        if len(close) >= 4:
            three_days_ago = float(close.iloc[-4])
            if three_days_ago > 0:
                gain_3d = (last - three_days_ago) / three_days_ago * 100
                if gain_3d > max_3d_gain_pct:
                    return True, f"ركض +{gain_3d:.1f}% في 3 أيام — احتمال جني أرباح"

        # 2) المسافة عن SMA20
        sma20 = close.rolling(20).mean().iloc[-1]
        if sma20 and sma20 > 0:
            distance = (last - sma20) / sma20 * 100
            if distance > max_distance_from_sma20_pct:
                return True, f"بعيد {distance:.1f}% عن SMA20 — مرشح لتصحيح"

        # 3) RSI يومي > 80
        if 'rsi' in df_daily.columns:
            rsi_val = float(df_daily['rsi'].iloc[-1])
            if rsi_val > 80:
                return True, f"RSI يومي {rsi_val:.0f} — منطقة تشبع شراء"

        return False, ""
    except Exception as e:
        logger.debug(f"is_overextended error: {e}")
        return False, ""

# ============================================================
# 3) Multi-Timeframe Alignment Gate
# ============================================================

def is_multi_timeframe_aligned(cached_download_fn, symbol: str) -> tuple[bool, list[str]]:
    """
    يرجع True فقط لو الفريمات الثلاثة (1m, 5m, 15m) صاعدة.

    منطق "صاعد" = آخر إغلاق فوق EMA9 من نفس الفريم.
    """
    aligned: list[str] = []
    issues: list[str] = []
    for interval, period in [("15m", "5d"), ("5m", "1d"), ("1m", "1d")]:
        try:
            df = cached_download_fn(symbol, period=period, interval=interval)
            if df is None or df.empty or len(df) < 10:
                issues.append(f"{interval}: بيانات قليلة")
                continue
            df = df.copy()
            df.columns = [c.lower() for c in df.columns]
            ema9 = df['close'].ewm(span=9, adjust=False).mean()
            last_close = float(df['close'].iloc[-1])
            last_ema = float(ema9.iloc[-1])
            if last_close >= last_ema:
                aligned.append(interval)
            else:
                issues.append(f"{interval}: تحت EMA9")
        except Exception as e:
            issues.append(f"{interval}: خطأ ({e})")

    is_ok = len(aligned) >= 2  # نطلب 2 من 3 على الأقل
    return is_ok, (aligned if is_ok else issues)

# ============================================================
# 4) Sector Strength Gate
# ============================================================

_sector_cache: dict[str, tuple[float, float]] = {}
_sector_cache_lock = threading.RLock()

def _get_sector_change_pct(cached_download_fn, etf_symbol: str) -> Optional[float]:
    """نسبة تغير ETF القطاع اليوم (مع كاش 5 دقائق)."""
    with _sector_cache_lock:
        cached = _sector_cache.get(etf_symbol)
        if cached and time.time() - cached[0] < 300:
            return cached[1]

    try:
        df = cached_download_fn(etf_symbol, period="2d", interval="5m")
        if df is None or df.empty or len(df) < 5:
            return None
        df.columns = [c.lower() for c in df.columns]
        opens = df['open'].iloc[0]
        last = df['close'].iloc[-1]
        if opens <= 0:
            return None
        change_pct = (last - opens) / opens * 100
        with _sector_cache_lock:
            _sector_cache[etf_symbol] = (time.time(), change_pct)
        return change_pct
    except Exception:
        return None

def sector_strength_gate(cached_download_fn, symbol: str,
                         min_sector_pct: float = -0.5) -> tuple[bool, str]:
    """
    يرفض السهم إذا قطاعه أحمر بشكل واضح اليوم.

    Args:
        symbol: رمز السهم.
        min_sector_pct: أدنى نسبة تغير مقبولة لـ ETF القطاع (افتراضي -0.5%).
    """
    if not finnhub.enabled:
        return True, "تخطي فحص القطاع (Finnhub غير مفعل)"

    sector = finnhub.get_sector(symbol)
    if not sector:
        return True, "قطاع غير معروف"

    etf = SECTOR_ETF_MAP.get(sector)
    if not etf:
        return True, f"لا يوجد ETF لقطاع {sector}"

    change = _get_sector_change_pct(cached_download_fn, etf)
    if change is None:
        return True, f"تعذر جلب أداء {etf}"

    if change < min_sector_pct:
        return False, f"قطاع {sector} ({etf}) {change:+.2f}% — ضعيف"

    return True, f"قطاع {sector} ({etf}) {change:+.2f}% ✅"

# ============================================================
# 5) Opening Range Breakout (ORB) Scanner
# ============================================================

def detect_orb_breakout(df_5m: pd.DataFrame, range_minutes: int = 15) -> tuple[bool, dict]:
    """
    يكشف اختراق قمة أول 15 دقيقة من الجلسة العادية (Opening Range Breakout).

    Returns:
        (is_breakout, info_dict).
    """
    try:
        if df_5m is None or df_5m.empty or not isinstance(df_5m.index, pd.DatetimeIndex):
            return False, {}

        df = df_5m.copy()
        df.columns = [c.lower() for c in df.columns]

        # نأخذ شموع اليوم فقط
        try:
            today = df.index[-1].date()
        except Exception:
            return False, {}
        today_df = df[df.index.date == today]
        if today_df.empty:
            return False, {}

        # نفلتر شموع الجلسة العادية فقط (9:30 - 16:00 ET)
        try:
            local_ts = today_df.index.tz_convert(EASTERN_TZ)
        except Exception:
            local_ts = today_df.index
        mask = [(ts.hour * 60 + ts.minute) >= 9 * 60 + 30 and
                (ts.hour * 60 + ts.minute) < 16 * 60 for ts in local_ts]
        regular_df = today_df[mask]
        if len(regular_df) < (range_minutes // 5) + 2:
            return False, {}

        # أول 3 شموع 5m = أول 15 دقيقة
        opening_candles = regular_df.iloc[: range_minutes // 5]
        or_high = float(opening_candles['high'].max())
        or_low = float(opening_candles['low'].min())

        last = regular_df.iloc[-1]
        last_close = float(last['close'])
        last_vol = float(last['volume'])
        avg_vol = float(opening_candles['volume'].mean())

        # شرط الاختراق: إغلاق فوق قمة OR + حجم أعلى من متوسط شموع الافتتاح
        is_breakout = last_close > or_high and last_vol > avg_vol * 1.2

        return is_breakout, {
            "or_high": or_high,
            "or_low": or_low,
            "last_close": last_close,
            "last_vol": last_vol,
            "avg_open_vol": avg_vol,
        }
    except Exception as e:
        logger.debug(f"detect_orb_breakout error: {e}")
        return False, {}

# ============================================================
# 6) VWAP Reclaim Signal
# ============================================================

def detect_vwap_reclaim(df_5m: pd.DataFrame, lookback: int = 6) -> tuple[bool, dict]:
    """
    يكشف عبور السعر فوق VWAP من تحت (إعادة استرداد قوية).

    شرط الإشارة:
    - السعر كان تحت VWAP في 3 من آخر 6 شمعات.
    - الشمعة الأخيرة أغلقت فوق VWAP.
    - حجم الشمعة الأخيرة أعلى من المتوسط.
    """
    try:
        if df_5m is None or df_5m.empty or len(df_5m) < lookback + 1:
            return False, {}

        df = df_5m.copy()
        df.columns = [c.lower() for c in df.columns]

        # حساب VWAP لو غير موجود
        if 'vwap' not in df.columns:
            tp = (df['high'] + df['low'] + df['close']) / 3
            cum_vol = df['volume'].cumsum().replace(0, np.nan)
            cum_pv = (tp * df['volume']).cumsum()
            df['vwap'] = cum_pv / cum_vol

        recent = df.tail(lookback + 1)
        last = recent.iloc[-1]
        prev = recent.iloc[:-1]

        below_count = int((prev['close'] < prev['vwap']).sum())
        last_above = bool(last['close'] > last['vwap'])
        avg_vol = float(prev['volume'].mean())
        vol_ok = float(last['volume']) > avg_vol * 1.2

        is_reclaim = below_count >= 3 and last_above and vol_ok

        return is_reclaim, {
            "vwap": float(last['vwap']),
            "close": float(last['close']),
            "below_candles": below_count,
            "vol_ratio": round(float(last['volume']) / avg_vol, 2) if avg_vol > 0 else 0,
        }
    except Exception as e:
        logger.debug(f"detect_vwap_reclaim error: {e}")
        return False, {}

# ============================================================
# 7) Earnings Proximity Filter
# ============================================================

def is_near_earnings(symbol: str, days: int = 2) -> tuple[bool, str]:
    """يحذر إذا فيه إعلان أرباح خلال X يوم."""
    if not finnhub.enabled:
        return False, ""
    event = finnhub.get_earnings_within(symbol, days=days)
    if not event:
        return False, ""
    when = event.get("date") or event.get("hour", "")
    return True, f"⚠️ أرباح قادمة ({when}) — حذر من Gap عكسي"

# ============================================================
# Background Scanners (تستدعى من main.py كـ Threads)
# ============================================================

def opening_range_breakout_scanner(state, state_lock, cached_download_fn,
                                   send_telegram_fn, get_market_phase_fn,
                                   now_est_fn, save_state_fn):
    """
    ماسح Gap & Go: يفحص الأسهم بين 9:45 - 10:30 ET للـ ORB.
    يرسل تنبيه واحد لكل سهم في اليوم.
    """
    while True:
        try:
            now = now_est_fn()
            phase = get_market_phase_fn()

            # نفعل بين 9:45 - 10:30 ET فقط
            in_window = (now.hour == 9 and now.minute >= 45) or \
                        (now.hour == 10 and now.minute <= 30)
            if phase != "REGULAR" or not in_window:
                time.sleep(60)
                continue

            with state_lock:
                tickers = list(state.get("tickers", []))[:300]

            today_key = now.date().isoformat()
            for symbol in tickers:
                try:
                    signal_key = f"orb_{symbol}_{today_key}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue

                    df = cached_download_fn(symbol, period="2d", interval="5m")
                    if df is None or df.empty:
                        continue

                    is_orb, info = detect_orb_breakout(df)
                    if not is_orb:
                        continue

                    msg = (
                        f"🎯 *ORB Breakout: {symbol}*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💰 السعر: ${info['last_close']:.2f}\n"
                        f"📈 قمة OR (أول 15د): ${info['or_high']:.2f}\n"
                        f"🛡️ قاع OR: ${info['or_low']:.2f}\n"
                        f"📊 حجم: {int(info['last_vol']):,} (متوسط الافتتاح: {int(info['avg_open_vol']):,})\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💡 *Gap & Go Setup* — إدخال فوق ${info['or_high']:.2f} مع وقف تحت ${info['or_low']:.2f}"
                    )
                    send_telegram_fn(msg)

                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state_fn()
                except Exception:
                    continue
                time.sleep(0.4)

            time.sleep(120)
        except Exception as e:
            logger.error(f"ORB scanner error: {e}")
            time.sleep(60)

def vwap_reclaim_scanner(state, state_lock, cached_download_fn,
                         send_telegram_fn, get_market_phase_fn,
                         now_est_fn, save_state_fn):
    """ماسح VWAP Reclaim — كل 5 دقائق خلال الجلسة العادية."""
    while True:
        try:
            phase = get_market_phase_fn()
            if phase != "REGULAR":
                time.sleep(120)
                continue

            with state_lock:
                tickers = list(state.get("tickers", []))[:200]

            for symbol in tickers:
                try:
                    signal_key = f"vwapR_{symbol}_{int(time.time() / 3600)}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue

                    df = cached_download_fn(symbol, period="2d", interval="5m")
                    if df is None or df.empty:
                        continue

                    is_reclaim, info = detect_vwap_reclaim(df)
                    if not is_reclaim:
                        continue

                    msg = (
                        f"🌊 *VWAP Reclaim: {symbol}*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💰 السعر: ${info['close']:.2f}\n"
                        f"📊 VWAP: ${info['vwap']:.2f}\n"
                        f"🔄 كان تحت VWAP لـ {info['below_candles']} شمعات\n"
                        f"📈 حجم: {info['vol_ratio']}x من المتوسط\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💡 إعادة استرداد VWAP — إشارة قوة قصيرة المدى"
                    )
                    send_telegram_fn(msg)

                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state_fn()
                except Exception:
                    continue
                time.sleep(0.4)

            time.sleep(300)
        except Exception as e:
            logger.error(f"VWAP reclaim scanner error: {e}")
            time.sleep(60)

# ============================================================
# Quality Gate — يجمع الفلاتر معاً
# ============================================================

def quality_gate(symbol: str, df_5m: pd.DataFrame,
                 cached_download_fn, df_daily: Optional[pd.DataFrame] = None,
                 strict: bool = False) -> tuple[bool, list[str], list[str]]:
    """
    البوابة الرئيسية: تفحص كل عوامل الجودة قبل إرسال أي إشارة.

    Returns:
        (passed, reasons_pass, reasons_fail)
    """
    reasons_pass: list[str] = []
    reasons_fail: list[str] = []

    # 1) فلتر مضاد للـ Pump
    if df_daily is not None:
        is_ext, why = is_overextended(df_daily)
        if is_ext:
            reasons_fail.append(f"🚫 Overextended: {why}")
            if strict:
                return False, reasons_pass, reasons_fail
        else:
            reasons_pass.append("✅ ليس Overextended")

    # 2) Multi-timeframe alignment
    aligned, info = is_multi_timeframe_aligned(cached_download_fn, symbol)
    if aligned:
        reasons_pass.append(f"✅ Multi-TF: {', '.join(info)} متوافقة")
    else:
        reasons_fail.append(f"⚠️ Multi-TF ضعيف: {', '.join(info)}")
        if strict:
            return False, reasons_pass, reasons_fail

    # 3) Sector strength (لو Finnhub مفعل)
    if finnhub.enabled:
        ok, why = sector_strength_gate(cached_download_fn, symbol)
        if ok:
            reasons_pass.append(f"✅ {why}")
        else:
            reasons_fail.append(f"🚫 {why}")
            if strict:
                return False, reasons_pass, reasons_fail

    # 4) Earnings proximity (تحذير فقط، لا يرفض)
    near, msg = is_near_earnings(symbol, days=2)
    if near:
        reasons_fail.append(msg)

    return True, reasons_pass, reasons_fail

# ================= MERGED FROM BLOCKS =================

matplotlib.use('Agg')

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
AUTHORIZED_CHAT_ID = int(CHAT_ID) if CHAT_ID and CHAT_ID.lstrip("-").isdigit() else None
STATE_DIR = os.getenv("STATE_DIR", ".")

# Trading Parameters
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 50
DAILY_LOSS_LIMIT = 300.0
STATE_FILE = os.path.join(STATE_DIR, "state_v32.json")

# Scanner Settings
SCAN_INTERVAL_SEC = 540
CHUNK_SIZE = 100
FAST_FILTER_WORKERS = 3       # تقليل لتخفيف الضغط على السيرفر (كان 5)
DEEP_ANALYSIS_WORKERS = 3
DELAY_BETWEEN_REQUESTS = 0.5
BREAK_BETWEEN_CHUNKS = 5
TRADE_MONITOR_INTERVAL = 60
MAX_TICKERS_TO_SCAN = 4800

TELEGRAM_DELAY = 1.0

# فترة الحجب بين إشارات نفس السهم (ساعتين)
SIGNAL_COOLDOWN = 7200

# ✅ حد أدنى لـ RVOL بحسب الجلسة — لمنع إرسال إشارات بـ 0.0x
MIN_RVOL_BY_PHASE = {
    "PRE":     1.5,   # في pre-market نطلب على الأقل 1.5x من معدل أيام pre السابقة
    "REGULAR": 1.2,
    "AFTER":   1.5,
    "CLOSED":  999,
}

# ================= CACHE SYSTEM SETTINGS =================
CACHE_TTL = {
    "1m": 15,      # بيانات الدقيقة تخزن لمدة 15 ثانية فقط
    "5m": 60,      # بيانات 5 دقائق تخزن لمدة دقيقة
    "15m": 180,    # بيانات 15 دقيقة تخزن لمدة 3 دقائق
    "1d": 900,     # بيانات يومية تخزن لمدة 15 دقيقة
}
_cache = {}
_cache_lock = threading.RLock()

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

# توافق عكسي مع أي أجزاء قديمة ما زالت تستخدم الاسم السابق
TP_PCT = BASE_TP_PCT

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
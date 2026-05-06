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
# Trading Parameters - VERSION PENNY HUNTER
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5
DAILY_LOSS_LIMIT = 150.0
STATE_FILE = "state_penny_hunter.json"

# Scanner Settings
SCAN_INTERVAL_SEC = 540
CHUNK_SIZE = 100
FAST_FILTER_WORKERS = 3
DEEP_ANALYSIS_WORKERS = 3
DELAY_BETWEEN_REQUESTS = 0.5
BREAK_BETWEEN_CHUNKS = 5
TRADE_MONITOR_INTERVAL = 60
MAX_TICKERS_TO_SCAN = 4800

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
    "PRE":     {"min_score": 130, "size_multiplier": 0.5, "vol_surge_mult": 2.0, "description": "🟡 Pre-Market"},
    "REGULAR": {"min_score": 120, "size_multiplier": 0.8, "vol_surge_mult": 1.5, "description": "🟢 Regular Hours"},
    "AFTER":   {"min_score": 140, "size_multiplier": 0.3, "vol_surge_mult": 2.5, "description": "🔵 After-Hours"},
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
                    
                    if rvol >= 5:
                        signal_key = f"rvol_{symbol}"
                        with state_lock:
                            last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                        if time.time() - last_sent > 3600:  # مرة كل ساعة
                            last = df.iloc[-1]
                            msg = f"🔥 *RVOL SPIKE: {symbol}*\n"
                            msg += f"━━━━━━━━━━━━━━━━\n"
                            msg += f"📊 RVOL: *{rvol:.1f}x* (نشاط استثنائي!)\n"
                            msg += f"💰 السعر: ${last['close']:.2f}\n"
                            msg += f"📈 التغير: +{(last['close'] - df['close'].iloc[-5]) / df['close'].iloc[-5] * 100:.1f}% (5 شمعات)\n"
                            msg += f"🔥 الحجم: {int(last['volume']):,}\n"
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

                    # شرط 3: حجم اليوم > 2.5x من متوسط 5 أيام
                    avg_vol_5d = float(df_d['volume'].iloc[-6:-1].mean())
                    if avg_vol_5d <= 0:
                        continue
                    vol_ratio = today_vol / avg_vol_5d
                    if vol_ratio < 2.5:
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

                    # شرط 6: RSI 5m بين 55 و 80
                    delta = closes.diff()
                    gain = delta.where(delta > 0, 0).rolling(14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                    rs = gain / loss.replace(0, 1e-10)
                    rsi5 = float((100 - (100 / (1 + rs))).iloc[-1])
                    if rsi5 < 55 or rsi5 > 80:
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

def analyze_news_sentiment(title, desc=""):
    """تحليل إيجابية / سلبية الخبر مع حساب النقاط بالوزن"""
    text = (title + " " + desc).lower()

    for kw in NEGATIVE_WORDS:
        if kw in text:
            return "negative", 0

    score = 0
    for kw in STRONG_POSITIVE:
        if kw in text:
            if kw in ["fda approval", "fda approved", "acquisition", "merger", "buyout"]:
                score += 30
            elif kw in ["partnership", "contract awarded", "beats estimates", "record revenue"]:
                score += 20
            else:
                score += 10

    if score > 0:
        return "positive", min(score, 100)
    return "neutral", 0

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

                    sentiment, score = analyze_news_sentiment(title, item.get('desc', ''))
                    if sentiment != "positive" or score < 40:
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

                            # ✅ فلتر السعر: فقط أسهم تحت $30
                            if price > 30:
                                continue

                            vol_now  = df['volume'].iloc[-1]
                            vol_avg  = df['volume'].mean()
                            gain_pct = (price - df['open'].iloc[0]) / df['open'].iloc[0] * 100

                            vol_spike    = vol_avg > 0 and vol_now > vol_avg * 2
                            not_too_late = gain_pct < 25

                            if not vol_spike or not not_too_late:
                                continue

                            entry = price
                            tp = round(entry * 1.10, 2)
                            sl = round(entry * 0.95, 2)

                            try:
                                translator = GoogleTranslator(source='en', target='ar')
                                title_ar = translator.translate(title[:200])
                            except Exception:
                                title_ar = title

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

                            msg = (
                                f"📰🚀 *إشارة خبر إيجابي*\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"📊 *{symbol}* (تحت ${price:.2f})\n"
                                f"🏷️ النوع: {news_type}\n"
                                f"📝 الخبر: {title_ar[:150]}\n"
                                f"🔗 [رابط الخبر]({link})\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"⏰ وقت الدخول المقترح: *{now_time}*\n"
                                f"💰 سعر الدخول: *${entry:.2f}*\n"
                                f"🎯 الهدف: *${tp:.2f}* (+10%)\n"
                                f"🛑 وقف الخسارة: *${sl:.2f}* (-5%)\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"📈 الارتفاع من الافتتاح: +{gain_pct:.1f}%\n"
                                f"🔥 الحجم: {int(vol_now):,} ({vol_now/vol_avg:.1f}x المتوسط)\n"
                                f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"
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

if __name__ == "__main__":
    load_state()
    ensure_state_schema()

    threading.Thread(target=background_scanner,       daemon=True).start()
    threading.Thread(target=background_monitor,       daemon=True).start()
    threading.Thread(target=rss_news_scanner,         daemon=True).start()
    threading.Thread(target=post_halt_entry_monitor,  daemon=True).start()
    threading.Thread(target=short_squeeze_scanner,    daemon=True).start()
    threading.Thread(target=premarket_gap_scanner,    daemon=True).start()
    threading.Thread(target=correlation_scanner,      daemon=True).start()
    threading.Thread(target=sector_strength_scanner,  daemon=True).start()
    threading.Thread(target=edgar_8k_scanner,         daemon=True).start()
    threading.Thread(target=premarket_gapper_scanner, daemon=True).start()
    threading.Thread(target=fetch_news_catalyst,      daemon=True).start()
    threading.Thread(target=scan_vshape_opportunities, daemon=True).start()
    threading.Thread(target=rvol_spike_scanner,       daemon=True).start()
    threading.Thread(target=power_runner_scanner,     daemon=True).start()  # 🚀 HTCO-style runners
    threading.Thread(target=elite_3_summary_loop,     daemon=True).start()
    threading.Thread(target=daily_report_scheduler,   daemon=True).start()
    threading.Thread(target=cleaner_loop,             daemon=True).start()
    threading.Thread(target=cache_cleaner_loop,       daemon=True).start()  # 💾 Cache

    if bot:
        threading.Thread(target=run_telegram_bot, daemon=True).start()
    
    logger.info("=" * 50)
    logger.info("✅ Bot started successfully!")
    logger.info(f"📊 Universe: {len(state.get('tickers', []))} stocks")
    logger.info("=" * 50)
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)

# ================= CACHED DATA FETCHER =================



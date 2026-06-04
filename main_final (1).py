
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

# ================= MARKET PHASE SETTINGS =================
PHASE_SETTINGS = {
    "PRE":     {"min_score": 65,  "size_multiplier": 0.5, "vol_surge_mult": 1.5, "description": "🟡 Pre-Market"},
    "REGULAR": {"min_score": 70,  "size_multiplier": 1.0, "vol_surge_mult": 2.0, "description": "🟢 Regular Hours"},
    "AFTER":   {"min_score": 70,  "size_multiplier": 0.3, "vol_surge_mult": 2.0, "description": "🔵 After-Hours"},
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
                    bot.send_photo(CHAT_ID, photo, caption=message, parse_mode="Markdown")
                else:
                    bot.send_message(CHAT_ID, message, parse_mode="Markdown")
                _last_telegram_time = time.time()
            except Exception as e:
                logger.error(f"Telegram send error: {e}")

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    if not ensure_authorized(message):
        return
    send_telegram("مرحباً بك في بوت التداول!\nاستخدم /status لمعرفة حالة الصفقات.\nاستخدم /performance لمعرفة الأداء العام.\nاستخدم /daily_report لتقرير اليوم.")

@bot.message_handler(commands=['status'])
def send_status(message):
    if not ensure_authorized(message):
        return
    with state_lock:
        if not state["open_trades"]:
            send_telegram("لا توجد صفقات مفتوحة حالياً.")
            return
        msg = "*Open Trades:*\n"
        for symbol, trade in state["open_trades"].items():
            msg += f"- {symbol}: Entry ${trade['entry']:.2f}, SL ${trade['sl']:.2f}, TP ${trade['tp']:.2f}\n"
        send_telegram(msg)

@bot.message_handler(commands=['performance'])
def send_performance(message):
    if not ensure_authorized(message):
        return
    with state_lock:
        perf = state["performance"]
        msg = (
            f"*Performance Summary:*\n"
            f"Wins: {perf['wins']}\n"
            f"Losses: {perf['losses']}\n"
            f"Total PnL: ${perf['total_pnl']:.2f}"
        )
        send_telegram(msg)

@bot.message_handler(commands=['daily_report'])
def send_daily_report(message):
    if not ensure_authorized(message):
        return
    send_telegram(build_daily_report_message())

@bot.message_handler(commands=['halted'])
def send_halted_stocks(message):
    if not ensure_authorized(message):
        return
    with state_lock:
        if not state["halted_stocks"]:
            send_telegram("لا توجد أسهم موقوفة حالياً.")
            return
        msg = "*Halted Stocks:*\n"
        for symbol, details in state["halted_stocks"].items():
            msg += f"- {symbol}: Reason: {details['reason']}, Time: {datetime.fromtimestamp(details['time']).strftime('%H:%M')}\n"
        send_telegram(msg)

@bot.message_handler(commands=['clear_cache'])
def clear_cache_command(message):
    if not ensure_authorized(message):
        return
    with _cache_lock:
        _cache.clear()
    send_telegram("تم مسح الكاش بنجاح.")

@bot.message_handler(commands=['update_tickers'])
def update_tickers_command(message):
    if not ensure_authorized(message):
        return
    send_telegram("جاري تحديث قائمة الأسهم، قد يستغرق الأمر بضع دقائق...")
    threading.Thread(target=update_all_tickers, daemon=True).start()

# ================= DATA ACQUISITION =================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def safe_download(symbol, period, interval, timeout=10):
    """تحميل البيانات التاريخية للسهم باستخدام curl_cffi لتجاوز الحظر."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={period}&interval={interval}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
    }
    
    with _cache_lock:
        cache_key = f"{symbol}_{period}_{interval}"
        if cache_key in _cache and (time.time() - _cache[cache_key]["timestamp"]) < CACHE_TTL.get(interval, 60):
            return _cache[cache_key]["data"]

    try:
        response = requests.get(url, headers=headers, impersonate="chrome107", timeout=timeout)
        response.raise_for_status()
        data = response.json()

        chart = data['chart']
        result = chart['result']
        if not result or result[0].get('indicators') is None:
            return pd.DataFrame()

        timestamps = result[0]['timestamp']
        quotes = result[0]['indicators']['quote'][0]
        volumes = quotes['volume']
        opens = quotes['open']
        closes = quotes['close']
        highs = quotes['high']
        lows = quotes['low']

        df = pd.DataFrame({
            'timestamp': timestamps,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes
        })
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_convert(EASTERN_TZ)
        df = df.set_index('datetime')
        df = df.dropna()
        
        with _cache_lock:
            _cache[cache_key] = {"data": df, "timestamp": time.time()}
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error downloading {symbol} ({period}, {interval}): {e}")
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for {symbol}: {e}")
    except Exception as e:
        logger.error(f"Error downloading {symbol} ({period}, {interval}): {e}")
    return pd.DataFrame()

def fast_filter(symbol):
    """فلتر سريع لتقليل الرموز قبل التحليل العميق"""
    try:
        df = safe_download(symbol, period="1d", interval="5m", timeout=5)
        if df.empty or len(df) < 3:
            return False
        price = df["close"].iloc[-1]
        volume = df["volume"].iloc[-1]

        if price < MIN_PRICE or price > MAX_PRICE:
            return False
        if volume < MIN_VOLUME:
            return False

        return True
    except:
        return False


def detect_silent_accumulation(df):
    """🕵️ يكتشف التجميع الصامت: حجم عالٍ مع حركة سعر قليلة"""
    try:
        if len(df) < 20:
            return False, 0
        avg_vol = df['volume'].rolling(20).mean().iloc[-1]
        current_vol = df['volume'].iloc[-1]
        price_change = abs((df['close'].iloc[-1] - df['close'].iloc[-3]) / (df['close'].iloc[-3] + 1e-9))

        if avg_vol > 0 and current_vol > avg_vol * 3 and price_change < 0.02:
            return True, round(current_vol / avg_vol, 2)
        return False, 0
    except:
        return False, 0


def generate_chart(symbol, df, entry, tp, sl):
    """إنشاء رسم بياني للإشارة"""
    try:
        df_plot = df.tail(60).copy()
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df_plot.index, df_plot['close'], 'cyan', linewidth=1.5, label='Price')
        ax.axhline(y=entry, color='lime', linestyle='--', linewidth=1.5, label=f'Entry ${entry:.2f}')
        ax.axhline(y=tp, color='green', linestyle='--', linewidth=1.5, label=f'TP ${tp:.2f}')
        ax.axhline(y=sl, color='red', linestyle='--', linewidth=1.5, label=f'SL ${sl:.2f}')
        ax.set_title(f'{symbol} - Signal', fontsize=14, color='white')
        ax.set_facecolor('#0d1117')
        fig.patch.set_facecolor('#0d1117')
        ax.tick_params(colors='white')
        ax.legend()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100, facecolor='#0d1117')
        buf.seek(0)
        plt.close(fig)
        return buf
    except Exception as e:
        logger.error(f"Chart error: {e}")
        return None


def cached_download(symbol, period="5d", interval="15m", timeout=10, force_refresh=False):
    """تحميل البيانات مع التخزين المؤقت المحسّن"""
    cache_key = f"{symbol}_{period}_{interval}"
    
    if not force_refresh:
        with _cache_lock:
            if cache_key in _cache:
                cached_data, timestamp = _cache[cache_key]
                ttl = CACHE_TTL.get(interval, 60)
                if time.time() - timestamp < ttl:
                    return cached_data.copy() if not cached_data.empty else pd.DataFrame()
    
    df = safe_download(symbol, period, interval, timeout)
    
    with _cache_lock:
        _cache[cache_key] = (df, time.time())
    
    return df.copy() if not df.empty else pd.DataFrame()


def update_all_tickers():
    """تحديث قائمة الأسهم من مصادر متعددة (SEC, GitHub) لتشمل الأسهم الجديدة."""
    logger.info("Updating all tickers from SEC and GitHub...")
    new_tickers = set()
    
    # 1. من قائمة SEC (الأسهم النشطة)
    try:
        sec_url = "https://www.sec.gov/files/company_tickers.json"
        with urllib.request.urlopen(sec_url) as url:
            data = json.load(url)
            for item in data:
                new_tickers.add(item['ticker'])
        logger.info(f"Found {len(new_tickers)} tickers from SEC.")
    except Exception as e:
        logger.error(f"Error fetching tickers from SEC: {e}")

    # 2. من قائمة GitHub (أسهم الـ Penny Stocks النشطة)
    try:
        github_url = "https://raw.githubusercontent.com/ryancbutler/penny-stocks/main/penny_stocks.csv"
        df_penny = pd.read_csv(github_url)
        new_tickers.update(df_penny['Ticker'].tolist())
        logger.info(f"Found {len(df_penny)} penny stocks from GitHub.")
    except Exception as e:
        logger.error(f"Error fetching penny stocks from GitHub: {e}")

    # 3. إضافة أسهم الـ NASDAQ100 و S&P500 (للتغطية الشاملة)
    try:
        nasdaq_url = "https://raw.githubusercontent.com/datasets/nasdaq-100/main/data/nasdaq-100.csv"
        sp500_url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        df_nasdaq = pd.read_csv(nasdaq_url)
        df_sp500 = pd.read_csv(sp500_url)
        new_tickers.update(df_nasdaq['Symbol'].tolist())
        new_tickers.update(df_sp500['Symbol'].tolist())
        logger.info(f"Found {len(df_nasdaq)} NASDAQ100 and {len(df_sp500)} S&P500 tickers.")
    except Exception as e:
        logger.error(f"Error fetching index tickers: {e}")

    with state_lock:
        state["tickers"] = sorted(list(new_tickers))
        state["last_ticker_update"] = time.time()
        save_state()
    logger.info(f"Total {len(state['tickers'])} tickers updated.")

def get_daily_metrics(symbol):
    """يحسب المؤشرات اليومية للسهم (SMA50, PDH) من بيانات 1d."""
    df_daily = safe_download(symbol, period="60d", interval="1d")
    if df_daily.empty or len(df_daily) < 50:
        return None
    
    df_daily['sma50'] = df_daily['close'].rolling(window=50).mean()
    last_day = df_daily.iloc[-1]
    prev_day = df_daily.iloc[-2]
    
    is_bullish = last_day['close'] > last_day['sma50']
    is_breakout_pdh = last_day['high'] > prev_day['high'] # اختراق قمة اليوم السابق
    
    return {"is_bullish": is_bullish, "is_breakout_pdh": is_breakout_pdh}

# ================= INDICATORS & SCANNERS =================
def compute_indicators(df):
    """يحسب المؤشرات الفنية الأساسية."""
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['sma50'] = df['close'].rolling(window=50).mean()
    df['sma200'] = df['close'].rolling(window=200).mean()
    
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(span=14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(span=14, adjust=False).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # Bollinger Bands
    df['20sma'] = df['close'].rolling(window=20).mean()
    df['stddev'] = df['close'].rolling(window=20).std()
    df['upper_bb'] = df['20sma'] + (df['stddev'] * 2)
    df['lower_bb'] = df['20sma'] - (df['stddev'] * 2)
    
    # VWAP (Volume Weighted Average Price)
    df['vwap'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
    
    # ATR (Average True Range)
    high_low = df['high'] - df['low']
    high_prev_close = abs(df['high'] - df['close'].shift())
    low_prev_close = abs(df['low'] - df['close'].shift())
    tr = pd.DataFrame({'hl': high_low, 'hpc': high_prev_close, 'lpc': low_prev_close}).max(axis=1)
    df['atr'] = tr.ewm(span=14, adjust=False).mean()
    
    # Volume Moving Average
    df['vol_ma'] = df['volume'].rolling(window=20).mean()
    
    return df

def detect_accumulation(df):
    """يكتشف التجميع (Accumulation) بناءً على مؤشر OBV وحركة السعر."""
    if len(df) < 20:
        return False, 0
    
    df['obv'] = (np.sign(df['close'].diff()) * df['volume']).cumsum()
    
    # هل OBV يتزايد بينما السعر ثابت أو يتراجع قليلاً؟
    obv_rising = df['obv'].iloc[-1] > df['obv'].iloc[-10:].mean()
    price_stable = (df['close'].iloc[-1] - df['close'].iloc[-10:].mean()) / df['close'].iloc[-10:].mean() < 0.02
    
    score = 0
    if obv_rising: score += 15
    if price_stable: score += 10
    
    return score > 20, score

def detect_explosion(df):
    """يكتشف الانفجار السعري (Price Explosion) مع حجم تداول مرتفع."""
    if len(df) < 5:
        return False, 0
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # ارتفاع السعر بأكثر من 3% في شمعة واحدة
    price_jump = (last['close'] - prev['close']) / prev['close'] > 0.03
    
    # حجم تداول أعلى من متوسط 20 شمعة بـ 200% على الأقل
    vol_surge = last['volume'] > df['vol_ma'].iloc[-1] * 2.0
    
    score = 0
    if price_jump: score += 20
    if vol_surge: score += 30
    
    return score > 40, score

def detect_pre_breakout(df):
    """يكتشف السهم الذي يستعد لاختراق (Pre-Breakout) بناءً على تضييق البولينجر باندز."""
    if len(df) < 20:
        return False
    
    # تضييق البولينجر باندز (Bollinger Bands Squeeze)
    bb_width = (df['upper_bb'] - df['lower_bb']) / df['20sma']
    is_squeezing = bb_width.iloc[-1] < bb_width.iloc[-5:].mean() * 0.9 # عرض الباندز أقل بـ 10%
    
    # السعر قريب من الحد العلوي للبولينجر باندز
    price_near_upper_bb = df['close'].iloc[-1] > df['upper_bb'].iloc[-1] * 0.99
    
    return is_squeezing and price_near_upper_bb

def detect_momentum_start(symbol):
    """
    🚀 يفحص شموع الدقيقة الواحدة (1-minute). 
    إذا وجد 3 شمعات خضراء متتالية بحجم متزايد، فهذه بداية اندفاع (Raw Momentum).
    """
    try:
        df_1min = safe_download(symbol, period="1d", interval="1m")
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

def detect_whale_accumulation(df):
    """🐋 يكتشف التجميع من قبل الحيتان (Whale Accumulation) باستخدام مؤشر A/D Line."""
    if len(df) < 20:
        return False
    
    # Accumulation/Distribution Line
    clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'] + 1e-9)
    ad_line = (clv * df['volume']).cumsum()
    
    # هل خط A/D يتزايد بينما السعر ثابت أو يتراجع قليلاً؟
    ad_rising = ad_line.iloc[-1] > ad_line.iloc[-10:].mean()
    price_stable = (df['close'].iloc[-1] - df['close'].iloc[-10:].mean()) / df['close'].iloc[-10:].mean() < 0.02
    
    return ad_rising and price_stable

def detect_bb_squeeze(df):
    """يكتشف ضغط البولينجر باندز (Bollinger Bands Squeeze) الذي يسبق الانفجار."""
    if len(df) < 20:
        return False
    
    bb_width = (df['upper_bb'] - df['lower_bb']) / df['20sma']
    # هل عرض الباندز في أدنى مستوياته خلال 20 شمعة؟
    is_squeezing = bb_width.iloc[-1] < bb_width.rolling(window=20).min().iloc[-1]
    
    return is_squeezing

def calculate_rvol(df):
    """
    حساب الحجم النسبي (Relative Volume).
    RVOL > 2  = اهتمام متزايد
    RVOL > 3  = انفجار حجم قوي
    RVOL > 5  = نشاط استثنائي
    """
    try:
        current_vol = df['volume'].iloc[-1]
        avg_vol = df['volume'].rolling(20).mean().iloc[-1]
        if avg_vol <= 0:
            return 1.0
        return round(current_vol / avg_vol, 2)
    except:
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

def calculate_atr(df):
    """يحسب مؤشر Average True Range (ATR) لتقييم التقلب."""
    if 'atr' in df.columns:
        return df['atr'].iloc[-1]
    return 0

def calculate_position_size(price, sl_price, capital, risk_per_trade):
    """يحسب حجم الصفقة بناءً على إدارة المخاطر."""
    if sl_price >= price:
        return 0 # منع القسمة على صفر أو حساب خاطئ
    
    risk_amount = capital * risk_per_trade
    stop_loss_per_share = price - sl_price
    
    if stop_loss_per_share <= 0:
        return 0 # لا يمكن حساب حجم إذا كان وقف الخسارة غير صحيح
        
    num_shares = risk_amount / stop_loss_per_share
    
    # التأكد من أن حجم الصفقة لا يتجاوز 10% من رأس المال الكلي
    max_shares_by_capital = (capital * 0.10) / price
    
    return min(int(num_shares), int(max_shares_by_capital))

def open_trade(symbol, entry_price, score, df, is_accumulating, is_pre_breakout, phase, settings, **kwargs):
    with state_lock:
        if symbol in state["open_trades"]:
            return
        if len(state["open_trades"]) >= MAX_OPEN_TRADES:
            return
        if state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT:
            return

        rvol = kwargs.get("rvol", 1.0)
        tp, sl, tp_pct = get_trade_levels(entry_price, rvol)
        size = calculate_position_size(entry_price, sl, CAPITAL, RISK_PER_TRADE)

        if size == 0:
            return

        state["open_trades"][symbol] = {
            "entry": entry_price,
            "sl": sl,
            "tp": tp,
            "size": size,
            "time": time.time(),
            "score": score,
            "phase": phase,
            "rvol": rvol,
            "tp_pct": tp_pct,
            "is_accumulating": is_accumulating,
            "is_pre_breakout": is_pre_breakout,
            "bullish": kwargs.get("is_bullish", True),
            "pdh": kwargs.get("is_pdh", False),
            "silent_acc": kwargs.get("is_silent_acc", False),
            "raw_mom": kwargs.get("is_raw_mom", False),
            "psych_level": kwargs.get("psych_level", 0),
            "gap_pct": kwargs.get("gap_pct", 0),
            "v_shape": kwargs.get("is_v_shape", False),
            "whale_acc": kwargs.get("is_whale_acc", False),
            "bb_squeeze": kwargs.get("is_bb_squeeze", False),
            "effective_min_score": kwargs.get("effective_min_score", 0)
        }
        save_state()
        update_daily_report_on_open(symbol, entry_price, score)

    reason_list = []
    if kwargs.get("is_silent_acc"): reason_list.append("تجميع صامت")
    if kwargs.get("is_raw_mom"): reason_list.append("بداية زخم")
    if kwargs.get("psych_level"): reason_list.append(f"اختراق نفسي ${kwargs['psych_level']}")
    if kwargs.get("gap_pct"): reason_list.append(f"فجوة سعرية {kwargs['gap_pct']}% ")
    if kwargs.get("is_v_shape"): reason_list.append("ارتداد V-Shape")
    if kwargs.get("is_pdh"): reason_list.append("اختراق قمة أمس")
    if kwargs.get("is_whale_acc"): reason_list.append("سيولة مخفية")
    if kwargs.get("is_bb_squeeze"): reason_list.append("ضغط انفجار")
    if not kwargs.get("is_bullish"): reason_list.append("تحت SMA50 مع خصم جودة")
    
    reason_str = " + ".join(reason_list) if reason_list else "اختراق فني قوي"

    msg = (
        f"✅ *OPEN: {symbol}*\n"
        f"💰 Entry: ${entry_price:.2f}\n"
        f"🛑 SL: ${sl:.2f}\n"
        f"🎯 TP: ${tp:.2f}\n"
        f"📊 Score: {score}/100\n"
        f"🔥 RVOL: {rvol:.1f}x\n"
        f"📝 Reason: {reason_str}"
    )
    send_telegram(msg)

    # إضافة المرشحين لقائمة النخبة
    with state_lock:
        state.setdefault("elite_candidates", []).append({
            "symbol": symbol,
            "score": score,
            "price": entry_price,
            "reason": reason_str,
            "bullish": kwargs.get("is_bullish", True),
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

        df = safe_download(symbol, period="5d", interval="15m")
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
        
        if not explosion and not is_silent_acc:
            return

        last = df.iloc[-1]
        
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
        if vol_surge: score += 25  # زيادة وزن الحجم
        if price_break: score += 25 # زيادة وزن السعر
        if 40 < last['rsi'] < 70: score += 10
        if last['ema9'] > last['ema21']: score += 10

        # 📊 RVOL - الحجم النسبي (أهم من الحجم المطلق)
        rvol = calculate_rvol(df)
        
        # ✅ حماية الجودة: لا ترسل إشارات بـ RVOL ضعيف جداً
        min_rvol = MIN_RVOL_BY_PHASE.get(phase, 1.0)
        if rvol < min_rvol:
            return
        
        if rvol >= 5:    score += 15   # نشاط استثنائي
        elif rvol >= 3:  score += 10   # انفجار حجم قوي
        elif rvol >= 2:  score += 5    # اهتمام متزايد
        
        # 🎯 Low Float - أسهم رخيصة تتحرك أسرع
        low_float = is_low_float(last['close'], last['volume'])
        if low_float:
            score += 10
            
        # 🕵️ بونص التجميع الصامت
        if is_silent_acc:
            score += 20
            
        # 🚀 بونص بداية الزخم
        if is_raw_momentum:
            score += 15

        # 📊 ATR - تقييم التقلب (Volatility)
        atr = calculate_atr(df)
        price = last['close']
        # فلتر: تجاهل الأسهم شديدة التقلب (أكثر من 30% من السعر)
        if atr > price * 0.3:
            logger.info(f"Skipping {symbol}: Extreme volatility (ATR {atr} > 30% of price {price})")
            return
        # بونص في السكور: إذا التقلب معقول وصحي (بين 3% و 20% من السعر)
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

        # خصم جودة الاتجاه إذا كان السهم أسفل SMA50 بدل رفضه نهائياً
        score -= trend_penalty
        effective_min_score = get_effective_min_score(phase, settings)

        # 🛡️ فلتر VWAP + RSI (تم تخفيفه لزيادة الفرص)
        price_above_vwap = last['close'] > last['vwap'] * 0.99 # نسمح بـ 1% تحت الـ VWAP
        rsi_ok           = last['rsi'] > 45 # خفضنا الـ RSI من 50 لـ 45

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
        elif score >= 60:
            # إرسال تنبيه "فرصة للمراقبة" لزيادة التفاعل
            now = time.time()
            signal_key = f"watch_{symbol}_{int(now/3600)}"
            with state_lock:
                if signal_key not in state.get("seen_signals", {}):
                    msg = f"👀 *فرصة للمراقبة: {symbol}*\n"
                    msg += f"━━━━━━━━━━━━━━━━\n"
                    msg += f"📊 السكور: *{score}/100*\n"
                    msg += f"💰 السعر: ${last['close']:.2f}\n"
                    msg += f"🔍 الحالة: {reason_str}\n"
                    msg += f"💡 السهم يظهر بوادر إيجابية، تابعه يدوياً!"
                    send_telegram(msg)
                    state.setdefault("seen_signals", {})[signal_key] = now
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


def rss_catalyst_scanner():
    """ماسح مخصص لصيد الأسهم التي ترتفع بناءً على أخبار إيجابية قوية."""
    RSS_FEEDS = [
        "https://www.benzinga.com/feed",
        "https://www.businesswire.com/rss/home/?rss=G1",
        "https://www.prnewswire.com/rss/news-releases-list.rss",
    ]
    
    IMPORTANT_KEYWORDS = [
        "clinical supply agreement", "partnership", "acquisition", "merger", 
        "fda approval", "breakthrough", "contract awarded", "buyout"
    ]

    while True:
        try:
            for feed_url in RSS_FEEDS:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries[:20]:
                    title = entry.title.lower()
                    match = any(kw in title for kw in IMPORTANT_KEYWORDS)
                    if not match:
                        continue

                    symbol_match = re.search(r'\(([A-Z]{1,5})\)', entry.title)
                    if not symbol_match:
                        symbol_match = re.search(r'\b([A-Z]{2,5})\b', entry.title.split()[0])
                    if not symbol_match:
                        continue
                    
                    symbol = symbol_match.group(1).upper()

                    df = cached_download(symbol, period="2d", interval="5m")
                    if df.empty or len(df) < 10:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    price = df['close'].iloc[-1]
                    current_vol = df['volume'].iloc[-1]
                    avg_vol = df['volume'].rolling(20).mean().iloc[-1]
                    prev_close = df['close'].iloc[-6]

                    if price < prev_close * 1.05:
                        continue
                    if current_vol < avg_vol * 2:
                        continue

                    signal_key = f"cat_{symbol}_{int(time.time()/3600)}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue
                    
                    entry_price = price
                    tp = round(entry_price * 1.12, 2)
                    sl = round(entry_price * 0.96, 2)

                    msg = (
                        f"📰🔥 *خبر محفز: {symbol}*\n"
                        f"من: {feed_url.split('/')[2]}\n"
                        f"📝 الخبر: {entry.title[:150]}...\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💰 السعر الحالي: *${entry_price:.2f}*\n"
                        f"📈 الارتفاع الأولي: +{(price/prev_close-1)*100:.1f}%\n"
                        f"🔥 الحجم: {int(current_vol):,} ({current_vol/avg_vol:.1f}x)\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🎯 الهدف الموصى به: *${tp}* (+12%)\n"
                        f"🛑 وقف الخسارة الموصى به: *${sl}* (-4%)\n"
                        f"⚠️ *إشارة وليست توصية، فرصة قصيرة المدى*"
                    )
                    send_telegram(msg)
                    
                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()
                    
                    time.sleep(3)

        except Exception as e:
            logger.error(f"RSS Catalyst Scanner Error: {e}")
        time.sleep(300)


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
                            futures = [executor.submit(process_symbol, sym) for sym in filtered]
                            for future in as_completed(futures):
                                try:
                                    future.result()
                                except Exception as e:
                                    logger.error(f"Deep analysis failed: {e}")
                    
            update_trades()
            time.sleep(SCAN_INTERVAL_SEC) # الانتظار قبل المسح التالي

        except Exception as e:
            logger.error(f"Background scanner error: {e}")
            time.sleep(60)

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
                    df = safe_download(symbol, period="5d", interval="15m")
                    if df.empty or len(df) < 20:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    rvol = calculate_rvol(df)
                    
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

def run_telegram_bot():
    """تشغيل بوت التليجرام في خيط منفصل."""
    if bot:
        logger.info("Telegram bot started.")
        bot.polling(non_stop=True)

def cache_cleaner_loop():
    """تنظيف الكاش بشكل دوري."""
    while True:
        time.sleep(300) # كل 5 دقائق
        with _cache_lock:
            keys_to_delete = []
            for key, (_, timestamp) in _cache.items():
                interval = key.split('_')[-1] # استخراج الفاصل الزمني من المفتاح
                if (time.time() - timestamp) > CACHE_TTL.get(interval, 60):
                    keys_to_delete.append(key)
            for key in keys_to_delete:
                del _cache[key]
            if keys_to_delete:
                logger.debug(f"Cleaned {len(keys_to_delete)} items from cache.")

# ================= MAIN EXECUTION =================
if __name__ == "__main__":
    load_state()
    ensure_state_schema()
    update_all_tickers()

    # بدء الخيوط (Threads) للماسحات والمهام الخلفية
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=rss_catalyst_scanner, daemon=True).start()
    threading.Thread(target=update_trades, daemon=True).start()
    threading.Thread(target=daily_report_scheduler, daemon=True).start()
    threading.Thread(target=rvol_spike_scanner, daemon=True).start()
    threading.Thread(target=elite_3_summary_loop, daemon=True).start()
    threading.Thread(target=cache_cleaner_loop, daemon=True).start()

    # تشغيل بوت التليجرام (يجب أن يكون في الخيط الرئيسي أو خيط خاص به)
    if bot:
        threading.Thread(target=run_telegram_bot, daemon=True).start()

    # تشغيل Flask app (يجب أن يكون في الخيط الرئيسي أو خيط خاص به)
    # في بيئات الإنتاج مثل Railway، يتم تشغيل Flask بواسطة Gunicorn أو ما شابه
    # هنا نستخدمه للـ healthcheck فقط
    app.run(host='0.0.0.0', port=os.getenv("PORT", 5000), debug=False)

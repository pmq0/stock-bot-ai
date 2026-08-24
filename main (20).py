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
import gc
try:
    import psutil
except ImportError:
    psutil = None
try:
    import pandas_ta as ta
except ImportError:
    ta = None

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
import yfinance as yf

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
    "1m": 30,
    "5m": 120,
    "15m": 300,
    "1d": 1800,
}
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
# AUTHORIZED_CHAT_ID يُقرأ من البيئة في قسم الإعدادات أعلاه.
CAPITAL = float(os.getenv("CAPITAL", "10000"))
# وضع المحاكاة مفعل افتراضيًا؛ لا يوجد تنفيذ وساطة حقيقي في هذا الملف.
PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() in ("1", "true", "yes", "on")
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.005"))  # 0.5% كحد محافظ
MAX_POSITION_VALUE = float(os.getenv("MAX_POSITION_VALUE", str(CAPITAL * 0.25)))
MAX_OPEN_TRADES = 5
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "150"))
STATE_FILE = "state_penny_hunter.json"

# حماية أوامر Telegram: يجب ضبط AUTHORIZED_CHAT_ID صراحةً.
AUTHORIZED_CHAT_ID = os.getenv("AUTHORIZED_CHAT_ID", "").strip()

# محرك التوصية الموحد: AI يفسر، والقواعد المحلية تملك حق الرفض.
RECOMMENDATION_MIN_CONFIDENCE = 75
RECOMMENDATION_MIN_TECH_SCORE = 70
RECOMMENDATION_MIN_RVOL = 2.0
RECOMMENDATION_MIN_RR = 2.0
RECOMMENDATION_EXPIRY_MINUTES = 30
RECOMMENDATION_MAX_HISTORY = 500

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
PENNY_MIN_GAIN = 15.0
PENNY_MIN_RVOL = 4.0

# Scanner General Settings
MIN_PRICE = 0.2
MAX_PRICE = 30.0
MIN_VOLUME = 10000
MAX_TICKERS_TO_SCAN = 500   # 500 سهم فقط - الأكثر نشاطاً (Explosive Mode)
CHUNK_SIZE = 100
MIN_RVOL_BY_PHASE = {"PRE": 2.0, "REGULAR": 1.5, "AFTER": 2.5}
TELEGRAM_DELAY = 3.0  # 3 ثوانٍ بين كل رسالة لتجنب حد 429
TRADE_MONITOR_INTERVAL = 30
SCAN_INTERVAL_SEC = 300     # 5 دقائق بدلاً من 15 (أسرع للمضاربة)
BREAK_BETWEEN_CHUNKS = 5
DEEP_ANALYSIS_WORKERS = 4
SIGNAL_COOLDOWN = 3600         # ساعة واحدة كولداون (كان ساعتين — خُفّف)
MAX_DAILY_SIGNALS = 150       # رُفع من 50 لأيام الحركة القوية

# ================= AI PROCESS SYMBOL SETTINGS =================
AI_PROCESS_ENABLED = True
AI_CONFIDENCE_THRESHOLD = 70
AI_MAX_ANALYSIS_PER_MINUTE = 10
AI_MODEL = "llama-3.1-8b-instant"
AI_SCANNER_INTERVAL = 300
AI_MAX_CANDIDATES = 5

# Fast Momentum Scanner Settings
MOMENTUM_SCAN_INTERVAL = 120
MOMENTUM_PRICE_MIN = 0.5
MOMENTUM_PRICE_MAX = 30.0
MOMENTUM_VOL_MIN = 100000
MOMENTUM_GAIN_PCT = 10.0

# ================= QUICK JUMP SCANNER (15% في دقيقة أو نص دقيقة) =================
QUICK_JUMP_SCAN_INTERVAL = 30     # فحص كل 30 ثانية
QUICK_JUMP_MIN_GAIN_1M   = 15.0   # قفزة 15% خلال آخر شمعة 1 دقيقة
QUICK_JUMP_MIN_VOL       = 30000  # حجم أدنى للشمعة
QUICK_JUMP_PRICE_MIN     = 0.3
QUICK_JUMP_PRICE_MAX     = 50.0
QUICK_JUMP_COOLDOWN      = 600    # 10 دقائق بين تنبيهات نفس السهم


# Real-Time Support & Resistance Scanner Settings
SR_SCAN_INTERVAL_SEC = 60          # فحص لحظي كل دقيقة تقريباً
SR_MAX_TICKERS = 250               # نركز على أكثر الأسهم نشاطاً لتخفيف الضغط
SR_PRICE_MIN = 0.3
SR_PRICE_MAX = 30.0
SR_MIN_LAST_VOLUME = 50000         # حجم آخر شمعة 1m/5m كحد أدنى للسهم النشط
SR_MIN_RVOL = 2.5                  # رُفع من 1.8 → 2.5 لتقليل الإشارات الضعيفة
SR_PROXIMITY_PCT = 1.0             # تنبيه عند الاقتراب من الدعم/المقاومة ضمن 1%
SR_BREAKOUT_BUFFER_PCT = 0.35      # هامش اختراق فوق المقاومة لتقليل الإشارات الكاذبة
SR_LEVEL_COOLDOWN = 1800           # رُفع من 15 دقيقة → 30 دقيقة بين تنبيهات نفس السهم
SR_MIN_TOUCHES = 3                 # رُفع من 2 → 3 لمسات لاعتماد المستوى
SR_MIN_TARGET_PCT = 3.0            # الهدف (المقاومة) لازم يكون 3%+ من السعر الحالي
SR_MIN_RANGE_PCT  = 4.0            # الفجوة بين الدعم والمقاومة لازم 4%+ (نسبة ربح منطقية)

# ================= STRONG NEWS SCANNER SETTINGS =================
STRONG_NEWS_KEYWORDS = [
    # استحواذات واندماجات
    "acquire", "acquisition", "merger", "buyout", "takeover", "going private",
    # عقود واتفاقيات
    "contract awarded", "contract award", "agreement", "development agreement",
    "supply agreement", "license agreement", "exclusive agreement",
    "partnership", "collaboration", "joint venture", "strategic agreement",
    # موافقات تنظيمية وبيانات
    "fda approval", "fda approved", "fda grants", "fda clearance",
    "breakthrough", "positive results", "positive data", "phase 3",
    "clinical trial", "patent granted",
    # نتائج مالية
    "record revenue", "beats estimates", "raised guidance", "record earnings",
    "revenue increase", "record sales",
    # أخرى
    "exclusive license", "uplisting", "nasdaq listing", "nyse listing",
    "share repurchase", "buyback", "special dividend",
    # كلمات عربية عامة تدل على خبر قوي (للفلترة بعد الترجمة)
    "enters into", "signs", "awarded", "wins contract", "secures",
]

STRONG_NEWS_FEEDS = [
    "https://www.benzinga.com/feed",
    "https://www.businesswire.com/rss/home/?rss=G1",
    "https://www.prnewswire.com/rss/news-releases-list.rss",
    "https://www.globenewswire.com/RssFeed/industry/Technology/feed",
    "https://www.globenewswire.com/RssFeed/industry/Health+Care/feed",
    "https://www.globenewswire.com/RssFeed/industry/Financial+Services/feed",
    "https://feeds.content.dowjones.com/public/rss/mw_news",
    "https://seekingalpha.com/market_currents.xml",
]

STRONG_NEWS_SCAN_INTERVAL = 120    # فحص الأخبار كل دقيقتين
STRONG_NEWS_PRICE_MIN = 0.5        # الحد الأدنى للسعر
STRONG_NEWS_RVOL_MIN = 1.5         # الحد الأدنى للحجم النسبي

# ================= LOCKS & CACHE =================
_cache = {}
_cache_lock = threading.RLock()
state_lock = threading.RLock()
_ai_request_count = 0
_ai_request_reset = time.time()
_ai_request_lock = threading.Lock()


def can_send_ai_request():
    """محدد طلبات Groq: لا يتجاوز الحد الداخلي الآمن بالدقيقة."""
    global _ai_request_count, _ai_request_reset
    with _ai_request_lock:
        now = time.time()
        if now - _ai_request_reset >= 60:
            _ai_request_count = 0
            _ai_request_reset = now
        if _ai_request_count >= AI_MAX_ANALYSIS_PER_MINUTE:
            return False
        _ai_request_count += 1
        return True

# ================= GROQ INIT =================
try:
    from groq import Groq
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    if GROQ_API_KEY:
        client = Groq(api_key=GROQ_API_KEY)
        OPENAI_AVAILABLE = True  # الاسم محفوظ للتوافق مع بقية الكود
        logger.info("Groq client initialized successfully")
    else:
        client = None
        OPENAI_AVAILABLE = False
        logger.warning("GROQ_API_KEY not set")
except Exception as e:
    logger.warning(f"Groq not available: {e}")
    client = None
    OPENAI_AVAILABLE = False

# ================= REMOVED: unreliable volatility swing scanner and unused broker integration =================
# تم حذف الماسح القديم غير الموثوق والتكامل غير المستخدم مع الوسيط.

def fast_momentum_scanner():
    """
    ماسح الزخم السريع — يصطاد الأسهم التي ترتفع +8%+ من الافتتاح.
    يعمل كل دقيقتين طوال وقت التداول (بما فيه Pre/After).
    """
    while True:
        try:
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(120)
                continue

            with state_lock:
                tickers = list(state.get("tickers", []))

            saudi_now = now_saudi().strftime("%I:%M %p")

            for symbol in tickers[:300]:
                try:
                    df = cached_download(symbol, period="1d", interval="5m")
                    if df.empty or len(df) < 5:
                        continue

                    df.columns = [c.lower() for c in df.columns]
                    price_now = float(df['close'].iloc[-1])
                    price_open = float(df['open'].iloc[0])
                    last_vol   = float(df['volume'].iloc[-1])
                    avg_vol    = float(df['volume'].iloc[-21:-1].mean()) if len(df) >= 22 else last_vol

                    if not (MOMENTUM_PRICE_MIN < price_now < MOMENTUM_PRICE_MAX):
                        continue

                    gain_pct = (price_now - price_open) / price_open * 100
                    if gain_pct < MOMENTUM_GAIN_PCT:   # 10% من الافتتاح
                        continue

                    rvol = last_vol / max(avg_vol, 1)
                    if rvol < 1.5:   # لازم يكون فيه حجم معقول
                        continue

                    signal_key = f"mom_{symbol}_{now_est().strftime('%Y%m%d_%H')}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue

                    tp  = round(price_now * 1.10, 4)
                    sl  = round(price_now * 0.94, 4)

                    msg = (
                        f"⚡ *MOMENTUM: {symbol}* ⚡\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"⏰ *{saudi_now} 🇸🇦*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💰 السعر: *${price_now:.4f}*\n"
                        f"📈 ربح من الافتتاح: *+{gain_pct:.1f}%*\n"
                        f"🔥 RVOL: *{rvol:.1f}x*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🎯 الهدف: ${tp} (+10%)\n"
                        f"🛑 الوقف: ${sl} (-6%)\n"
                        f"⚠️ إشارة وليست توصية"
                    )
                    send_telegram(msg)
                    logger.info(f"⚡ Momentum: {symbol} +{gain_pct:.1f}% RVOL={rvol:.1f}x")
                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()
                    time.sleep(2)

                except Exception as sym_err:
                    logger.debug(f"[Momentum] {symbol}: {sym_err}")
                    continue
                time.sleep(0.2)

        except Exception as e:
            logger.error(f"[Momentum] Error: {e}")

        time.sleep(MOMENTUM_SCAN_INTERVAL)
# ================= MARKET PHASE SETTINGS =================
PHASE_SETTINGS = {
    "PRE":     {"min_score": 55, "size_multiplier": 0.5, "vol_surge_mult": 2.0, "description": "🟡 Pre-Market"},
    "REGULAR": {"min_score": 60, "size_multiplier": 0.8, "vol_surge_mult": 1.5, "description": "🟢 Regular Hours"},
    "AFTER":   {"min_score": 65, "size_multiplier": 0.3, "vol_surge_mult": 2.5, "description": "🔵 After-Hours"},
    "CLOSED":  {"min_score": 999, "size_multiplier": 0,   "vol_surge_mult": 0,   "description": "⚫ Market Closed"}
}

logger = logging.getLogger()

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
    "last_daily_report_sent": None,
    "seen_support_resistance": {},
    "recommendation_history": [],
    "recommendation_stats": {"BUY": 0, "WATCH": 0, "AVOID": 0, "NO_TRADE": 0},
    "hot_watchlist": [],
    "hot_watchlist_timestamp": 0
}


def now_est():
    return datetime.now(EASTERN_TZ)

def now_saudi():
    return datetime.now(SAUDI_TZ)

def saudi_time_str():
    """يرجع الوقت الحالي بصيغة السعودية"""
    return now_saudi().strftime("%I:%M %p")


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
        state.setdefault("seen_support_resistance", {})
        state.setdefault("recommendation_history", [])
        state.setdefault("recommendation_stats", {"BUY": 0, "WATCH": 0, "AVOID": 0, "NO_TRADE": 0})
        state.setdefault("hot_watchlist", [])
        state.setdefault("hot_watchlist_timestamp", 0)

        # ── تنظيف halted_stocks: إزالة المفاتيح القديمة بصيغة SYMBOL_HH:MM:SS ──
        halted = state.get("halted_stocks", {})
        stale_cutoff = time.time() - 86400  # أقدم من 24 ساعة
        cleaned_halted = {}
        for k, v in halted.items():
            # تجاهل المفاتيح المكسورة (مثل MTEN_14:28:40.366)
            sym = k.split("_")[0]
            if not sym or not sym.isalpha() or len(sym) > 5:
                continue
            # تجاهل المدخلات القديمة (أقدم من 24 ساعة)
            entry_time = v.get("timestamp", v.get("time", 0)) if isinstance(v, dict) else 0
            if isinstance(entry_time, (int, float)) and entry_time < stale_cutoff:
                continue
            cleaned_halted[k] = v
        state["halted_stocks"] = cleaned_halted

        # ── تنظيف pending_halts القديمة (أقدم من ساعتين) ──
        pending = state.get("pending_halts", {})
        state["pending_halts"] = {
            sym: info for sym, info in pending.items()
            if isinstance(info, dict) and time.time() - info.get("timestamp", 0) < 7200
        }

        # ── تنظيف قائمة التيكرات: إزالة أي رمز يحتوي على _ أو : ──
        tickers = state.get("tickers", [])
        clean_tickers = [
            t for t in tickers
            if isinstance(t, str) and t.isalpha() and 1 <= len(t) <= 5
        ]
        if len(clean_tickers) != len(tickers):
            logger.info(f"🧹 Cleaned tickers: {len(tickers)} → {len(clean_tickers)} (removed corrupted entries)")
            state["tickers"] = clean_tickers

        # ── تنظيف seen_signals القديمة (أقدم من 48 ساعة) ──
        signals = state.get("seen_signals", {})
        cutoff_48h = time.time() - 172800
        state["seen_signals"] = {k: v for k, v in signals.items() if isinstance(v, (int, float)) and v > cutoff_48h}

        save_state()
        logger.info("✅ State schema verified and cleaned")


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
        open_positions = len(state.get("open_trades", {}))
        daily_loss = float(state.get("daily_loss", 0.0) or 0.0)

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
        f"📦 Open Paper Positions: {open_positions}\n"
        f"🛡️ Daily Loss Used: ${daily_loss:.2f}\n"
        f"🏆 Best Trade: {best_line}\n"
        f"📉 Worst Trade: {worst_line}\n"
        f"🧪 Paper Trading Report — لا تنفيذ حقيقي"
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
    """لا تسمح بأوامر Telegram إذا لم يحدد المالك Chat ID صراحةً."""
    if not AUTHORIZED_CHAT_ID:
        logger.error("AUTHORIZED_CHAT_ID is not configured; command rejected")
        return False
    chat = getattr(message, "chat", None)
    return str(getattr(chat, "id", "")) == str(AUTHORIZED_CHAT_ID)

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
        is_priority_signal = any(k in message for k in ["INSANE", "SUPERNOVA", "🌋"])
        if is_signal and not is_priority_signal:
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
            for attempt in range(3):
                try:
                    if photo:
                        bot.send_photo(CHAT_ID, photo, caption=message, parse_mode='Markdown')
                    else:
                        bot.send_message(CHAT_ID, message, parse_mode='Markdown')
                    _last_telegram_time = time.time()
                    break  # نجح الإرسال — اخرج من الحلقة
                except Exception as e:
                    err_str = str(e)
                    if "429" in err_str or "Too Many Requests" in err_str:
                        # استخرج retry_after من رسالة الخطأ
                        import re as _re
                        m = _re.search(r'retry after (\d+)', err_str, _re.IGNORECASE)
                        wait_sec = int(m.group(1)) if m else 30
                        wait_sec = min(wait_sec, 120)  # لا تنتظر أكثر من دقيقتين
                        logger.warning(f"Telegram 429 — waiting {wait_sec}s (attempt {attempt+1}/3)")
                        time.sleep(wait_sec)
                        continue
                    else:
                        logger.error(f"Telegram error: {e}")
                        break

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


def update_all_tickers():
    global state
    tickers = set()
    
    # 1. جلب الأسهم من GitHub
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
        except: pass
    
    # 2. 🔥 رتب حسب الحجم (لجلب الأسهم النشطة أولاً)
    logger.info(f"📊 Sorting {len(tickers)} stocks by volume to find leaders...")
    tickers_with_volume = []
    # فحص أول 1500 سهم لسرعة التحديث
    all_symbols = list(tickers)
    random.shuffle(all_symbols) # خلط لضمان تنوع العينة
    
    for symbol in all_symbols[:1500]:
        try:
            df = fetch_from_yahoo(symbol, period="1d", interval="15m", timeout=5)
            if not df.empty:
                volume = df['volume'].iloc[-1]
                tickers_with_volume.append((symbol, volume))
        except: pass
        time.sleep(0.02)
    
    # رتب تنازلياً حسب الحجم
    tickers_with_volume.sort(key=lambda x: x[1], reverse=True)
    
    # خذ أول 500 سهم بالحجم (هذول اللي يتحركون)
    final_list = [sym for sym, vol in tickers_with_volume[:MAX_TICKERS_TO_SCAN]]
    
    if not final_list:
        final_list = ["AAPL", "NVDA", "TSLA", "AMD", "MSFT", "AMZN", "META", "GOOGL", "NFLX", "PLTR"]
    
    with state_lock:
        state["tickers"]      = final_list
        # 🆕 احفظ القائمة الكاملة (6700+ سهم) للـ full_sweep_scanner
        state["full_tickers"] = list(all_symbols)
        save_state()
    
    logger.info(f"✅ Universe: {len(final_list)} hot stocks | {len(all_symbols)} full tickers stored")
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
def calculate_position_size_from_stop(entry_price, stop_loss, risk_fraction=None):
    """يحسب الحجم من المخاطرة الفعلية والوقف الفعلي مع حد قيمة المركز."""
    try:
        entry_price = float(entry_price)
        stop_loss = float(stop_loss)
        if entry_price <= 0 or stop_loss >= entry_price:
            return 0
        risk_fraction = RISK_PER_TRADE if risk_fraction is None else float(risk_fraction)
        risk_amount = CAPITAL * max(0.0, risk_fraction)
        risk_per_share = entry_price - stop_loss
        by_risk = int(risk_amount / risk_per_share)
        by_value = int(MAX_POSITION_VALUE / entry_price)
        return max(0, min(by_risk, by_value, 10000))
    except (TypeError, ValueError, ZeroDivisionError):
        return 0


def calculate_position_size(price):
    stop_loss = float(price) * SL_PCT
    size = calculate_position_size_from_stop(price, stop_loss)
    return size if size >= 1 else 0

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



def rvol_spike_scanner():
    """
    🌋 ماسح انفجار الحجم النسبي (RVOL 10x)
    يصطاد الأسهم التي يدخلها "سيولة مؤسساتية" فجأة
    """
    while True:
        try:
            phase = get_market_phase()
            if phase not in ("REGULAR", "PRE", "AFTER"):
                time.sleep(120)
                continue
            
            with state_lock:
                # فحص أكثر الأسهم نشاطاً (أول 500 سهم)
                tickers = list(state.get("tickers", []))[:500]
            
            for symbol in tickers:
                try:
                    # فحص 15 دقيقة للانفجار اللحظي
                    df = cached_download(symbol, period="5d", interval="15m")
                    if df.empty or len(df) < 20:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    
                    # حساب RVOL (حجم آخر شمعة مقارنة بمتوسط آخر 20 شمعة 15m)
                    last_vol = df['volume'].iloc[-1]
                    avg_vol = df['volume'].iloc[-21:-1].mean()
                    rvol = last_vol / max(avg_vol, 1)
                    
                    # 🎯 التنبيه عند انفجار حجم 3x أو أكثر (كان 10x — خُفّف)
                    if rvol >= 3.0:
                        price = df['close'].iloc[-1]
                        prev_price = df['close'].iloc[-2]
                        price_change = (price - prev_price) / prev_price * 100
                        
                        # نركز على الانفجار مع صعود سعري (Bullish Spike)
                        if price_change < 0.5: 
                            continue

                        signal_key = f"rvol10x_{symbol}_{int(time.time() // 3600)}" # مرة كل ساعة
                        with state_lock:
                            if signal_key in state.get("seen_signals", {}):
                                continue
                        
                        saudi_now = now_saudi().strftime("%I:%M %p")
                        
                        msg = (
                            f"🌋 *EXPLOSIVE RVOL: {symbol}* 🌋\n"
                            f"━━━━━━━━━━━━━━━━\n"
                            f"⏰ *{saudi_now} 🇸🇦*\n"
                            f"━━━━━━━━━━━━━━━━\n"
                            f"💰 السعر الحالي: *${price:.2f}*\n"
                            f"📈 حركة الشمعة: *{price_change:+.1f}%*\n"
                            f"🔥 الحجم النسبي (RVOL): *{rvol:.1f}x* 🚀\n"
                            f"📊 حجم الانفجار: {int(last_vol):,}\n"
                            f"━━━━━━━━━━━━━━━━\n"
                            f"💡 *دخول سيولة ضخمة جداً (10 أضعاف الطبيعي)*\n"
                            f"⚠️ *السهم مرشح لانفجار سعري وشيك*"
                        )
                        send_telegram(msg)
                        
                        with state_lock:
                            state.setdefault("seen_signals", {})[signal_key] = time.time()
                            save_state()
                        
                        logger.info(f"🌋 RVOL 10x SPIKE: {symbol} rvol={rvol:.1f}")
                        time.sleep(2) # راحة بعد التنبيه
                        
                except Exception as sym_err:
                    continue
                time.sleep(0.4) # تجنب الحظر
                
        except Exception as e:
            logger.error(f"RVOL 10x scanner error: {e}")
        
        time.sleep(60) # فحص كل دقيقة للأخبار الجديدة


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

                    # شرط 2: قفزة +15% أو أكثر لتقليل الإشارات الضعيفة
                    gain_pct = (price - prev_close) / prev_close * 100
                    if gain_pct < 15:
                        continue

                    # شرط 3: حجم اليوم > 2x من متوسط 5 أيام (كان 3x — خُفّف)
                    avg_vol_5d = float(df_d['volume'].iloc[-6:-1].mean())
                    if avg_vol_5d <= 0:
                        continue
                    vol_ratio = today_vol / avg_vol_5d
                    if vol_ratio < 2.0:
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

                    # شرط 6: RSI 5m بين 55 و70؛ زخم صحي دون ضعف أو تشبع شراء
                    delta = closes.diff()
                    gain = delta.where(delta > 0, 0).rolling(14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                    rs = gain / loss.replace(0, 1e-10)
                    rsi5 = float((100 - (100 / (1 + rs))).iloc[-1])
                    if rsi5 < 55 or rsi5 > 70:
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
        base_size = calculate_position_size(price)
        if base_size < 1:
            logger.info(f"Skipping {symbol}: calculated position size is below 1 share")
            return
        size = int(base_size * settings["size_multiplier"])
        if size < 1:
            logger.info(f"Skipping {symbol}: adjusted position size is below 1 share")
            return
        
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
            rvol_check = get_unified_rvol(df, phase=phase)
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
        rvol = get_unified_rvol(df, phase=phase)

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
        score = clamp_score_100(score)
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

def handle_halt_gap_risk(symbol, trade, df):
    """يحمي Paper Trades من فجوة هابطة أو سهم موقوف؛ لا ينفذ وسيطًا حقيقيًا."""
    try:
        if df is None or df.empty or len(df) < 2:
            return None, None
        current = float(df["close"].iloc[-1])
        previous = float(df["close"].iloc[-2])
        if previous <= 0:
            return None, None
        gap_pct = (current - previous) / previous * 100
        symbol_halted = False
        with state_lock:
            symbol_halted = any(str(key).split("_")[0] == symbol for key in state.get("halted_stocks", {}))
            symbol_halted = symbol_halted or symbol in state.get("pending_halts", {})
        if gap_pct <= -15:
            return "HALT/GAP DOWN", f"🚨 *خطر Halt/Gap هابط: {symbol}*\n📉 الفجوة: {gap_pct:.1f}%\n🛑 إغلاق Paper Trade للحماية"
        if symbol_halted:
            return None, f"⚠️ *السهم موقوف/قيد التحقق: {symbol}*\n📌 لا يتم فتح مراكز جديدة حتى عودة التداول"
        if gap_pct >= 25 and float(trade.get("sl", 0)) < float(trade.get("entry", current)):
            with state_lock:
                live = state.get("open_trades", {}).get(symbol)
                if live:
                    live["sl"] = live["entry"]
                    live["halt_gap_protected"] = True
                    save_state()
            return None, f"⚡ *Gap Up قوي: {symbol} +{gap_pct:.1f}%*\n🔒 تم رفع وقف Paper Trade إلى نقطة الدخول"
    except Exception as error:
        logger.debug(f"Halt/gap protection failed for {symbol}: {error}")
    return None, None


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
            halt_close_reason, halt_message = handle_halt_gap_risk(symbol, state.get("open_trades", {}).get(symbol, {}), df)
            if halt_close_reason:
                close_reason = halt_close_reason

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

            if halt_message:
                send_telegram(halt_message)
            if near_target_message:
                send_telegram(near_target_message)
            if trailing_message:
                send_telegram(trailing_message)
            if close_reason:
                close_trade(symbol, price, close_reason)
                    
        except Exception as e:
            logger.warning(f"Trade update failed for {symbol}: {e}")

# ================= SCANNER ENGINE =================

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
        logger.error("❌ Telegram bot is None! Check TELEGRAM_TOKEN")
        return
    if not CHAT_ID:
        logger.error("❌ CHAT_ID is not set! Check environment variables")
        return
    
    logger.info(f"✅ Telegram bot initialized successfully")
    logger.info(f"✅ CHAT_ID: {CHAT_ID}")
    
    try:
        # إزالة أي webhook موجود لمنع خطأ 409 Conflict
        bot.remove_webhook()
        time.sleep(2)
    except Exception as e:
        logger.debug(f"Webhook removal skipped: {e}")
    
    logger.info("🤖 Starting Telegram bot polling...")
    # إرسال رسالة اختبار
    try:
        send_telegram("✅ البوت بدأ التشغيل بنجاح! جاهز لاستقبال الأوامر.")
    except Exception as e:
        logger.error(f"Failed to send startup message: {e}")
    
    try:
        # polling بسيط بدون threading معقد — يمنع تعارض النسخ
        bot.polling(non_stop=True, interval=1, timeout=20)
    except Exception as e:
        logger.error(f"Telebot polling error: {e}")

if bot:
    @bot.message_handler(commands=['start'])
    def cmd_start(message):
        logger.info(f"📨 Received /start from {message.chat.id}")
        if not ensure_authorized(message):
            logger.warning(f"Unauthorized message from {message.chat.id}")
            return
        phase = get_market_phase()
        msg = f"👋 Trading Bot v33!\n📊 {len(state['tickers'])} stocks\n🕐 {PHASE_SETTINGS[phase]['description']}\n\n📋 *الأوامر المتاحة:*\n\n📊 *الحالة والتقارير*\n/status - الأداء العام\n/positions - الصفقات المفتوحة\n/recommend SYMBOL - توصية Paper موحدة\n/recommendations - سجل التوصيات\n/signals - آخر الإشارات\n\n🔎 *التحليل والماسحات*\n/scan SYMBOL - تحليل سهم\n/sr SYMBOL - دعم ومقاومة\n/news SYMBOL - أخبار سهم\n/top - أفضل المرشحين\n/movers - أسباب ارتفاع 100%+\n/gainers - أكبر الرابحين\n/losers - أكبر الخاسرين\n/vwap SYMBOL - تحليل VWAP\n/candle SYMBOL - تحليل الشموع\n/adv SYMBOL - تحليل EMA/Fibonacci متقدم\n\n👀 *قائمة المراقبة والتنبيهات*\n/watchlist - عرض القائمة\n/add SYMBOL - إضافة سهم\n/remove SYMBOL - حذف سهم\n/alert SYMBOL PRICE - تنبيه سعري\n/alerts - عرض التنبيهات\n/delalert SYMBOL - حذف تنبيه\n\n🛡️ *Paper Trading والحماية*\n/b SYMBOL QUANTITY - شراء Paper\n/s SYMBOL - بيع Paper\n/close SYMBOL - إغلاق Paper\n/halt - الأسهم الموقوفة\n/clearhalt SYMBOL - إزالة إيقاف قديم\n\n/help - عرض هذه القائمة\n⚠️ كل العمليات Paper Trading فقط وليست توصية مالية"
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


    @bot.message_handler(commands=['sr'])
    def cmd_support_resistance(message):
        """فحص يدوي للدعم والمقاومة: /sr SYMBOL"""
        logger.info(f"📨 Received /sr command: {message.text}")
        if not ensure_authorized(message):
            logger.warning(f"Unauthorized /sr from {message.chat.id}")
            return
        try:
            args = message.text.split()
            if len(args) != 2:
                send_telegram("Usage: /sr <SYMBOL>")
                return
            symbol = args[1].upper().strip()
            send_telegram(f"🔎 جاري تحليل الدعم والمقاومة لـ *{symbol}*...")
            df = cached_download(symbol, period="1d", interval="1m", force_refresh=True)
            interval_label = "1m"
            if df.empty or len(df) < 35:
                df = cached_download(symbol, period="5d", interval="5m", force_refresh=True)
                interval_label = "5m"
            if df.empty or len(df) < 35:
                send_telegram(f"❌ لا توجد بيانات كافية لحساب الدعم والمقاومة لـ *{symbol}*")
                return
            analysis = calculate_support_resistance(df)
            if not analysis:
                send_telegram(f"❌ لم تظهر مستويات دعم/مقاومة واضحة لـ *{symbol}*")
                return
            send_telegram(format_support_resistance_message(symbol, analysis, interval_label=interval_label))
        except Exception as e:
            logger.error(f"/sr command error: {e}")
            send_telegram(f"❌ خطأ في تحليل الدعم والمقاومة: {e}")

if bot:
    @bot.message_handler(commands=['b'])
    def cmd_buy(message):
        """شراء يدوي: /b SYMBOL QUANTITY — مثال: /b SMX 50"""
        if not ensure_authorized(message):
            return
        # تشغيل في خيط منفصل لمنع تعليق البوت
        threading.Thread(target=handle_manual_buy, args=(message,), daemon=True).start()

    def handle_manual_buy(message):
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

            with state_lock:
                if symbol in state["open_trades"]:
                    send_telegram(f"⚠️ *{symbol}* مفتوح مسبقاً!")
                    return
                if len(state["open_trades"]) >= MAX_OPEN_TRADES:
                    send_telegram(f"❌ وصلت الحد الأقصى للصفقات المفتوحة ({MAX_OPEN_TRADES})")
                    return

            send_telegram(f"🔍 جاري جلب سعر *{symbol}*...")
            df = cached_download(symbol, period="1d", interval="5m")
            if df.empty:
                send_telegram(f"❌ لم أستطع جلب بيانات *{symbol}*")
                return

            df.columns = [c.lower() for c in df.columns]
            price = float(df['close'].iloc[-1])
            phase = get_market_phase()
            tp, sl, tp_pct = get_trade_levels(price, 1.0)
            max_size = calculate_position_size_from_stop(price, sl)
            if max_size <= 0 or quantity > max_size:
                send_telegram(f"❌ الكمية تتجاوز حد المخاطرة. الحد المسموح تقريبًا: {max_size} سهم")
                return

            with state_lock:
                state["open_trades"][symbol] = {
                    "entry": price, "tp": tp, "sl": sl, "size": quantity,
                    "time": time.time(), "phase": phase, "manual": True,
                    "paper": PAPER_TRADING, "risk_amount": (price - sl) * quantity
                }
                save_state()

            send_telegram(f"✅ *Paper Entry* — {symbol} بسعر *${price:.2f}* — البوت يراقب الآن\n⚠️ لا يوجد تنفيذ وساطة فعلي في هذا الملف")
        except Exception as e:
            logger.error(f"Manual buy error: {e}")
            send_telegram(f"❌ خطأ: {e}")

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


if bot:
    @bot.message_handler(commands=['movers'])
    def cmd_movers(message):
        """يشرح أسباب ارتفاع الأسهم 100%+ ويعرض آخر إشارات البوت"""
        if not ensure_authorized(message):
            return
        msg = (
            "🚀 *كيف تعرف سهم سيرتفع 100%+ قبل ما يرتفع؟*\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "📋 *أسباب الارتفاع الكبير (محفزات):*\n\n"
            "1️⃣ *قرار FDA* 💊\n"
            "   موافقة على عقار = +50% إلى +300%\n"
            "   البوت يراقبها عبر الأخبار الرسمية\n\n"
            "2️⃣ *خبر استحواذ أو شراكة* 🤝\n"
            "   شركة كبرى تشتري شركة صغيرة = +30% إلى +100%\n"
            "   البوت يراقبها → RSS Catalyst Scanner\n\n"
            "3️⃣ *شراء الداخليين* 🐋\n"
            "   المدير يشتري بأمواله = ثقة بالارتفاع\n"
            "   البوت يراقبها → Form 4 Scanner\n\n"
            "4️⃣ *Short Squeeze* 🔥\n"
            "   البائعون على المكشوف مضطرون للشراء\n"
            "   البوت يراقبها → Short Squeeze Scanner\n\n"
            "5️⃣ *إيقاف تداول ثم فتح* 🔓\n"
            "   السهم يُوقف على خبر ثم يُفتح بقفزة\n"
            "   البوت يراقبها → Post-Halt Scanner\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "✅ بوتك يراقب *كل هذه المحفزات* تلقائياً 24/7\n"
            "📱 الإشارات تصلك هنا فور اكتشافها"
        )
        send_telegram(msg)

# ================= NEWS CATALYST SCANNER =================





# ================= GROQ SAFE COMPLETION =================
GROQ_MODEL_FALLBACKS = [AI_MODEL, 'llama-3.3-70b-versatile', 'llama-3.1-8b-instant']
_groq_model_lock = threading.Lock()
_groq_active_model = AI_MODEL


def groq_chat_completion(**kwargs):
    """استدعاء Groq مع تبديل تلقائي للنموذج عند 404 فقط."""
    global _groq_active_model
    if client is None:
        raise RuntimeError('Groq client is not initialized')
    requested = kwargs.pop('model', None) or _groq_active_model
    models = []
    for model_name in [requested, _groq_active_model] + GROQ_MODEL_FALLBACKS:
        if model_name and model_name not in models:
            models.append(model_name)
    last_error = None
    with _groq_model_lock:
        for model_name in models:
            try:
                response = client.chat.completions.create(model=model_name, **kwargs)
                if model_name != _groq_active_model:
                    _groq_active_model = model_name
                    logger.info(f'Groq active model switched to {model_name}')
                return response
            except Exception as error:
                last_error = error
                status = getattr(error, 'status_code', None)
                response = getattr(error, 'response', None)
                if status is None and response is not None:
                    status = getattr(response, 'status_code', None)
                if status == 404 or '404' in str(error):
                    logger.warning(f'Groq model unavailable: {model_name}; trying fallback')
                    continue
                raise
    raise last_error or RuntimeError('No Groq model available')


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
    # "https://www.marketwatch.com/newsviewer/rssfeed.aspx",  # محجوب 403
    "https://seekingalpha.com/feed.xml",
    # "https://www.investors.com/feed/",  # محجوب 403
    "https://www.nasdaq.com/feed/rssoutbound",
]

def fetch_rss(url):
    """جلب وتحليل RSS Feed"""
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*'
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


def ai_analyze_news(title, desc=""):
    """تحليل خبر سريع عبر Groq مع Fallback محلي وحدود طلبات آمنة."""
    if not OPENAI_AVAILABLE or client is None:
        return analyze_news_sentiment_basic(title, desc)
    if not can_send_ai_request():
        logger.debug("Groq request limit reached; using basic news analysis")
        return analyze_news_sentiment_basic(title, desc)
    try:
        prompt = f"""Analyze this stock-market news and return JSON only.
Title: {str(title)[:300]}
Description: {str(desc)[:200]}
JSON schema: {{"sentiment":"positive|negative|neutral","impact_score":0,"category":"FDA|Merger|Contract|Earnings|Partnership|General","is_catalyst":false,"catalyst_quality":0,"is_new_information":false,"is_confirmed_source":false,"likely_priced_in":false,"dilution_risk":false,"offering_risk":false,"confidence":0,"reason_ar":"سبب مختصر بالعربية"}}"""
        response = groq_chat_completion(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "You are a cautious penny-stock news analyst. Respond in valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=200,
            timeout=8,
        )
        content = response.choices[0].message.content
        result = json.loads(content) if isinstance(content, str) else {}
        return result if isinstance(result, dict) else analyze_news_sentiment_basic(title, desc)
    except Exception as error:
        logger.debug(f"Groq News Analysis fallback: {error}")
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


def get_unified_rvol(df, phase=None):
    """المصدر الموحد لـ RVOL في الماسحات الجديدة والتوصيات."""
    try:
        value = calculate_rvol(df, phase=phase)
        if value is None or pd.isna(value) or float(value) <= 0:
            return 0.0
        return round(float(value), 3)
    except Exception:
        return 0.0


def clamp_score_100(score):
    """يضمن أن كل السكورات المعروضة تقع بين 0 و100."""
    try:
        return max(0, min(100, int(round(float(score)))))
    except (TypeError, ValueError):
        return 0


# ================================================================
# 📊 FINNHUB SAFE NEWS ENGINE — خفيف ومناسب للموارد المحدودة
# ================================================================
try:
    import finnhub
except Exception:
    finnhub = None

FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY', '').strip()
FINNHUB_ENABLED = bool(FINNHUB_API_KEY and finnhub is not None)
FINNHUB_COOLDOWN = 600
FINNHUB_MAX_SYMBOLS_PER_SCAN = 100
FINNHUB_MIN_SENTIMENT = 0.65
FINNHUB_SCAN_INTERVAL = 420
FINNHUB_MAX_NEWS_PER_SYMBOL = 5
_finnhub_last_scan = {}
_finnhub_news_cache = {}
_finnhub_error_count = 0
_finnhub_last_error_time = 0.0
_finnhub_client = None

if FINNHUB_ENABLED:
    try:
        _finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
        logger.info('✅ Finnhub client initialized')
    except Exception as error:
        FINNHUB_ENABLED = False
        logger.warning(f'⚠️ Finnhub initialization failed: {error}')
elif not FINNHUB_API_KEY:
    logger.info('ℹ️ FINNHUB_API_KEY not set; Finnhub scanner disabled')
else:
    logger.warning('⚠️ finnhub-python is not installed; Finnhub scanner disabled')

_FINNHUB_IMPACT_KEYWORDS = {
    'acquisition': 12, 'merger': 12, 'buyout': 12, 'contract': 10,
    'partnership': 8, 'fda approval': 15, 'breakthrough': 12,
    'record revenue': 10, 'beats estimates': 10, 'positive guidance': 10,
    'strategic agreement': 10, 'order': 7, 'award': 7,
}
_FINNHUB_NEGATIVE_KEYWORDS = {
    'offering', 'registered direct', 'atm offering', 'dilution',
    'convertible', 'warrant', 'bankruptcy', 'delisting', 'investigation',
}


def _finnhub_local_score(headline, summary):
    text = f'{headline} {summary}'.lower()
    impact = min(50, sum(points for word, points in _FINNHUB_IMPACT_KEYWORDS.items() if word in text))
    negative = any(word in text for word in _FINNHUB_NEGATIVE_KEYWORDS)
    basic = analyze_news_sentiment_basic(headline, summary) if 'analyze_news_sentiment_basic' in globals() else {}
    sentiment = 0.75 if impact >= 10 else 0.55
    if basic.get('sentiment') == 'positive':
        sentiment = max(sentiment, 0.70)
    if negative or basic.get('sentiment') == 'negative':
        sentiment = min(sentiment, 0.25)
    return round(sentiment, 3), impact, negative


def get_finnhub_news_safe(symbol):
    global _finnhub_error_count, _finnhub_last_error_time
    if not FINNHUB_ENABLED or _finnhub_client is None:
        return []
    now = time.time()
    if _finnhub_error_count >= 3 and now - _finnhub_last_error_time < 300:
        return []
    cached = _finnhub_news_cache.get(symbol)
    if cached and now - cached[0] < FINNHUB_COOLDOWN:
        return cached[1]
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        raw_news = _finnhub_client.company_news(symbol, _from=yesterday, to=today) or []
        filtered = []
        for item in raw_news[:FINNHUB_MAX_NEWS_PER_SYMBOL]:
            headline = str(item.get('headline', '')).strip()
            summary = str(item.get('summary', '')).strip()
            if not headline:
                continue
            sentiment, impact, negative = _finnhub_local_score(headline, summary)
            if negative or sentiment < FINNHUB_MIN_SENTIMENT:
                continue
            timestamp = float(item.get('datetime', now) or now)
            filtered.append({
                'headline': headline[:180], 'summary': summary[:300],
                'url': str(item.get('url', '')), 'sentiment': sentiment,
                'impact_score': impact, 'datetime': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M'),
                'timestamp': timestamp,
            })
        _finnhub_news_cache[symbol] = (now, filtered)
        _finnhub_error_count = 0
        return filtered
    except Exception as error:
        _finnhub_error_count += 1
        _finnhub_last_error_time = now
        logger.warning(f'⚠️ Finnhub error {symbol} ({_finnhub_error_count}/3): {error}')
        return []


def _finnhub_current_price(symbol):
    try:
        df = cached_download(symbol, period='1d', interval='5m')
        if df is not None and not df.empty:
            return float(df['close'].iloc[-1])
    except Exception as error:
        logger.debug(f'[Finnhub price] {symbol}: {error}')
    return 0.0


def finnhub_safe_scanner():
    """ماسح Finnhub أحادي الخيط: 100 سهم، كولداون 10 دقائق، وذاكرة محدودة."""
    global _finnhub_news_cache
    if not FINNHUB_ENABLED:
        logger.info('ℹ️ Finnhub safe scanner disabled; no API key or dependency')
        return
    while True:
        try:
            if get_market_phase() == 'CLOSED':
                time.sleep(60)
                continue
            with state_lock:
                tickers = list(state.get('tickers', []))[:FINNHUB_MAX_SYMBOLS_PER_SCAN]
            for symbol in tickers:
                now = time.time()
                if now - _finnhub_last_scan.get(symbol, 0) < FINNHUB_COOLDOWN:
                    continue
                # فلتر رخيص قبل طلب Finnhub: الأخبار أهم للأسهم المتحركة فقط.
                quick_df = cached_download(symbol, period='1d', interval='5m')
                if quick_df is None or quick_df.empty or len(quick_df) < 2:
                    _finnhub_last_scan[symbol] = now
                    time.sleep(0.1)
                    continue
                try:
                    prev_close = float(quick_df['close'].iloc[-2])
                    last_close = float(quick_df['close'].iloc[-1])
                    price_change = (last_close - prev_close) / max(abs(prev_close), 1e-9) * 100
                except Exception:
                    price_change = 0.0
                if abs(price_change) < 2.0:
                    _finnhub_last_scan[symbol] = now
                    time.sleep(0.1)
                    continue
                items = get_finnhub_news_safe(symbol)
                _finnhub_last_scan[symbol] = now
                if not items:
                    time.sleep(0.2)
                    continue
                best = max(items, key=lambda item: (item['impact_score'], item['sentiment']))
                if best['sentiment'] < 0.70 and best['impact_score'] < 30:
                    time.sleep(0.2)
                    continue
                key = f"finnhub_{symbol}_{best['timestamp']}"
                with state_lock:
                    if key in state.get('seen_signals', {}):
                        time.sleep(0.2)
                        continue
                price = _finnhub_current_price(symbol)
                url = best.get('url') or ''
                link = f"\n🔗 [الخبر الكامل]({url})" if url else ''
                msg = (f"📰 *Finnhub Catalyst: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                       f"📌 {best['headline']}\n💰 السعر: ${price:.4f}\n"
                       f"📊 المشاعر: {best['sentiment'] * 100:.0f}% إيجابي\n"
                       f"💥 الأثر: {best['impact_score']}/50\n⏰ {best['datetime']}"
                       f"{link}\n━━━━━━━━━━━━━━━━\n⚠️ Paper alert وليست توصية")
                send_telegram(msg)
                with state_lock:
                    state.setdefault('seen_signals', {})[key] = time.time()
                    save_state()
                if AI_PROCESS_ENABLED and (best['impact_score'] >= 30 or best['sentiment'] >= 0.80):
                    threading.Thread(target=final_trade_recommendation,
                                     args=(symbol, min(int(best['impact_score']), 25), 'NEWS_MOMENTUM', False, True),
                                     daemon=True, name=f'FinnhubAI-{symbol}').start()
                time.sleep(1.0)
            _finnhub_news_cache = {k: v for k, v in _finnhub_news_cache.items() if time.time() - v[0] < 3600}
            gc.collect()
            time.sleep(FINNHUB_SCAN_INTERVAL)
        except Exception as error:
            logger.error(f'🔥 Finnhub scanner error: {error}')
            time.sleep(60)


def start_safe_finnhub():
    if FINNHUB_ENABLED:
        threading.Thread(target=finnhub_safe_scanner, daemon=True, name='FinnhubSafeScanner').start()
        logger.info('🛡️ Finnhub Safe Scanner started')
    else:
        logger.info('ℹ️ Finnhub Safe Scanner not started')


def rss_news_scanner():
    """يقرأ 8 مصادر RSS ويصيد الأخبار الإيجابية القوية على الأسهم تحت $30"""
    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(300)
                continue

            for rss_url in RSS_SOURCES:
                items = fetch_rss(rss_url)
                for item in items[:5]:
                    title = item['title']
                    link  = item['link']
                    desc = item.get('desc', '')
                    if not any(keyword in (title + ' ' + desc).lower() for keyword in STRONG_POSITIVE):
                        continue

                    news_id = title[:80]
                    with state_lock:
                        if news_id in state.get("seen_news", {}):
                            continue
                        state.setdefault("seen_news", {})[news_id] = time.time()

                    news_text = title + " " + desc
                    if has_dilution_risk(news_text):
                        logger.info(f"Skipping dilution-risk news: {title[:80]}")
                        continue
                    ai_res = ai_analyze_news(title, item.get('desc', ''))
                    sentiment = ai_res.get("sentiment", "neutral")
                    impact_score = ai_res.get("impact_score", 0)
                    if ai_res.get("dilution_risk") or ai_res.get("offering_risk") or ai_res.get("likely_priced_in"):
                        continue
                    if sentiment != "positive" or impact_score < 70:
                        continue

                    symbol = _extract_symbol_from_news(title, item.get('desc', ''))
                    symbols = [symbol] if symbol else []
                    if not symbols:
                        continue

                    for symbol in symbols[:2]:
                        try:
                            with state_lock:
                                state.setdefault("pre_breakout_news_impact", {})[symbol] = {
                                    "impact": min(int(impact_score), 25),
                                    "time": time.time(),
                                }
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

                            if AI_PROCESS_ENABLED:
                                threading.Thread(
                                    target=ai_process_symbol,
                                    args=(symbol, min(int(impact_score), 25)),
                                    daemon=True,
                                ).start()
                                # توصية موحدة تلقائية، مع كولداون لمنع ضغط Groq.
                                rec_key = f"auto_rec_{symbol}"
                                with state_lock:
                                    last_rec = float(state.setdefault("seen_signals", {}).get(rec_key, 0) or 0)
                                if time.time() - last_rec >= 900:
                                    with state_lock:
                                        state["seen_signals"][rec_key] = time.time()
                                        save_state()
                                    threading.Thread(
                                        target=final_trade_recommendation,
                                        args=(symbol, min(int(impact_score), 25), "NEWS_MOMENTUM", False, True),
                                        daemon=True,
                                    ).start()

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


# ================================================================
# 🧹 PENDING HALTS CLEANUP — إزالة حالات الإيقاف القديمة أو الوهمية
# ================================================================
def clean_stale_pending_halts():
    """ينظف الحالات القديمة، ويتحقق من أن السهم ما زال يتداول قبل إبقائه."""
    with state_lock:
        pending = dict(state.get('pending_halts', {}))
    if not pending:
        return

    to_remove = []
    now = time.time()
    for symbol, info in pending.items():
        info = info if isinstance(info, dict) else {}
        timestamp = float(info.get('timestamp', 0) or 0)
        if now - timestamp > 3600:
            to_remove.append(symbol)
            logger.info(f'🧹 Removed {symbol} from pending_halts (expired)')
            continue
        try:
            df = cached_download(symbol, period='1d', interval='5m', force_refresh=True)
            if df is not None and not df.empty and len(df) > 2:
                df.columns = [str(c).lower() for c in df.columns]
                volume = float(df['volume'].iloc[-1]) if 'volume' in df.columns else 0.0
                if volume > 0:
                    to_remove.append(symbol)
                    logger.info(f'🧹 Removed {symbol} from pending_halts (active trading detected)')
        except Exception as error:
            logger.debug(f'[Pending Halts] {symbol}: {error}')

    if to_remove:
        with state_lock:
            for symbol in to_remove:
                state.setdefault('pending_halts', {}).pop(symbol, None)
            save_state()


def pending_halts_cleaner_loop():
    while True:
        try:
            clean_stale_pending_halts()
        except Exception as error:
            logger.error(f'[Pending Halts] cleaner error: {error}')
        time.sleep(300)


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
                time.sleep(10)
                continue

            with state_lock:
                pending = dict(state.get("pending_halts", {}))

            if not pending:
                time.sleep(2) # لا يوجد أسهم موقوفة، انتظر قليلاً
                continue

            for symbol, halt_info in list(pending.items()):
                try:
                    # تجاهل لو مضى أكثر من 30 دقيقة على الإيقاف
                    halt_age = time.time() - halt_info.get("timestamp", 0)
                    if halt_age > 1800:
                        with state_lock:
                            state["pending_halts"].pop(symbol, None)
                        continue

                    # استخدام force_refresh=True لجلب بيانات حية لحظية
                    df = cached_download(symbol, period="1d", interval="1m", force_refresh=True)
                    if df.empty or len(df) < 3:
                        continue

                    price      = df['close'].iloc[-1]
                    vol_now    = df['volume'].iloc[-1]
                    vol_prev   = df['volume'].iloc[-3:-1].mean()
                    price_halt = halt_info.get("price_at_halt", price)

                    # هل رُفع الإيقاف؟ = في بيانات جديدة بعد وقت الإيقاف
                    last_candle_time = df.index[-1]
                    halt_time_ts     = halt_info.get("timestamp", 0)
                    resumed = last_candle_time.timestamp() > (halt_time_ts + 30) # تقليل وقت الانتظار لسرعة الرصد

                    if resumed:
                        # إرسال تنبيه عودة التداول فوراً
                        resume_key = f"resume_{symbol}_{int(halt_time_ts)}"
                        with state_lock:
                            if resume_key not in state.get("seen_signals", {}):
                                msg = f"✅ *عودة التداول (Resumed): {symbol}*\n"
                                msg += f"━━━━━━━━━━━━━━━━\n"
                                msg += f"💰 السعر الحالي: *${price:.2f}*\n"
                                msg += f"📈 التغير من وقت الإيقاف: {((price/price_halt)-1)*100:+.1f}%\n"
                                msg += f"💡 السهم متاح للتداول الآن!"
                                send_telegram(msg)
                                state.setdefault("seen_signals", {})[resume_key] = time.time()
                                save_state()

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
                    saudi_now = now_saudi().strftime("%I:%M %p")

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
                    msg += f"⏰ وقت الدخول: *{now_time} EST*  |  *{saudi_now} 🇸🇦*\n"
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

        time.sleep(2)  # تم التسريع القصوى: يفحص كل ثانيتين لضمان تنبيه لحظي بالـ Resume والـ Halt


# ================= SHORT INTEREST SCANNER =================





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

def fast_filter(symbol):
    """فلتر قوي - يصطاد الأسهم المتفجرة فقط (Explosive Filter)"""
    try:
        df = cached_download(symbol, period="1d", interval="5m")
        if df.empty or len(df) < 5:
            return False
        
        price = df['close'].iloc[-1]
        volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].mean()
        
        # فلتر 1: سعر منخفض (بيني ستوكس) - لضمان حركة قوية
        if price < 0.5 or price > 15:
            return False
        
        # فلتر 2: حجم انفجار (نص مليون سهم كحد أدنى)
        if volume < 500000:
            return False
        
        # فلتر 3: RVOL (حجم نسبي) لازم يكون 2x على الأقل
        if avg_volume > 0 and volume < avg_volume * 2:
            return False
        
        # فلتر 4: السعر يتحرك (لازم يتحرك 3% على الأقل في آخر 5 شمعات)
        price_change = (price - df['close'].iloc[-5]) / df['close'].iloc[-5] * 100
        if abs(price_change) < 3:
            return False
        
        return True
    except:
        return False

# ================= YAHOO DATA FETCHER =================
# ── قائمة سوداء للأسهم التي تعطي 404 باستمرار (محذوفة من البورصة) ──
# {symbol: timestamp_of_first_404} — تُمسح بعد 6 ساعات لإعادة المحاولة
_bad_tickers: dict = {}
_bad_tickers_lock = threading.Lock()

# حماية من ضغط الطلبات المتزامنة على Yahoo وتقليل Connection Pool Full.
YAHOO_REQUEST_SEMAPHORE = threading.BoundedSemaphore(4)
YAHOO_REQUEST_LOCK = threading.Lock()
YAHOO_LAST_REQUEST = 0.0
YAHOO_MIN_REQUEST_GAP = 1.5

KNOWN_DELISTED = {
    'RCKY', 'GENK', 'REAL', 'FIG', 'TMUSL', 'TAK', 'SXC', 'UDR',
    'BMNP', 'HBANL', 'HQY', 'AGI', 'PACK', 'SFHG', 'CPK', 'MITT',
    'TACHU', 'NCO', 'JABRW', 'YCY', 'BFRI', 'DMO', 'BFRG', 'VACI',
    'MPT', 'CHGA', 'FRBA', 'CRGOW', 'TGL', 'NERV', 'GE', 'GLMD',
    'AXINU', 'ECX', 'NDAQ', 'SND', 'ENTA', 'BML', 'FBIO', 'DAN',
    'TYRA', 'THRM', 'ALIS', 'NECB', 'CLST', 'COLL', 'NML', 'ANVS',
    'QETAR', 'WLY', 'IIIV', 'ADAML', 'UNIT', 'TRTX', 'SYRE', 'UROY',
    'VTOL', 'BCYC', 'GAP', 'KLC', 'NPACW', 'MANU', 'EQIX', 'VSXY',
    'NG', 'CNTY', 'BBT', 'NMR', 'KFRC', 'NMRK', 'ETX', 'VIOT', 'CHI',
    'ONL', 'VSTS', 'AMRZ', 'JBHT', 'EPR', 'CFTR', 'BTO', 'ETB', 'EMF',
    'KALU', 'MOGU', 'DGNX', 'SYPR', 'ETI', 'YHNAU', 'MAN', 'NXST',
    'PSQH', 'SMTK', 'ZKP', 'JACK', 'EQH', 'SCCG', 'CNX', 'ESLA',
    'TRMD', 'GAB', 'TMCWW', 'BPRN', 'ALGS', 'TPR', 'QUBT', 'PPLI',
    'MAA', 'RVSN', 'GOODO', 'IEP', 'STAG', 'COLAR', 'EXK', 'XXII',
    'YSWY', 'EBS', 'ZM', 'AFRIW', 'ASST', 'MYND', 'SWK', 'CNXN',
    'CRVL', 'PEBO', 'SSACR', 'MLCO', 'ILLU', 'SBAC', 'SEER', 'TSI',
    'INTC', 'JBLU', 'MYXXU', 'CHMG', 'LATA', 'SCNX', 'NOW', 'TGE',
    'APGE', 'PDS', 'VIA', 'SSII', 'DYN', 'NMM', 'RACD', 'DAVEW',
    'FBGL', 'MDBH', 'FAST', 'AUST', 'EOI', 'DRS', 'STRL', 'YYAI',
    'POLA', 'BME', 'NDRA', 'KMPR', 'OFAL', 'FTCI', 'LIQT', 'ASPN',
    'IBN', 'DSS', 'AER', 'MARPS', 'RIG', 'FERAU', 'NCA', 'STTK',
    'ZION', 'CINF', 'FEAM', 'BRR', 'ATGL', 'GEO', 'LSTA', 'IR', 'UCB',
    'MCB', 'ECAT', 'WALD', 'GCGRU', 'DNA', 'INFQ', 'JHX', 'TANH',
    'ICHR', 'CGON', 'NUAI', 'ARMK', 'NNNN', 'UNM', 'CEPU', 'CSQR',
    'CGCFU', 'UYSCR', 'NVMI', 'ANNA', 'AMRX', 'MITK', 'CUBI', 'HCWB',
    'PCYO', 'AVY', 'APC', 'FCCO', 'OXLCL', 'MTAKU', 'CNK', 'HYFT',
    'BVC', 'STIM', 'RGTIW', 'CLSK', 'CCOI', 'CNCKW', 'CORZW', 'AZIO',
    'AACG', 'INCR', 'BHFAP', 'GHC', 'NOVT', 'RMIX', 'SYM', 'APYX',
    'TRVI', 'PLCIR', 'WRAP', 'IBTA', 'ACTU', 'ADIG', 'PRLD', 'NXXT',
    'NCV', 'IFN', 'EVO', 'XPL', 'MGTX', 'OLB', 'SITE', 'LEGN', 'MHD',
    'MPWR', 'SAMG', 'GFL', 'HGTY', 'BIXIU', 'LCLN', 'ILMN', 'GLE',
    'ASUR', 'EDTK', 'ISSC', 'BZAIW', 'RPT', 'INBK', 'MTEK', 'RDDT',
    'LTGO', 'PENN', 'SGMT', 'WLDS', 'ERIE', 'AQNB', 'BRK', 'SBXD',
    'RGC', 'GHG', 'DDD', 'MTG', 'CMND', 'ONCH', 'LRMR', 'ZTS', 'NVT',
    'HOVR', 'IDACW', 'FOA', 'WY', 'VGI', 'IMTE', 'REBN', 'INFU', 'WFRD',
    'PNI', 'MCHPP', 'EMO', 'RDAGW', 'AMWL', 'SIGI', 'CRGO', 'WPM',
    'INSM', 'EVI', 'FTEK', 'AMX', 'WOOF', 'ACRS', 'ZEO', 'STKE',
    'MDXG', 'NMIH', 'MBRX', 'STXS', 'IART', 'GLNG', 'CRH', 'SAIL',
    'MYSE', 'PN', 'AEF', 'FCF', 'BANF', 'IVF', 'AIV', 'RPD', 'CZNC',
    'FISI', 'ALAR', 'ANGO', 'MGNX'
}
BAD_TICKER_TTL = 43200  # 12 ساعة لتقليل إعادة طلب الرموز الفاشلة


def _is_bad_ticker(symbol: str) -> bool:
    with _bad_tickers_lock:
        ts = _bad_tickers.get(symbol)
        if ts is None:
            return False
        if time.time() - ts > BAD_TICKER_TTL:
            del _bad_tickers[symbol]
            return False
        return True


def _mark_bad_ticker(symbol: str):
    with _bad_tickers_lock:
        _bad_tickers.setdefault(symbol, time.time())


def fetch_from_yahoo(symbol, period="5d", interval="15m", timeout=10, prepost=False):
    """جلب البيانات من Yahoo مع دعم بيانات Pre-Market وAfter-Hours."""
    if get_market_phase() in ("PRE", "AFTER"):
        prepost = True
    # ── تحقق سريع: هل الرمز في القائمة السوداء؟ ──
    symbol = str(symbol).upper().strip()
    if symbol in KNOWN_DELISTED or _is_bad_ticker(symbol):
        _mark_bad_ticker(symbol)
        return pd.DataFrame()

    try:
        days_map = {"1d": 1, "2d": 2, "5d": 5, "10d": 10, "1mo": 30}
        days = days_map.get(period, 5)
        
        end_date = int(datetime.now().timestamp())
        start_date = int((datetime.now() - timedelta(days=days)).timestamp())
        
        interval_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1d": "1d", "1h": "60m"}
        yf_interval = interval_map.get(interval, "15m")
        
        include_prepost = "true" if prepost else "false"
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
               f"?interval={yf_interval}&period1={start_date}&period2={end_date}"
               f"&includePrePost={include_prepost}")
        
        # تأخير محافظ بين الطلبات وتقليل التزامن لمنع الحظر وامتلاء Connection Pool.
        global YAHOO_LAST_REQUEST
        with YAHOO_REQUEST_SEMAPHORE:
            with YAHOO_REQUEST_LOCK:
                elapsed = time.time() - YAHOO_LAST_REQUEST
                if elapsed < YAHOO_MIN_REQUEST_GAP:
                    time.sleep(YAHOO_MIN_REQUEST_GAP - elapsed)
                time.sleep(random.uniform(0.1, 0.5))
                YAHOO_LAST_REQUEST = time.time()

            response = None
            data = None
            for attempt in range(3):
                try:
                    response = requests.get(url, impersonate="chrome120", timeout=timeout)
                    if response.status_code == 404:
                        logger.debug(f"⛔ {symbol} → 404 (added to blacklist for 12h)")
                        _mark_bad_ticker(symbol)
                        return pd.DataFrame()
                    if response.status_code in (429, 500, 502, 503, 504):
                        wait = 2 ** attempt + random.uniform(0.5, 1.5)
                        logger.warning(f"Yahoo {response.status_code} for {symbol}; retry {attempt + 1}/3 in {wait:.1f}s")
                        time.sleep(wait)
                        continue
                    if response.status_code != 200:
                        logger.warning(f"Yahoo Error {response.status_code} for {symbol}")
                        return pd.DataFrame()
                    try:
                        data = response.json()
                    except ValueError:
                        logger.warning(f"Yahoo returned empty/invalid JSON for {symbol}; retry {attempt + 1}/3")
                        time.sleep(2 ** attempt + random.uniform(0.5, 1.5))
                        continue
                    break
                except Exception as request_error:
                    if attempt == 2:
                        raise
                    time.sleep(2 ** attempt + random.uniform(0.5, 1.5))

        if not isinstance(data, dict):
            _mark_bad_ticker(symbol)
            return pd.DataFrame()
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
        df.columns = [c.lower() for c in df.columns]
        
        # التأكد من تحويل التوقيت لتوقيت نيويورك
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC').tz_convert(EASTERN_TZ)
        else:
            df.index = df.index.tz_convert(EASTERN_TZ)
            
        return df
        
    except Exception as e:
        logger.warning(f"Yahoo download skipped for {symbol}: {e}")
        _mark_bad_ticker(symbol)
        return pd.DataFrame()

def halt_breaker_scanner():
    """
    💣 ماسح متخصص لصيد الأسهم بعد رفع الإيقاف مباشرة (مثل SUGP)
    يشتغل كل 30 ثانية، فلاتر أقل، أسرع رد فعل
    """
    BULLISH_HALT_CODES = {"T1", "T2", "LUDP", "T5", "T6", "M1", "H10", "H11"}
    _last_alerts = {}
    
    while True:
        try:
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(30)
                continue

            with state_lock:
                pending = dict(state.get("pending_halts", {}))
            
            # أيضاً راقب الأسهم اللي موقوفة حالياً
            # halt_key بالشكل "SYMBOL_HH:MM:SS" — نستخرج الرمز فقط
            for halt_key in list(state.get("halted_stocks", {}).keys()):
                symbol = halt_key.split("_")[0]  # MTEN_14:28:40 → MTEN
                if not symbol or not symbol.isalpha():
                    continue
                if symbol not in pending:
                    pending[symbol] = {"timestamp": time.time() - 60, "reason": "UNKNOWN"}
            
            for symbol, halt_info in list(pending.items()):
                try:
                    # منع التكرار (مرة كل ساعة)
                    last_alert = _last_alerts.get(symbol, 0)
                    if time.time() - last_alert < 3600:
                        continue
                    
                    # جلب بيانات 1 دقيقة مع force_refresh
                    df = cached_download(symbol, period="1d", interval="1m", force_refresh=True)
                    if df.empty or len(df) < 5:
                        continue
                    
                    df.columns = [c.lower() for c in df.columns]
                    price = df['close'].iloc[-1]
                    volume = df['volume'].iloc[-1]
                    price_halt = halt_info.get("price_at_halt", price)
                    
                    # شرط 1: السعر ارتفع عن سعر الإيقاف
                    if price <= price_halt * 1.01:  # لازم يكون أعلى بـ 1% على الأقل
                        continue
                    
                    # شرط 2: السهم في فترة الـ 30 دقيقة الأولى بعد الإيقاف
                    halt_time_ts = halt_info.get("timestamp", 0)
                    if time.time() - halt_time_ts > 1800:  # أكثر من 30 دقيقة
                        continue
                    
                    # شرط 3: حجم عالي (مرن)
                    vol_prev = df['volume'].iloc[-5:-2].mean() if len(df) >= 5 else 1
                    if volume < max(vol_prev * 1.5, 50000):  # 1.5x أو 50k على الأقل
                        continue
                    
                    # شرط 4: زخم واضح
                    recent_prices = df['close'].iloc[-5:].values
                    if not (recent_prices[0] < recent_prices[-1]):  # السعر ارتفع خلال آخر 5 شمعات
                        continue
                    
                    # حساب الهدف والوقف
                    entry = price
                    tp = round(entry * 1.15, 2)   # هدف +15%
                    sl = round(entry * 0.94, 2)   # وقف -6%
                    
                    change_from_halt = (price - price_halt) / price_halt * 100
                    
                    msg = (
                        f"💥 *HALT BREAKER: {symbol}*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🔓 *رفع إيقاف التداول*\n"
                        f"📈 التغير من وقت الإيقاف: *+{change_from_halt:.1f}%*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💰 سعر الدخول: *${entry:.2f}*\n"
                        f"🎯 الهدف: *${tp}* (+15%)\n"
                        f"🛑 وقف الخسارة: *${sl}* (-6%)\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🔥 الحجم: {int(volume):,}\n"
                        f"💡 *السهم خرج من الإيقاف ومعه زخم قوي!*\n"
                        f"⚠️ *إشارة وليست توصية — ادرس قبل الدخول*"
                    )
                    send_telegram(msg)
                    
                    _last_alerts[symbol] = time.time()
                    with state_lock:
                        state.setdefault("seen_signals", {})[f"haltbreak_{symbol}"] = time.time()
                        save_state()
                    
                    logger.info(f"💥 HALT BREAKER: {symbol} +{change_from_halt:.1f}%")
                    time.sleep(2)
                    
                except Exception as sym_err:
                    logger.debug(f"Halt breaker error {symbol}: {sym_err}")
                    
        except Exception as e:
            logger.error(f"Halt breaker scanner error: {e}")
        
        time.sleep(30)  # كل 30 ثانية


def cached_download(symbol, period="5d", interval="15m", timeout=10, force_refresh=False, prepost=False):
    """جلب البيانات مع Cache ودعم Pre-Market وAfter-Hours."""
    if get_market_phase() in ("PRE", "AFTER"):
        prepost = True
    cache_key = f"{symbol}_{period}_{interval}_{'prepost' if prepost else 'regular'}"
    
    if not force_refresh:
        with _cache_lock:
            if cache_key in _cache:
                cached_data, timestamp = _cache[cache_key]
                ttl = CACHE_TTL.get(interval, 60)
                if time.time() - timestamp < ttl:
                    logger.debug(f"Cache HIT: {symbol} ({interval})")
                    return cached_data.copy() if not cached_data.empty else pd.DataFrame()
    
    logger.debug(f"Cache MISS: {symbol} ({interval})")
    df = fetch_from_yahoo(symbol, period, interval, timeout, prepost=prepost)
    
    with _cache_lock:
        _cache[cache_key] = (df, time.time())
    
    return df.copy() if not df.empty else pd.DataFrame()


# ================= REAL-TIME SUPPORT & RESISTANCE SCANNER =================
def _safe_float(value, default=0.0):
    """تحويل آمن للأرقام لتفادي تعطل الماسح بسبب NaN أو None."""
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _sr_atr(df, period=14):
    """ATR داخلي خفيف لا يعتمد على ترتيب تعريف الدوال الأخرى."""
    try:
        high = df['high'].astype(float)
        low = df['low'].astype(float)
        close = df['close'].astype(float)
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        if pd.isna(atr) or atr <= 0:
            atr = (high.tail(20).max() - low.tail(20).min()) / 10
        return max(float(atr), 0.01)
    except Exception:
        return 0.01


def _cluster_price_levels(points, current_price, atr):
    """يجمع نقاط القمم والقيعان المتقاربة في مستويات سعرية ذات قوة محسوبة."""
    if not points:
        return []

    tolerance = max(current_price * 0.006, atr * 0.35, 0.01)
    clusters = []

    for price, volume, idx in sorted(points, key=lambda x: x[0]):
        price = _safe_float(price)
        volume = max(_safe_float(volume), 1.0)
        if price <= 0:
            continue

        merged = False
        for cluster in clusters:
            if abs(price - cluster['level']) <= tolerance:
                total_weight = cluster['weight'] + volume
                cluster['level'] = ((cluster['level'] * cluster['weight']) + (price * volume)) / total_weight
                cluster['weight'] = total_weight
                cluster['touches'] += 1
                cluster['last_idx'] = max(cluster['last_idx'], idx)
                cluster['volume'] += volume
                merged = True
                break
        if not merged:
            clusters.append({
                'level': price,
                'weight': volume,
                'touches': 1,
                'last_idx': idx,
                'volume': volume
            })

    max_idx = max((c['last_idx'] for c in clusters), default=1)
    max_volume = max((c['volume'] for c in clusters), default=1.0)
    for cluster in clusters:
        recency = 1.0 + (cluster['last_idx'] / max(max_idx, 1))
        volume_score = min(cluster['volume'] / max_volume, 1.0) * 2.0
        cluster['strength'] = (cluster['touches'] * 1.7) + recency + volume_score
        cluster['tolerance'] = tolerance

    return [c for c in clusters if c['touches'] >= SR_MIN_TOUCHES]


def calculate_support_resistance(df):
    """يحسب أقرب دعم ومقاومة من بيانات لحظية باستخدام Pivot High/Low + Volume Clustering + VWAP."""
    if df is None or df.empty or len(df) < 35:
        return None

    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    required = {'open', 'high', 'low', 'close', 'volume'}
    if not required.issubset(set(df.columns)):
        return None

    df = df.dropna(subset=['high', 'low', 'close'])
    if len(df) < 35:
        return None

    current_price = _safe_float(df['close'].iloc[-1])
    if current_price <= 0:
        return None

    atr = _sr_atr(df)
    lookback = min(len(df), 180)
    work = df.tail(lookback).copy()
    highs = work['high'].astype(float)
    lows = work['low'].astype(float)
    volumes = work['volume'].fillna(0).astype(float)

    pivot_points = []
    window = 2
    for i in range(window, len(work) - window):
        high_slice = highs.iloc[i - window:i + window + 1]
        low_slice = lows.iloc[i - window:i + window + 1]
        if highs.iloc[i] >= high_slice.max():
            pivot_points.append((highs.iloc[i], volumes.iloc[i], i))
        if lows.iloc[i] <= low_slice.min():
            pivot_points.append((lows.iloc[i], volumes.iloc[i], i))

    # أضف قمة/قاع اليوم حتى لا تفوت مستويات مهمة في الأسهم النشطة جداً.
    pivot_points.append((highs.max(), volumes.max(), len(work) - 1))
    pivot_points.append((lows.min(), volumes.max(), len(work) - 1))

    levels = _cluster_price_levels(pivot_points, current_price, atr)
    if not levels:
        return None

    supports = sorted([lvl for lvl in levels if lvl['level'] <= current_price], key=lambda x: (current_price - x['level'], -x['strength']))
    resistances = sorted([lvl for lvl in levels if lvl['level'] >= current_price], key=lambda x: (x['level'] - current_price, -x['strength']))

    support = supports[0] if supports else None
    resistance = resistances[0] if resistances else None

    last_volume = _safe_float(work['volume'].iloc[-1])
    avg_volume = _safe_float(work['volume'].tail(40).mean(), 1.0)
    rvol = last_volume / max(avg_volume, 1.0)

    vwap = None
    try:
        typical = (work['high'] + work['low'] + work['close']) / 3
        cum_vol = work['volume'].replace(0, np.nan).fillna(0).cumsum()
        if cum_vol.iloc[-1] > 0:
            vwap = float((typical * work['volume']).cumsum().iloc[-1] / cum_vol.iloc[-1])
    except Exception:
        vwap = None

    support_dist = ((current_price - support['level']) / current_price * 100) if support else None
    resistance_dist = ((resistance['level'] - current_price) / current_price * 100) if resistance else None

    breakout = False
    bounce_support = False
    rejection_resistance = False

    if resistance:
        breakout = (
            current_price > resistance['level'] * (1 + SR_BREAKOUT_BUFFER_PCT / 100)
            and rvol >= SR_MIN_RVOL
            and _safe_float(work['close'].iloc[-1]) > _safe_float(work['open'].iloc[-1])
        )
        rejection_resistance = (
            resistance_dist is not None
            and 0 <= resistance_dist <= SR_PROXIMITY_PCT
            and _safe_float(work['close'].iloc[-1]) < _safe_float(work['open'].iloc[-1])
        )

    if support:
        lower_wick = min(_safe_float(work['open'].iloc[-1]), _safe_float(work['close'].iloc[-1])) - _safe_float(work['low'].iloc[-1])
        candle_range = max(_safe_float(work['high'].iloc[-1]) - _safe_float(work['low'].iloc[-1]), 0.01)
        bounce_support = (
            support_dist is not None
            and 0 <= support_dist <= SR_PROXIMITY_PCT
            and _safe_float(work['close'].iloc[-1]) >= _safe_float(work['close'].iloc[-2])
            and (lower_wick / candle_range) >= 0.25
        )

    return {
        'price': current_price,
        'support': support,
        'resistance': resistance,
        'support_dist': support_dist,
        'resistance_dist': resistance_dist,
        'rvol': rvol,
        'last_volume': last_volume,
        'avg_volume': avg_volume,
        'vwap': vwap,
        'atr': atr,
        'breakout': breakout,
        'bounce_support': bounce_support,
        'rejection_resistance': rejection_resistance,
    }


def format_support_resistance_message(symbol, analysis, interval_label='1m'):
    """يبني رسالة مختصرة وواضحة للدعم والمقاومة بدون وعود شراء/بيع مضللة."""
    price = analysis['price']
    support = analysis.get('support')
    resistance = analysis.get('resistance')
    support_dist = analysis.get('support_dist')
    resistance_dist = analysis.get('resistance_dist')
    rvol = analysis.get('rvol', 0)
    vwap = analysis.get('vwap')

    if analysis.get('breakout'):
        title = f"🚀 *اختراق مقاومة لحظي: {symbol}*"
        status = "اختراق مقاومة مؤكد بحجم أعلى من المتوسط"
    elif analysis.get('bounce_support'):
        title = f"🛡️ *ارتداد من دعم لحظي: {symbol}*"
        status = "السعر قريب من الدعم مع محاولة ارتداد"
    elif analysis.get('rejection_resistance'):
        title = f"⚠️ *رفض عند مقاومة: {symbol}*"
        status = "السعر قريب من المقاومة وظهر ضغط بيع"
    else:
        title = f"📍 *منطقة دعم/مقاومة مهمة: {symbol}*"
        status = "السعر داخل منطقة مراقبة لحظية"

    support_line = "غير واضح"
    if support:
        support_line = f"${support['level']:.4f} | لمسات: {support['touches']} | بعد: {support_dist:.2f}%"

    resistance_line = "غير واضح"
    if resistance:
        resistance_line = f"${resistance['level']:.4f} | لمسات: {resistance['touches']} | بعد: {resistance_dist:.2f}%"

    vwap_line = "غير متاح"
    if vwap:
        relation = "فوق VWAP" if price >= vwap else "تحت VWAP"
        vwap_line = f"${vwap:.4f} ({relation})"

    msg = f"{title}\n"
    msg += "━━━━━━━━━━━━━━━━\n"
    msg += f"💰 السعر الحالي: *${price:.4f}*\n"
    msg += f"🟢 الدعم الأقرب: *{support_line}*\n"
    msg += f"🔴 المقاومة الأقرب: *{resistance_line}*\n"
    msg += f"📊 RVOL آخر شمعة: *{rvol:.1f}x* | الفريم: {interval_label}\n"
    msg += f"📏 VWAP: *{vwap_line}*\n"
    msg += "━━━━━━━━━━━━━━━━\n"
    msg += f"🧭 الحالة: *{status}*\n"
    msg += "⚠️ مراقبة فنية فقط وليست توصية شراء أو بيع. انتظر تأكيد الحجم والثبات فوق/تحت المستوى."
    return msg


def should_alert_support_resistance(symbol, analysis):
    """فلتر جودة يمنع الرسائل الكثيرة ولا يرسل إلا عند مستويات قريبة أو اختراق واضح."""
    if not analysis:
        return False, None

    price = analysis['price']
    if not (SR_PRICE_MIN <= price <= SR_PRICE_MAX):
        return False, None
    if analysis.get('last_volume', 0) < SR_MIN_LAST_VOLUME:
        return False, None
    if analysis.get('rvol', 0) < 1.0:
        return False, None

    # ── فلتر الفجوة: لازم المقاومة بعيدة 3%+ والفجوة الكاملة 4%+ ──────────
    support_dist    = analysis.get('support_dist')
    resistance_dist = analysis.get('resistance_dist')
    support_lvl     = analysis.get('support', {}).get('level') if analysis.get('support') else None
    resistance_lvl  = analysis.get('resistance', {}).get('level') if analysis.get('resistance') else None

    # للارتداد من دعم: الهدف (المقاومة) لازم 3%+ من السعر
    if analysis.get('bounce_support') and (resistance_dist is None or resistance_dist < SR_MIN_TARGET_PCT):
        return False, None

    # الفجوة الكاملة بين الدعم والمقاومة لازم 4%+
    if support_lvl and resistance_lvl:
        range_pct = (resistance_lvl - support_lvl) / support_lvl * 100
        if range_pct < SR_MIN_RANGE_PCT:
            return False, None
    # ─────────────────────────────────────────────────────────────────────

    event = None
    if analysis.get('breakout'):
        event = 'breakout'
    elif analysis.get('bounce_support'):
        event = 'support_bounce'
    elif analysis.get('rejection_resistance'):
        event = 'resistance_reject'
    else:
        sd = analysis.get('support_dist')
        rd = analysis.get('resistance_dist')
        if sd is not None and 0 <= sd <= SR_PROXIMITY_PCT and analysis.get('rvol', 0) >= SR_MIN_RVOL:
            event = 'near_support'
        elif rd is not None and 0 <= rd <= SR_PROXIMITY_PCT and analysis.get('rvol', 0) >= SR_MIN_RVOL:
            event = 'near_resistance'

    if not event:
        return False, None

    now_ts = time.time()
    with state_lock:
        seen = state.setdefault('seen_support_resistance', {})
        last_ts = seen.get(f"{symbol}_{event}", 0)
        if now_ts - last_ts < SR_LEVEL_COOLDOWN:
            return False, None
        seen[f"{symbol}_{event}"] = now_ts
        save_state()

    return True, event


def support_resistance_scanner():
    """ماسح لحظي للأسهم النشطة يركز على مناطق الدعم والمقاومة بدل إشارات RSI العشوائية."""
    while True:
        try:
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(180)
                continue

            with state_lock:
                tickers = list(state.get('tickers', []))[:SR_MAX_TICKERS]

            interval = "1m" if phase == "REGULAR" else "5m"
            period = "1d" if interval == "1m" else "5d"

            for symbol in tickers:
                try:
                    df = cached_download(symbol, period=period, interval=interval)
                    if df.empty or len(df) < 35:
                        if interval == "1m":
                            df = cached_download(symbol, period="5d", interval="5m")
                        if df.empty or len(df) < 35:
                            continue

                    df.columns = [str(c).lower() for c in df.columns]
                    analysis = calculate_support_resistance(df)
                    ok, _event = should_alert_support_resistance(symbol, analysis)
                    if ok:
                        msg = format_support_resistance_message(symbol, analysis, interval_label=interval)
                        send_telegram(msg)
                        time.sleep(2)
                except Exception as sym_err:
                    logger.debug(f"SR scanner skip {symbol}: {sym_err}")
                    continue
                time.sleep(0.2)
        except Exception as e:
            logger.error(f"Support/Resistance scanner error: {e}")
        time.sleep(SR_SCAN_INTERVAL_SEC)


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




# ================= RSS CATALYST SCANNER (صائد المحفزات الخبري) =================

def _extract_symbol_from_news(title, summary=""):
    """
    يستخرج رمز السهم من عنوان الخبر أو ملخصه.
    يدعم أنماط: (XXXX), NASDAQ:XXXX, NYSE:XXXX, AMEX:XXXX
    """
    full_text = title + " " + summary

    # نمط 1: (XXXX) أو (Nasdaq: XXXX) أو (NYSE: XXXX)
    m = re.search(r'\((?:NASDAQ:|NYSE:|AMEX:)?([A-Z]{1,5})\)', full_text)
    if m:
        return m.group(1)

    # نمط 2: NASDAQ:XXXX أو NYSE:XXXX مباشرة بدون قوسين
    m = re.search(r'(?:NASDAQ|NYSE|AMEX):([A-Z]{1,5})\b', full_text)
    if m:
        return m.group(1)

    # نمط 3: أول كلمة بحروف كبيرة 2-5 أحرف في العنوان (بعد تجاهل الكلمات الشائعة)
    EXCLUDE = {
        'THE', 'AND', 'FOR', 'INC', 'LLC', 'LTD', 'CEO', 'CFO', 'CTO', 'COO',
        'IPO', 'FDA', 'SEC', 'NYSE', 'USD', 'ETF', 'RSS', 'NEW', 'NASDAQ',
        'AMEX', 'OTC', 'PRN', 'GNW', 'PRE', 'POST', 'FORM', 'CORP', 'CO',
        'AI', 'US', 'UK', 'EU', 'UN', 'WHO', 'CDC', 'EPS', 'Q1', 'Q2', 'Q3', 'Q4',
    }
    words = re.findall(r'\b[A-Z]{2,5}\b', title)
    for w in words:
        if w not in EXCLUDE:
            return w

    return None




# ================= SEC FORM 4 INSIDER BUYING SCANNER =================

def form4_insider_scanner():
    """
    يراقب ملفات Form 4 على SEC EDGAR — إفصاحات شراء الداخليين (المدراء والمساهمين الكبار).
    شراء الداخلي = إشارة قوية جداً أن الإدارة واثقة من ارتفاع السهم.
    يشتغل كل 15 دقيقة.
    """
    # الحد الأدنى لقيمة الشراء الداخلي (فوق 50 ألف دولار = شراء جدي)
    MIN_INSIDER_VALUE = 50_000

    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(300)
                continue

            # RSS Feed لأحدث ملفات Form 4 من EDGAR
            rss_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&search_text=&output=atom"
            req = urllib.request.Request(rss_url, headers={
                "User-Agent": "TradingBot research@example.com"
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                content_xml = resp.read()

            root = ET.fromstring(content_xml)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = root.findall("atom:entry", ns)

            for entry in entries:
                try:
                    title   = entry.findtext("atom:title",   "", ns).strip()
                    link_el = entry.find("atom:link", ns)
                    link    = link_el.get("href", "") if link_el is not None else ""
                    summary = entry.findtext("atom:summary", "", ns).strip()
                    filing_id = entry.findtext("atom:id",   "", ns).strip()

                    # تجنب التكرار
                    with state_lock:
                        if filing_id in state.get("seen_edgar", {}):
                            continue
                        state.setdefault("seen_edgar", {})[filing_id] = time.time()

                    full_text = (title + " " + summary).lower()

                    # نبحث عن مؤشرات الشراء (P = Purchase, A = Acquisition)
                    # ونتجنب البيع (S = Sale, D = Disposition)
                    is_purchase = any(kw in full_text for kw in [
                        " purchase", " acquired", " acquisition", "transaction code p",
                        "shares acquired", "direct ownership"
                    ])
                    is_sale = any(kw in full_text for kw in [
                        " sale", " sold", " disposed", "transaction code s",
                        "shares disposed"
                    ])

                    if not is_purchase or is_sale:
                        continue

                    # استخرج رمز السهم
                    symbol = _extract_symbol_from_news(title, summary)
                    symbols = [symbol] if symbol else []
                    if not symbols:
                        # محاولة ثانية: استخراج من رابط الملف
                        sym_in_link = re.findall(r"[?&]company=([A-Z]{1,5})", link.upper())
                        if sym_in_link:
                            symbols = sym_in_link[:1]
                    if not symbols:
                        continue

                    # استخرج قيمة الشراء إن وُجدت
                    value_match = re.search(r"\$([\d,]+)", summary)
                    insider_value = 0
                    if value_match:
                        try:
                            insider_value = int(value_match.group(1).replace(",", ""))
                        except:
                            pass

                    for symbol in symbols[:2]:
                        try:
                            signal_key = f"form4_{symbol}"
                            with state_lock:
                                last_sent = state.get("seen_signals", {}).get(signal_key, 0)
                            if time.time() - last_sent < SIGNAL_COOLDOWN:
                                continue

                            # جلب بيانات السهم
                            df = cached_download(symbol, period="5d", interval="15m")
                            if df.empty or len(df) < 5:
                                continue

                            df.columns = [c.lower() for c in df.columns]
                            price    = df["close"].iloc[-1]
                            vol_now  = df["volume"].iloc[-1]
                            vol_avg  = df["volume"].rolling(20).mean().iloc[-1]

                            # فلتر السعر: نتجنب الأسهم فوق $200
                            if price <= 0 or price > 200:
                                continue

                            # حساب التغير خلال آخر 5 شموع
                            price_change = (price - df["close"].iloc[-5]) / df["close"].iloc[-5] * 100 if len(df) >= 5 else 0

                            entry_price = price
                            tp = round(entry_price * 1.15, 2)   # هدف +15% (المدير يعرف أكثر!)
                            sl = round(entry_price * 0.94, 2)   # وقف -6%
                            now_time = now_est().strftime("%I:%M %p")

                            # تقدير قيمة الشراء لو ما وُجدت
                            value_text = f"${insider_value:,}" if insider_value >= MIN_INSIDER_VALUE else "غير محددة"

                            msg = (
                                f"🐋 *إشارة شراء داخلي — Form 4*\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"📊 *{symbol}*\n"
                                f"👤 أحد المدراء أو المساهمين الكبار اشترى أسهماً!\n"
                                f"💵 قيمة الشراء المُفصَح عنها: *{value_text}*\n"
                                f"📋 [رابط ملف Form 4]({link})\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"⏰ وقت الاكتشاف: *{now_time}*\n"
                                f"💰 السعر الحالي: *${entry_price:.2f}*\n"
                                f"📈 التغير (آخر ساعة): {price_change:+.1f}%\n"
                                f"🔥 الحجم: {int(vol_now):,} ({vol_now/max(vol_avg,1):.1f}x)\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"🎯 الهدف المقترح: *${tp}* (+15%)\n"
                                f"🛑 وقف الخسارة: *${sl}* (-6%)\n"
                                f"━━━━━━━━━━━━━━━━\n"
                                f"💡 *لماذا هذه الإشارة مهمة؟*\n"
                                f"المدير التنفيذي يشتري بأمواله الخاصة = ثقة داخلية بالارتفاع\n"
                                f"⚠️ *هذه إشارة وليست توصية — ادرس قبل الدخول*"
                            )
                            send_telegram(msg)
                            logger.info(f"🐋 Form 4 Insider Buy: {symbol} @ ${entry_price:.2f}")

                            with state_lock:
                                state.setdefault("seen_signals", {})[signal_key] = time.time()
                                save_state()

                            time.sleep(2)

                        except Exception as sym_err:
                            logger.warning(f"Form 4 symbol error {symbol}: {sym_err}")

                except Exception as entry_err:
                    logger.debug(f"Form 4 entry error: {entry_err}")
                    continue

        except Exception as e:
            logger.error(f"Form 4 Insider Scanner Error: {e}")

        time.sleep(900)  # كل 15 دقيقة


# ================= CLOSING HOURS POWER SCANNER (آخر ساعتين) =================

def closing_hours_scanner():
    """
    يشتغل فقط في آخر ساعتين من التداول (2:00 PM - 3:50 PM EST).
    يصطاد الأسهم اللي تتحرك بقوة قبل الإغلاق — غالباً تستمر الحركة يوم التالي.
    """
    while True:
        try:
            now = now_est()
            hour   = now.hour
            minute = now.minute

            # يشتغل فقط بين 2:00 PM و 3:50 PM EST
            is_closing_window = (hour == 14) or (hour == 15 and minute <= 50)
            if not is_closing_window or get_market_phase() != "REGULAR":
                time.sleep(60)
                continue

            saudi_now = now_saudi().strftime("%I:%M %p")

            with state_lock:
                tickers = list(state.get("tickers", []))[:600]

            if not tickers:
                time.sleep(60)
                continue

            logger.info(f"⏳ Closing Scanner active — {now.strftime('%I:%M %p')} EST | {saudi_now} 🇸🇦")

            for symbol in tickers:
                try:
                    signal_key = f"closing_{symbol}_{now.date()}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue

                    df = cached_download(symbol, period="2d", interval="5m")
                    if df.empty or len(df) < 20:
                        continue

                    df.columns = [c.lower() for c in df.columns]

                    price      = df["close"].iloc[-1]
                    open_price = df["open"].iloc[0]

                    # نطاق السعر: $0.5 - $30
                    if not (0.5 <= price <= 30):
                        continue

                    # ربح اليوم لازم +8% أو أكثر
                    gain_pct = (price - open_price) / open_price * 100
                    if gain_pct < 8:
                        continue

                    # الحجم في آخر 6 شموع (30 دقيقة) مرتفع
                    recent_vol = df["volume"].iloc[-6:].mean()
                    avg_vol    = df["volume"].rolling(20).mean().iloc[-10]
                    if avg_vol <= 0 or recent_vol < avg_vol * 1.5:
                        continue

                    # السعر يرتفع في آخر 3 شموع (زخم نهاية اليوم)
                    last3 = df["close"].iloc[-3:].values
                    if not (last3[0] < last3[1] < last3[2]):
                        continue

                    # حجم اليوم الكلي
                    today_candles = df[df.index.date == now.date()]
                    day_vol = int(today_candles["volume"].sum()) if not today_candles.empty else 0

                    tp = round(price * 1.10, 2)   # هدف +10%
                    sl = round(price * 0.94, 2)   # وقف -6%

                    msg = (
                        f"⏳ *إشارة آخر ساعتين: {symbol}*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"⏰ *{now.strftime('%I:%M %p')} EST  |  {saudi_now} 🇸🇦*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💰 السعر: *${price:.2f}*\n"
                        f"📈 ربح اليوم: *+{gain_pct:.1f}%*\n"
                        f"🔥 الحجم الكلي: {day_vol:,}\n"
                        f"📊 زخم الإغلاق: 3 شموع صاعدة ✅\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🎯 الهدف: *${tp}* (+10%)\n"
                        f"🛑 الوقف: *${sl}* (-6%)\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💡 *لماذا آخر ساعتين مهمة؟*\n"
                        f"الأسهم القوية عند الإغلاق غالباً تفتح أقوى يوم التالي\n"
                        f"⚠️ *إشارة وليست توصية*"
                    )
                    send_telegram(msg)
                    logger.info(f"⏳ Closing signal: {symbol} +{gain_pct:.1f}%")

                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()

                    time.sleep(2)

                except Exception as sym_err:
                    logger.debug(f"Closing scanner {symbol}: {sym_err}")
                    continue
                time.sleep(0.3)

        except Exception as e:
            logger.error(f"Closing hours scanner error: {e}")

        time.sleep(180)  # كل 3 دقائق


# ================= OPENING OPPORTUNITY SCANNER (فرصة الافتتاح) =================

def opening_opportunity_scanner():
    """
    يشتغل في أول 30 دقيقة من التداول (9:30 AM - 10:00 AM EST).
    يراقب:
      1. الاسهم اللي كانت قوية بالامس (من ماسح اخر ساعتين)
      2. اي سهم يفتح بزخم قوي مع حجم غير طبيعي في اول 10 دقائق
    """
    _sent_today = set()

    while True:
        try:
            now    = now_est()
            hour   = now.hour
            minute = now.minute

            is_opening_window = (hour == 9 and minute >= 30) or (hour == 10 and minute <= 15)
            if not is_opening_window or get_market_phase() != "REGULAR":
                if hour >= 10 and minute > 15:
                    _sent_today.clear()
                time.sleep(30)
                continue

            saudi_now = now_saudi().strftime("%I:%M %p")

            yesterday_strong = set()
            with state_lock:
                for key in state.get("seen_signals", {}):
                    if key.startswith("closing_"):
                        parts = key.split("_")
                        if len(parts) >= 2:
                            yesterday_strong.add(parts[1])

            with state_lock:
                all_tickers = list(state.get("tickers", []))[:800]

            candidates = list(yesterday_strong) + [t for t in all_tickers if t not in yesterday_strong]
            candidates = candidates[:500]

            logger.info(f"Opening Scanner active {now.strftime('%I:%M %p')} EST | {saudi_now}")

            for symbol in candidates:
                try:
                    today_key = f"opening_{symbol}_{now.date()}"
                    if today_key in _sent_today:
                        continue
                    with state_lock:
                        if today_key in state.get("seen_signals", {}):
                            _sent_today.add(today_key)
                            continue

                    df = cached_download(symbol, period="2d", interval="5m")
                    if df.empty or len(df) < 10:
                        continue

                    df.columns = [c.lower() for c in df.columns]

                    price = df["close"].iloc[-1]
                    if not (0.5 <= price <= 30):
                        continue

                    today_candles = df[df.index.date == now.date()]
                    if today_candles.empty:
                        continue

                    open_price  = today_candles["open"].iloc[0]
                    close_price = today_candles["close"].iloc[-1]

                    gain_from_open = (close_price - open_price) / open_price * 100
                    if gain_from_open < 5:
                        continue

                    vol_open = today_candles["volume"].iloc[:2].sum()
                    yest_candles = df[df.index.date < now.date()]
                    vol_avg_2 = yest_candles["volume"].rolling(2).mean().iloc[-1] if len(yest_candles) >= 2 else 0
                    if vol_avg_2 <= 0 or vol_open <= vol_avg_2 * 2.5:
                        continue

                    was_strong_yesterday = symbol in yesterday_strong
                    day_vol = int(today_candles["volume"].sum())
                    tp = round(close_price * 1.12, 2)
                    sl = round(close_price * 0.94, 2)
                    vol_ratio = vol_open / max(vol_avg_2, 1)

                    yesterday_line = ""
                    if was_strong_yesterday:
                        yesterday_line = "✨ *قوي بالامس ايضاً* — استمرار محتمل\n"

                    msg = (
                        "🔔 *فرصة الافتتاح: " + symbol + "*\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "⏰ *" + now.strftime("%I:%M %p") + " EST  |  " + saudi_now + " 🇸🇦*\n"
                        "━━━━━━━━━━━━━━━━\n"
                        + yesterday_line +
                        "💰 السعر الحالي: *$" + f"{close_price:.2f}" + "*\n"
                        "📈 ارتفاع عن الافتتاح: *+" + f"{gain_from_open:.1f}" + "%*\n"
                        "🔥 حجم اول 10 دقائق: " + f"{int(vol_open):,}" + " (" + f"{vol_ratio:.1f}" + "x المعتاد)\n"
                        "📊 الحجم الكلي حتى الآن: " + f"{day_vol:,}" + "\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "🎯 الهدف: *$" + f"{tp}" + "* (+12%)\n"
                        "🛑 الوقف: *$" + f"{sl}" + "* (-6%)\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "💡 *اول 30 دقيقة تحدد اتجاه اليوم*\n"
                        "⚠️ اشارة وليست توصية — ادرس قبل الدخول"
                    )

                    send_telegram(msg)
                    logger.info(f"Opening signal: {symbol} +{gain_from_open:.1f}% from open")

                    _sent_today.add(today_key)
                    with state_lock:
                        state.setdefault("seen_signals", {})[today_key] = time.time()
                        save_state()

                    time.sleep(2)

                except Exception as sym_err:
                    logger.debug(f"Opening scanner {symbol}: {sym_err}")
                    continue
                time.sleep(0.3)

        except Exception as e:
            logger.error(f"Opening opportunity scanner error: {e}")

        time.sleep(60)


# ================= PRE-MARKET SCANNER (ما قبل الافتتاح) =================
# يشتغل: 4:00 AM - 9:25 AM EST = 11:00 صباحاً - 4:25 مساءً بتوقيت السعودية

def is_trading_day():
    """يتحقق من يوم التداول الأمريكي ويمنع إشارات عطلة نهاية الأسبوع."""
    now = now_est()
    if now.weekday() >= 5:
        return False
    # عطلات أمريكية ثابتة معروفة في 2026، ويمكن تحديثها من مصدر رسمي لاحقًا.
    holidays = {
        "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
        "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
        "2026-11-26", "2026-12-24", "2026-12-31",
    }
    return now.strftime("%Y-%m-%d") not in holidays


def premarket_scanner():
    """
    يراقب الأسهم اللي تتحرك قبل فتح السوق الرسمي.
    وقت التشغيل: 4:00 AM - 9:25 AM EST (11:00 ص - 4:25 م بالسعودية)
    المصدر: yfinance مع بيانات ما قبل السوق (prepost=True)
    """
    _sent_today = set()

    while True:
        try:
            now    = now_est()
            hour   = now.hour
            minute = now.minute

            # لا تستخدم بيانات Yahoo القديمة في عطلة نهاية الأسبوع أو العطلات.
            if not is_trading_day():
                time.sleep(3600)
                continue

            # نافذة ما قبل السوق: 4:00 AM حتى 9:25 AM EST
            is_premarket = (4 <= hour < 9) or (hour == 9 and minute <= 25)
            if not is_premarket:
                if hour >= 10:
                    _sent_today.clear()
                time.sleep(60)
                continue

            saudi_now = now_saudi().strftime("%I:%M %p")

            with state_lock:
                # نمسح full_tickers (6700+ سهم) بدلاً من أعلى 600 فقط
                tickers = list(state.get("full_tickers") or state.get("tickers", []))

            if not tickers:
                time.sleep(120)
                continue

            logger.info(f"Pre-Market Scanner active {now.strftime('%I:%M %p')} EST | {saudi_now} KSA | {len(tickers)} tickers")

            for symbol in tickers:
                try:
                    today_key = f"premarket_{symbol}_{now.date()}"
                    if today_key in _sent_today:
                        continue
                    with state_lock:
                        if today_key in state.get("seen_signals", {}):
                            _sent_today.add(today_key)
                            continue

                    # استخدام cached_download بدلاً من yf.Ticker
                    df_pre = cached_download(symbol, period="2d", interval="1m", prepost=True)
                    if df_pre.empty or len(df_pre) < 5:
                        continue

                    df_pre.columns = [c.lower() for c in df_pre.columns]

                    # فصل شموع ما قبل السوق اليوم
                    pre_today = df_pre[
                        (df_pre.index.date == now.date()) &
                        (
                            (df_pre.index.hour < 9) |
                            ((df_pre.index.hour == 9) & (df_pre.index.minute < 30))
                        )
                    ]
                    if pre_today.empty:
                        continue

                    pre_price = pre_today["close"].iloc[-1]

                    # السعر بين $0.5 و $30
                    if not (0.5 <= pre_price <= 30):
                        continue

                    # سعر إغلاق أمس
                    prev_day = df_pre[df_pre.index.date < now.date()]
                    if prev_day.empty:
                        continue
                    prev_close = prev_day["close"].iloc[-1]

                    # تغيير % عن إغلاق أمس
                    change_pct = (pre_price - prev_close) / prev_close * 100

                    # شرط: +10% على الأقل، مع تجاهل الارتفاعات المفرطة غير القابلة للمطاردة
                    if change_pct < 10 or change_pct > 50:
                        continue

                    # حجم ما قبل السوق (فحص آمن)
                    if pre_today.empty:
                        continue
                    pre_vol = int(pre_today["volume"].fillna(0).sum())
                    # لا ترسل بيانات قديمة/افتراضية بحجم صفر، خصوصًا في عطلة أو جلسة بلا تداول.
                    if pre_vol == 0:
                        continue

                    # تجاهل حجم صغير جدًا للحركات الكبيرة
                    # Yahoo أحياناً يُرجع Vol=0 لأسهم مثل RGNT حتى لو تحركت +400%
                    # لا نحذف السهم إذا الحركة كبيرة (>30%) حتى لو الحجم صفر
                    if pre_vol < 500 and change_pct < 30:
                        continue

                    tp = round(pre_price * 1.15, 2)   # هدف +15%
                    sl = round(pre_price * 0.93, 2)   # وقف -7%

                    msg = (
                        "🌅 *حركة ما قبل الافتتاح: " + symbol + "*\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "⏰ *" + now.strftime("%I:%M %p") + " EST  |  " + saudi_now + " 🇸🇦*\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "💰 السعر الآن: *$" + f"{pre_price:.2f}" + "*\n"
                        "📊 إغلاق أمس: $" + f"{prev_close:.2f}" + "\n"
                        "📈 التغير: *" + f"{change_pct:+.1f}" + "%* قبل الافتتاح\n"
                        "🔥 الحجم ما قبل السوق: " + f"{pre_vol:,}" + "\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "🎯 الهدف بعد الافتتاح: *$" + f"{tp}" + "* (+15%)\n"
                        "🛑 الوقف: *$" + f"{sl}" + "* (-7%)\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "⏳ *السوق يفتح الساعة 4:30 م بتوقيت السعودية*\n"
                        "💡 هذه الحركة قبل الافتتاح — قد تستمر أو تتراجع\n"
                        "⚠️ اشارة وليست توصية — ادرس قبل الدخول"
                    )

                    send_telegram(msg)
                    logger.info(f"Pre-market signal: {symbol} {change_pct:+.1f}% vol={pre_vol:,}")

                    _sent_today.add(today_key)
                    with state_lock:
                        state.setdefault("seen_signals", {})[today_key] = time.time()
                        save_state()

                    time.sleep(2)

                except Exception as sym_err:
                    logger.debug(f"Premarket scanner {symbol}: {sym_err}")
                    continue
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"Premarket scanner error: {e}")

        time.sleep(180)   # كل 3 دقائق


# ================= AFTER-HOURS SCANNER (بعد الإغلاق) =================
# يشتغل: 4:00 PM - 7:50 PM EST = 11:00 م - 2:50 ص بتوقيت السعودية
# فلاتر صارمة — أقل رسائل، لكن كل رسالة ذات قيمة

def after_hours_discovery_scanner():
    """اكتشاف After-Hours خفيف يغذي hot_watchlist دون Telegram أو AI."""
    batch_limit = 60
    interval = 120
    last_seen = {}
    while True:
        try:
            now = now_est()
            is_after = (now.hour == 16 and now.minute >= 5) or (17 <= now.hour <= 19)
            if not is_after:
                time.sleep(120)
                continue
            with state_lock:
                tickers = list(state.get('full_tickers', [])) or list(state.get('tickers', []))
            tickers = list(dict.fromkeys(str(x).upper().strip() for x in tickers if x))[:batch_limit]
            discovered = 0
            for symbol in tickers:
                try:
                    df = cached_download(symbol, period='2d', interval='5m', prepost=True)
                    if df is None or df.empty or len(df) < 5:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    if not {'close', 'volume'}.issubset(df.columns):
                        continue
                    ah = df[(df.index.date == now.date()) & (df.index.hour >= 16)]
                    regular = df[(df.index.date == now.date()) & (df.index.hour < 16)]
                    if ah.empty or regular.empty:
                        continue
                    price = float(ah['close'].iloc[-1])
                    close = float(regular['close'].iloc[-1])
                    volume = float(ah['volume'].sum() or 0)
                    change = (price - close) / max(abs(close), 1e-9) * 100
                    if not (0.30 <= price <= MAX_PRICE and change >= 3.0 and volume >= 10000):
                        continue
                    if time.time() - last_seen.get(symbol, 0) < 600:
                        continue
                    last_seen[symbol] = time.time()
                    candidate = {'symbol': symbol, 'price': price, 'change': change, 'volume': volume,
                                 'rvol': 0.0, 'phase': 'AFTER', 'timestamp': time.time(),
                                 'source': 'AFTER_HOURS_DISCOVERY'}
                    with state_lock:
                        hot = [item for item in state.get('hot_watchlist', []) if item.get('symbol') != symbol]
                        hot.append(candidate)
                        hot.sort(key=lambda item: float(item.get('change', 0)), reverse=True)
                        state['hot_watchlist'] = hot[:150]
                        state['hot_watchlist_timestamp'] = time.time()
                    discovered += 1
                except Exception as error:
                    logger.debug(f'[AfterHoursDiscovery] {symbol}: {error}')
                time.sleep(0.15)
            with state_lock:
                save_state()
            logger.info(f'[AfterHoursDiscovery] cycle complete: {discovered} candidates')
            time.sleep(interval)
        except Exception as error:
            logger.error(f'[AfterHoursDiscovery] error: {error}')
            time.sleep(120)


def after_hours_scanner():
    """
    يراقب تحركات ما بعد الإغلاق (After-Hours).
    وقت التشغيل: 4:05 PM - 7:50 PM EST (11:05 م - 2:50 ص بالسعودية)
    فلاتر مشددة: +12% وحجم 50,000 على الأقل.
    """
    _sent_today = set()

    while True:
        try:
            now    = now_est()
            hour   = now.hour
            minute = now.minute

            # نافذة ما بعد الإغلاق: 4:05 PM حتى 7:50 PM EST
            is_after = (hour == 16 and minute >= 5) or (17 <= hour <= 19) or (hour == 19 and minute <= 50)
            if not is_after:
                if hour >= 20:
                    _sent_today.clear()
                time.sleep(60)
                continue

            saudi_now = now_saudi().strftime("%I:%M %p")

            with state_lock:
                tickers = list(state.get("tickers", []))[:500]

            if not tickers:
                time.sleep(120)
                continue

            logger.info(f"After-Hours Scanner active {now.strftime('%I:%M %p')} EST | {saudi_now} KSA")

            for symbol in tickers:
                try:
                    today_key = f"afterhours_{symbol}_{now.date()}"
                    if today_key in _sent_today:
                        continue
                    with state_lock:
                        if today_key in state.get("seen_signals", {}):
                            _sent_today.add(today_key)
                            continue

                    # استخدام cached_download بدلاً من yf.Ticker
                    df = cached_download(symbol, period="2d", interval="1m", prepost=True)
                    if df.empty or len(df) < 5:
                        continue

                    df.columns = [c.lower() for c in df.columns]

                    # شموع ما بعد الإغلاق فقط (بعد 4:00 PM)
                    ah_today = df[
                        (df.index.date == now.date()) &
                        (df.index.hour >= 16)
                    ]
                    if ah_today.empty:
                        continue

                    ah_price = ah_today["close"].iloc[-1]
                    if not (0.5 <= ah_price <= 30):
                        continue

                    # سعر إغلاق السوق الرسمي (آخر شمعة قبل 4 PM)
                    regular_today = df[
                        (df.index.date == now.date()) &
                        (df.index.hour < 16)
                    ]
                    if regular_today.empty:
                        continue
                    close_price = regular_today["close"].iloc[-1]

                    change_pct = (ah_price - close_price) / close_price * 100

                    # فلتر صارم: +12% فقط
                    if change_pct < 12:
                        continue

                    # حجم ما بعد الإغلاق — 50,000 على الأقل (فحص آمن)
                    if ah_today.empty:
                        continue
                    ah_vol = int(ah_today["volume"].sum())
                    if ah_vol < 50_000:
                        continue

                    tp = round(ah_price * 1.15, 2)
                    sl = round(ah_price * 0.92, 2)

                    msg = (
                        "🌙 *حركة ما بعد الإغلاق: " + symbol + "*\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "⏰ *" + now.strftime("%I:%M %p") + " EST  |  " + saudi_now + " 🇸🇦*\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "💰 السعر بعد الإغلاق: *$" + f"{ah_price:.2f}" + "*\n"
                        "📊 إغلاق السوق: $" + f"{close_price:.2f}" + "\n"
                        "📈 التغير بعد الإغلاق: *" + f"{change_pct:+.1f}" + "%*\n"
                        "🔥 الحجم بعد الإغلاق: " + f"{ah_vol:,}" + "\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "🎯 هدف الغد: *$" + f"{tp}" + "* (+15%)\n"
                        "🛑 الوقف: *$" + f"{sl}" + "* (-8%)\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "⏳ *السوق يفتح غداً 4:30 م بتوقيت السعودية*\n"
                        "💡 حركة After-Hours قوية = احتمال استمرار عند الافتتاح\n"
                        "⚠️ اشارة وليست توصية"
                    )

                    send_telegram(msg)
                    logger.info(f"After-hours signal: {symbol} {change_pct:+.1f}% vol={ah_vol:,}")

                    _sent_today.add(today_key)
                    with state_lock:
                        state.setdefault("seen_signals", {})[today_key] = time.time()
                        save_state()

                    time.sleep(3)

                except Exception as sym_err:
                    logger.debug(f"After-hours scanner {symbol}: {sym_err}")
                    continue
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"After-hours scanner error: {e}")

        time.sleep(300)   # كل 5 دقائق


# ════════════════════════════════════════════════════════════
#          PENNY STOCK POWER SCANNER — صياد البيني ستوك
# ════════════════════════════════════════════════════════════
# السعر: $0.10 - $5.00
# يشتغل كل 10 دقائق طوال وقت التداول الرسمي
# شروط: +15% يومي + حجم 3x + زخم صاعد + سيولة كافية

PENNY_PRICE_MIN   = 0.10   # أدنى سعر
PENNY_PRICE_MAX   = 5.00   # أقصى سعر
PENNY_GAIN_MIN    = 15.0   # أدنى ارتفاع يومي %
PENNY_RVOL_MIN    = 3.0    # أدنى نسبة حجم (relative volume)
PENNY_DOLLAR_VOL  = 50_000 # حجم دولاري أدنى (سيولة)
PENNY_INTERVAL    = 600    # كل 10 دقائق

def penny_stock_scanner():
    """
    ماسح مخصص لأسهم Penny Stocks ($0.10 - $5.00).
    يبحث عن: ارتفاع +15%، حجم 3x المعتاد، زخم صاعد، وسيولة كافية.
    يشتغل طوال ساعات التداول الرسمي كل 10 دقائق.
    """
    while True:
        try:
            now   = now_est()
            phase = get_market_phase()

            if phase != "REGULAR":
                time.sleep(60)
                continue

            saudi_now = now_saudi().strftime("%I:%M %p")

            with state_lock:
                tickers = list(state.get("tickers", []))

            if not tickers:
                time.sleep(PENNY_INTERVAL)
                continue

            logger.info(f"[PennyScanner] Start scan {now.strftime('%I:%M %p')} EST | {saudi_now} KSA — {len(tickers)} tickers")

            found = 0
            for symbol in tickers:
                try:
                    signal_key = f"penny_{symbol}_{now.strftime('%Y-%m-%d_%H')}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue

                    df = cached_download(symbol, period="5d", interval="5m")
                    if df.empty or len(df) < 20:
                        continue

                    df.columns = [c.lower() for c in df.columns]

                    price = df["close"].iloc[-1]

                    # ── فلتر 1: نطاق السعر $0.10 - $5.00 ──
                    if not (PENNY_PRICE_MIN <= price <= PENNY_PRICE_MAX):
                        continue

                    # ── فلتر 2: الارتفاع اليومي +15% ──
                    today_candles = df[df.index.date == now.date()]
                    if today_candles.empty:
                        continue
                    open_price = today_candles["open"].iloc[0]
                    if open_price <= 0:
                        continue
                    gain_pct = (price - open_price) / open_price * 100
                    if gain_pct < PENNY_GAIN_MIN:
                        continue

                    # ── فلتر 3: الحجم النسبي RVOL > 3x ──
                    # متوسط حجم الأيام الـ4 الماضية في نفس الوقت
                    past_days = df[df.index.date < now.date()]
                    avg_vol = past_days["volume"].mean() if len(past_days) >= 20 else 0
                    cur_vol = today_candles["volume"].sum()
                    rvol    = cur_vol / max(avg_vol, 1)
                    if rvol < PENNY_RVOL_MIN:
                        continue

                    # ── فلتر 4: سيولة دولارية (price × volume) ──
                    dollar_vol = price * cur_vol
                    if dollar_vol < PENNY_DOLLAR_VOL:
                        continue

                    # ── فلتر 5: زخم صاعد — آخر 3 شموع ──
                    last3 = df["close"].iloc[-3:].values
                    if len(last3) < 3:
                        continue
                    momentum_up = last3[2] > last3[1] > last3[0]

                    # ── فلتر 6: السهم بالقرب من أعلى سعر اليوم (قوة) ──
                    high_today  = today_candles["high"].max()
                    near_high   = price >= high_today * 0.95

                    # لا يمر إلا إذا عنده زخم صاعد أو قريب من القمة
                    if not (momentum_up or near_high):
                        continue

                    # ── حساب الأهداف ──
                    tp1 = round(price * 1.20, 3)   # +20%
                    tp2 = round(price * 1.40, 3)   # +40%
                    sl  = round(price * 0.90, 3)   # -10%

                    # ── وسم الجودة ──
                    quality = ""
                    if momentum_up and near_high:
                        quality = "🔥 *زخم قوي + قريب من القمة*"
                    elif momentum_up:
                        quality = "⚡ *زخم صاعد*"
                    elif near_high:
                        quality = "📍 *قريب من أعلى سعر اليوم*"

                    msg = (
                        "💎 *Penny Stock — فرصة: " + symbol + "*\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "⏰ *" + now.strftime("%I:%M %p") + " EST  |  " + saudi_now + " 🇸🇦*\n"
                        "━━━━━━━━━━━━━━━━\n"
                        + (quality + "\n" if quality else "") +
                        "💰 السعر الحالي: *$" + f"{price:.3f}" + "*\n"
                        "📈 ارتفاع اليوم: *+" + f"{gain_pct:.1f}" + "%*\n"
                        "🔥 الحجم النسبي (RVOL): *" + f"{rvol:.1f}" + "x*\n"
                        "💵 حجم دولاري: $" + f"{dollar_vol:,.0f}" + "\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "🎯 هدف 1: *$" + f"{tp1}" + "* (+20%)\n"
                        "🎯 هدف 2: *$" + f"{tp2}" + "* (+40%)\n"
                        "🛑 الوقف: *$" + f"{sl}" + "* (-10%)\n"
                        "━━━━━━━━━━━━━━━━\n"
                        "⚠️ أسهم البيني ستوك عالية المخاطرة — حجم صغير فقط"
                    )

                    send_telegram(msg)
                    logger.info(f"[PennyScanner] Signal: {symbol} +{gain_pct:.1f}% RVOL={rvol:.1f}x ${price:.3f}")
                    found += 1

                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()

                    time.sleep(2)

                except Exception as sym_err:
                    logger.debug(f"[PennyScanner] {symbol}: {sym_err}")
                    continue
                time.sleep(0.3)

            logger.info(f"[PennyScanner] Done — {found} signals found")

        except Exception as e:
            logger.error(f"[PennyScanner] Error: {e}")

        time.sleep(PENNY_INTERVAL)

# ================================================================
# 🌐 FULL MARKET SWEEP — يمسح كل 6700+ سهم دفعة بدفعة (50 سهم × طلب)
#    يصطاد ZTG / FAP / MTEN / CCTG / INHD وكل الأسهم المجهولة اللي تنفجر
#    الميزة: لا يعتمد على الحجم التاريخي — يفحص الكل كل 3 دقائق
# ================================================================

def full_sweep_scanner():
    """Full Rotation Discovery: يكتشف الحركة ويغذي hot_watchlist فقط."""
    BATCH_SIZE = 40
    MIN_GAIN = 3.0
    BATCH_SLEEP = 1.5
    SYMBOL_COOLDOWN = 300
    last_seen = {}

    while True:
        try:
            phase = get_market_phase()
            with state_lock:
                all_tickers = list(state.get('full_tickers', []))
                if not all_tickers:
                    all_tickers = list(state.get('tickers', []))
            all_tickers = list(dict.fromkeys(str(x).upper().strip() for x in all_tickers if x))
            if not all_tickers:
                logger.warning('[Discovery] No tickers available')
                time.sleep(60)
                continue

            use_prepost = phase in ('PRE', 'AFTER', 'CLOSED')
            logger.info(f'[Discovery] Starting rotation: {len(all_tickers)} symbols | phase={phase}')
            now = time.time()

            for offset in range(0, len(all_tickers), BATCH_SIZE):
                batch = all_tickers[offset:offset + BATCH_SIZE]
                try:
                    raw = yf.download(
                        tickers=batch, period='2d', interval='5m', group_by='ticker',
                        auto_adjust=False, progress=False, threads=False, prepost=use_prepost,
                    )
                except Exception as error:
                    logger.warning(f'[Discovery] Yahoo batch error: {error}')
                    time.sleep(5)
                    continue

                for symbol in batch:
                    try:
                        if len(batch) == 1:
                            df = raw
                        elif not hasattr(raw, 'columns') or not isinstance(raw.columns, pd.MultiIndex):
                            continue
                        elif symbol not in raw.columns.get_level_values(0):
                            continue
                        else:
                            df = raw[symbol]
                        if df is None or df.empty or 'Close' not in df.columns:
                            continue
                        df = df.dropna(subset=['Close'])
                        if len(df) < 3:
                            continue
                        current_price = float(df['Close'].iloc[-1])
                        if current_price <= 0 or not (0.30 <= current_price <= MAX_PRICE):
                            continue

                        today = now_est().date()
                        today_df = df[df.index.date == today]
                        previous_df = df[df.index.date < today]
                        if today_df.empty or previous_df.empty:
                            continue
                        prev_close = float(previous_df['Close'].iloc[-1])
                        if prev_close <= 0:
                            continue
                        day_change = (current_price - prev_close) / prev_close * 100
                        if day_change < MIN_GAIN:
                            continue

                        current_volume = float(today_df['Volume'].iloc[-1] or 0) if 'Volume' in today_df.columns else 0.0
                        avg_volume = float(today_df['Volume'].tail(20).mean() or 0) if 'Volume' in today_df.columns else 0.0
                        rvol = current_volume / max(avg_volume, 1.0)
                        p1 = float(today_df['Close'].iloc[-2]) if len(today_df) >= 2 else current_price
                        momentum_5m = (current_price - p1) / max(abs(p1), 1e-9) * 100
                        day_high = float(today_df['High'].max()) if 'High' in today_df.columns else current_price
                        near_hod = current_price >= day_high * 0.98
                        if now - last_seen.get(symbol, 0) < SYMBOL_COOLDOWN:
                            continue
                        last_seen[symbol] = now

                        candidate = {
                            'symbol': symbol, 'price': current_price, 'change': day_change,
                            'volume': current_volume, 'rvol': rvol, 'momentum_5m': momentum_5m,
                            'day_high': day_high, 'near_hod': near_hod, 'phase': phase,
                            'timestamp': now, 'source': 'FULL_ROTATION',
                        }
                        with state_lock:
                            hot = [item for item in state.get('hot_watchlist', []) if item.get('symbol') != symbol]
                            hot.append(candidate)
                            hot.sort(key=lambda item: (float(item.get('change', 0)), float(item.get('rvol', 0))), reverse=True)
                            state['hot_watchlist'] = hot[:150]
                            state['hot_watchlist_timestamp'] = now
                        logger.info(f'[Discovery] {symbol} +{day_change:.1f}% RVOL={rvol:.1f}x 5m={momentum_5m:+.1f}% phase={phase}')
                    except Exception as symbol_error:
                        logger.debug(f'[Discovery] {symbol}: {symbol_error}')
                time.sleep(BATCH_SLEEP)

            with state_lock:
                save_state()
            logger.info('[Discovery] Full market rotation completed')
            time.sleep(15)
        except Exception as error:
            logger.error(f'[Discovery] Fatal error: {error}')
            time.sleep(60)


# ================================================================
# 🏆 TOP GAINERS SCANNER — يصطاد الأسهم اللي تصعد 50%+ 100%+ 1000%+ اليوم
#    يسحب قائمة أكبر الرابحين حية من Yahoo Finance كل 60 ثانية
#    هذا الوحيد اللي يقدر يصطاد أسهم مثل INHD +4000%
# ================================================================

# ================================================================
# 🏆 TOP GAINERS DISCOVERY ENGINE
# مصدر اكتشاف خفيف: يحدّث hot_watchlist فقط، بلا Telegram أو AI.
# ================================================================
def top_gainers_scanner():
    """يجلب أكبر 100 رابح كل 60 ثانية ويحدّث قائمة المرشحين فقط."""
    gainers_url = (
        'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved'
        '?scrIds=day_gainers&count=100&formatted=false&includePrePost=true'
    )
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
        'Accept': 'application/json',
    }
    while True:
        try:
            if get_market_phase() == 'CLOSED':
                time.sleep(120)
                continue
            response = requests.get(gainers_url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning(f'[TopGainers] Yahoo status={response.status_code}')
                time.sleep(60)
                continue
            result = response.json().get('finance', {}).get('result', [])
            quotes = result[0].get('quotes', []) if result else []
            candidates = []
            for quote in quotes:
                try:
                    symbol = str(quote.get('symbol', '')).upper().strip()
                    price = float(quote.get('regularMarketPrice', 0) or 0)
                    change = float(quote.get('regularMarketChangePercent', 0) or 0)
                    volume = int(quote.get('regularMarketVolume', 0) or 0)
                    market_cap = float(quote.get('marketCap', 0) or 0)
                    if symbol and price > 0 and change >= 3:
                        candidates.append({
                            'symbol': symbol, 'price': price, 'change': change,
                            'volume': volume, 'market_cap': market_cap,
                            'timestamp': time.time(),
                        })
                except Exception:
                    continue
            candidates.sort(key=lambda item: item.get('change', 0), reverse=True)
            with state_lock:
                state['hot_watchlist'] = candidates[:100]
                state['hot_watchlist_timestamp'] = time.time()
                save_state()
            logger.info(f'[TopGainers] Discovery updated: {len(candidates[:100])} candidates')
        except Exception as error:
            logger.error(f'[TopGainers] discovery error: {error}')
        time.sleep(60)


def quick_jump_scanner():
    """
    ماسح فوري يفحص بيانات الشمعة 1 دقيقة كل 30 ثانية.
    يرسل تنبيه فوري لأي سهم يقفز 15%+ في آخر شمعة أو الشمعتين الأخيرتين.
    
    طريقة العمل:
    - آخر شمعة 1m: نسبة (close - open) / open >= 15% → قفزة خلال دقيقة
    - أو: (close[-1] - close[-2]) / close[-2] >= 15% → قفزة بين دقيقتين
    """
    _seen_jumps = {}  # {symbol: timestamp} — كاش داخلي للتنبيهات

    while True:
        try:
            phase = get_market_phase()
            if phase not in ("REGULAR", "PRE", "AFTER"):
                time.sleep(60)
                continue

            with state_lock:
                tickers = list(state.get("tickers", []))[:400]

            if not tickers:
                time.sleep(QUICK_JUMP_SCAN_INTERVAL)
                continue

            now_ts  = time.time()
            saudi_t = now_saudi().strftime("%I:%M:%S %p")
            est_t   = now_est().strftime("%I:%M:%S %p")

            for symbol in tickers:
                try:
                    # تحقق من Cooldown
                    last_sent = _seen_jumps.get(symbol, 0)
                    if now_ts - last_sent < QUICK_JUMP_COOLDOWN:
                        continue

                    # جلب بيانات 1 دقيقة (آخر ساعتين كافٍ)
                    df = cached_download(symbol, period="1d", interval="1m")
                    if df is None or df.empty or len(df) < 3:
                        continue

                    df.columns = [c.lower() for c in df.columns]
                    df = df.dropna(subset=['open', 'close', 'volume'])
                    if len(df) < 3:
                        continue

                    price    = float(df['close'].iloc[-1])
                    open_1m  = float(df['open'].iloc[-1])   # فتح الشمعة الحالية
                    close_2  = float(df['close'].iloc[-2])  # إغلاق الشمعة السابقة
                    vol_1m   = float(df['volume'].iloc[-1])

                    # فلتر السعر
                    if not (QUICK_JUMP_PRICE_MIN <= price <= QUICK_JUMP_PRICE_MAX):
                        continue

                    # فلتر الحجم (لا نريد تحركات على حجم صفر)
                    if vol_1m < QUICK_JUMP_MIN_VOL:
                        continue

                    # ── حساب القفزة ──
                    # طريقة 1: قفزة داخل الشمعة الحالية (نص دقيقة إلى دقيقة)
                    jump_incandle = (price - open_1m) / max(open_1m, 0.0001) * 100

                    # طريقة 2: قفزة بين شمعتين متتاليتين (إغلاق السابقة → إغلاق الحالية)
                    jump_2candles = (price - close_2) / max(close_2, 0.0001) * 100

                    best_jump = max(jump_incandle, jump_2candles)

                    if best_jump < QUICK_JUMP_MIN_GAIN_1M:
                        continue

                    # حساب RVOL
                    avg_vol = float(df['volume'].tail(20).mean()) if len(df) >= 20 else vol_1m
                    rvol    = vol_1m / max(avg_vol, 1)

                    # إغلاق أمس للمقارنة
                    prev_close_candidates = df[df.index.date < now_est().date()]
                    prev_close = float(prev_close_candidates['close'].iloc[-1]) if not prev_close_candidates.empty else close_2
                    day_gain   = (price - prev_close) / max(prev_close, 0.0001) * 100

                    # أعلى سعر اليوم
                    today_df   = df[df.index.date == now_est().date()]
                    day_high   = float(today_df['high'].max()) if not today_df.empty else price

                    # أهداف سريعة
                    tp1 = round(price * 1.10, 4)
                    tp2 = round(price * 1.20, 4)
                    sl  = round(price * 0.92, 4)

                    jump_type = "🕐 قفزة داخل الدقيقة" if jump_incandle >= QUICK_JUMP_MIN_GAIN_1M else "🕑 قفزة بين دقيقتين"

                    msg = (
                        f"🚨🚨 *QUICK JUMP +{best_jump:.0f}%: {symbol}* 🚨🚨\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"⏰ *{est_t} EST  |  {saudi_t} 🇸🇦*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"⚡ *{jump_type}*\n"
                        f"💰 السعر الحالي: *${price:.4f}*\n"
                        f"📈 القفزة السريعة: *+{best_jump:.1f}%*\n"
                        f"📊 ربح اليوم الكلي: *+{day_gain:.1f}%*\n"
                        f"🔝 أعلى سعر اليوم: ${day_high:.4f}\n"
                        f"🔥 الحجم (RVOL): *{rvol:.1f}x* | {int(vol_1m):,} سهم\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🎯 T1: ${tp1} (+10%) | T2: ${tp2} (+20%)\n"
                        f"🛑 وقف الخسارة: ${sl} (-8%)\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"⚡ *قفزة مفاجئة — تحقق من الأخبار فوراً!*\n"
                        f"⚠️ إشارة وليست توصية"
                    )
                    send_telegram(msg)
                    logger.info(f"🚨 QUICK JUMP {symbol} +{best_jump:.1f}% in 1m | vol={int(vol_1m):,}")

                    _seen_jumps[symbol] = now_ts
                    with state_lock:
                        state.setdefault("seen_signals", {})[f"quickjump_{symbol}"] = now_ts
                        save_state()

                    time.sleep(1.5)

                except Exception as sym_err:
                    logger.debug(f"Quick jump {symbol}: {sym_err}")
                    continue

                time.sleep(0.1)  # سريع جداً — 100ms بين كل سهم

        except Exception as e:
            logger.error(f"Quick Jump Scanner error: {e}")

        time.sleep(QUICK_JUMP_SCAN_INTERVAL)


# ================================================================
# 🌊 SECOND WAVE & EARLY BREAKOUT SCANNER
# استراتيجية صيد الموجة الثانية والانفجار المبكر
# ================================================================

def detect_rejection_candles(df):
    """كشف شموع الرفض الصاعدة والهابطة."""
    if len(df) < 5:
        return [], []
    bullish_rejections, bearish_rejections = [], []
    for i in range(1, len(df)):
        candle = df.iloc[i]
        body = abs(float(candle['close']) - float(candle['open']))
        range_ = float(candle['high']) - float(candle['low'])
        if range_ == 0 or body == 0:
            continue
        upper_wick = float(candle['high']) - max(float(candle['open']), float(candle['close']))
        lower_wick = min(float(candle['open']), float(candle['close'])) - float(candle['low'])
        if lower_wick >= 2 * body:
            bullish_rejections.append({'price': float(candle['close']), 'level': float(candle['low']), 'ratio': round(lower_wick / body, 2)})
        if upper_wick >= 2 * body:
            bearish_rejections.append({'price': float(candle['close']), 'level': float(candle['high']), 'ratio': round(upper_wick / body, 2)})
    return bullish_rejections, bearish_rejections


def detect_engulfing(df):
    """كشف الابتلاع الصاعد بمنطق يدوي موثوق، مع دعم pandas-ta إن توفر."""
    if len(df) < 3:
        return None
    if ta is not None:
        try:
            pattern = ta.cdl_pattern(df['open'], df['high'], df['low'], df['close'], name='engulfing')
            if pattern is not None and not pattern.empty:
                values = pattern.iloc[:, 0]
                hits = df[values > 0]
                if not hits.empty:
                    last = hits.iloc[-1]
                    return {'pattern': 'ENGULFING_BULLISH', 'type': '🔄 Bullish Engulfing', 'price': float(last['close']), 'index': last.name, 'bullish': True}
        except Exception:
            pass
    for i in range(1, len(df)):
        prev, curr = df.iloc[i - 1], df.iloc[i]
        if (curr['close'] > curr['open'] and prev['close'] < prev['open'] and
                curr['open'] <= prev['close'] and curr['close'] >= prev['open']):
                return {'pattern': 'ENGULFING_BULLISH', 'type': '🔄 Bullish Engulfing', 'price': float(curr['close']), 'index': df.index[i], 'bullish': True}
    return None


def detect_support_resistance(df, lookback=20):
    """كشف أقرب مستويات الدعم والمقاومة."""
    if len(df) < lookback:
        return None
    recent = df.tail(lookback)
    resistance, support = float(recent['high'].max()), float(recent['low'].min())
    current = float(df['close'].iloc[-1])
    return {
        'resistance': resistance,
        'support': support,
        'near_resistance': (resistance - current) / max(current, 1e-9) < 0.05,
        'near_support': (current - support) / max(current, 1e-9) < 0.05,
    }


def detect_breakout_setup(df_daily, df_4h):
    """تقييم الهبوط الحاد، ارتداد السعر، EMA50، والشموع الداعمة."""
    if len(df_daily) < 60 or len(df_4h) < 20:
        return None
    daily = df_daily.copy()
    result = {'signals': [], 'score': 0, 'entry': None, 'stop': None, 'targets': []}
    price = float(daily['close'].iloc[-1])
    monthly_low = float(daily['low'].tail(30).min())
    monthly_high = float(daily['high'].tail(30).max())
    drop = (monthly_high - monthly_low) / max(monthly_high, 1e-9) * 100
    recovery = (price - monthly_low) / max(monthly_low, 1e-9) * 100
    if drop >= 60 and 7 <= recovery <= 10:
        result['signals'].append(f"ارتداد {recovery:.1f}% بعد هبوط {drop:.1f}%")
        result['score'] += 25
    if ta is not None:
        try:
            daily['ema50'] = ta.ema(daily['close'], length=50)
        except Exception:
            daily['ema50'] = daily['close'].ewm(span=50, adjust=False).mean()
    else:
        daily['ema50'] = daily['close'].ewm(span=50, adjust=False).mean()
    if len(daily) > 90 and pd.notna(daily['ema50'].iloc[-1]) and price > daily['ema50'].iloc[-1]:
        before = daily.iloc[-90:-1]
        if (before['close'] < before['ema50']).all():
            result['signals'].append("اختراق EMA50 لأول مرة منذ 3 شهور")
            result['score'] += 30
    bullish_rejections, _ = detect_rejection_candles(df_4h)
    engulfing = detect_engulfing(df_4h)
    if bullish_rejections:
        last_rej = bullish_rejections[-1]
        result['signals'].append(f"شمعة رفض صاعد عند ${last_rej['price']:.2f}")
        result['score'] += 25
        result['entry'] = last_rej['price'] * 1.01
        result['stop'] = last_rej['level'] * 0.98
    if engulfing:
        result['signals'].append(f"شمعة ابتلاع صاعدة عند ${engulfing['price']:.2f}")
        result['score'] += 20
        if result['entry'] is None:
            result['entry'], result['stop'] = engulfing['price'] * 1.01, engulfing['price'] * 0.97
    levels = detect_support_resistance(daily)
    if levels and levels['near_support']:
        result['signals'].append(f"قرب الدعم ${levels['support']:.2f}")
        result['score'] += 10
    if result['score'] < 40:
        return None
    if result['entry'] is None:
        result['entry'], result['stop'] = price, price * 0.95
    result['targets'] = [round(result['entry'] * m, 2) for m in (1.15, 1.30, 1.50)]
    return result


def early_breakout_scanner():
    """ماسح الانفجار المبكر، يعمل كل 30 دقيقة على قائمة الأسهم الحالية."""
    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(1800)
                continue
            with state_lock:
                tickers = list(state.get("tickers", []))[:300]
            for symbol in tickers:
                try:
                    signal_key = f"early_{symbol}_{now_est().date()}"
                    with state_lock:
                        if signal_key in state.get("seen_signals", {}):
                            continue
                    df_daily = cached_download(symbol, period="120d", interval="1d")
                    if df_daily.empty or len(df_daily) < 60:
                        continue
                    df_daily.columns = [str(c).lower() for c in df_daily.columns]
                    df_4h = cached_download(symbol, period="30d", interval="4h")
                    if df_4h.empty or len(df_4h) < 20:
                        df_4h = cached_download(symbol, period="30d", interval="1h")
                    if df_4h.empty or len(df_4h) < 20:
                        continue
                    df_4h.columns = [str(c).lower() for c in df_4h.columns]
                    result = detect_breakout_setup(df_daily, df_4h)
                    if not result:
                        continue
                    msg = (f"🚀 *انفجار مبكر: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                           f"💰 السعر الحالي: *${float(df_daily['close'].iloc[-1]):.2f}*\n"
                           f"📊 درجة القوة: *{result['score']}/100*\n━━━━━━━━━━━━━━━━\n"
                           f"📊 *إشارات الدخول:*\n")
                    msg += ''.join(f"  ✅ {s}\n" for s in result['signals'])
                    msg += (f"━━━━━━━━━━━━━━━━\n🎯 *سعر الدخول:* ${result['entry']:.2f}\n"
                            f"🛑 *وقف الخسارة:* ${result['stop']:.2f}\n━━━━━━━━━━━━━━━━\n"
                            f"🎯 *الأهداف:*\n  • T1: ${result['targets'][0]} (+15%)\n"
                            f"  • T2: ${result['targets'][1]} (+30%)\n  • T3: ${result['targets'][2]} (+50%)\n"
                            f"━━━━━━━━━━━━━━━━\n⚠️ *إشارة وليست توصية - إدارة المخاطر إجبارية*")
                    send_telegram(msg)
                    with state_lock:
                        state.setdefault("seen_signals", {})[signal_key] = time.time()
                        save_state()
                    logger.info(f"Early breakout: {symbol} score={result['score']}")
                except Exception as e:
                    logger.debug(f"Early breakout error {symbol}: {e}")
                time.sleep(0.3)
        except Exception as e:
            logger.error(f"Early breakout scanner error: {e}")
        time.sleep(1800)




def detect_sharp_drop_recovery(df_daily):
    """يكتشف هبوطًا حادًا ثم ارتدادًا محدودًا قرب مقاومة، كمرشح مراقبة فقط."""
    if df_daily is None or len(df_daily) < 30:
        return None
    df = df_daily.copy()
    df.columns = [str(c).lower() for c in df.columns]
    high_30 = float(df['high'].tail(30).max())
    low_30 = float(df['low'].tail(30).min())
    current = float(df['close'].iloc[-1])
    if high_30 <= 0 or low_30 <= 0:
        return None
    drop_pct = (high_30 - low_30) / high_30 * 100
    recovery_pct = (current - low_30) / low_30 * 100
    resistance = float(df['high'].tail(10).max())
    near_resistance = (resistance - current) / max(current, 1e-9) * 100
    if drop_pct >= 60 and 7 <= recovery_pct <= 15 and near_resistance <= 3:
        return {'pattern': 'SHARP_DROP_RECOVERY', 'drop_pct': round(drop_pct, 1), 'recovery_pct': round(recovery_pct, 1), 'resistance': round(resistance, 4), 'current': round(current, 4), 'signal': 'اختراق المقاومة'}
    return None


def detect_reverse_split_support(df_daily):
    """مرشح تقريبي للقفزة الناتجة عن تقسيم عكسي مع اختبار دعم؛ يحتاج تحققًا رسميًا."""
    if df_daily is None or len(df_daily) < 60:
        return None
    df = df_daily.copy()
    df.columns = [str(c).lower() for c in df.columns]
    prices = pd.to_numeric(df['close'], errors='coerce').dropna().tolist()
    split_indexes = [i for i in range(1, len(prices)) if prices[i] > prices[i - 1] * 3]
    if not split_indexes:
        return None
    after_split = df.iloc[split_indexes[-1]:]
    if len(after_split) < 10:
        return None
    support = float(after_split['low'].min())
    if support <= 0:
        return None
    touches = sum(abs(float(row['close']) - support) / support < 0.02 for _, row in after_split.iloc[:-5].iterrows())
    current = float(df['close'].iloc[-1])
    if touches >= 2 and current > support * 1.02:
        return {'pattern': 'REVERSE_SPLIT_SUPPORT', 'support': round(support, 4), 'touches': touches, 'current': round(current, 4), 'signal': 'اختبار دعم متكرر — تحقق من الشركة'}
    return None


def detect_falling_wedge(df_daily):
    """مرشح محافظ لوتد هابط مع اختراق آخر قمة محلية."""
    if df_daily is None or len(df_daily) < 30:
        return None
    df = df_daily.copy()
    df.columns = [str(c).lower() for c in df.columns]
    recent = df.tail(30)
    highs, lows = recent['high'].astype(float).to_numpy(), recent['low'].astype(float).to_numpy()
    peak_slope = (highs[-1] - highs[0]) / 29
    trough_slope = (lows[-1] - lows[0]) / 29
    current = float(df['close'].iloc[-1])
    last_high = float(highs[-2])
    narrowing = peak_slope < 0 and trough_slope < 0 and trough_slope < peak_slope
    if narrowing and current > last_high * 1.01:
        return {'pattern': 'FALLING_WEDGE_BREAKOUT', 'wedge_high': round(last_high, 4), 'current': round(current, 4), 'signal': 'اختراق وتد هابط'}
    return None


def detect_ema50_breakout(df_daily):
    """يكتشف أول إغلاق فوق EMA50 بعد بقاء السعر تحته لفترة طويلة."""
    if df_daily is None or len(df_daily) < 90:
        return None
    df = df_daily.copy()
    df.columns = [str(c).lower() for c in df.columns]
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    last_90 = df.tail(90)
    current, ema50 = float(df['close'].iloc[-1]), float(df['ema50'].iloc[-1])
    if (last_90['close'] < last_90['ema50']).all() and current > ema50 and float(df['close'].iloc[-2]) < float(df['ema50'].iloc[-2]):
        return {'pattern': 'EMA50_BREAKOUT', 'ema50': round(ema50, 4), 'current': round(current, 4), 'signal': 'أول اختراق EMA50'}
    return None


def detect_near_ema_breakout(df_daily):
    """يكتشف سهمًا قريبًا من EMA20/EMA50 وتحت EMA100 كمرشح مبكر."""
    if df_daily is None or len(df_daily) < 60:
        return None
    df = df_daily.copy()
    df.columns = [str(c).lower() for c in df.columns]
    for span in (20, 50, 100):
        df[f'ema{span}'] = df['close'].ewm(span=span, adjust=False).mean()
    current = float(df['close'].iloc[-1])
    ema20, ema50, ema100 = [float(df[f'ema{x}'].iloc[-1]) for x in (20, 50, 100)]
    candidates = [('EMA50', ema50), ('EMA20', ema20)]
    target_ema, target = min(candidates, key=lambda pair: abs(current - pair[1]) / max(pair[1], 1e-9))
    distance = abs(current - target) / max(target, 1e-9)
    below_ema100 = current < ema100 * 0.90
    if distance <= 0.025 and below_ema100:
        avg_volume = max(float(df['volume'].tail(20).mean()), 1.0)
        rvol = get_unified_rvol(df, phase=get_market_phase())
        return {'pattern': 'NEAR_EMA_BREAKOUT', 'current': round(current, 4), 'target_ema': target_ema, 'target_price': round(target, 4), 'distance_pct': round(distance * 100, 2), 'rvol': rvol, 'signal': f'قرب {target_ema} بنسبة {distance * 100:.2f}%'}
    return None


def early_hunter_scanner():
    """يجمع نماذج الصيد المبكر على البيانات اليومية كل ساعة، بتنبيه Paper فقط."""
    while True:
        try:
            if get_market_phase() == 'CLOSED':
                time.sleep(1800)
                continue
            with state_lock:
                tickers = list(state.get('full_tickers', state.get('tickers', [])))[:500]
            for symbol in tickers:
                try:
                    key = f'early_hunter_{symbol}_{now_est().date()}'
                    with state_lock:
                        if key in state.get('seen_signals', {}):
                            continue
                    df = cached_download(symbol, period='120d', interval='1d')
                    if df.empty or len(df) < 90:
                        continue
                    patterns = [detect_sharp_drop_recovery(df), detect_reverse_split_support(df), detect_falling_wedge(df), detect_ema50_breakout(df), detect_near_ema_breakout(df)]
                    patterns = [p for p in patterns if p]
                    if not patterns:
                        continue
                    msg = f'🎯 *EARLY HUNTER: {symbol}*\\n━━━━━━━━━━━━━━━━\\n'
                    for pattern in patterns:
                        msg += f"📊 {pattern['pattern']} | 💰 ${pattern['current']:.4f}\\n💡 {pattern['signal']}\\n━━━━━━━━━━━━━━━━\\n"
                    msg += '⚠️ مرشح مراقبة فقط — تحقق من الخبر والسيولة ولا تطارد الحركة'
                    send_telegram(msg)
                    if AI_PROCESS_ENABLED:
                        threading.Thread(target=final_trade_recommendation, args=(symbol, 10, 'EARLY_BREAKOUT', False, True), daemon=True).start()
                    with state_lock:
                        state.setdefault('seen_signals', {})[key] = time.time()
                        save_state()
                except Exception as error:
                    logger.debug(f'Early hunter error {symbol}: {error}')
                time.sleep(0.2)
        except Exception as error:
            logger.error(f'Early hunter scanner error: {error}')
        time.sleep(3600)


def second_wave_scanner():
    """يبحث عن تصحيح بعد موجة أولى ثم رفض وابتلاع صاعد على فريم 4 ساعات/ساعة."""
    while True:
        try:
            if get_market_phase() == 'CLOSED':
                time.sleep(1800)
                continue
            with state_lock:
                tickers = list(state.get('full_tickers', state.get('tickers', [])))[:600]
            for symbol in tickers:
                try:
                    key = f'second_wave_{symbol}'
                    with state_lock:
                        if time.time() - float(state.get('seen_signals', {}).get(key, 0) or 0) < 43200:
                            continue
                    df = cached_download(symbol, period='60d', interval='1h')
                    if df.empty or len(df) < 30:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
                    current = float(df['close'].iloc[-1])
                    ema50 = float(df['ema50'].iloc[-1])
                    first_wave_high = float(df['high'].tail(60).max())
                    correction = current <= first_wave_high * 0.85
                    bullish_rejections, _ = detect_rejection_candles(df)
                    engulfing = detect_engulfing(df)
                    avg_vol = max(float(df['volume'].tail(20).mean()), 1.0)
                    volume_confirmed = float(df['volume'].iloc[-1]) > avg_vol * 1.3
                    if not (correction and current > ema50 and bullish_rejections and engulfing and volume_confirmed):
                        continue
                    entry = float(engulfing['price']) * 1.005
                    stop = min(float(engulfing.get('level', current)), float(df['low'].iloc[-2])) * 0.99
                    if stop >= entry:
                        continue
                    t1, t2 = entry * 1.15, max(float(df['high'].tail(20).max()) * 1.05, entry * 1.30)
                    msg = (f'🚀 *SECOND WAVE WATCH: {symbol}*\\n━━━━━━━━━━━━━━━━\\n'
                           f'💰 السعر: ${current:.4f} | EMA50: ${ema50:.4f}\\n'
                           f'✅ تصحيح بعد موجة أولى | ✅ شموع رفض | ✅ ابتلاع صاعد | ✅ حجم مؤكد\\n'
                           f'🎯 تأكيد فوق: ${entry:.4f}\\n🛑 وقف مراقبة: ${stop:.4f}\\n'
                           f'🎯 T1: ${t1:.4f} | T2: ${t2:.4f}\\n'
                           f'⚠️ Paper Trading — لا تنفيذ تلقائي')
                    send_telegram(msg)
                    if AI_PROCESS_ENABLED:
                        threading.Thread(target=final_trade_recommendation, args=(symbol, 10, 'SECOND_WAVE', False, True), daemon=True).start()
                    with state_lock:
                        state.setdefault('seen_signals', {})[key] = time.time()
                        save_state()
                except Exception as error:
                    logger.debug(f'Second wave error {symbol}: {error}')
                time.sleep(0.2)
        except Exception as error:
            logger.error(f'Second wave scanner error: {error}')
        time.sleep(3600)


def after_hours_breakout_scanner():
    """يراقب الأسهم القريبة من EMA20/EMA50 في PRE وAFTER، بتنبيه فقط كل 5 دقائق."""
    while True:
        try:
            phase = get_market_phase()
            if phase not in ('PRE', 'AFTER'):
                time.sleep(60)
                continue
            with state_lock:
                tickers = list(state.get('full_tickers', state.get('tickers', [])))[:500]
            for symbol in tickers:
                try:
                    key = f'ah_ema_{symbol}_{now_est().date()}'
                    with state_lock:
                        if key in state.get('seen_signals', {}):
                            continue
                    df_ext = cached_download(symbol, period='2d', interval='1m', prepost=True)
                    df_daily = cached_download(symbol, period='120d', interval='1d')
                    if df_ext.empty or len(df_ext) < 5 or df_daily.empty or len(df_daily) < 60:
                        continue
                    df_ext.columns = [str(c).lower() for c in df_ext.columns]
                    df_daily.columns = [str(c).lower() for c in df_daily.columns]
                    current = float(df_ext['close'].iloc[-1])
                    near = detect_near_ema_breakout(df_daily)
                    if current < 0.5 or current > 30 or not near:
                        continue
                    regular = df_ext[df_ext.index.hour < 16]
                    if regular.empty:
                        continue
                    regular_close = float(regular['close'].iloc[-1])
                    change = (current - regular_close) / max(regular_close, 1e-9) * 100
                    if abs(change) < 3 and near.get('rvol', 0) < 1.2:
                        continue
                    target = float(near['target_price'])
                    entry = target * 1.005
                    stop = target * 0.93
                    msg = (f'🎯 *{phase} EMA BREAKOUT WATCH: {symbol}*\\n━━━━━━━━━━━━━━━━\\n'
                           f'💰 السعر: ${current:.4f} | {near["target_ema"]}: ${target:.4f}\\n'
                           f'📈 تغير الجلسة الممتدة: {change:+.1f}% | RVOL: {near.get("rvol", 0):.1f}x\\n'
                           f'🚨 شرط التأكيد: ${entry:.4f}\\n🛑 وقف مراقبة: ${stop:.4f}\\n'
                           f'⚠️ تنبيه Paper فقط — تحقق من السيولة والسبريد')
                    send_telegram(msg)
                    with state_lock:
                        state.setdefault('seen_signals', {})[key] = time.time()
                        save_state()
                except Exception as error:
                    logger.debug(f'After-hours EMA error {symbol}: {error}')
                time.sleep(0.15)
        except Exception as error:
            logger.error(f'After-hours breakout scanner error: {error}')
        time.sleep(300)

# ================================================================
# 🎯 PRE-BREAKOUT CONFIRMATION SYSTEM
# نظام اكتشاف التماسك والاختراق قبل الانفجار
# ================================================================
PRE_BREAKOUT_WEIGHTS = {
    "news_impact": 25,
    "rvol": 20,
    "volume_spike": 15,
    "vwap_above": 15,
    "higher_highs": 10,
    "consolidation": 10,
    "low_float": 5,
}
DILUTION_KEYWORDS = [
    "offering", "registered direct", "atm", "warrants",
    "convertible notes", "dilution", "resale"
]
PRE_BREAKOUT_SCAN_INTERVAL = 30
PRE_BREAKOUT_ALERT_SCORE = 70
PRE_BREAKOUT_CONFIRM_SCORE = 80
PRE_BREAKOUT_COOLDOWN = 900


def has_dilution_risk(text):
    """يمنع إشارات ما قبل الانفجار عند وجود مؤشرات طرح أو تخفيف."""
    lowered = (text or "").lower()
    return any(keyword in lowered for keyword in DILUTION_KEYWORDS)


def vwap_confirmation(df):
    """يتحقق من استعادة VWAP مع اختراق قمة محلية وحجم مؤكد."""
    if df.empty or len(df) < 21:
        return False
    typical = (df['high'] + df['low'] + df['close']) / 3
    cumulative_value = (typical * df['volume']).cumsum()
    cumulative_volume = df['volume'].cumsum().replace(0, np.nan)
    vwap = cumulative_value / cumulative_volume
    current = df.iloc[-1]
    previous_high = df['high'].iloc[-21:-1].max()
    average_volume = df['volume'].tail(20).mean()
    return bool(
        current['close'] > vwap.iloc[-1] and
        current['close'] > previous_high and
        current['volume'] > max(average_volume, 1) * 1.5
    )


def calculate_pre_breakout_score(df, news_impact=0):
    """يحسب درجة مرشح ما قبل الانفجار من السعر والحجم والبنية الفنية."""
    if df.empty or len(df) < 21:
        return {"score": 0, "rvol": 0.0, "vwap_above": False, "confirmed": False, "details": []}
    work = df.copy()
    for column in ('open', 'high', 'low', 'close', 'volume'):
        work[column] = pd.to_numeric(work[column], errors='coerce')
    work = work.dropna(subset=['high', 'low', 'close', 'volume'])
    if len(work) < 21:
        return {"score": 0, "rvol": 0.0, "vwap_above": False, "confirmed": False, "details": []}
    average_volume = max(float(work['volume'].tail(20).mean()), 1.0)
    last_volume = float(work['volume'].iloc[-1])
    rvol = last_volume / average_volume
    typical = (work['high'] + work['low'] + work['close']) / 3
    vwap = (typical * work['volume']).cumsum() / work['volume'].cumsum().replace(0, np.nan)
    last_close = float(work['close'].iloc[-1])
    vwap_above = last_close > float(vwap.iloc[-1])
    previous_high = float(work['high'].iloc[-5:-1].max())
    higher_highs = float(work['high'].iloc[-1]) > previous_high
    range_pct = (float(work['high'].tail(10).max()) - float(work['low'].tail(10).min())) / max(last_close, 1e-9) * 100
    consolidation = 2.0 <= range_pct <= 12.0
    score = min(max(int(news_impact or 0), 0), 25)
    details = []
    if news_impact:
        details.append(f"خبر مؤثر +{min(int(news_impact), 25)}")
    if rvol >= 3:
        score += 20; details.append(f"RVOL {rvol:.1f}x")
    elif rvol >= 2:
        score += 10; details.append(f"RVOL {rvol:.1f}x")
    if last_volume > average_volume * 2:
        score += 15; details.append("حجم انفجاري")
    if vwap_above:
        score += 15; details.append("فوق VWAP")
    if higher_highs:
        score += 10; details.append("قمة أعلى")
    if consolidation:
        score += 10; details.append(f"تماسك {range_pct:.1f}%")
    price = last_close
    if is_low_float(price, last_volume):
        score += 5; details.append("Low Float محتمل")
    return {
        "score": min(score, 100),
        "rvol": rvol,
        "vwap_above": vwap_above,
        "confirmed": vwap_confirmation(work),
        "details": details,
    }


def pre_breakout_confirmation_scanner():
    """يرسل تنبيه مراقبة عند 70+ وتأكيدًا عند 80+ مع اختراق VWAP."""
    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(PRE_BREAKOUT_SCAN_INTERVAL)
                continue
            with state_lock:
                tickers = list(state.get("tickers", []))[:300]
            for symbol in tickers:
                try:
                    df = cached_download(symbol, period="1d", interval="5m")
                    if df.empty or len(df) < 25:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    price = float(df['close'].iloc[-1])
                    if not 0.3 <= price <= 30:
                        continue
                    with state_lock:
                        news_record = state.get("pre_breakout_news_impact", {}).get(symbol, {})
                    news_impact = 0
                    if isinstance(news_record, dict) and time.time() - float(news_record.get("time", 0) or 0) <= 1800:
                        news_impact = int(news_record.get("impact", 0) or 0)
                    score_data = calculate_pre_breakout_score(df, news_impact=news_impact)
                    score = score_data["score"]
                    if score < PRE_BREAKOUT_ALERT_SCORE:
                        continue
                    alert_type = "confirm" if score >= PRE_BREAKOUT_CONFIRM_SCORE and score_data["confirmed"] else "watch"
                    signal_key = f"pre_breakout_{alert_type}_{symbol}"
                    with state_lock:
                        seen = state.setdefault("seen_signals", {})
                        last_time = float(seen.get(signal_key, 0) or 0)
                        if time.time() - last_time < PRE_BREAKOUT_COOLDOWN:
                            continue
                    confirmed = score >= PRE_BREAKOUT_CONFIRM_SCORE and score_data["confirmed"]
                    status = "✅ تأكيد اختراق" if confirmed else "👀 مراقبة مبكرة"
                    level = "تأكيد" if confirmed else "مراقبة"
                    msg = (f"🎯 *Pre-Breakout {level}: {symbol}*\n"
                           f"━━━━━━━━━━━━━━━━\n{status}\n"
                           f"💰 السعر: *${price:.4f}*\n"
                           f"📊 الدرجة: *{score}/100*\n"
                           f"🔥 RVOL: *{score_data['rvol']:.1f}x*\n"
                           f"📌 {' | '.join(score_data['details'])}\n"
                           f"━━━━━━━━━━━━━━━━\n"
                           f"⚠️ إشارة مراقبة وليست توصية — تحقق من الخبر والسيولة وإدارة المخاطر")
                    send_telegram(msg)
                    with state_lock:
                        state["seen_signals"][signal_key] = time.time()
                        save_state()
                except Exception as symbol_error:
                    logger.debug(f"Pre-breakout error {symbol}: {symbol_error}")
                time.sleep(0.2)
        except Exception as error:
            logger.error(f"Pre-breakout scanner error: {error}")
        time.sleep(PRE_BREAKOUT_SCAN_INTERVAL)


# ================================================================
# 🤖 AI PROCESS SYMBOL & AI SCANNER
# طبقة تحليل إضافية؛ لا تفتح صفقات تلقائيًا بقرار AI وحده
# ================================================================

def ai_process_symbol(symbol, news_impact=0):
    """تحليل شامل للسهم عبر Groq وإرسال تنبيه فقط عند الثقة الكافية."""
    if not AI_PROCESS_ENABLED or not OPENAI_AVAILABLE or client is None:
        return None
    try:
        df = cached_download(symbol, period="5d", interval="5m")
        if df.empty or len(df) < 20:
            return None
        df.columns = [str(c).lower() for c in df.columns]
        df = compute_indicators(df)
        price = float(df['close'].iloc[-1])
        if not 0.3 <= price <= 30:
            return None
        rvol = get_unified_rvol(df)
        atr = float(calculate_atr(df))
        vwap = float(df['vwap'].iloc[-1]) if 'vwap' in df and pd.notna(df['vwap'].iloc[-1]) else price
        rsi = float(df['rsi'].iloc[-1]) if 'rsi' in df and pd.notna(df['rsi'].iloc[-1]) else 50.0
        high_10 = float(df['high'].tail(10).max())
        low_10 = float(df['low'].tail(10).min())
        range_pct = (high_10 - low_10) / max(price, 1e-9) * 100
        closes = df['close'].tail(5).to_numpy()
        momentum = (float(closes[-1]) - float(closes[0])) / max(float(closes[0]), 1e-9) * 100
        average_volume = max(float(df['volume'].tail(20).mean()), 1.0)
        volume_spike = float(df['volume'].iloc[-1]) > average_volume * 2
        if rvol < 1.5 and not volume_spike and news_impact <= 0:
            return None
        if not can_send_ai_request():
            logger.info(f"AI request limit reached; skipping {symbol}")
            return None
        prompt = f"""
حلل السهم {symbol} كمحلل زخم، ولا تفترض ضمان الربح.
السعر: ${price:.4f}; أعلى/أدنى 5 أيام: ${float(df['high'].max()):.4f}/${float(df['low'].min()):.4f}
RVOL: {rvol:.2f}x; ATR: ${atr:.4f}; RSI: {rsi:.1f}; VWAP: ${vwap:.4f}
فوق VWAP: {price > vwap}; التماسك: {range_pct:.1f}%; الزخم: {momentum:+.1f}%; حجم مضاعف: {volume_spike}
تأثير خبر سابق إن وجد: {news_impact}/25
أجب JSON فقط:
{{
  "decision": "BUY" | "HOLD" | "AVOID",
  "confidence": 0-100,
  "target_1": 0.0,
  "target_2": 0.0,
  "stop_loss": 0.0,
  "risk_level": "LOW" | "MEDIUM" | "HIGH",
  "reason_ar": "سبب مختصر بالعربية",
  "key_factors": ["عامل1", "عامل2"]
}}
"""
        response = groq_chat_completion(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "You are a cautious penny-stock momentum analyst. Respond in JSON only. Never guarantee profit."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=350,
            timeout=10,
        )
        result = json.loads(response.choices[0].message.content)
        confidence = max(0, min(int(result.get('confidence', 0) or 0), 100))
        decision = str(result.get('decision', 'HOLD')).upper()
        if decision == 'BUY' and confidence >= AI_CONFIDENCE_THRESHOLD:
            target_1 = float(result.get('target_1') or price * 1.10)
            stop_loss = float(result.get('stop_loss') or price * 0.95)
            factors = ', '.join(str(x) for x in result.get('key_factors', [])[:3])
            msg = (f"🤖 *AI WATCH: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                   f"🧠 الثقة: *{confidence}/100*\n📊 المخاطرة: *{result.get('risk_level', 'MEDIUM')}*\n"
                   f"💰 السعر: *${price:.4f}*\n🎯 الهدف المقترح: *${target_1:.4f}*\n"
                   f"🛑 الوقف المقترح: *${stop_loss:.4f}*\n"
                   f"💡 {result.get('reason_ar', '')}\n🔑 {factors}\n"
                   f"⚠️ *تنبيه تحليلي فقط؛ لا تنفيذ تلقائي*")
            send_telegram(msg)
        return result
    except Exception as error:
        logger.warning(f"AI Process Symbol error {symbol}: {error}")
        return None


def ai_scanner_loop():
    """يفحص مرشحين محدودين كل خمس دقائق ويرسل تحليلات AI دون تنفيذ صفقات."""
    last_scan = {}
    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(AI_SCANNER_INTERVAL)
                continue
            with state_lock:
                tickers = list(state.get("tickers", []))[:200]
            candidates = []
            for symbol in tickers:
                try:
                    df = cached_download(symbol, period="1d", interval="5m")
                    if df.empty or len(df) < 20:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    price = float(df['close'].iloc[-1])
                    if not 0.5 <= price <= 30:
                        continue
                    rvol = get_unified_rvol(df)
                    gain = (price - float(df['open'].iloc[0])) / max(float(df['open'].iloc[0]), 1e-9) * 100
                    if rvol >= 1.5 and gain >= 3:
                        candidates.append((symbol, rvol))
                except Exception:
                    continue
                time.sleep(0.05)
            candidates.sort(key=lambda item: item[1], reverse=True)
            for symbol, _ in candidates[:AI_MAX_CANDIDATES]:
                if time.time() - last_scan.get(symbol, 0) < 3600:
                    continue
                last_scan[symbol] = time.time()
                threading.Thread(target=ai_process_symbol, args=(symbol, 0), daemon=True).start()
                time.sleep(1)
            gc.collect()
        except Exception as error:
            logger.error(f"AI Scanner error: {error}")
        time.sleep(AI_SCANNER_INTERVAL)


# ================================================================
# 💣 PUMP & DUMP HUNTER
# كشف خطر التلاعب المحتمل — تنبيه فقط دون تنفيذ تلقائي
# ================================================================
PUMP_DETECTION_SETTINGS = {
    "rvol_threshold": 5.0,
    "volume_spike": 3.0,
    "small_cap_max": 5.0,
    "scan_interval": 30,
    "cooldown": 1800,
}


def detect_pre_pump_patterns(df_1m, df_5m):
    """يحسب أنماط التراكم والضغط السعري دون ادعاء التنبؤ المؤكد."""
    patterns, score = [], 0
    if len(df_1m) >= 10:
        volumes = pd.to_numeric(df_1m['volume'].tail(10), errors='coerce').fillna(0)
        if volumes.iloc[-1] > max(volumes.iloc[0], 1) * 2:
            score += 20; patterns.append("حجم متزايد تدريجيًا")
    if len(df_5m) >= 15:
        highs, lows = df_5m['high'].tail(10), df_5m['low'].tail(10)
        if highs.iloc[-1] > highs.iloc[-5] and lows.iloc[-1] > lows.iloc[-5]:
            score += 25; patterns.append("قمم وقيعان صاعدة")
    if len(df_1m) >= 5:
        gaps = []
        for i in range(1, 5):
            previous_close = float(df_1m['close'].iloc[-i-1])
            if previous_close > 0:
                gap = (float(df_1m['open'].iloc[-i]) - previous_close) / previous_close * 100
                if gap > 1:
                    gaps.append(gap)
        if len(gaps) >= 3:
            score += 25; patterns.append(f"فجوات صغيرة متتالية ({len(gaps)})")
    if len(df_5m) >= 20:
        atr = float(calculate_atr(df_5m))
        volatility = float(df_5m['high'].tail(20).std())
        average_volume = max(float(df_5m['volume'].tail(20).mean()), 1)
        if volatility > 0 and atr < volatility * 0.5 and float(df_5m['volume'].iloc[-1]) / average_volume > 2:
            score += 30; patterns.append("ضغط سعري مع ارتفاع الحجم")
    return min(score, 100), patterns


def calculate_pump_entry_exit(df_1m, df_5m, current_price):
    """حساب مستويات مشروطة من VWAP وATR والقمم المحلية."""
    typical = (df_5m['high'] + df_5m['low'] + df_5m['close']) / 3
    vwap = (typical * df_5m['volume']).cumsum() / df_5m['volume'].cumsum().replace(0, np.nan)
    atr = max(float(calculate_atr(df_5m)), current_price * 0.02)
    recent_high = float(df_5m['high'].tail(5).max())
    recent_low = float(df_5m['low'].tail(5).min())
    confirm_entry = max(recent_high * 1.005, float(vwap.iloc[-1]))
    stop_loss = max(recent_low - atr, current_price * 0.94)
    target_1 = current_price + atr * 2
    target_2 = current_price + atr * 3
    return {
        "early_entry": round(recent_low * 1.005, 4),
        "confirm_entry": round(confirm_entry, 4),
        "target_1": round(target_1, 4),
        "target_2": round(target_2, 4),
        "stop_loss": round(stop_loss, 4),
        "vwap": float(vwap.iloc[-1]),
    }


# ================================================================
# 🧠 SMART AI CONFIRMATION — تحليل محدود للإشارات القوية فقط
# ================================================================
SMART_AI_ENABLED = True
SMART_AI_MIN_SCORE = 75
SMART_AI_MIN_RVOL = 3.0
SMART_AI_COOLDOWN = 1800
_smart_ai_cache = {}
_smart_ai_lock = threading.Lock()


def _smart_pattern_names(patterns):
    names = []
    for item in patterns or []:
        if isinstance(item, dict):
            names.append(str(item.get('pattern') or item.get('type') or '').upper())
        else:
            names.append(str(item).upper())
    return names


def should_use_smart_ai(symbol, score, rvol, patterns, df=None):
    """مصفاة رخيصة: لا ترسل إلى Groq إلا عند اجتماع score وRVOL ونمط قوي وترند داعم."""
    del symbol
    strong = {'HAMMER', 'ENGULFING_BULLISH', 'MORNING_STAR', 'DOUBLE_BOTTOM', 'INVERSE_HEAD_SHOULDERS', 'CUP_AND_HANDLE'}
    if not SMART_AI_ENABLED or float(score or 0) < SMART_AI_MIN_SCORE or float(rvol or 0) < SMART_AI_MIN_RVOL:
        return False
    if not strong.intersection(_smart_pattern_names(patterns)):
        return False
    if df is None or df.empty:
        return False
    work = df.copy()
    work.columns = [str(c).lower() for c in work.columns]
    if 'close' not in work or len(work) < 20:
        return False
    close = work['close'].astype(float)
    ema50 = close.ewm(span=50, adjust=False).mean().iloc[-1]
    return float(close.iloc[-1]) >= float(ema50) * 0.98


def smart_ai_analyze(symbol, signal_data):
    """يؤكد المرشح القوي فقط؛ لا ينفذ شراءً أو بيعاً ولا يحول Paper Trading إلى تداول حقيقي."""
    if not SMART_AI_ENABLED or not OPENAI_AVAILABLE or client is None:
        return None
    score = float(signal_data.get('score', 0) or 0)
    rvol = float(signal_data.get('rvol', 0) or 0)
    df = signal_data.get('df')
    patterns = signal_data.get('patterns', [])
    if not should_use_smart_ai(symbol, score, rvol, patterns, df):
        return None
    cache_key = f"{symbol}_{now_est().date()}"
    with _smart_ai_lock:
        cached = _smart_ai_cache.get(cache_key)
        last = float(_smart_ai_cache.get(f'{symbol}_last', 0) or 0)
    if cached is not None or time.time() - last < SMART_AI_COOLDOWN:
        return cached
    if not can_send_ai_request():
        return None
    try:
        work = df.copy()
        work.columns = [str(c).lower() for c in work.columns]
        price = float(work['close'].iloc[-1])
        ema50 = float(work['close'].astype(float).ewm(span=50, adjust=False).mean().iloc[-1])
        vwap = float(work['vwap'].iloc[-1]) if 'vwap' in work.columns else price
        pattern_text = '، '.join(_smart_pattern_names(patterns)[:3]) or 'غير محدد'
        prompt = f"""حلل المرشح {symbol} بحذر شديد وبناءً على البيانات التالية، ولا تضمن الربح.
السعر: ${price:.4f}; RVOL: {rvol:.2f}x; السكور الفني: {score:.0f}/100;
الأنماط: {pattern_text}; فوق EMA50: {price >= ema50}; فوق VWAP: {price >= vwap}.
أجب JSON فقط بهذا الشكل:
{{"decision":"BUY|WATCH|AVOID","confidence":0,"target_1":0,"target_2":0,"stop_loss":0,"reason_ar":"سبب مختصر","key_risks":"مخاطر مختصرة"}}"""
        response = groq_chat_completion(
            model=AI_MODEL,
            messages=[
                {'role': 'system', 'content': 'You are a conservative penny-stock analyst. Return valid JSON only. Never guarantee profit. Paper trading only.'},
                {'role': 'user', 'content': prompt},
            ],
            response_format={'type': 'json_object'},
            temperature=0.2,
            max_tokens=220,
            timeout=8,
        )
        raw = response.choices[0].message.content
        result = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(result, dict):
            return None
        result['confidence'] = max(0, min(100, int(float(result.get('confidence', 0) or 0))))
        result['decision'] = str(result.get('decision', 'WATCH')).upper()
        if result['decision'] not in {'BUY', 'WATCH', 'AVOID'}:
            result['decision'] = 'WATCH'
        with _smart_ai_lock:
            _smart_ai_cache[cache_key] = result
            _smart_ai_cache[f'{symbol}_last'] = time.time()
        return result
    except Exception as error:
        logger.debug(f'[SmartAI] {symbol}: {error}')
        return None


def smart_ai_message_addendum(symbol, price, patterns, score, rvol, df):
    """يعيد جزء الرسالة فقط؛ لا يرسل رسالة منفصلة ولا يفتح صفقة تلقائياً."""
    result = smart_ai_analyze(symbol, {'price': price, 'patterns': patterns, 'score': score, 'rvol': rvol, 'df': df})
    if not result:
        return ''
    return (f"\\n━━━━━━━━━━━━━━━━\\n🧠 *تأكيد Smart AI لـ {symbol}:*\\n"
            f"📌 القرار التحليلي: *{result.get('decision', 'WATCH')}*\\n"
            f"🎯 الثقة: *{result.get('confidence', 0)}/100*\\n"
            f"💡 {result.get('reason_ar', 'لا يوجد سبب مختصر')}\\n"
            f"⚠️ المخاطر: {result.get('key_risks', 'غير محددة')}\\n"
            f"🎯 الأهداف/الوقف: ${float(result.get('target_1', 0) or 0):.4f} / ${float(result.get('target_2', 0) or 0):.4f} / ${float(result.get('stop_loss', 0) or 0):.4f}\\n"
            f"⚠️ Paper Trading فقط؛ لا تنفيذ تلقائي")


def ai_analyze_pump(symbol, df_1m, df_5m, patterns):
    """تحليل AI محدود لدرجة الخطر؛ يعيد نصًا للتنبيه ولا ينفذ صفقة."""
    if not OPENAI_AVAILABLE or client is None or not can_send_ai_request():
        return None
    try:
        price = float(df_1m['close'].iloc[-1])
        rvol = get_unified_rvol(df_5m)
        prompt = f"""حلل خطر Pump & Dump المحتمل للسهم {symbol}.
السعر ${price:.4f}، RVOL {rvol:.1f}x، الأنماط: {', '.join(patterns)}.
أجب JSON فقط: {{"pump_likelihood":0-100,"dump_risk":"LOW|MEDIUM|HIGH","recommendation":"WATCH|AVOID","reason_ar":"سبب مختصر"}}"""
        response = groq_chat_completion(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "You detect manipulation risk conservatively. Respond JSON only; never guarantee profit."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=220,
            timeout=10,
        )
        result = json.loads(response.choices[0].message.content)
        return f"احتمالية البامب: {result.get('pump_likelihood', 0)}% | خطر التفريغ: {result.get('dump_risk', 'MEDIUM')} | {result.get('reason_ar', '')}"
    except Exception as error:
        logger.debug(f"AI Pump analysis error {symbol}: {error}")
        return None


def pump_and_dump_hunter():
    """يمسح مرشحين محدودين ويصدر تحذير تلاعب محتمل فقط."""
    last_alerts = {}
    while True:
        try:
            if get_market_phase() == "CLOSED":
                time.sleep(60)
                continue
            with state_lock:
                tickers = list(state.get("tickers", []))[:100]
            for symbol in tickers:
                try:
                    if time.time() - last_alerts.get(symbol, 0) < PUMP_DETECTION_SETTINGS["cooldown"]:
                        continue
                    df_1m = cached_download(symbol, period="1d", interval="1m")
                    if df_1m.empty or len(df_1m) < 10:
                        continue
                    df_1m.columns = [str(c).lower() for c in df_1m.columns]
                    price = float(df_1m['close'].iloc[-1])
                    if price > PUMP_DETECTION_SETTINGS["small_cap_max"]:
                        continue
                    df_5m = cached_download(symbol, period="5d", interval="5m")
                    if df_5m.empty or len(df_5m) < 20:
                        continue
                    df_5m.columns = [str(c).lower() for c in df_5m.columns]
                    score, patterns = detect_pre_pump_patterns(df_1m, df_5m)
                    average_volume = max(float(df_1m['volume'].tail(10).mean()), 1)
                    volume_spike = float(df_1m['volume'].iloc[-1]) / average_volume
                    momentum = (price - float(df_1m['close'].iloc[-5])) / max(float(df_1m['close'].iloc[-5]), 1e-9) * 100
                    rvol = get_unified_rvol(df_5m)
                    if score < 60 or volume_spike < PUMP_DETECTION_SETTINGS["volume_spike"] or momentum <= 2:
                        continue
                    levels = calculate_pump_entry_exit(df_1m, df_5m, price)
                    ai_text = ai_analyze_pump(symbol, df_1m, df_5m, patterns)
                    message = (f"💣 *PUMP RISK ALERT: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                               f"⚠️ خطر تلاعب محتمل، وليست توقعًا مضمونًا\n"
                               f"📊 الدرجة: *{score}/100* | RVOL: *{rvol:.1f}x*\n"
                               f"📈 الزخم: *{momentum:+.1f}%* | حجم: *{volume_spike:.1f}x*\n"
                               f"🔎 الأنماط: {', '.join(patterns)}\n━━━━━━━━━━━━━━━━\n"
                               f"🎯 دخول تأكيدي مشروط: ${levels['confirm_entry']:.4f}\n"
                               f"🎯 هدف 1: ${levels['target_1']:.4f} | هدف 2: ${levels['target_2']:.4f}\n"
                               f"🛑 وقف حسابي: ${levels['stop_loss']:.4f}\n")
                    if ai_text:
                        message += f"🧠 AI: {ai_text}\n"
                    message += "⚠️ لا تنفيذ تلقائي؛ تحقق من السيولة وخطر التخفيف قبل أي قرار"
                    send_telegram(message)
                    logger.info(f"Pump risk alert: {symbol} score={score}")
                    last_alerts[symbol] = time.time()
                except Exception as error:
                    logger.debug(f"Pump hunter error {symbol}: {error}")
                time.sleep(0.1)
            gc.collect()
        except Exception as error:
            logger.error(f"Pump & Dump Hunter error: {error}")
        time.sleep(PUMP_DETECTION_SETTINGS["scan_interval"])



# ================= PDUFA CALENDAR (AI-assisted, unverified) =================
# Groq لا يتصفح الإنترنت؛ هذه النتائج تحتاج تحققًا من SEC/FDA/إعلانات الشركة.
PDUFA_CACHE = []
PDUFA_LAST_UPDATE = 0.0
PDUFA_UPDATE_INTERVAL = 21600
PDUFA_ALERT_DAYS = [14, 7, 3, 1]
PDUFA_ALERT_ENABLED = True
PDUFA_MAX_ALERTS_PER_DAY = 10


def fetch_pdufa_dates_ai():
    global PDUFA_CACHE, PDUFA_LAST_UPDATE
    if PDUFA_CACHE and time.time() - PDUFA_LAST_UPDATE < PDUFA_UPDATE_INTERVAL:
        return PDUFA_CACHE
    if not OPENAI_AVAILABLE or client is None or not can_send_ai_request():
        return PDUFA_CACHE
    try:
        response = groq_chat_completion(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "Return JSON only. Do not claim web browsing. Mark every date as unverified unless a source is included."},
                {"role": "user", "content": "List known PDUFA dates from your knowledge as unverified candidates. JSON only: {\"dates\":[{\"symbol\":\"CAPR\",\"drug\":\"\",\"date\":\"YYYY-MM-DD\",\"indication\":\"\",\"source\":\"\",\"confidence\":\"LOW|MEDIUM|HIGH\"}]} If uncertain, omit the item."},
            ], response_format={"type": "json_object"}, temperature=0.1, max_tokens=500, timeout=15
        )
        payload = json.loads(response.choices[0].message.content)
        dates = payload.get("dates", []) if isinstance(payload, dict) else []
        clean = []
        for item in dates:
            try:
                symbol = str(item.get("symbol", "")).upper().strip()
                event_date = str(item.get("date", ""))
                datetime.strptime(event_date, "%Y-%m-%d")
                if symbol.isalpha() and 1 <= len(symbol) <= 5:
                    clean.append({"symbol": symbol, "drug": str(item.get("drug", ""))[:120], "date": event_date, "indication": str(item.get("indication", ""))[:160], "source": str(item.get("source", "AI knowledge; verify officially"))[:160], "confidence": str(item.get("confidence", "LOW")).upper(), "unverified": True})
            except (TypeError, ValueError):
                continue
        if clean:
            PDUFA_CACHE = clean[:100]
            PDUFA_LAST_UPDATE = time.time()
            logger.warning("PDUFA AI cache refreshed; dates are unverified and require official confirmation")
    except Exception as error:
        logger.error(f"PDUFA AI fetch error: {error}")
    return PDUFA_CACHE


def get_pdufa_calendar():
    return fetch_pdufa_dates_ai()


def pdufa_biotech_scanner():
    sent_alerts = set()
    while True:
        try:
            if not PDUFA_ALERT_ENABLED or get_market_phase() == "CLOSED":
                time.sleep(300)
                continue
            today = now_est().date()
            daily_count = 0
            for item in get_pdufa_calendar():
                try:
                    days_left = (datetime.strptime(item["date"], "%Y-%m-%d").date() - today).days
                    if days_left not in PDUFA_ALERT_DAYS:
                        continue
                    key = f"pdufa_{item['symbol']}_{item['date']}_{days_left}"
                    if key in sent_alerts or daily_count >= PDUFA_MAX_ALERTS_PER_DAY:
                        continue
                    df = cached_download(item["symbol"], period="10d", interval="1d")
                    price = float(df["close"].iloc[-1]) if not df.empty else 0.0
                    msg = (f"📅 *PDUFA مرشح غير موثق — {item['symbol']}*\\n━━━━━━━━━━━━━━━━\\n"
                           f"💊 {item.get('drug', '')}\\n📅 الموعد المدخل: {item['date']} | بعد {days_left} يوم\\n"
                           f"💰 السعر: ${price:.2f}\\n📎 المصدر المدخل: {item.get('source', 'غير محدد')}\\n"
                           f"⚠️ AI لا يتصفح؛ تحقق رسميًا قبل أي قرار — معلومة وليست توصية")
                    send_telegram(msg)
                    sent_alerts.add(key)
                    daily_count += 1
                except Exception as error:
                    logger.warning(f"PDUFA check error: {error}")
        except Exception as error:
            logger.error(f"PDUFA scanner error: {error}")
        time.sleep(1800)


if bot:
    @bot.message_handler(commands=['calendar'])
    def cmd_calendar(message):
        if not ensure_authorized(message):
            return
        dates = get_pdufa_calendar()
        if not dates:
            send_telegram("📅 لا توجد بيانات PDUFA متاحة من AI حاليًا. تحقق من المصادر الرسمية.")
            return
        msg = "📅 *تقويم PDUFA — AI-assisted / غير موثق*\\n━━━━━━━━━━━━━━━━\\n"
        today = now_est().date()
        for item in sorted(dates, key=lambda x: x["date"])[:15]:
            days = (datetime.strptime(item["date"], "%Y-%m-%d").date() - today).days
            if -3 <= days <= 90:
                msg += f"\\n🔹 *{item['symbol']}* — {item.get('drug', '')}\\n   📅 {item['date']} ({days} يوم)\\n   📎 {item.get('source', 'غير محدد')}\\n"
        msg += "\\n⚠️ التواريخ غير مؤكدة آليًا وليست توصية استثمارية."
        send_telegram(msg)


# ================================================================
# 🧠 FINAL TRADE RECOMMENDATION ENGINE
# AI يفسر، والقواعد المحلية تملك حق الرفض — لا تنفيذ تلقائي
# ================================================================

def _safe_number(value, default=0.0):
    try:
        value = float(value)
        return default if not np.isfinite(value) else value
    except (TypeError, ValueError):
        return default


def _record_recommendation(record):
    record.setdefault("time", time.time())
    record.setdefault("expires_at", record["time"] + RECOMMENDATION_EXPIRY_MINUTES * 60)
    with state_lock:
        history = state.setdefault("recommendation_history", [])
        history.append(record)
        state["recommendation_history"] = history[-RECOMMENDATION_MAX_HISTORY:]
        stats = state.setdefault("recommendation_stats", {})
        decision = record.get("final_decision", "NO_TRADE")
        stats[decision] = int(stats.get(decision, 0)) + 1
        save_state()


def _format_recommendation_message(result):
    factors = "\n".join(f"  • {x}" for x in result.get("reasons", [])[:5]) or "  • لا توجد عوامل كافية"
    invalidations = ", ".join(str(x) for x in result.get("invalidations", [])[:4]) or "لا يوجد"
    return (
        f"🧠 *FINAL RECOMMENDATION: {result['symbol']}*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🤖 قرار AI: *{result.get('ai_decision', 'N/A')}* | الثقة: *{result.get('confidence', 0)}/100*\n"
        f"🛡️ القرار النهائي: *{result['final_decision']}*\n"
        f"📌 Setup: *{result.get('setup', 'NONE')}* | المخاطرة: *{result.get('risk_level', 'HIGH')}*\n"
        f"💰 السعر: *${result['price']:.4f}* | RVOL: *{result['rvol']:.1f}x*\n"
        f"🎯 منطقة الدخول: ${result.get('entry_zone_low', 0):.4f} - ${result.get('entry_zone_high', 0):.4f}\n"
        f"✅ تأكيد: ${result.get('confirmation_price', 0):.4f}\n"
        f"🛑 الوقف: ${result.get('stop_loss', 0):.4f}\n"
        f"🎯 الأهداف: ${result.get('target_1', 0):.4f} / ${result.get('target_2', 0):.4f}\n"
        f"📐 R:R: *{result.get('risk_reward', 0):.2f}* | الصلاحية: {result.get('expires_in_minutes', RECOMMENDATION_EXPIRY_MINUTES)} دقيقة\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🔍 *العوامل:*\n{factors}\n"
        f"🚫 *يلغى القرار عند:* {invalidations}\n"
        f"💡 {result.get('reason_ar', '')}\n"
        f"⚠️ *توصية تحليلية Paper Trading — لا تنفيذ تلقائي*"
    )


def final_trade_recommendation(symbol, news_impact=0, setup="NONE", pump_risk=False, send_alert=True):
    """يصدر BUY/WATCH/AVOID/NO_TRADE بعد دمج AI مع فلاتر محلية صارمة."""
    symbol = str(symbol or "").upper().strip()
    base = {
        "symbol": symbol, "final_decision": "NO_TRADE", "ai_decision": "N/A",
        "confidence": 0, "setup": setup, "risk_level": "HIGH",
        "price": 0.0, "rvol": 0.0, "entry_zone_low": 0.0,
        "entry_zone_high": 0.0, "confirmation_price": 0.0,
        "stop_loss": 0.0, "target_1": 0.0, "target_2": 0.0,
        "risk_reward": 0.0, "expires_in_minutes": RECOMMENDATION_EXPIRY_MINUTES,
        "reasons": [], "invalidations": [], "reason_ar": ""
    }
    try:
        if not symbol or get_market_phase() == "CLOSED":
            base["reason_ar"] = "السوق مغلق أو الرمز غير صالح"
            return base
        df = cached_download(symbol, period="5d", interval="5m")
        if df.empty or len(df) < 25:
            base["reason_ar"] = "بيانات غير كافية"
            return base
        df.columns = [str(c).lower() for c in df.columns]
        df = compute_indicators(df)
        price = _safe_number(df['close'].iloc[-1])
        volume = _safe_number(df['volume'].iloc[-1])
        avg_volume = max(_safe_number(df['volume'].tail(20).mean()), 1.0)
        rvol = get_unified_rvol(df, phase=get_market_phase())
        vwap = _safe_number(df['vwap'].iloc[-1], price)
        rsi = _safe_number(df['rsi'].iloc[-1], 50.0)
        atr = max(_safe_number(calculate_atr(df)), price * 0.02)
        high_10 = _safe_number(df['high'].tail(10).max(), price)
        breakout = price > _safe_number(df['high'].iloc[-11:-1].max(), price * 1.01)
        above_vwap = price >= vwap
        ema_bullish = _safe_number(df['ema9'].iloc[-1]) > _safe_number(df['ema21'].iloc[-1])
        volume_spike = volume >= avg_volume * 1.5
        technical_score = 0
        if rvol >= 2: technical_score += 25
        if volume_spike: technical_score += 15
        if above_vwap: technical_score += 15
        if ema_bullish: technical_score += 10
        if 40 <= rsi <= 72: technical_score += 10
        if breakout: technical_score += 15
        if price <= MAX_PRICE: technical_score += 10
        technical_score = clamp_score_100(technical_score)
        if price < MIN_PRICE or volume < MIN_VOLUME:
            base["invalidations"].append("سيولة أو سعر غير مناسب")
        if pump_risk:
            base["invalidations"].append("خطر تلاعب محتمل")
        if rvol < RECOMMENDATION_MIN_RVOL:
            base["invalidations"].append(f"RVOL أقل من {RECOMMENDATION_MIN_RVOL}x")
        if not above_vwap:
            base["invalidations"].append("السعر تحت VWAP")
        entry_low = max(price - atr * 0.35, vwap * 0.995)
        entry_high = price + atr * 0.15
        confirmation = max(high_10, price + atr * 0.25)
        stop_loss = max(price - atr * 1.2, price * 0.94)
        target_1 = price + atr * 2.4
        target_2 = price + atr * 3.6
        risk_reward = (target_1 - price) / max(price - stop_loss, 1e-9)
        base.update({"price": price, "rvol": rvol, "entry_zone_low": entry_low,
                     "entry_zone_high": entry_high, "confirmation_price": confirmation,
                     "stop_loss": stop_loss, "target_1": target_1, "target_2": target_2,
                     "risk_reward": risk_reward})
        if technical_score < RECOMMENDATION_MIN_TECH_SCORE:
            base["invalidations"].append(f"الدرجة الفنية {technical_score} أقل من {RECOMMENDATION_MIN_TECH_SCORE}")
        if risk_reward < RECOMMENDATION_MIN_RR:
            base["invalidations"].append(f"R:R أقل من {RECOMMENDATION_MIN_RR}")
        if not can_send_ai_request() or not OPENAI_AVAILABLE or client is None:
            base["final_decision"] = "WATCH" if technical_score >= 55 and not pump_risk else "NO_TRADE"
            base["reason_ar"] = "تعذر تحليل AI أو تم بلوغ حد الطلبات؛ القرار المحلي تحفظي"
            _record_recommendation({**base, "time": time.time(), "technical_score": technical_score})
            if send_alert and base["final_decision"] != "NO_TRADE": send_telegram(_format_recommendation_message(base))
            return base
        prompt = f"""حلل المرشح {symbol} بحذر. لا تضمن ربحًا ولا تتجاوز الفلاتر المحلية.
السعر {price:.4f}; RVOL {rvol:.2f}; RSI {rsi:.1f}; فوق VWAP {above_vwap}; اختراق {breakout};
الدرجة الفنية المحلية {technical_score}/100; R:R المحسوب {risk_reward:.2f}; تأثير الخبر {news_impact}/25.
أجب JSON فقط: {{"decision":"BUY|WATCH|AVOID","confidence":0-100,"setup":"PRE_BREAKOUT|NEWS_MOMENTUM|PUMP_RISK|NONE","risk_level":"LOW|MEDIUM|HIGH","reasons":[],"invalidations":[],"reason_ar":"سبب مختصر"}}"""
        response = groq_chat_completion(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "You are a conservative trading analyst. Return JSON only. Never guarantee profit."},
                {"role": "user", "content": prompt},
            ], response_format={"type": "json_object"}, temperature=0.2, max_tokens=300, timeout=10)
        ai = json.loads(response.choices[0].message.content)
        ai_decision = str(ai.get("decision", "WATCH")).upper()
        confidence = max(0, min(int(ai.get("confidence", 0) or 0), 100))
        local_ok = (technical_score >= RECOMMENDATION_MIN_TECH_SCORE and
                    rvol >= RECOMMENDATION_MIN_RVOL and above_vwap and
                    risk_reward >= RECOMMENDATION_MIN_RR and not pump_risk and
                    not base["invalidations"])
        if ai_decision == "AVOID":
            final_decision = "AVOID"
        elif ai_decision == "BUY" and confidence >= RECOMMENDATION_MIN_CONFIDENCE and local_ok:
            final_decision = "BUY"
        elif ai_decision in ("BUY", "WATCH") and technical_score >= 55 and not pump_risk:
            final_decision = "WATCH"
        else:
            final_decision = "NO_TRADE"
        base.update({"ai_decision": ai_decision, "confidence": confidence,
                     "final_decision": final_decision, "setup": ai.get("setup", setup),
                     "risk_level": ai.get("risk_level", "HIGH"),
                     "reasons": ai.get("reasons", []), "reason_ar": ai.get("reason_ar", "")})
        base["invalidations"].extend(ai.get("invalidations", [])[:4])
        _record_recommendation({**base, "time": time.time(), "technical_score": technical_score})
        if send_alert and final_decision in ("BUY", "WATCH", "AVOID"):
            send_telegram(_format_recommendation_message(base))
        return base
    except Exception as error:
        logger.error(f"Final recommendation error {symbol}: {error}")
        base["reason_ar"] = "حدث خطأ؛ تم إلغاء التوصية حفاظًا على رأس المال"
        return base


if bot:
    @bot.message_handler(commands=['recommend'])
    def cmd_recommend(message):
        if not ensure_authorized(message):
            return
        args = message.text.split()
        if len(args) != 2:
            send_telegram("الاستخدام: /recommend SYMBOL")
            return
        symbol = args[1].upper().strip()
        send_telegram(f"🧠 جاري بناء توصية موحدة لـ *{symbol}*...")
        threading.Thread(target=final_trade_recommendation, args=(symbol,), daemon=True).start()


if bot:
    @bot.message_handler(commands=['recommendations'])
    def cmd_recommendations(message):
        """يعرض إحصائيات وآخر التوصيات المسجلة."""
        if not ensure_authorized(message):
            return
        with state_lock:
            stats = dict(state.get("recommendation_stats", {}))
            history = list(state.get("recommendation_history", []))[-10:]
        total = sum(int(v or 0) for v in stats.values())
        msg = ("📊 *إحصائيات التوصيات*\n━━━━━━━━━━━━━━━━\n"
               f"🟢 BUY: {stats.get('BUY', 0)}\n"
               f"👀 WATCH: {stats.get('WATCH', 0)}\n"
               f"🚫 AVOID: {stats.get('AVOID', 0)}\n"
               f"⏸️ NO_TRADE: {stats.get('NO_TRADE', 0)}\n"
               f"📦 المجموع: {total}\n━━━━━━━━━━━━━━━━\n📋 *آخر التوصيات:*\n")
        for rec in reversed(history):
            symbol = rec.get("symbol", "???")
            decision = rec.get("final_decision", "N/A")
            price = _safe_number(rec.get("price", 0))
            age = max(0, int((time.time() - float(rec.get("time", time.time()))) / 60))
            expired = time.time() > float(rec.get("expires_at", 0) or 0)
            msg += f"• {symbol} → *{decision}* @ ${price:.2f} | منذ {age}د{' | منتهية' if expired else ''}\n"
        send_telegram(msg)


def premarket_flash_scanner():
    """ماسح سريع محدود الحجم؛ تنبيه Paper فقط، ولا يفتح صفقات."""
    last_alert = {}
    while True:
        try:
            if get_market_phase() != "PRE":
                time.sleep(30)
                continue
            with state_lock:
                symbols = list(state.get("tickers", []))[:100]
            for symbol in symbols:
                try:
                    df = cached_download(symbol, period="1d", interval="1m", prepost=True)
                    if df.empty or len(df) < 3:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    price = float(df["close"].iloc[-1])
                    previous = float(df["close"].iloc[-2])
                    if previous <= 0 or price <= 0:
                        continue
                    change = (price - previous) / previous * 100
                    rvol = get_unified_rvol(df, phase="PRE")
                    if change < 8 or rvol < 2:
                        continue
                    if time.time() - float(last_alert.get(symbol, 0)) < 60:
                        continue
                    last_alert[symbol] = time.time()
                    msg = (f"⚡ *PRE-MARKET FLASH: {symbol}*\\n━━━━━━━━━━━━━━━━\\n"
                           f"💰 ${price:.4f} | 📈 +{change:.1f}%\\n🔥 RVOL موحد: {rvol:.1f}x\\n"
                           f"⚠️ مراقبة فقط — لا تطارد القفزة ولا يوجد تنفيذ تلقائي")
                    send_telegram(msg)
                    if AI_PROCESS_ENABLED and time.time() - float(last_alert.get(f"ai_{symbol}", 0)) >= 900:
                        last_alert[f"ai_{symbol}"] = time.time()
                        threading.Thread(target=final_trade_recommendation, args=(symbol, 0, "PRE_BREAKOUT", False, True), daemon=True).start()
                except Exception as error:
                    logger.debug(f"Pre-market flash error {symbol}: {error}")
            time.sleep(10)
        except Exception as error:
            logger.error(f"Pre-market flash loop error: {error}")
            time.sleep(30)




# ================================================================
# 🔔 أوامر المراقبة والتنبيهات والسوق — Paper Trading فقط
# ================================================================

WATCHLIST_MOVE_THRESHOLD = 5.0
WATCHLIST_ALERT_COOLDOWN = 1800
WATCHLIST_UPDATE_INTERVAL = 3600


def watchlist_monitor():
    """يراقب الأسهم المضافة ويرسل حركة/اختراق/تحديثًا دوريًا مع كولداون."""
    while True:
        try:
            scanner_heartbeat("watchlist") if 'scanner_heartbeat' in globals() else None
            with state_lock:
                watchlist = list(state.get('watchlist', []))
                watch_state = state.setdefault('watchlist_monitor', {})
            now = time.time()
            summary = []
            for symbol in watchlist[:50]:
                symbol = str(symbol).upper().strip()
                if not symbol:
                    continue
                try:
                    df = cached_download(symbol, period='1d', interval='5m')
                    if df.empty or len(df) < 5:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    df = compute_indicators(df)
                    price = float(df['close'].iloc[-1])
                    open_price = float(df['open'].iloc[0])
                    if price <= 0 or open_price <= 0:
                        continue
                    change = (price - open_price) / open_price * 100
                    rvol = float(get_unified_rvol(df, phase=get_market_phase()))
                    rsi = float(df['rsi'].iloc[-1]) if 'rsi' in df and pd.notna(df['rsi'].iloc[-1]) else 0.0
                    item = watch_state.setdefault(symbol, {})
                    last_alert = float(item.get('last_alert', 0) or 0)
                    last_breakout = float(item.get('last_breakout', 0) or 0)
                    if abs(change) >= WATCHLIST_MOVE_THRESHOLD and now - last_alert >= WATCHLIST_ALERT_COOLDOWN:
                        send_telegram(
                            f'👀 *Watchlist Move: {symbol}*\\n━━━━━━━━━━━━━━━━\\n'
                            f'💰 السعر: ${price:.4f}\\n📈 التغير: {change:+.1f}%\\n'
                            f'🔥 RVOL: {rvol:.1f}x | RSI: {rsi:.0f}\\n⚠️ تنبيه مراقبة فقط'
                        )
                        item['last_alert'] = now
                    breakout = False
                    try:
                        sr = calculate_support_resistance(df)
                        resistance_data = sr.get('resistance') or {} if isinstance(sr, dict) else {}
                        resistance = float(resistance_data.get('level') or 0) if isinstance(resistance_data, dict) else 0.0
                        previous_price = float(df['close'].iloc[-2])
                        breakout = resistance > 0 and price > resistance and previous_price <= resistance
                    except Exception:
                        breakout = False
                    if breakout and now - last_breakout >= WATCHLIST_ALERT_COOLDOWN:
                        send_telegram(
                            f'🚀 *Watchlist Breakout: {symbol}*\\n━━━━━━━━━━━━━━━━\\n'
                            f'💰 السعر: ${price:.4f}\\n🎯 المقاومة: ${resistance:.4f}\\n'
                            f'🔥 RVOL: {rvol:.1f}x | RSI: {rsi:.0f}\\n⚠️ اختراق يحتاج تأكيدًا؛ ليس توصية'
                        )
                        item['last_breakout'] = now
                    item.update({'price': price, 'change': change, 'rvol': rvol, 'updated_at': now})
                    summary.append(f'{symbol} ${price:.4f} ({change:+.1f}%)')
                except Exception as error:
                    logger.debug(f'Watchlist monitor error {symbol}: {error}')
            last_summary = float(state.get('watchlist_last_summary', 0) or 0)
            if watchlist and summary and now - last_summary >= WATCHLIST_UPDATE_INTERVAL:
                send_telegram('📊 *تحديث قائمة المراقبة*\\n━━━━━━━━━━━━━━━━\\n' + '\\n'.join(summary[:20]) + '\\n⚠️ تحديث Paper/مراقبة فقط')
                state['watchlist_last_summary'] = now
            with state_lock:
                state['watchlist_monitor'] = watch_state
                save_state()
            time.sleep(60)
        except Exception as error:
            logger.error(f'Watchlist monitor error: {error}')
            time.sleep(60)


# ================================================================
# 📊 VWAP BOUNCE & BREAKOUT SCANNER
# ================================================================
VWAP_BOUNCE_ENABLED = True
VWAP_BOUNCE_INTERVAL = 60
VWAP_BOUNCE_COOLDOWN = 1800
VWAP_BOUNCE_MIN_PRICE = 0.5
VWAP_BOUNCE_MAX_PRICE = 50.0
VWAP_BOUNCE_MIN_VOLUME = 50000
VWAP_BOUNCE_MIN_PRE_MARKET_DROP = 10.0
VWAP_BOUNCE_TARGETS = [1.08, 1.15, 1.25, 1.40]
VWAP_BOUNCE_STOP_PCT = 0.06
_sent_vwap_bounce = {}


def analyze_vwap_bounce(symbol, df_1m=None, df_5m=None, df_daily=None):
    """تحليل ارتداد VWAP واختراق المقاومة؛ يعيد Paper setup أو None."""
    try:
        if df_5m is None:
            df_5m = cached_download(symbol, period='5d', interval='5m')
        if df_5m is None or df_5m.empty or len(df_5m) < 20:
            return None
        df_5m = df_5m.copy()
        df_5m.columns = [str(c).lower() for c in df_5m.columns]
        df_5m = compute_indicators(df_5m)
        required = {'open', 'high', 'low', 'close', 'volume'}
        if not required.issubset(df_5m.columns):
            return None
        price = float(df_5m['close'].iloc[-1])
        if not (VWAP_BOUNCE_MIN_PRICE <= price <= VWAP_BOUNCE_MAX_PRICE):
            return None
        volume = float(df_5m['volume'].iloc[-1] or 0)
        avg_volume = float(df_5m['volume'].tail(20).mean() or 0)
        if volume < VWAP_BOUNCE_MIN_VOLUME or avg_volume <= 0:
            return None
        rvol = volume / max(avg_volume, 1.0)
        if rvol < 1.5:
            return None
        typical = (df_5m['high'] + df_5m['low'] + df_5m['close']) / 3
        cumulative_volume = df_5m['volume'].cumsum().replace(0, np.nan)
        vwap_series = (typical * df_5m['volume']).cumsum() / cumulative_volume
        vwap = float(vwap_series.iloc[-1]) if pd.notna(vwap_series.iloc[-1]) else price
        std = float(df_5m['close'].rolling(20).std().iloc[-1] or 0)
        upper_band = vwap + std * 1.5
        lower_band = vwap - std * 1.5
        below_lower_band = price < lower_band
        low = float(df_5m['low'].tail(20).min())
        high = float(df_5m['high'].tail(20).max())
        support = min(low, lower_band)
        resistance = high
        previous_close = float(df_5m['close'].iloc[-2])
        bounce = price > support * 1.01 and price > previous_close and price >= vwap * 0.98
        pre_market_drop = 0.0
        if df_1m is not None and not df_1m.empty:
            try:
                df_1m = df_1m.copy()
                df_1m.columns = [str(c).lower() for c in df_1m.columns]
                first_price = float(df_1m['close'].iloc[0])
                last_price = float(df_1m['close'].iloc[-1])
                if first_price > 0:
                    pre_market_drop = (last_price - first_price) / first_price * 100
            except Exception:
                pre_market_drop = 0.0
        conditions_met = below_lower_band and bounce and (pre_market_drop <= -VWAP_BOUNCE_MIN_PRE_MARKET_DROP or rvol >= 3.0)
        if not conditions_met:
            return None
        entry = max(price * 1.002, resistance * 1.002)
        stop = min(support * 0.98, price * (1 - VWAP_BOUNCE_STOP_PCT))
        if entry <= stop or stop <= 0:
            return None
        targets = [round(entry * pct, 4) for pct in VWAP_BOUNCE_TARGETS]
        risk = entry - stop
        rr = (targets[0] - entry) / max(risk, 0.0001)
        score = 0
        signals = []
        if rvol >= 3:
            score += 25; signals.append(f'🔥 RVOL {rvol:.1f}x')
        elif rvol >= 2:
            score += 15; signals.append(f'⚡ RVOL {rvol:.1f}x')
        if pre_market_drop <= -15:
            score += 20; signals.append(f'📉 انخفاض Pre-Market {pre_market_drop:.1f}%')
        if below_lower_band:
            score += 15; signals.append('📊 تحت النطاق السفلي')
        if bounce:
            score += 20; signals.append('🔄 ارتداد من الدعم')
        if rr >= 3:
            score += 20; signals.append(f'📐 R:R {rr:.1f}:1')
        score = min(100, score)
        return {
            'symbol': symbol, 'price': round(price, 4), 'entry': round(entry, 4),
            'stop': round(stop, 4), 'targets': targets, 'support': round(support, 4),
            'resistance': round(resistance, 4), 'vwap': round(vwap, 4),
            'upper_band': round(upper_band, 4), 'lower_band': round(lower_band, 4),
            'rvol': round(rvol, 2), 'pre_market_drop': round(pre_market_drop, 2),
            'rr': round(rr, 2), 'score': score, 'signals': signals, 'timestamp': time.time()
        }
    except Exception as error:
        logger.debug(f'[VWAP Bounce] {symbol}: {error}')
        return None


def vwap_bounce_scanner():
    """يفحص الأسهم النشطة كل دقيقة ويرسل تنبيهات Paper عند تحقق الارتداد."""
    while True:
        try:
            scanner_heartbeat('vwap_bounce') if 'scanner_heartbeat' in globals() else None
            if not VWAP_BOUNCE_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(VWAP_BOUNCE_INTERVAL)
                continue
            with state_lock:
                symbols = list(dict.fromkeys(state.get('tickers', []) + state.get('watchlist', [])))[:300]
            for symbol in symbols:
                symbol = str(symbol).upper().strip()
                if not symbol or time.time() - _sent_vwap_bounce.get(symbol, 0) < VWAP_BOUNCE_COOLDOWN:
                    continue
                try:
                    df_5m = cached_download(symbol, period='5d', interval='5m')
                    if df_5m is None or df_5m.empty:
                        continue
                    df_1m = cached_download(symbol, period='2d', interval='1m', prepost=True)
                    result = analyze_vwap_bounce(symbol, df_1m, df_5m)
                    if not result or result['score'] < 50:
                        continue
                    signal_key = f"vwap_bounce_{symbol}_{now_est().date()}"
                    with state_lock:
                        if signal_key in state.get('seen_signals', {}):
                            continue
                    targets = ' / '.join(f"${x:.2f}" for x in result['targets'][:3])
                    signals = '\n   • '.join(result['signals']) or 'ارتداد VWAP'
                    level = '🔴 HIGH' if result['score'] >= 80 else '🟠 MEDIUM' if result['score'] >= 65 else '🟡 LOW'
                    msg = (f"📊 *VWAP BOUNCE: {symbol}*\\n━━━━━━━━━━━━━━━━\\n"
                           f"📊 السكور: *{result['score']}/100* | {level}\\n"
                           f"💰 السعر: *${result['price']:.4f}* | RVOL: *{result['rvol']:.1f}x*\\n"
                           f"📐 VWAP: ${result['vwap']:.4f} | دعم: ${result['support']:.4f}\\n"
                           f"🚧 مقاومة: ${result['resistance']:.4f}\\n"
                           f"🎯 دخول نظري: ${result['entry']:.4f}\\n🛑 وقف نظري: ${result['stop']:.4f}\\n"
                           f"🎯 أهداف: {targets} | R:R: {result['rr']:.1f}:1\\n"
                           f"📌 الإشارات:\\n   • {signals}\\n⚠️ Paper فقط وليست توصية أو تنفيذًا")
                    send_telegram(msg)
                    _sent_vwap_bounce[symbol] = time.time()
                    with state_lock:
                        state.setdefault('seen_signals', {})[signal_key] = time.time()
                        save_state()
                except Exception as error:
                    logger.debug(f'[VWAP Bounce] scan {symbol}: {error}')
            time.sleep(VWAP_BOUNCE_INTERVAL)
        except Exception as error:
            logger.error(f'[VWAP Bounce] scanner error: {error}')
            time.sleep(VWAP_BOUNCE_INTERVAL)


if bot:
    @bot.message_handler(commands=['vwap'])
    def cmd_vwap_analysis(message):
        if not ensure_authorized(message):
            return
        args = message.text.split()
        if len(args) != 2:
            send_telegram('⚠️ الاستخدام: /vwap SYMBOL')
            return
        symbol = args[1].upper().strip()
        df_5m = cached_download(symbol, period='5d', interval='5m')
        df_1m = cached_download(symbol, period='2d', interval='1m', prepost=True)
        result = analyze_vwap_bounce(symbol, df_1m, df_5m)
        if not result:
            send_telegram(f'❌ لا تنطبق شروط VWAP Bounce على *{symbol}* حاليًا')
            return
        send_telegram(f"📊 *VWAP {symbol}*\\nالسكور: {result['score']}/100\\nالسعر: ${result['price']:.4f}\\n"
                      f"الدخول النظري: ${result['entry']:.4f}\\nالوقف: ${result['stop']:.4f}\\n"
                      f"الأهداف: {' / '.join(f'${x:.2f}' for x in result['targets'][:3])}\\n"
                      f"⚠️ Paper فقط وليست توصية")


# ================================================================
# 🕯️ CANDLESTICK + CHART PATTERN DETECTORS
# ================================================================
CANDLE_PATTERNS_ENABLED = True
CANDLE_PATTERNS_INTERVAL = 300
CANDLE_PATTERNS_COOLDOWN = 3600
CHART_PATTERNS_ENABLED = True
CHART_PATTERNS_INTERVAL = 600
CHART_PATTERNS_COOLDOWN = 7200
_sent_candle_patterns = {}
_sent_chart_patterns = {}


def _candle_values(row):
    return (float(row['open']), float(row['high']), float(row['low']), float(row['close']))


def detect_doji(df, body_threshold=0.10):
    if df is None or df.empty:
        return None
    o, h, l, c = _candle_values(df.iloc[-1])
    span = h - l
    if span <= 0 or abs(c - o) / span >= body_threshold:
        return None
    upper, lower = h - max(o, c), min(o, c) - l
    return {'pattern': 'DOJI', 'type': '🕯️ Doji — تردد', 'price': c,
            'bullish': lower > upper * 2, 'bearish': upper > lower * 2}


def detect_hammer(df, inverted=False):
    if df is None or df.empty:
        return None
    o, h, l, c = _candle_values(df.iloc[-1])
    body = abs(c - o)
    if body <= 0:
        return None
    upper, lower = h - max(o, c), min(o, c) - l
    if not inverted and lower > body * 2 and upper < body * 0.5:
        return {'pattern': 'HAMMER', 'type': '🔨 Hammer — انعكاس صاعد محتمل', 'price': c, 'bullish': True}
    if inverted and upper > body * 2 and lower < body * 0.5:
        return {'pattern': 'INVERTED_HAMMER', 'type': '🔨 Inverted Hammer — يحتاج تأكيدًا', 'price': c, 'bullish': True}
    return None


def detect_shooting_star(df):
    if df is None or df.empty:
        return None
    o, h, l, c = _candle_values(df.iloc[-1])
    body = abs(c - o)
    if body <= 0:
        return None
    upper, lower = h - max(o, c), min(o, c) - l
    if upper > body * 2 and lower < body * 0.5:
        return {'pattern': 'SHOOTING_STAR', 'type': '🌠 Shooting Star — انعكاس هابط محتمل', 'price': c, 'bearish': True}
    return None


def detect_morning_evening_star(df):
    if df is None or len(df) < 3:
        return None
    a, b, c = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    ao, ac = float(a['open']), float(a['close'])
    bo, bc = float(b['open']), float(b['close'])
    co, cc = float(c['open']), float(c['close'])
    a_body, b_body = abs(ac - ao), abs(bc - bo)
    if a_body <= 0 or b_body > a_body * 0.3:
        return None
    if ac < ao and cc > co and cc > (ao + ac) / 2:
        return {'pattern': 'MORNING_STAR', 'type': '⭐ Morning Star', 'price': cc, 'bullish': True}
    if ac > ao and cc < co and cc < (ao + ac) / 2:
        return {'pattern': 'EVENING_STAR', 'type': '⭐ Evening Star', 'price': cc, 'bearish': True}
    return None


def detect_harami(df):
    if df is None or len(df) < 2:
        return None
    p, x = df.iloc[-2], df.iloc[-1]
    po, pc = float(p['open']), float(p['close'])
    xo, xc = float(x['open']), float(x['close'])
    pbody, xbody = abs(pc - po), abs(xc - xo)
    if pbody <= 0 or xbody >= pbody * 0.5:
        return None
    if pc < po and xc > xo and pc < xo < xc < po:
        return {'pattern': 'HARAMI_BULLISH', 'type': '🌙 Bullish Harami', 'price': xc, 'bullish': True}
    if pc > po and xc < xo and po < xc < xo < pc:
        return {'pattern': 'HARAMI_BEARISH', 'type': '🌙 Bearish Harami', 'price': xc, 'bearish': True}
    return None


def analyze_candle_patterns(df):
    patterns = []
    for result in (detect_doji(df), detect_hammer(df), detect_hammer(df, True),
                   detect_shooting_star(df), detect_engulfing(df),
                   detect_morning_evening_star(df), detect_harami(df)):
        if result:
            patterns.append(result)
    return patterns


def _local_extrema(df, lookback=50):
    if df is None or len(df) < lookback:
        return [], []
    highs, lows = df['high'].astype(float).to_numpy(), df['low'].astype(float).to_numpy()
    peaks, troughs = [], []
    for i in range(2, len(df) - 2):
        if highs[i] > max(highs[i-2:i]) and highs[i] > max(highs[i+1:i+3]):
            peaks.append((i, highs[i]))
        if lows[i] < min(lows[i-2:i]) and lows[i] < min(lows[i+1:i+3]):
            troughs.append((i, lows[i]))
    return peaks, troughs


def detect_double_top(df):
    peaks, troughs = _local_extrema(df)
    if len(peaks) < 2:
        return None
    a, b = peaks[-2], peaks[-1]
    if abs(a[1] - b[1]) / max(a[1], 0.01) <= 0.02 and any(a[0] < t[0] < b[0] for t in troughs):
        neckline = min(t[1] for t in troughs if a[0] < t[0] < b[0])
        return {'pattern': 'DOUBLE_TOP', 'type': '🔺 Double Top — هابط محتمل', 'price': float(df['close'].iloc[-1]), 'bearish': True, 'neckline': neckline}
    return None


def detect_double_bottom(df):
    peaks, troughs = _local_extrema(df)
    if len(troughs) < 2:
        return None
    a, b = troughs[-2], troughs[-1]
    if abs(a[1] - b[1]) / max(a[1], 0.01) <= 0.02 and any(a[0] < p[0] < b[0] for p in peaks):
        neckline = max(p[1] for p in peaks if a[0] < p[0] < b[0])
        return {'pattern': 'DOUBLE_BOTTOM', 'type': '🔻 Double Bottom — صاعد محتمل', 'price': float(df['close'].iloc[-1]), 'bullish': True, 'neckline': neckline}
    return None


def detect_head_shoulders(df):
    peaks, troughs = _local_extrema(df)
    if len(peaks) < 3:
        return None
    a, head, b = peaks[-3:]
    if head[1] > a[1] and head[1] > b[1] and abs(a[1] - b[1]) / max(a[1], 0.01) <= 0.03:
        between = [t[1] for t in troughs if a[0] < t[0] < b[0]]
        if len(between) >= 2:
            neckline = sum(between[-2:]) / 2
            return {'pattern': 'HEAD_SHOULDERS', 'type': '👤 Head & Shoulders — هابط محتمل', 'price': float(df['close'].iloc[-1]), 'bearish': True, 'neckline': neckline}
    return None


def detect_inverse_head_shoulders(df):
    peaks, troughs = _local_extrema(df)
    if len(troughs) < 3:
        return None
    a, head, b = troughs[-3:]
    if head[1] < a[1] and head[1] < b[1] and abs(a[1] - b[1]) / max(a[1], 0.01) <= 0.03:
        between = [p[1] for p in peaks if a[0] < p[0] < b[0]]
        if len(between) >= 2:
            neckline = sum(between[-2:]) / 2
            return {'pattern': 'INVERSE_HEAD_SHOULDERS', 'type': '👤 Inverse H&S — صاعد محتمل', 'price': float(df['close'].iloc[-1]), 'bullish': True, 'neckline': neckline}
    return None


def detect_cup_handle(df):
    if df is None or len(df) < 50:
        return None
    close = df['close'].astype(float).to_numpy()[-50:]
    bottom = int(close.argmin())
    if bottom < 10 or bottom > 40:
        return None
    left, right = float(close[:bottom].max()), float(close[bottom:].max())
    depth = (min(left, right) - close[bottom]) / max(min(left, right), 0.01)
    handle = close[-10:]
    if depth >= 0.10 and (handle.max() - handle.min()) / max(handle.max(), 0.01) < depth * 0.5:
        return {'pattern': 'CUP_AND_HANDLE', 'type': '☕ Cup & Handle — استمرار صاعد محتمل', 'price': float(close[-1]), 'bullish': True, 'entry': max(left, right)}
    return None


def chart_patterns(df):
    return [x for x in (detect_double_top(df), detect_double_bottom(df), detect_head_shoulders(df), detect_inverse_head_shoulders(df), detect_cup_handle(df)) if x]


def candle_pattern_scanner():
    while True:
        try:
            scanner_heartbeat('candle_patterns') if 'scanner_heartbeat' in globals() else None
            if not CANDLE_PATTERNS_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(CANDLE_PATTERNS_INTERVAL); continue
            with state_lock:
                tickers = list(state.get('tickers', []))[:300]
            for symbol in tickers:
                if time.time() - _sent_candle_patterns.get(symbol, 0) < CANDLE_PATTERNS_COOLDOWN:
                    continue
                try:
                    df = cached_download(symbol, period='5d', interval='5m')
                    if df is None or df.empty or len(df) < 20:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    price = float(df['close'].iloc[-1])
                    if not (0.5 <= price <= 30):
                        continue
                    patterns = analyze_candle_patterns(df)
                    if not patterns:
                        continue
                    key = f'candle_patterns_{symbol}_{now_est().date()}'
                    with state_lock:
                        if key in state.get('seen_signals', {}):
                            continue
                    rvol = float(get_unified_rvol(df) if 'get_unified_rvol' in globals() else calculate_rvol(df))
                    score = min(100, 60 + 10 * len(patterns) + (15 if any(p.get('pattern') in {'HAMMER', 'ENGULFING_BULLISH', 'MORNING_STAR'} for p in patterns) else 0))
                    ai_addendum = smart_ai_message_addendum(symbol, price, patterns, score, rvol, df)
                    send_telegram(f"🕯️ *CANDLE PATTERN: {symbol}*\\nالسعر: ${price:.4f}\\n" + '\\n'.join(f"• {p['type']}" for p in patterns[:5]) + '\\n⚠️ Paper فقط وليست توصية' + ai_addendum)
                    _sent_candle_patterns[symbol] = time.time()
                    with state_lock:
                        state.setdefault('seen_signals', {})[key] = time.time(); save_state()
                except Exception as error:
                    logger.debug(f'[Candle Patterns] {symbol}: {error}')
            time.sleep(CANDLE_PATTERNS_INTERVAL)
        except Exception as error:
            logger.error(f'[Candle Patterns] scanner error: {error}'); time.sleep(60)


def chart_pattern_scanner():
    while True:
        try:
            scanner_heartbeat('chart_patterns') if 'scanner_heartbeat' in globals() else None
            if not CHART_PATTERNS_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(CHART_PATTERNS_INTERVAL); continue
            with state_lock:
                tickers = list(state.get('tickers', []))[:200]
            for symbol in tickers:
                if time.time() - _sent_chart_patterns.get(symbol, 0) < CHART_PATTERNS_COOLDOWN:
                    continue
                try:
                    df = cached_download(symbol, period='90d', interval='1d')
                    if df is None or df.empty or len(df) < 50:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    price = float(df['close'].iloc[-1])
                    if not (0.5 <= price <= 50):
                        continue
                    patterns = chart_patterns(df)
                    if not patterns:
                        continue
                    key = f'chart_patterns_{symbol}_{now_est().date()}'
                    with state_lock:
                        if key in state.get('seen_signals', {}):
                            continue
                    details = '\\n'.join(f"• {p['type']}" for p in patterns[:4])
                    rvol = float(get_unified_rvol(df) if 'get_unified_rvol' in globals() else calculate_rvol(df))
                    score = min(100, 65 + 10 * len(patterns))
                    ai_addendum = smart_ai_message_addendum(symbol, price, patterns, score, rvol, df)
                    send_telegram(f"📐 *CHART PATTERN: {symbol}*\\nالسعر: ${price:.4f}\\n{details}\\n⚠️ يحتاج تأكيد حجم وإغلاق؛ Paper فقط" + ai_addendum)
                    _sent_chart_patterns[symbol] = time.time()
                    with state_lock:
                        state.setdefault('seen_signals', {})[key] = time.time(); save_state()
                except Exception as error:
                    logger.debug(f'[Chart Patterns] {symbol}: {error}')
            time.sleep(CHART_PATTERNS_INTERVAL)
        except Exception as error:
            logger.error(f'[Chart Patterns] scanner error: {error}'); time.sleep(60)


def calculate_risk_reward_advanced(entry, stop_loss, target_1):
    risk = abs(float(entry) - float(stop_loss))
    reward = abs(float(target_1) - float(entry))
    return round(reward / risk, 2) if risk > 0 else 0.0


def get_optimal_targets(entry, stop_loss, rr_ratio=2.0):
    entry, stop_loss = float(entry), float(stop_loss)
    risk = abs(entry - stop_loss)
    direction = 1 if entry > stop_loss else -1
    reward = risk * float(rr_ratio)
    return {'target_1': round(entry + direction * reward, 4), 'target_2': round(entry + direction * reward * 1.5, 4), 'target_3': round(entry + direction * reward * 2, 4), 'rr': float(rr_ratio)}


def validate_trade_setup(entry, stop_loss, target_1, min_rr=2.0):
    rr = calculate_risk_reward_advanced(entry, stop_loss, target_1)
    return {'valid': rr >= min_rr, 'rr': rr, 'reason': f'نسبة RR {rr}:1 أقل من الحد {min_rr}:1' if rr < min_rr else f'نسبة RR مقبولة {rr}:1'}


def price_alert_monitor():
    """يراقب التنبيهات السعرية ويرسل إشعارًا مرة واحدة عند الوصول."""
    while True:
        try:
            with state_lock:
                alerts = dict(state.get('price_alerts', {}))
            for symbol, data in alerts.items():
                try:
                    if isinstance(data, dict) and data.get('triggered', False):
                        continue
                    target = float(data.get('price', 0) if isinstance(data, dict) else data)
                    if target <= 0:
                        continue
                    df = cached_download(symbol, period='1d', interval='1m')
                    if df.empty:
                        continue
                    current = float(df['close'].iloc[-1])
                    if current < target:
                        continue
                    msg = (f'🔔 *تنبيه سعري!*\n━━━━━━━━━━━━━━━━\n'
                           f'📊 *{symbol}*\n💰 السعر الحالي: *${current:.2f}*\n'
                           f'🎯 المستهدف: *${target:.2f}*\n✅ تم الوصول للسعر المستهدف!')
                    send_telegram(msg)
                    with state_lock:
                        if symbol in state.get('price_alerts', {}):
                            state['price_alerts'][symbol]['triggered'] = True
                            save_state()
                    logger.info(f'PRICE ALERT: {symbol} reached ${current:.2f}')
                except Exception as error:
                    logger.debug(f'Alert monitor error {symbol}: {error}')
            time.sleep(30)
        except Exception as error:
            logger.error(f'Price alert monitor error: {error}')
            time.sleep(60)


if bot:
    @bot.message_handler(commands=['adv'])
    def cmd_advanced_analysis(message):
        if not ensure_authorized(message):
            return
        args = message.text.split()
        if len(args) != 2:
            send_telegram('⚠️ الاستخدام: /adv SYMBOL')
            return
        symbol = args[1].upper().strip()
        send_telegram(f'🔍 جاري التحليل المتقدم لـ *{symbol}*...')
        result = advanced_analysis(symbol)
        send_telegram(_format_advanced_analysis(result) if result else f'❌ لا توجد بيانات كافية لتحليل *{symbol}*')

    @bot.message_handler(commands=['help'])
    def cmd_help(message):
        if not ensure_authorized(message): return
        send_telegram("""📋 *قائمة الأوامر*\n━━━━━━━━━━━━━━━━\n/start /help /status /positions\n/scan SYMBOL /sr SYMBOL /news SYMBOL\n/recommend SYMBOL /recommendations\n/top /signals /gainers /losers /halt\n/watchlist /add SYMBOL /remove SYMBOL\n/alert SYMBOL PRICE /alerts /delalert SYMBOL\n/b SYMBOL QTY /s SYMBOL /realbuy SYMBOL QTY /realsell SYMBOL\n/calendar /movers\n━━━━━━━━━━━━━━━━\n⚠️ التوصيات Paper Trading وليست ضمان ربح""")

    @bot.message_handler(commands=['top'])
    def cmd_top(message):
        if not ensure_authorized(message): return
        with state_lock:
            candidates = list(state.get('elite_candidates', []))[:5]
        if not candidates:
            send_telegram('📭 لا يوجد مرشحون حاليًا')
            return
        msg = '🏆 *أفضل المرشحين:*\n━━━━━━━━━━━━━━━━\n'
        for index, candidate in enumerate(candidates, 1):
            symbol = candidate.get('symbol', '???')
            score = candidate.get('score', 0)
            price = candidate.get('price', 0)
            reason = str(candidate.get('reason', ''))[:60]
            msg += f'{index}. *{symbol}* — {score}/100 — ${float(price):.2f}\n   {reason}\n'
        send_telegram(msg)

    @bot.message_handler(commands=['signals'])
    def cmd_signals(message):
        if not ensure_authorized(message): return
        with state_lock:
            signals = list(state.get('seen_signals', {}).items())[-10:]
        if not signals:
            send_telegram('📭 لا توجد إشارات مسجلة')
            return
        msg = '📡 *آخر الإشارات:*\n━━━━━━━━━━━━━━━━\n'
        for key, timestamp in reversed(signals):
            symbol = str(key).split('_')[1] if '_' in str(key) else str(key)[:12]
            msg += f'• *{symbol}* — {datetime.fromtimestamp(timestamp).strftime("%I:%M %p")}\n'
        send_telegram(msg)

    @bot.message_handler(commands=['watchlist'])
    def cmd_watchlist(message):
        if not ensure_authorized(message): return
        with state_lock:
            watchlist = list(state.get('watchlist', []))
        if not watchlist:
            send_telegram('📭 قائمة المراقبة فارغة\nاستخدم /add SYMBOL')
            return
        msg = '👀 *قائمة المراقبة:*\n━━━━━━━━━━━━━━━━\n'
        for symbol in watchlist[:20]:
            try:
                df = cached_download(symbol, period='1d', interval='5m')
                if df.empty:
                    msg += f'❓ *{symbol}* — لا توجد بيانات\n'
                    continue
                df.columns = [str(c).lower() for c in df.columns]
                df = compute_indicators(df)
                price = float(df['close'].iloc[-1])
                open_price = float(df['open'].iloc[0])
                change = (price - open_price) / max(open_price, 1e-9) * 100
                rsi = float(df['rsi'].iloc[-1]) if 'rsi' in df and pd.notna(df['rsi'].iloc[-1]) else 0.0
                volume = int(float(df['volume'].iloc[-1] or 0))
                rvol = float(get_unified_rvol(df, phase=get_market_phase()))
                emoji = '🟢' if change > 0 else '🔴' if change < 0 else '⚪'
                msg += (f'{emoji} *{symbol}* — ${price:.4f} ({change:+.1f}%)\n'
                        f'   📊 RSI: {rsi:.0f} | RVOL: {rvol:.1f}x | حجم: {volume:,}\n')
            except Exception as error:
                logger.debug(f'Watchlist display error {symbol}: {error}')
                msg += f'❓ *{symbol}* — تعذر الجلب\n'
        if len(watchlist) > 20:
            msg += f'... و{len(watchlist) - 20} سهم آخر\n'
        msg += '\n🔔 التنبيهات التلقائية: حركة ±5%، اختراق مقاومة، وتحديث كل ساعة.'
        send_telegram(msg)

    @bot.message_handler(commands=['add'])
    def cmd_add_watchlist(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram('⚠️ الاستخدام: /add SYMBOL')
            return
        symbol = args[1].upper().strip()
        df = cached_download(symbol, period='1d', interval='5m')
        if df.empty:
            send_telegram(f'❌ لا توجد بيانات لـ *{symbol}*')
            return
        with state_lock:
            watchlist = state.setdefault('watchlist', [])
            if symbol in watchlist:
                send_telegram(f'⚠️ *{symbol}* موجود بالفعل')
                return
            watchlist.append(symbol)
            save_state()
        send_telegram(f'✅ تمت إضافة *{symbol}* إلى قائمة المراقبة')

    @bot.message_handler(commands=['remove'])
    def cmd_remove_watchlist(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram('⚠️ الاستخدام: /remove SYMBOL')
            return
        symbol = args[1].upper().strip()
        with state_lock:
            watchlist = state.setdefault('watchlist', [])
            if symbol not in watchlist:
                send_telegram(f'⚠️ *{symbol}* غير موجود')
                return
            watchlist.remove(symbol)
            save_state()
        send_telegram(f'✅ تم حذف *{symbol}* من قائمة المراقبة')

    @bot.message_handler(commands=['alert'])
    def cmd_alert(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 3:
            send_telegram('⚠️ الاستخدام: /alert SYMBOL PRICE')
            return
        symbol = args[1].upper().strip()
        try:
            target = float(args[2])
        except ValueError:
            send_telegram('❌ السعر غير صالح')
            return
        if target <= 0:
            send_telegram('❌ السعر يجب أن يكون أكبر من صفر')
            return
        df = cached_download(symbol, period='1d', interval='5m')
        if df.empty:
            send_telegram(f'❌ لا توجد بيانات لـ *{symbol}*')
            return
        current = float(df['close'].iloc[-1])
        with state_lock:
            alerts = state.setdefault('price_alerts', {})
            if symbol in alerts:
                send_telegram(f'⚠️ يوجد تنبيه بالفعل لـ *{symbol}*')
                return
            alerts[symbol] = {'price': target, 'triggered': False, 'created_at': time.time()}
            save_state()
        send_telegram(f'🔔 تم تعيين تنبيه *{symbol}* عند ${target:.2f}\nالسعر الحالي: ${current:.2f}')

    @bot.message_handler(commands=['alerts'])
    def cmd_alerts(message):
        if not ensure_authorized(message): return
        with state_lock:
            alerts = dict(state.get('price_alerts', {}))
        if not alerts:
            send_telegram('📭 لا توجد تنبيهات')
            return
        msg = '🔔 *التنبيهات السعرية:*\n━━━━━━━━━━━━━━━━\n'
        for symbol, data in alerts.items():
            price = float(data.get('price', 0) if isinstance(data, dict) else data)
            triggered = data.get('triggered', False) if isinstance(data, dict) else False
            msg += f'• *{symbol}* — ${price:.2f} ({"✅ منفذ" if triggered else "🟢 مفعل"})\n'
        send_telegram(msg)

    @bot.message_handler(commands=['delalert'])
    def cmd_delalert(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram('⚠️ الاستخدام: /delalert SYMBOL')
            return
        symbol = args[1].upper().strip()
        with state_lock:
            alerts = state.setdefault('price_alerts', {})
            if symbol not in alerts:
                send_telegram(f'⚠️ لا يوجد تنبيه لـ *{symbol}*')
                return
            del alerts[symbol]
            save_state()
        send_telegram(f'✅ تم حذف التنبيه لـ *{symbol}*')

    @bot.message_handler(commands=['realbuy'])
    def cmd_realbuy(message):
        if not ensure_authorized(message): return
        # الاسم قديم في المرفق؛ التنفيذ هنا Paper Trading وتوصية تحليلية فقط.
        threading.Thread(target=handle_realbuy, args=(message,), daemon=True).start()

    def handle_realbuy(message):
        try:
            args = message.text.split()
            if len(args) != 3:
                send_telegram('⚠️ الاستخدام: /realbuy SYMBOL QTY')
                return
            symbol, quantity = args[1].upper().strip(), int(args[2])
            if quantity <= 0:
                send_telegram('❌ الكمية يجب أن تكون أكبر من صفر')
                return
            send_telegram(f'🔍 جاري تحليل Paper Trading لـ *{symbol}*...')
            df = cached_download(symbol, period='5d', interval='5m')
            if df.empty or len(df) < 20:
                send_telegram(f'❌ لا توجد بيانات كافية لـ *{symbol}*')
                return
            df.columns = [str(c).lower() for c in df.columns]
            df = compute_indicators(df)
            price = float(df['close'].iloc[-1])
            rvol = get_unified_rvol(df)
            rsi = float(df['rsi'].iloc[-1]) if 'rsi' in df else 50.0
            vwap = float(df['vwap'].iloc[-1]) if 'vwap' in df else price
            atr = float(calculate_atr(df))
            stop = round(price - max(atr * 1.5, price * 0.05), 4)
            if stop <= 0 or stop >= price:
                send_telegram('❌ لا يمكن حساب وقف صالح؛ لا توجد توصية')
                return
            size = min(quantity, calculate_position_size_from_stop(price, stop), 1000)
            if size < 1:
                send_telegram('❌ حجم الصفقة المحسوب أقل من سهم واحد؛ لا توجد توصية')
                return
            score = 0
            reasons, warnings = [], []
            if rvol >= 3: score += 30; reasons.append(f'RVOL قوي {rvol:.1f}x')
            elif rvol >= 2: score += 15; reasons.append(f'RVOL مقبول {rvol:.1f}x')
            else: warnings.append(f'RVOL ضعيف {rvol:.1f}x')
            if 40 <= rsi <= 70: score += 20; reasons.append(f'RSI متوازن {rsi:.0f}')
            elif rsi > 70: warnings.append(f'RSI مرتفع {rsi:.0f}')
            if price > vwap: score += 20; reasons.append('فوق VWAP')
            else: warnings.append('تحت VWAP')
            if atr < price * 0.10: score += 15; reasons.append('تقلب قابل للإدارة')
            score = min(max(score, 0), 100)
            target = round(price + 2 * (price - stop), 4)
            decision = 'BUY' if score >= 70 and len(warnings) <= 1 else 'WATCH' if score >= 45 else 'AVOID'
            msg = (f'🧠 *Paper Recommendation: {symbol}*\n━━━━━━━━━━━━━━━━\n'
                   f'📌 القرار: *{decision}* | السكور: *{score}/100*\n'
                   f'💰 الدخول الافتراضي: ${price:.4f}\n📦 الحجم المحسوب: {size} سهم\n'
                   f'🎯 الهدف: ${target:.4f}\n🛑 الوقف: ${stop:.4f}\n'
                   f'📐 R:R: 1:2\n━━━━━━━━━━━━━━━━\n'
                   f'✅ {" | ".join(reasons) if reasons else "لا توجد عوامل كافية"}\n'
                   f'⚠️ {" | ".join(warnings) if warnings else "لا تحذيرات رئيسية"}\n'
                   f'⚠️ توصية Paper Trading وليست تنفيذًا حقيقيًا')
            send_telegram(msg)
            with state_lock:
                state.setdefault('real_trades', {})[symbol] = {'entry': price, 'tp': target, 'sl': stop, 'size': size, 'score': score, 'time': time.time(), 'status': 'PAPER_ACTIVE'}
                save_state()
        except Exception as error:
            logger.error(f'Paper recommendation error: {error}')
            send_telegram(f'❌ خطأ: {error}')

    @bot.message_handler(commands=['realsell'])
    def cmd_realsell(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram('⚠️ الاستخدام: /realsell SYMBOL')
            return
        symbol = args[1].upper().strip()
        with state_lock:
            trade = state.setdefault('real_trades', {}).pop(symbol, None)
            save_state()
        if not trade:
            send_telegram(f'❌ لا توجد توصية Paper مفتوحة لـ *{symbol}*')
            return
        df = cached_download(symbol, period='1d', interval='5m')
        price = float(df['close'].iloc[-1]) if not df.empty else 0.0
        pnl = (price - float(trade['entry'])) * int(trade.get('size', 0))
        send_telegram(f'📊 *إغلاق Paper Recommendation: {symbol}*\nالدخول: ${float(trade["entry"]):.4f}\nالخروج: ${price:.4f}\nPnL: ${pnl:+.2f}')

    @bot.message_handler(commands=['halt'])
    def cmd_halt(message):
        if not ensure_authorized(message): return
        with state_lock:
            halted = list(state.get('halted_stocks', {}).keys())[:20]
        if not halted:
            send_telegram('✅ لا توجد أسهم موقوفة حاليًا')
            return
        send_telegram('⛔ *الأسهم الموقوفة:*\n━━━━━━━━━━━━━━━━\n' + ''.join(f'• *{h.split("_")[0]}*\n' for h in halted))

    @bot.message_handler(commands=['clearhalt'])
    def cmd_clearhalt(message):
        """/clearhalt SYMBOL — حذف حالة إيقاف قديمة من القائمة المؤقتة."""
        if not ensure_authorized(message):
            return
        args = message.text.split()
        if len(args) != 2:
            send_telegram('⚠️ الاستخدام: /clearhalt SYMBOL')
            return
        symbol = args[1].upper().strip()
        with state_lock:
            existed = symbol in state.setdefault('pending_halts', {})
            state['pending_halts'].pop(symbol, None)
            if existed:
                save_state()
        send_telegram(f'✅ تم حذف *{symbol}* من قائمة الإيقاف المؤقتة' if existed else f'ℹ️ *{symbol}* غير موجود في قائمة الإيقاف المؤقتة')

    def _market_list_command(message, screener_id, title, emoji):
        if not ensure_authorized(message): return
        try:
            url = f'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds={screener_id}&count=10&formatted=false'
            response = requests.get(url, impersonate='chrome120', timeout=10)
            if response.status_code != 200:
                send_telegram('❌ تعذر جلب بيانات السوق')
                return
            quotes = response.json().get('finance', {}).get('result', [{}])[0].get('quotes', [])
            msg = f'{emoji} *{title}:*\n━━━━━━━━━━━━━━━━\n'
            for index, quote in enumerate(quotes[:10], 1):
                symbol = quote.get('symbol', '???')
                price = quote.get('regularMarketPrice', 0) or 0
                change = quote.get('regularMarketChangePercent', 0) or 0
                if price:
                    msg += f'{index}. *{symbol}* — ${float(price):.2f} ({float(change):+.1f}%)\n'
            send_telegram(msg if len(msg) > 60 else '📭 لا توجد بيانات')
        except Exception as error:
            logger.warning(f'Market list error: {error}')
            send_telegram('❌ تعذر جلب قائمة السوق حاليًا')

    @bot.message_handler(commands=['gainers'])
    def cmd_gainers(message):
        _market_list_command(message, 'day_gainers', 'أكبر الرابحين اليوم', '📈')

    @bot.message_handler(commands=['losers'])
    def cmd_losers(message):
        _market_list_command(message, 'day_losers', 'أكبر الخاسرين اليوم', '📉')


    threading.Thread(target=price_alert_monitor, daemon=True).start()
    threading.Thread(target=watchlist_monitor, daemon=True, name='WatchlistMonitor').start()
    logger.info('Price Alert Monitor started')



# ================================================================
# 🔍 DRCT STYLE ANALYSIS — تحليل شمعة 4 ساعات والشورت
# ================================================================

def analyze_candle_pattern(df_4h, df_daily):
    """يحلل شمعة 4 ساعات هابطة مع ذيل طويل وRSI منخفض؛ تحليل Paper فقط."""
    if df_4h is None or df_daily is None or df_4h.empty or len(df_4h) < 5:
        return None
    last = df_4h.iloc[-1]
    prev = df_4h.iloc[-2]
    high, low = float(prev['high']), float(prev['low'])
    close, open_price = float(prev['close']), float(prev['open'])
    body = abs(close - open_price)
    lower_wick = min(open_price, close) - low
    upper_wick = high - max(open_price, close)
    rsi_4h = float(df_4h['rsi'].iloc[-1]) if 'rsi' in df_4h.columns else 50.0
    rsi_daily = float(df_daily['rsi'].iloc[-1]) if 'rsi' in df_daily.columns else 50.0
    daily = df_daily.iloc[-1]
    daily_close, daily_high, daily_low = float(daily['close']), float(daily['high']), float(daily['low'])
    distance = (close - low) / max(low, 1e-9) * 100
    reasons = []
    bearish = close < open_price
    ready = rsi_4h < 30 and distance < 10 and bearish and lower_wick > body * 1.5
    reasons.append(f'✅ السعر قريب من الذيل ({distance:.1f}%)' if distance < 10 else f'⏳ السعر بعيد عن الذيل ({distance:.1f}%)')
    reasons.append(f'✅ RSI مفرط في البيع ({rsi_4h:.0f})' if rsi_4h < 30 else f'⏳ RSI ليس مفرطًا ({rsi_4h:.0f})')
    reasons.append('✅ شمعة هابطة' if bearish else '⏳ ليست شمعة هابطة')
    reasons.append(f'✅ ذيل طويل ({lower_wick / max(body, 1e-9):.1f}x)' if lower_wick > body * 1.5 else '⏳ ذيل قصير نسبيًا')
    return {
        'candle_high': round(high, 4), 'candle_low': round(low, 4),
        'candle_close': round(close, 4), 'candle_open': round(open_price, 4),
        'is_bearish': bearish, 'lower_wick': round(lower_wick, 4),
        'upper_wick': round(upper_wick, 4), 'wick_ratio': round(lower_wick / max(body, 1e-9), 2),
        'rsi_4h': round(rsi_4h, 1), 'rsi_daily': round(rsi_daily, 1),
        'target': round(high, 4), 'stop': round(low * 0.95, 4),
        'entry_zone': round(low, 4), 'is_ready': ready, 'reasons': reasons,
        'daily_close': round(daily_close, 4), 'daily_high': round(daily_high, 4), 'daily_low': round(daily_low, 4),
    }


def get_short_interest(symbol):
    """يجلب بيانات الشورت اختياريًا؛ الفشل لا يعطل التحليل."""
    try:
        info = yf.Ticker(symbol).info
        return {
            'percent': round(float(info.get('shortPercentOfFloat') or 0) * 100, 2),
            'volume': int(info.get('shortVolume') or 0),
            'available': int(info.get('sharesShort') or 0),
        }
    except Exception as error:
        logger.debug(f'Short interest unavailable for {symbol}: {error}')
        return None


if bot:
    @bot.message_handler(commands=['candle'])
    def cmd_candle_analysis(message):
        """تحليل شمعة 4 ساعات + RSI + الشورت، دون تنفيذ تداول."""
        if not ensure_authorized(message):
            return
        args = message.text.split()
        if len(args) != 2:
            send_telegram('⚠️ الاستخدام: /candle SYMBOL')
            return
        symbol = args[1].upper().strip()
        try:
            send_telegram(f'🔍 جاري تحليل *{symbol}*...')
            df_4h = cached_download(symbol, period='5d', interval='1h')
            df_daily = cached_download(symbol, period='60d', interval='1d')
            if df_4h.empty or len(df_4h) < 10 or df_daily.empty or len(df_daily) < 5:
                send_telegram(f'❌ لا توجد بيانات كافية لـ *{symbol}*')
                return
            df_4h.columns = [str(c).lower() for c in df_4h.columns]
            df_daily.columns = [str(c).lower() for c in df_daily.columns]
            analysis = analyze_candle_pattern(compute_indicators(df_4h), compute_indicators(df_daily))
            if not analysis:
                send_telegram(f'❌ تعذر تحليل *{symbol}*')
                return
            short = get_short_interest(symbol)
            target, entry, stop = analysis['target'], analysis['entry_zone'], analysis['stop']
            short_line = f"📉 الشورت: {short['percent']:.2f}%\n" if short else '📉 بيانات الشورت: غير متاحة\n'
            readiness = '✅ جاهز للمراقبة عند اختبار الذيل' if analysis['is_ready'] else '⏳ غير جاهز حاليًا'
            msg = (f"📊 *تحليل الشمعة: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                   f"🕯️ High: ${analysis['candle_high']:.4f} | Low: ${analysis['candle_low']:.4f}\n"
                   f"🕯️ {'هابطة' if analysis['is_bearish'] else 'صاعدة'} | ذيل: {analysis['wick_ratio']:.1f}x\n"
                   f"📊 RSI 4h: {analysis['rsi_4h']:.1f} | RSI يومي: {analysis['rsi_daily']:.1f}\n"
                   f"{short_line}━━━━━━━━━━━━━━━━\n"
                   f"🎯 الدخول الافتراضي: ${entry:.4f}\n🎯 الهدف: ${target:.4f}\n🛑 الوقف: ${stop:.4f}\n"
                   f"━━━━━━━━━━━━━━━━\n{chr(10).join(analysis['reasons'])}\n\n{readiness}\n"
                   f"⚠️ تحليل Paper Trading وليس توصية أو تنفيذًا حقيقيًا")
            send_telegram(msg)
        except Exception as error:
            logger.error(f'Candle analysis error {symbol}: {error}')
            send_telegram(f'❌ تعذر تحليل *{symbol}*')


def bearish_candle_scanner():
    """يبحث كل ساعة عن نمط شمعة هابطة مؤهل ويرسل تنبيهًا Paper فقط."""
    while True:
        try:
            if not is_trading_day() or get_market_phase() == 'CLOSED':
                time.sleep(1800)
                continue
            with state_lock:
                tickers = list(state.get('full_tickers') or state.get('tickers', []))[:500]
            for symbol in tickers:
                key = f'bearish_candle_{symbol}_{now_est().date()}'
                with state_lock:
                    if key in state.get('seen_signals', {}):
                        continue
                df_4h = cached_download(symbol, period='5d', interval='1h')
                df_daily = cached_download(symbol, period='60d', interval='1d')
                if df_4h.empty or len(df_4h) < 10 or df_daily.empty or len(df_daily) < 5:
                    continue
                df_4h.columns = [str(c).lower() for c in df_4h.columns]
                df_daily.columns = [str(c).lower() for c in df_daily.columns]
                analysis = analyze_candle_pattern(compute_indicators(df_4h), compute_indicators(df_daily))
                if not analysis or not analysis['is_ready']:
                    continue
                send_telegram(f"🔍 *شمعة هابطة مؤهلة: {symbol}*\nRSI 4h: {analysis['rsi_4h']:.1f}\nالدخول الافتراضي: ${analysis['entry_zone']:.4f}\nالهدف: ${analysis['target']:.4f}\nالوقف: ${analysis['stop']:.4f}\n⚠️ Paper Trading فقط")
                with state_lock:
                    state.setdefault('seen_signals', {})[key] = time.time()
                    save_state()
                time.sleep(1)
            time.sleep(3600)
        except Exception as error:
            logger.error(f'Bearish candle scanner error: {error}')
            time.sleep(60)




# ================================================================
# 🚀 EARLY HUNTER ENGINE — Full Sweep → Hot Watchlist → 3-Level Alerts
# ================================================================

EARLY_HUNTER_ENABLED = True
EARLY_HUNTER_SWEEP_INTERVAL = 300       # 5 دقائق
EARLY_HUNTER_HOT_INTERVAL = 15          # 15 ثانية
EARLY_HUNTER_MAX_HOT = 300
EARLY_HUNTER_MAX_DEEP = 100
EARLY_HUNTER_ALERT_COOLDOWN = 3600


# ================================================================
# 🪙 PENNY EARLY HUNTER — فلتر مبكر للأسهم تحت دولار
# ================================================================
PENNY_EARLY_MIN_PRICE = 0.05
PENNY_EARLY_MAX_PRICE = 1.00
PENNY_EARLY_MIN_VOLUME = 50000
PENNY_EARLY_MIN_RVOL = 2.0
PENNY_EARLY_BONUS = 20


def penny_early_metrics(price, volume, rvol):
    """فلتر محافظ؛ الحجم هنا proxy للنشاط وليس إثباتًا لـ Low Float."""
    penny = PENNY_EARLY_MIN_PRICE <= float(price) <= PENNY_EARLY_MAX_PRICE
    volume_ok = float(volume) >= PENNY_EARLY_MIN_VOLUME
    rvol_ok = float(rvol) >= PENNY_EARLY_MIN_RVOL
    return {
        "is_penny": penny,
        "volume_ok": volume_ok,
        "rvol_ok": rvol_ok,
        "qualifies": penny and volume_ok and rvol_ok,
        # ليس Low Float حقيقيًا؛ يحتاج float shares من مصدر موثوق.
        "low_float_proxy": penny and float(volume) < 1_000_000,
    }


def full_sweep_quick():
    """مسح خفيف لقائمة الأسهم وإنتاج Hot Watchlist دون أي تنفيذ تداول."""
    try:
        if not EARLY_HUNTER_ENABLED or get_market_phase() == "CLOSED":
            return []
        with state_lock:
            all_tickers = list(state.get("full_tickers") or state.get("tickers", []))
        if not all_tickers:
            return []
        random.shuffle(all_tickers)
        candidates = []
        for symbol in all_tickers:
            try:
                df = cached_download(symbol, period="1d", interval="5m")
                if df.empty or len(df) < 5:
                    continue
                df.columns = [str(c).lower() for c in df.columns]
                price = float(df["close"].iloc[-1])
                volume = float(df["volume"].iloc[-1] or 0)
                avg_volume = float(df["volume"].tail(30).mean() or 0)
                open_price = float(df["open"].iloc[0])
                if not (PENNY_EARLY_MIN_PRICE <= price <= 30) or volume < 30000 or avg_volume <= 0 or open_price <= 0:
                    continue
                rvol = volume / max(avg_volume, 1.0)
                penny_metrics = penny_early_metrics(price, volume, rvol)
                if price <= PENNY_EARLY_MAX_PRICE and not penny_metrics["qualifies"]:
                    continue
                change = (price - open_price) / open_price * 100
                if rvol < 1.2 or change < 0.5 or change > 8:
                    continue
                day_low = float(df["low"].min())
                day_high = float(df["high"].max())
                range_pct = (price - day_low) / max(day_high - day_low, 0.01) * 100
                if range_pct > 60:
                    continue
                score = min(100, max(0, int(rvol * 10) + int(change * 2) + max(0, 20 - int(range_pct / 3))))
                if penny_metrics["qualifies"]:
                    score = min(100, score + PENNY_EARLY_BONUS)
                try:
                    cumulative_volume = df["volume"].cumsum().iloc[-1]
                    vwap = float((df["close"] * df["volume"]).cumsum().iloc[-1] / cumulative_volume) if cumulative_volume else price
                    if price > vwap:
                        score = min(100, score + 15)
                except Exception:
                    pass
                candidates.append({
                    "symbol": symbol, "price": round(price, 4), "rvol": round(rvol, 2),
                    "change": round(change, 2), "volume": int(volume), "score": score,
                    "penny_early": bool(penny_metrics["qualifies"]),
                    "timestamp": time.time()
                })
            except Exception as error:
                logger.debug(f"[FullSweepQuick] {symbol}: {error}")
                continue
        candidates.sort(key=lambda item: item["score"], reverse=True)
        candidates = candidates[:EARLY_HUNTER_MAX_HOT]
        with state_lock:
            state["hot_watchlist"] = candidates
            state["hot_watchlist_timestamp"] = time.time()
            save_state()
        logger.info(f"[FullSweepQuick] candidates={len(candidates)}")
        return candidates
    except Exception as error:
        logger.error(f"[FullSweepQuick] error: {error}")
        return []


def send_breakout_alert(symbol, price, score, rvol, level, details=None, atr=0.0):
    """يرسل مستوى Early/Pre-Breakout/Breakout مرة واحدة لكل مستوى خلال الكولداون."""
    level_key = level.replace("🔴 ", "").replace("🟠 ", "").replace("🟡 ", "").replace(" ", "_").lower()
    signal_key = f"early_hunter_{level_key}_{symbol}_{now_est().date()}"
    now_ts = time.time()
    with state_lock:
        last_sent = state.get("seen_signals", {}).get(signal_key, 0)
        if isinstance(last_sent, (int, float)) and now_ts - last_sent < EARLY_HUNTER_ALERT_COOLDOWN:
            return False
    tp1 = price * (1.05 if "EARLY" in level else 1.08)
    tp2 = price * 1.12
    sl = price * 0.95
    msg = (
        f"{level} — *{symbol}*\n━━━━━━━━━━━━━━━━\n"
        f"💰 السعر: *${price:.4f}*\n📊 السكور: *{score}/100*\n🔥 RVOL: *{rvol:.1f}x*\n"
        f"📏 ATR: ${atr:.4f}\n━━━━━━━━━━━━━━━━\n"
    )
    if details:
        msg += "📌 *العوامل:*\n" + "".join(f"   • {item}\n" for item in details[:6])
    if "EARLY" in level:
        msg += f"🎯 مراقبة: ${tp1:.4f} (+5%)\n🛑 وقف نظري: ${sl:.4f}\n💡 بداية حركة محتملة\n"
    elif "PRE-BREAKOUT" in level:
        msg += f"🎯 منطقة مراقبة: ${price:.4f}\n🎯 هدف: ${tp1:.4f} (+8%)\n🛑 وقف نظري: ${sl:.4f}\n💡 الزخم يتسارع\n"
    else:
        msg += f"🎯 هدف 1: ${tp1:.4f} (+8%)\n🎯 هدف 2: ${tp2:.4f} (+12%)\n🛑 وقف نظري: ${sl:.4f}\n🚀 الاختراق بدأ\n"
    msg += f"⏰ {now_saudi().strftime('%I:%M %p')} 🇸🇦\n⚠️ إشارة Paper وليست تنفيذًا أو ضمانًا"
    send_telegram(msg)
    with state_lock:
        state.setdefault("seen_signals", {})[signal_key] = now_ts
        save_state()
    return True


# ================================================================
# 🧠 RANKING ENGINE — Top Gainers → Technical Rank → AI
# ================================================================
def hot_watchlist_scanner():
    """يحلل أفضل 20 مرشحاً فنياً، ويحيل أفضل 3 فقط إلى AI النهائي."""
    last_ai_time = {}
    while True:
        try:
            scanner_heartbeat('ranking_engine') if 'scanner_heartbeat' in globals() else None
            if get_market_phase() == 'CLOSED':
                time.sleep(120)
                continue
            with state_lock:
                candidates = list(state.get('hot_watchlist', []))
            if not candidates:
                time.sleep(15)
                continue
            candidates.sort(key=lambda item: float(item.get('change', 0)) * max(1.0, float(item.get('volume', 0) or 0) ** 0.10), reverse=True)
            ranked = []
            for candidate in candidates[:20]:
                try:
                    symbol = str(candidate.get('symbol', '')).upper().strip()
                    if not symbol or symbol in KNOWN_DELISTED:
                        continue
                    df = cached_download(symbol, period='2d', interval='5m')
                    if df is None or df.empty or len(df) < 20:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    df = compute_indicators(df)
                    price = float(df['close'].iloc[-1])
                    if not (0.3 <= price <= MAX_PRICE):
                        continue
                    phase = get_market_phase()
                    rvol = float(get_unified_rvol(df, phase=phase))
                    rsi = float(df['rsi'].iloc[-1]) if 'rsi' in df.columns and pd.notna(df['rsi'].iloc[-1]) else 50.0
                    vwap = float(df['vwap'].iloc[-1]) if 'vwap' in df.columns and pd.notna(df['vwap'].iloc[-1]) else price
                    ema9 = float(df['ema9'].iloc[-1]) if 'ema9' in df.columns and pd.notna(df['ema9'].iloc[-1]) else price
                    ema21 = float(df['ema21'].iloc[-1]) if 'ema21' in df.columns and pd.notna(df['ema21'].iloc[-1]) else price
                    current_volume = float(df['volume'].iloc[-1] or 0)
                    avg_volume = max(float(df['volume'].tail(20).mean() or 0), 1.0)
                    volume_spike = current_volume >= avg_volume * 1.5
                    previous_high = float(df['high'].iloc[-11:-1].max())
                    breakout = price > previous_high
                    above_vwap = price >= vwap
                    ema_bullish = ema9 > ema21
                    day_change = float(candidate.get('change', 0) or 0)
                    score = 0
                    factors = []
                    if rvol >= 4:
                        score += 25; factors.append(f'🔥 RVOL {rvol:.1f}x')
                    elif rvol >= 2.5:
                        score += 18; factors.append(f'📊 RVOL {rvol:.1f}x')
                    elif rvol >= 1.5:
                        score += 10
                    if above_vwap:
                        score += 15; factors.append('فوق VWAP')
                    if ema_bullish:
                        score += 10; factors.append('EMA Bullish')
                    if 40 <= rsi <= 70:
                        score += 10; factors.append(f'RSI {rsi:.0f}')
                    elif 70 < rsi <= 78:
                        score += 5
                    if volume_spike:
                        score += 10; factors.append('Volume Spike')
                    if day_change >= 20:
                        score += 20; factors.append(f'+{day_change:.1f}%')
                    elif day_change >= 10:
                        score += 15; factors.append(f'+{day_change:.1f}%')
                    elif day_change >= 5:
                        score += 8
                    if breakout:
                        score += 10; factors.append('HOD/Breakout')
                    ranked.append({
                        'symbol': symbol, 'price': price, 'change': day_change,
                        'rvol': rvol, 'rsi': rsi, 'vwap': vwap,
                        'above_vwap': above_vwap, 'ema_bullish': ema_bullish,
                        'volume_spike': volume_spike, 'breakout': breakout,
                        'score': min(100, score), 'factors': factors, 'timestamp': time.time(),
                    })
                except Exception as error:
                    logger.debug(f'[Ranking] {candidate.get("symbol")}: {error}')
            ranked.sort(key=lambda item: item['score'], reverse=True)
            top5 = ranked[:5]
            with state_lock:
                state['ai_candidates'] = [dict(item) for item in top5]
                state['ranking_timestamp'] = time.time()
                save_state()
            ai_count = 0
            for candidate in top5:
                if ai_count >= 3 or candidate['score'] < 65:
                    break
                symbol = candidate['symbol']
                now = time.time()
                if now - last_ai_time.get(symbol, 0) < 3600:
                    continue
                last_ai_time[symbol] = now
                if AI_PROCESS_ENABLED:
                    threading.Thread(target=final_trade_recommendation, args=(symbol, 0, 'TOP_GAINER_RANKED', False, True), daemon=True, name=f'TopRankAI-{symbol}').start()
                    ai_count += 1
                    time.sleep(1)
            logger.info('[Ranking] candidates=%d | ranked=%d | top=%s | AI=%d', len(candidates), len(ranked), ', '.join(f"{x['symbol']}:{x['score']}" for x in top5), ai_count)
            time.sleep(30)
        except Exception as error:
            logger.error(f'[RankingEngine] error: {error}')
            time.sleep(60)


def early_hunter_sweep_loop():
    """يشغّل Full Sweep Quick كل خمس دقائق داخل خيط واحد."""
    while True:
        try:
            full_sweep_quick()
            time.sleep(EARLY_HUNTER_SWEEP_INTERVAL)
        except Exception as error:
            logger.error(f"[EarlyHunter] sweep loop error: {error}")
            time.sleep(60)




# ================================================================
# 🚨 PRE-EXPLOSION SCANNER V3 — تغطية السوق وAI كتعزيز فقط
# ================================================================
PRE_EXPLOSION_ENABLED = True
PRE_EXPLOSION_INTERVAL = 30
PRE_EXPLOSION_COOLDOWN = 1800
_sent_pre_explosion = {}

SCANNER_HEARTBEATS = {}
SCANNER_HEARTBEAT_LOCK = threading.Lock()
SCANNER_WATCHDOG_TIMEOUT = 180


def scanner_heartbeat(name):
    with SCANNER_HEARTBEAT_LOCK:
        SCANNER_HEARTBEATS[name] = time.time()


def scanner_watchdog():
    """يراقب حياة الماسحات ويسجل الخمول دون إعادة تشغيل عمياء."""
    while True:
        try:
            now = time.time()
            with SCANNER_HEARTBEAT_LOCK:
                heartbeats = dict(SCANNER_HEARTBEATS)
            for name, last_seen in heartbeats.items():
                age = now - last_seen
                if age > SCANNER_WATCHDOG_TIMEOUT:
                    logger.error(f"[Watchdog] scanner stale: {name} ({age:.0f}s)")
            time.sleep(60)
        except Exception as error:
            logger.error(f"[Watchdog] error: {error}")
            time.sleep(60)


def detect_pre_explosion_v3(symbol, df_1m, df_5m):
    """يكشف بداية الحركة من السعر والحجم دون جعل AI شرطًا قاتلًا."""
    if df_1m is None or df_5m is None or df_5m.empty or len(df_5m) < 20:
        return None
    price = float(df_5m['close'].iloc[-1])
    if price <= 0:
        return None
    phase = get_market_phase()
    rvol = float(get_unified_rvol(df_5m, phase=phase))
    history = []
    for i in range(1, 6):
        volume = float(df_5m['volume'].iloc[-i] or 0)
        prev = df_5m['volume'].iloc[-i-20:-i] if len(df_5m) > i + 20 else df_5m['volume'].iloc[:-i]
        avg_prev = float(prev.mean() or volume or 1)
        history.append(volume / max(avg_prev, 1.0))
    rvol_acceleration = len(history) >= 3 and history[-1] > history[-2] > history[-3]
    closes = df_5m['close'].tail(6).astype(float).tolist()
    lows = df_5m['low'].tail(6).astype(float).tolist()
    price_acceleration = len(closes) >= 5 and (closes[-1] - closes[-3]) > (closes[-3] - closes[-5])
    higher_lows = len(lows) >= 5 and lows[-1] > lows[-3] > lows[-5]
    sr = calculate_support_resistance(df_5m)
    resistance = 0.0
    if isinstance(sr, dict):
        resistance_data = sr.get('resistance') or {}
        if isinstance(resistance_data, dict):
            try:
                resistance = float(resistance_data.get('level') or 0)
            except (TypeError, ValueError):
                resistance = 0.0
    resistance_dist = (resistance - price) / max(price, 0.01) * 100 if resistance else 999
    near_resistance = 0 < resistance_dist < 3
    ema9 = float(df_5m['ema9'].iloc[-1]) if 'ema9' in df_5m else price
    ema21 = float(df_5m['ema21'].iloc[-1]) if 'ema21' in df_5m else price
    ema_bullish = ema9 > ema21
    ema_expanding = (ema9 - ema21) / max(ema21, 0.01) * 100 > 0.5
    vwap = float(df_5m['vwap'].iloc[-1]) if 'vwap' in df_5m else price
    above_vwap = price > vwap
    volumes_1m = df_1m['volume'].tail(5).astype(float).tolist() if not df_1m.empty else []
    volume_spike = len(volumes_1m) >= 3 and volumes_1m[-1] > volumes_1m[-2] * 1.3 and volumes_1m[-2] > volumes_1m[-3] * 1.1
    open_price = float(df_5m['open'].iloc[0] or price)
    day_change = (price - open_price) / max(open_price, 0.01) * 100
    if day_change > 10:
        return None
    score = 0
    signals = []
    if rvol >= 2.5: score += 20; signals.append(f"🔥 RVOL {rvol:.1f}x")
    elif rvol >= 1.8: score += 10; signals.append(f"⚡ RVOL {rvol:.1f}x")
    if rvol_acceleration: score += 15; signals.append("📈 تسارع RVOL")
    if price_acceleration: score += 15; signals.append("🚀 تسارع السعر")
    if higher_lows: score += 10; signals.append("📊 Higher Lows")
    if near_resistance:
        score += 10
        signals.append(f"🎯 قرب مقاومة ({resistance_dist:.1f}%)")
    elif not resistance and rvol >= 1.8 and above_vwap and price_acceleration:
        score += 10
        signals.append("🚀 لا توجد مقاومة قريبة مع تأكيد الزخم")
    if ema_bullish and ema_expanding: score += 10; signals.append("📈 تباعد EMA")
    if above_vwap: score += 10; signals.append("✅ فوق VWAP")
    if volume_spike: score += 10; signals.append("💥 انفجار حجم 1m")
    penny_metrics = penny_early_metrics(price, float(df_5m['volume'].iloc[-1] or 0), rvol)
    if penny_metrics["qualifies"]:
        score += PENNY_EARLY_BONUS
        signals.append("🪙 Penny Early: سعر منخفض + حجم/RVOL مؤكد")
    if score < 55:
        return None
    return {
        'score': min(100, score), 'signals': signals, 'entry': price,
        'target_1': round(price * 1.05, 4), 'target_2': round(price * 1.10, 4),
        'target_3': round(price * 1.15, 4), 'stop': round(price * 0.97, 4),
        'rvol': rvol, 'day_change': day_change, 'resistance_dist': resistance_dist,
        'level': '🔴 HIGH' if score >= 80 else '🟠 MEDIUM' if score >= 70 else '🟡 LOW'
    }


def ai_confirm_pre_explosion(symbol, price, rvol, score, signals, df_5m):
    """تحليل AI تعزيزي فقط؛ لا يمنع التنبيه الفني ولا ينفذ صفقة."""
    if not OPENAI_AVAILABLE or client is None or not can_send_ai_request():
        return None
    try:
        rsi = float(df_5m['rsi'].iloc[-1]) if 'rsi' in df_5m else 50.0
        vwap = float(df_5m['vwap'].iloc[-1]) if 'vwap' in df_5m else price
        prompt = (f"Analyze {symbol} pre-explosion setup. Price ${price:.4f}, RVOL {rvol:.1f}x, "
                  f"RSI {rsi:.1f}, VWAP ${vwap:.4f}, technical score {score}/100, "
                  f"signals: {', '.join(signals)}. Return JSON only: "
                  '{"confidence":0,"setup_quality":"EXCELLENT|GOOD|AVERAGE|POOR",'
                  '"reason_ar":"مختصر","key_risk":"مختصر","key_opportunity":"مختصر"}')
        response = groq_chat_completion(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "You are a conservative pre-explosion analyst. Respond in valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=180,
            timeout=8,
        )
        result = json.loads(response.choices[0].message.content)
        return result if isinstance(result, dict) else None
    except Exception as error:
        logger.debug(f"Pre-explosion AI fallback {symbol}: {error}")
        return None


def pre_explosion_scanner_v3():
    """يفحص Hot Watchlist ومرشحين من السوق الأوسع كل 30 ثانية."""
    while True:
        try:
            scanner_heartbeat("pre_explosion")
            if not PRE_EXPLOSION_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(60)
                continue
            with state_lock:
                hot = list(state.get('hot_watchlist', []))
                active = list(state.get('tickers', []))
                full = [str(s).upper() for s in state.get('full_tickers', []) if s]
            hot_symbols = [str(item.get('symbol') if isinstance(item, dict) else item).upper() for item in hot]
            hot_symbols = list(dict.fromkeys([s for s in hot_symbols if s]))
            active_symbols = [str(s).upper() for s in active if s]
            ordered = list(dict.fromkeys(hot_symbols + active_symbols + full))
            if not ordered:
                time.sleep(30)
                continue
            if not hasattr(pre_explosion_scanner_v3, '_cursor'):
                pre_explosion_scanner_v3._cursor = 0
            cursor = pre_explosion_scanner_v3._cursor
            chunk_size = 250
            if cursor >= len(ordered):
                cursor = 0
            chunk = ordered[cursor:cursor + chunk_size]
            pre_explosion_scanner_v3._cursor = cursor + chunk_size
            candidates = list(dict.fromkeys(hot_symbols + chunk))[:300]
            now = time.time()
            logger.debug(f"[PreExplosion] chunk={len(chunk)} hot={len(hot_symbols)} universe={len(ordered)}")
            for symbol in candidates:
                if _sent_pre_explosion.get(symbol, 0) > now - PRE_EXPLOSION_COOLDOWN:
                    continue
                check_key = f'{symbol}_pre_explosion_check'
                if _sent_pre_explosion.get(check_key, 0) > now - 60:
                    continue
                _sent_pre_explosion[check_key] = now
                try:
                    df_1m = cached_download(symbol, period='1d', interval='1m')
                    df_5m = cached_download(symbol, period='5d', interval='5m')
                    if df_1m.empty or len(df_1m) < 5 or df_5m.empty or len(df_5m) < 20:
                        continue
                    df_1m.columns = [str(c).lower() for c in df_1m.columns]
                    df_5m.columns = [str(c).lower() for c in df_5m.columns]
                    df_5m = compute_indicators(df_5m)
                    result = detect_pre_explosion_v3(symbol, df_1m, df_5m)
                    if not result:
                        continue
                    sig_key = f"pre_explosion_{symbol}_{now_est().date()}"
                    with state_lock:
                        if sig_key in state.get('seen_signals', {}):
                            continue
                    price = float(df_5m['close'].iloc[-1])
                    ai_result = ai_confirm_pre_explosion(symbol, price, result['rvol'], result['score'], result['signals'], df_5m) if result['score'] >= 65 else None
                    ai_text = ''
                    if ai_result:
                        ai_text = (f"━━━━━━━━━━━━━━━━\n🧠 *AI تعزيزي:* ثقة {int(ai_result.get('confidence', 0) or 0)}%\n"
                                   f"{ai_result.get('reason_ar', '')}\n⚠️ الخطر: {ai_result.get('key_risk', 'غير محدد')}\n")
                    msg = (f"🔥 *PRE-EXPLOSION ALERT: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                           f"📊 Score: *{result['score']}/100* | {result['level']}\n💰 السعر: *${price:.4f}*\n"
                           f"🔥 RVOL: *{result['rvol']:.1f}x* | تغير اليوم: {result['day_change']:+.1f}%\n"
                           f"📌 الإشارات:\n" + ''.join(f"   • {s}\n" for s in result['signals']) + ai_text +
                           f"🎯 دخول نظري: ${result['entry']:.4f}\n🎯 أهداف: ${result['target_1']:.4f} / ${result['target_2']:.4f} / ${result['target_3']:.4f}\n"
                           f"🛑 وقف نظري: ${result['stop']:.4f}\n⚠️ إشارة Paper وليست توصية أو تنفيذًا")
                    send_telegram(msg)
                    _sent_pre_explosion[symbol] = now
                    with state_lock:
                        state.setdefault('seen_signals', {})[sig_key] = now
                        save_state()
                    time.sleep(0.2)
                except Exception as error:
                    logger.debug(f'Pre-explosion error {symbol}: {error}')
            time.sleep(PRE_EXPLOSION_INTERVAL)
        except Exception as error:
            logger.error(f'Pre-explosion scanner error: {error}')
            time.sleep(30)


# ================================================================
# 📊 ADVANCED ANALYSIS SCANNER — EMA + Fibonacci + Support/Resistance
# ================================================================
ADVANCED_ANALYSIS_ENABLED = True
ADVANCED_ANALYSIS_INTERVAL = 180
ADVANCED_ANALYSIS_COOLDOWN = 3600
ADVANCED_ANALYSIS_MAX_SYMBOLS = 100
_sent_advanced_analysis = {}


def calculate_fibonacci_levels(high, low):
    high, low = float(high), float(low)
    if high <= low:
        return {}
    diff = high - low
    return {
        '0%': high, '23.6%': high - diff * 0.236,
        '38.2%': high - diff * 0.382, '50.0%': high - diff * 0.500,
        '61.8%': high - diff * 0.618, '78.6%': high - diff * 0.786,
        '100%': low,
    }


def detect_peaks_troughs(df, lookback=60):
    if df is None or len(df) < lookback:
        return [], []
    highs = df['high'].astype(float).to_numpy()
    lows = df['low'].astype(float).to_numpy()
    peaks, troughs = [], []
    for i in range(5, len(df) - 5):
        if highs[i] > max(highs[i-5:i]) and highs[i] > max(highs[i+1:i+6]):
            peaks.append(float(highs[i]))
        if lows[i] < min(lows[i-5:i]) and lows[i] < min(lows[i+1:i+6]):
            troughs.append(float(lows[i]))
    return peaks, troughs


def advanced_analysis(symbol):
    """تحليل يومي محافظ؛ يعيد بيانات فقط ولا ينفذ صفقة."""
    try:
        df = cached_download(symbol, period='120d', interval='1d')
        if df is None or df.empty or len(df) < 60:
            return None
        df.columns = [str(c).lower() for c in df.columns]
        required = {'close', 'high', 'low'}
        if not required.issubset(df.columns):
            return None
        close = df['close'].astype(float)
        price = float(close.iloc[-1])
        ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(close.ewm(span=50, adjust=False).mean().iloc[-1])
        ema100 = float(close.ewm(span=100, adjust=False).mean().iloc[-1])
        high_60 = float(df['high'].tail(60).max())
        low_60 = float(df['low'].tail(60).min())
        fib = calculate_fibonacci_levels(high_60, low_60)
        if not fib:
            return None
        peaks, troughs = detect_peaks_troughs(df)
        resistance_candidates = sorted({x for x in peaks if x > price})
        support_candidates = sorted({x for x in troughs if x < price}, reverse=True)
        resistance = resistance_candidates[0] if resistance_candidates else fib.get('23.6%')
        support = support_candidates[0] if support_candidates else fib.get('78.6%')
        stop_loss = min(float(support), price * 0.92) if support and support < price else price * 0.92
        risk = max(price - stop_loss, price * 0.01)
        targets = [float(level) for level in (fib.get('23.6%'), fib.get('38.2%'), fib.get('50.0%'), fib.get('61.8%')) if level and level > price]
        if not targets:
            targets = [x for x in resistance_candidates if x > price]
        targets = sorted(set(round(x, 4) for x in targets))[:4]
        rr = round((targets[0] - price) / risk, 2) if targets and targets[0] > price else 0.0
        return {
            'symbol': symbol, 'price': round(price, 4),
            'ema20': round(ema20, 4), 'ema50': round(ema50, 4), 'ema100': round(ema100, 4),
            'support': round(float(support), 4) if support else None,
            'resistance': round(float(resistance), 4) if resistance else None,
            'targets': targets, 'stop_loss': round(stop_loss, 4), 'rr': rr,
            'fib': fib, 'above_ema20': price >= ema20,
        }
    except Exception as error:
        logger.debug(f'[AdvancedAnalysis] {symbol}: {error}')
        return None


def _format_advanced_analysis(result):
    price = result['price']
    msg = f"📊 *تحليل متقدم: {result['symbol']}*\n━━━━━━━━━━━━━━━━\n💰 السعر الحالي: *${price:.4f}*\n"
    msg += '\n📊 *المتوسطات:*\n'
    for key in ('ema20', 'ema50', 'ema100'):
        mark = '✅' if price >= result[key] else '❌'
        msg += f"• {key.upper()}: ${result[key]:.4f} {mark}\n"
    msg += '\n📍 *المستويات:*\n'
    msg += f"• الدعم: ${result['support']:.4f}\n• المقاومة: ${result['resistance']:.4f}\n"
    msg += f"• وقف Paper: ${result['stop_loss']:.4f}\n"
    if result['targets']:
        msg += '\n🎯 *الأهداف:*\n'
        for index, target in enumerate(result['targets'], 1):
            gain = (target - price) / max(price, 1e-9) * 100
            msg += f"• T{index}: ${target:.4f} ({gain:+.1f}%)\n"
    msg += f"\n📐 *R:R: {result['rr']:.2f}:1*"
    msg += ' — ممتازة' if result['rr'] >= 3 else (' — جيدة' if result['rr'] >= 2 else ' — أقل من الحد المفضل')
    msg += '\n⚠️ تحليل Paper وليس توصية مالية'
    return msg


def advanced_analysis_scanner():
    while True:
        try:
            if not ADVANCED_ANALYSIS_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(ADVANCED_ANALYSIS_INTERVAL)
                continue
            with state_lock:
                tickers = list(state.get('tickers', []))[:ADVANCED_ANALYSIS_MAX_SYMBOLS]
            for symbol in tickers:
                if time.time() - _sent_advanced_analysis.get(symbol, 0) < ADVANCED_ANALYSIS_COOLDOWN:
                    continue
                result = advanced_analysis(symbol)
                if not result or not result['targets'] or result['rr'] < 1.5 or result['price'] < result['ema20'] * 0.95:
                    continue
                key = f"advanced_{symbol}_{now_est().date()}"
                with state_lock:
                    if key in state.get('seen_signals', {}):
                        continue
                send_telegram(_format_advanced_analysis(result))
                _sent_advanced_analysis[symbol] = time.time()
                with state_lock:
                    state.setdefault('seen_signals', {})[key] = time.time()
                    save_state()
                time.sleep(1.0)
            gc.collect()
            time.sleep(ADVANCED_ANALYSIS_INTERVAL)
        except Exception as error:
            logger.error(f'[AdvancedAnalysis] scanner error: {error}')
            time.sleep(60)


# ================================================================
# 🔗 UNIFIED SCANNERS — تقليل الطلبات والخيوط مع الحفاظ على التغطية
# ================================================================
UNIFIED_SCANNERS_ENABLED = True
UNIFIED_SR_INTERVAL = 60
UNIFIED_PATTERN_INTERVAL = 300
UNIFIED_POWER_INTERVAL = 120
UNIFIED_SCANNER_MAX_SYMBOLS = 150
_unified_sr_sent = {}
_unified_pattern_sent = {}
_unified_power_sent = {}


def _unified_should_send(store, symbol, cooldown=1800):
    return time.time() - store.get(symbol, 0) >= cooldown


def _format_unified_vwap(symbol, result):
    price = result.get('price', 0)
    targets = result.get('targets', [])
    msg = f"🔗 *VWAP/SR Unified: {symbol}*\n━━━━━━━━━━━━━━━━\n💰 السعر: ${price:.4f}\n"
    msg += f"📏 VWAP: ${result.get('vwap', 0):.4f}\n📍 الدعم: ${result.get('support', 0):.4f}\n📍 المقاومة: ${result.get('resistance', 0):.4f}\n"
    msg += f"🔥 RVOL: {result.get('rvol', 0):.1f}x | R:R: {result.get('rr', 0):.2f}:1\n"
    if targets:
        msg += '🎯 الأهداف: ' + ' → '.join(f'${float(x):.4f}' for x in targets[:3]) + '\n'
    msg += '⚠️ تحليل Paper ومراقبة فنية فقط'
    return msg


def unified_sr_scanner():
    """يجمع SR وVWAP من تحميل 5m واحد لكل سهم مع كولداون للإشعارات."""
    while True:
        try:
            scanner_heartbeat('unified_sr') if 'scanner_heartbeat' in globals() else None
            if not UNIFIED_SCANNERS_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(UNIFIED_SR_INTERVAL)
                continue
            with state_lock:
                tickers = list(state.get('tickers', []))[:UNIFIED_SCANNER_MAX_SYMBOLS]
            for symbol in tickers:
                if not _unified_should_send(_unified_sr_sent, symbol):
                    continue
                df = cached_download(symbol, period='5d', interval='5m')
                if df is None or df.empty:
                    continue
                df.columns = [str(c).lower() for c in df.columns]
                sr = calculate_support_resistance(df)
                vwap = analyze_vwap_bounce(symbol, df_5m=df)
                if vwap and vwap.get('score', 0) >= 50:
                    send_telegram(_format_unified_vwap(symbol, vwap))
                    _unified_sr_sent[symbol] = time.time()
                elif sr:
                    alert, _ = should_alert_support_resistance(symbol, sr)
                    if alert:
                        send_telegram(format_support_resistance_message(symbol, sr, interval_label='5m'))
                        _unified_sr_sent[symbol] = time.time()
                time.sleep(0.15)
            gc.collect()
            time.sleep(UNIFIED_SR_INTERVAL)
        except Exception as error:
            logger.error(f'[Unified SR] scanner error: {error}')
            time.sleep(60)


def should_send_candle_signal(symbol, price, pattern, df):
    """فلتر محافظ لإشارات الشموع: سعر، حجم، EMA50، RSI، ومنع الإشارات الضعيفة."""
    try:
        price = float(price)
        if not (0.50 <= price <= 200.0):
            return False, 'سعر خارج النطاق'
        work = df.copy()
        work.columns = [str(c).lower() for c in work.columns]
        if 'volume' not in work.columns or 'close' not in work.columns or len(work) < 20:
            return False, 'بيانات غير كافية'
        volume = float(work['volume'].iloc[-1] or 0)
        avg_volume = float(work['volume'].tail(20).mean() or 0)
        if avg_volume <= 0 or volume < avg_volume * 1.5:
            return False, 'حجم أقل من 1.5x المتوسط'
        ema50 = float(work['close'].astype(float).ewm(span=50, adjust=False).mean().iloc[-1])
        if price < ema50 * 0.98:
            return False, 'تحت EMA50'
        if 'rsi' not in work.columns:
            try:
                work = compute_indicators(work)
            except Exception:
                pass
        rsi = float(work['rsi'].iloc[-1]) if 'rsi' in work.columns and pd.notna(work['rsi'].iloc[-1]) else 50.0
        if rsi > 75:
            return False, 'RSI متشبع شراء'
        pattern_name = pattern.get('type', pattern.get('pattern', 'candle')) if isinstance(pattern, dict) else str(pattern)
        return True, f'✅ {pattern_name} | RVOL {volume / max(avg_volume, 1e-9):.1f}x | فوق EMA50 | RSI {rsi:.0f}'
    except Exception as error:
        logger.debug(f'[CandleFilter] {symbol}: {error}')
        return False, 'خطأ في الفلتر'


def unified_pattern_scanner():
    """يجمع أنماط الشموع والنماذج اليومية مع فلترة جودة ومنع الإزعاج."""
    while True:
        try:
            scanner_heartbeat('unified_patterns') if 'scanner_heartbeat' in globals() else None
            if not UNIFIED_SCANNERS_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(UNIFIED_PATTERN_INTERVAL)
                continue
            with state_lock:
                tickers = list(state.get('tickers', []))[:UNIFIED_SCANNER_MAX_SYMBOLS]
            for symbol in tickers:
                if not _unified_should_send(_unified_pattern_sent, symbol, 3600):
                    continue
                df = cached_download(symbol, period='90d', interval='1d')
                if df is None or df.empty or len(df) < 50:
                    continue
                df.columns = [str(c).lower() for c in df.columns]
                candles = analyze_candle_patterns(df)
                charts = chart_patterns(df)
                if not candles and not charts:
                    continue
                bullish = [p for p in candles + charts if isinstance(p, dict) and p.get('bullish')]
                if not bullish:
                    continue
                price = float(df['close'].iloc[-1])
                approved = []
                filter_reasons = []
                for pattern in bullish:
                    ok, reason = should_send_candle_signal(symbol, price, pattern, df)
                    if ok:
                        approved.append(pattern)
                    else:
                        filter_reasons.append(reason)
                if not approved:
                    logger.debug(f'[Unified Patterns] filtered {symbol}: {", ".join(filter_reasons[:3])}')
                    continue
                msg = f"🕯️ *Unified Patterns: {symbol}*\n━━━━━━━━━━━━━━━━\n💰 السعر: ${price:.4f}\n"
                for item in approved[:4]:
                    msg += f"• {item.get('type', item.get('pattern', 'Pattern'))}\n"
                msg += '📊 اجتاز فلتر السعر والحجم وEMA50 وRSI\n⚠️ يحتاج تأكيد الإغلاق؛ Paper فقط'
                send_telegram(msg)
                _unified_pattern_sent[symbol] = time.time()
                time.sleep(0.25)
            gc.collect()
            time.sleep(UNIFIED_PATTERN_INTERVAL)
        except Exception as error:
            logger.error(f'[Unified Patterns] scanner error: {error}')
            time.sleep(60)


def unified_power_scanner():
    """يجمع RVOL والزخم وPower Runner في تحميل 15m واحد لكل سهم."""
    while True:
        try:
            scanner_heartbeat('unified_power') if 'scanner_heartbeat' in globals() else None
            if not UNIFIED_SCANNERS_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(UNIFIED_POWER_INTERVAL)
                continue
            with state_lock:
                tickers = list(state.get('tickers', []))[:UNIFIED_SCANNER_MAX_SYMBOLS]
            for symbol in tickers:
                if not _unified_should_send(_unified_power_sent, symbol, 1200):
                    continue
                df = cached_download(symbol, period='5d', interval='15m')
                if df is None or df.empty or len(df) < 25:
                    continue
                df.columns = [str(c).lower() for c in df.columns]
                try:
                    price = float(df['close'].iloc[-1])
                    previous = float(df['close'].iloc[-2])
                    base = float(df['close'].iloc[0])
                    gain = (price - base) / max(abs(base), 1e-9) * 100
                    rvol = float(get_unified_rvol(df) if 'get_unified_rvol' in globals() else calculate_rvol(df))
                    work = compute_indicators(df)
                    rsi = float(work['rsi'].iloc[-1]) if 'rsi' in work.columns and pd.notna(work['rsi'].iloc[-1]) else 50.0
                    momentum = price > previous and price >= float(df['high'].tail(3).iloc[:-1].max())
                except Exception:
                    continue
                if not ((rvol >= 3.0 and momentum) or (gain >= 15.0 and 55 <= rsi <= 70)):
                    continue
                msg = (f"⚡ *Unified Power Signal: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                       f"💰 السعر: ${price:.4f}\n📈 التغير: {gain:+.1f}%\n🔥 RVOL: {rvol:.1f}x\n"
                       f"📊 RSI: {rsi:.1f}\n⚠️ Paper alert فقط؛ لا تطارد الارتفاع")
                send_telegram(msg)
                _unified_power_sent[symbol] = time.time()
                time.sleep(0.25)
            gc.collect()
            time.sleep(UNIFIED_POWER_INTERVAL)
        except Exception as error:
            logger.error(f'[Unified Power] scanner error: {error}')
            time.sleep(60)


# ================================================================
# 🧠 RAILWAY MEMORY MONITOR — حماية من تجاوز الذاكرة
# ================================================================
MEMORY_LIMIT_MB = 450
MEMORY_CHECK_INTERVAL = 60


def memory_monitor():
    """يراقب RSS؛ يعيد التشغيل عبر Railway عند تجاوز الحد الآمن فقط."""
    if psutil is None:
        logger.warning('⚠️ psutil unavailable; memory monitor disabled')
        return
    process = psutil.Process(os.getpid())
    while True:
        try:
            memory_mb = process.memory_info().rss / (1024 * 1024)
            if memory_mb > MEMORY_LIMIT_MB:
                logger.error(f'🚨 Memory usage {memory_mb:.1f} MB exceeds {MEMORY_LIMIT_MB} MB; requesting Railway restart')
                os._exit(1)
            logger.debug(f'[Memory] RSS={memory_mb:.1f} MB')
        except Exception as error:
            logger.warning(f'[Memory] monitor error: {error}')
        time.sleep(MEMORY_CHECK_INTERVAL)


if __name__ == "__main__":
    load_state()
    ensure_state_schema()

    # ✅ شغل Telegram فوراً (قبل أي شيء آخر)
    logger.info("✅ Starting Telegram bot immediately...")
    if bot:
        threading.Thread(target=run_telegram_bot, daemon=True).start()
        time.sleep(1)  # انتظر ثانية واحدة للتأكد من بدء Telegram
        send_telegram("✅ PENNY HUNTER BOT STARTING! جاري تحميل الأسهم...")
    else:
        logger.error("❌ Telegram bot not initialized!")
    
    # ===== شغل تحديث التيكرات في خيط منفصل (لا تحظر البوت الرئيسي) =====
    logger.info("📊 Loading ticker list in background...")
    threading.Thread(target=update_all_tickers, daemon=True).start()
    
    # أساسيات (إلزامية)
    threading.Thread(target=background_monitor, daemon=True).start()
    threading.Thread(target=memory_monitor, daemon=True, name='MemoryMonitor').start()
    threading.Thread(target=cleaner_loop, daemon=True).start()
    threading.Thread(target=pending_halts_cleaner_loop, daemon=True, name='PendingHaltsCleaner').start()
    threading.Thread(target=cache_cleaner_loop, daemon=True).start()

    # 🔥 ماسحات صيد البيني (تمت إعادة تفعيلها مع إعدادات النسخة المستقرة)
    # Disabled duplicate: rvol_spike_scanner -> unified_power_scanner
    # Disabled duplicate: power_runner_scanner -> unified_power_scanner
    # Disabled for Ranking Pipeline: fast_momentum_scanner
    # Disabled duplicate: support_resistance_scanner -> unified_sr_scanner
    # Disabled for clean Ranking Pipeline: halt_breaker_scanner

    # 🌐 FULL MARKET SWEEP — يمسح كل 6700+ سهم (يصطاد ZTG/FAP/MTEN/CCTG وأمثالهم)
    # 🔍 PRIMARY DISCOVERY ENGINE
    threading.Thread(target=full_sweep_scanner, daemon=True, name='FullRotationDiscovery').start()
    logger.info('🔍 Full Rotation Discovery started')
    threading.Thread(target=after_hours_discovery_scanner, daemon=True, name='AfterHoursDiscovery').start()
    logger.info('🌙 After-Hours Discovery started')

    # 🏆 TOP GAINERS SCANNER — يصطاد أسهم مثل INHD +4000% من قائمة Yahoo الحية
    threading.Thread(target=top_gainers_scanner, daemon=True, name='TopGainersDiscovery').start()
    logger.info('🏆 Top Gainers Discovery started')
    # Disabled for Ranking Pipeline: early_hunter_sweep_loop
    threading.Thread(target=hot_watchlist_scanner, daemon=True).start()
    logger.info('🎯 Hybrid pipeline started: TopGainers + FullRotation → Ranking → AI → Telegram')
    # Disabled for clean Ranking Pipeline: pre_explosion_scanner_v3
    # Disabled for Ranking Pipeline: vwap_bounce_scanner
    # Disabled for Ranking Pipeline: candle_pattern_scanner
    # Disabled for Ranking Pipeline: chart_pattern_scanner
    # Disabled for Ranking Pipeline: advanced_analysis_scanner
    # Disabled for Ranking Pipeline: unified_sr_scanner
    # Disabled for Ranking Pipeline: unified_pattern_scanner
    # Disabled for Ranking Pipeline: unified_power_scanner
    # Disabled for clean Ranking Pipeline: scanner_watchdog

    # 🚨 QUICK JUMP SCANNER — يصطاد قفزات 15%+ في دقيقة أو نص دقيقة
    # Disabled for Ranking Pipeline: quick_jump_scanner

    # 📰 أخبار RSS والمحفزات (مفعلة الآن!)
    threading.Thread(target=rss_news_scanner, daemon=True).start()
    start_safe_finnhub()
    # Disabled for clean Ranking Pipeline: form4_insider_scanner
    # Disabled for clean Ranking Pipeline: pdufa_biotech_scanner
    
    # ⏰ ماسحات الأوقات الحرجة (مفعلة الآن!)
    # Disabled for clean Ranking Pipeline: closing_hours_scanner
    # Disabled for clean Ranking Pipeline: opening_opportunity_scanner
    # Disabled for clean Ranking Pipeline: premarket_scanner
    # Disabled duplicate: bearish_candle_scanner -> unified_pattern_scanner
    # Disabled duplicate: premarket_flash_scanner -> quick_jump_scanner
    # Disabled for clean Ranking Pipeline: premarket_gap_scanner
    # Disabled duplicate: early_breakout_scanner -> advanced_analysis_scanner
    # Disabled duplicate: early_hunter_scanner -> advanced_analysis_scanner
    # Disabled duplicate: second_wave_scanner -> advanced_analysis_scanner
    # Disabled duplicate: after_hours_breakout_scanner -> after_hours_scanner
    # Disabled duplicate: pre_breakout_confirmation_scanner -> advanced_analysis_scanner
    # Disabled for clean Ranking Pipeline: ai_scanner_loop
    # Disabled for clean Ranking Pipeline: pump_and_dump_hunter
    # Disabled for clean Ranking Pipeline: after_hours_scanner
    # Disabled for clean Ranking Pipeline: penny_stock_scanner
    
    # Disabled for clean Ranking Pipeline: elite_3_summary_loop
    # Disabled for clean Ranking Pipeline: daily_report_scheduler
    
    logger.info("=" * 50)
    logger.info("🚀 PENNY HUNTER BOT STARTED!")
    logger.info("📊 Target: $0.3 - $20 penny stocks")
    logger.info("=" * 50)
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)






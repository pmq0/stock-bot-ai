# =====================================================================
# PENNY HUNTER — نسخة v2 (إصلاح الاكتشاف والتأخير والتكرار)
# ---------------------------------------------------------------------
# اللي تغيّر عن النسخة السابقة:
#  1) الاكتشاف: بدل عينة عشوائية (100 من ~6700 سهم، تُبنى مرة وحدة عند التشغيل) → يقرأ شاشات Yahoo الحيّة
#     (day_gainers + most_actives + small_cap_gainers) كل 45 ثانية، ويحسب الزخم اللحظي من فرق القراءات.
#  2) الأسهم الميّتة: hot_watchlist له انتهاء صلاحية (20 دقيقة بلا تأكيد)، والترتيب حسب الحيوية اللحظية
#     (زخم 15 دقيقة، تسارع حجم، بُعد عن القمة، عمر آخر شمعة) مو حسب % اليوم. سهم واقف أو متراجع = يُرفض.
#  3) البيانات المزيفة: أُزيل الـfallback اللي كان يخترع above_vwap/breakout=True لما تفشل الشموع.
#  4) التكرار: كولداون موحّد (45 دقيقة) لكل الماسحات + استثناء لو كمل السهم +8% (موجة جديدة). لا رسائل AVOID تلقائية.
#  5) التأخير: طلبات Yahoo أقل بكثير (شاشات بدل مسح سهم سهم) + إزالة قائمة سوداء كانت تحظر أسهم حيّة
#     (INTC, ZM, GE, QUBT, CLSK ...) + Telegram بفاصل 1.2ث بدل 3ث + إصلاح 60d/90d/120d (كانت تنجلب 5 أيام فقط).
#  6) Telegram: لو Markdown مكسور تُعاد الرسالة كنص عادي بدل ما تضيع بصمت.
#  7) /health يعرض نبضات الماسحات وحالة الاكتشاف (تعرف مين متأخر أو واقف).
#  8) طبقة جديدة كلياً ومنفصلة: Explosive Setup Engine (ESE) — تكتشف تحديداً نمط
#     "اندفاعة قوية بحجم → قاعدة ضيقة يجف فيها الحجم → اختراق" (نمط IMCC)، وتبني دخول/وقف/أهداف من بنية
#     السهم نفسها (قمم سابقة، فيبو التصحيح، مسافة العمود) بدل نسب ثابتة. تتابع الصفقة بعد الاختراق
#     (T1/T2/T3 مع اقتراح رفع الوقف، أو كسر الوقف). أمر تليجرام /setups يعرض المرصود حالياً.
#     كل صفقة تُقفل خطتها (دخول/وقف/أهداف) لحظة تأكيد الاختراق ولا تتغيّر بعدها، وتُؤرشف نتيجتها النهائية
#     (وصل T1/T2/T3؟ اختراق فاشل؟ أقصى صعود؟ كم دقيقة من التشكّل للاختراق؟) — أمر /esestats يلخّصها.
#     هذي أرقام حيّة تراكمية من التشغيل الفعلي، مو باكتست على بيانات تاريخية (Yahoo المجاني أصلاً ما يوفر
#     شموع دقيقة لأبعد من ~7 أيام، فباكتست حقيقي على أسهم انفجرت قديماً يحتاج مصدر بيانات تاريخي مدفوع).
#     ما تلمس هذه الطبقة BASE_TP_PCT ولا فلاتر /recommend القديمة — تعمل بجانبها كمصدر تنبيهات مستقل.
# متغيرات بيئة جديدة (كلها اختيارية): DISCOVERY_INTERVAL_SEC, DISCOVERY_SCREENERS, DISCOVERY_PRICE_MAX,
#   HOT_MAX_AGE_SEC, RANK_MIN_AI_SCORE, ALERT_SYMBOL_COOLDOWN, TELEGRAM_DELAY, YAHOO_MIN_REQUEST_GAP,
#   SETUP_ARM_MIN, SETUP_SCAN_INTERVAL, SETUP_MAX_ALERTS_PER_DAY (طبقة ESE).
# مصدر بيانات ثاني (مثل Webull): أضف دالة إلى DISCOVERY_EXTRA_PROVIDERS ترجع قائمة dicts فيها symbol/price/change/volume.
# =====================================================================
from deep_translator import GoogleTranslator
import os
import sys
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
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler(sys.stdout)]
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
RECOMMENDATION_MIN_CONFIDENCE = 80
RECOMMENDATION_MIN_TECH_SCORE = 75
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
MAX_PRICE = 15.0
MIN_VOLUME = 10000
MAX_TICKERS_TO_SCAN = 500   # 500 سهم فقط - الأكثر نشاطاً (Explosive Mode)
CHUNK_SIZE = 100
MIN_RVOL_BY_PHASE = {"PRE": 2.0, "REGULAR": 1.5, "AFTER": 2.5}
TELEGRAM_DELAY = float(os.getenv("TELEGRAM_DELAY", "1.2"))  # كانت 3 ثواني تحت قفل عام → التنبيهات تتراكم وتتأخر
TRADE_MONITOR_INTERVAL = 30
SCAN_INTERVAL_SEC = 300     # 5 دقائق بدلاً من 15 (أسرع للمضاربة)
BREAK_BETWEEN_CHUNKS = 5
DEEP_ANALYSIS_WORKERS = 4
SIGNAL_COOLDOWN = 3600         # ساعة واحدة كولداون (كان ساعتين — خُفّف)
MAX_DAILY_SIGNALS = 150       # رُفع من 50 لأيام الحركة القوية

# ================= DISCOVERY / FRESHNESS SETTINGS (v2) =================
# كل القيم تنعدّل من متغيرات البيئة (Environment) بدون لمس الكود.
DISCOVERY_INTERVAL_SEC   = int(os.getenv("DISCOVERY_INTERVAL_SEC", "45"))     # كل كم ثانية نقرأ شاشات Yahoo
DISCOVERY_SCREENERS      = [s.strip() for s in os.getenv("DISCOVERY_SCREENERS", "day_gainers,most_actives,small_cap_gainers").split(",") if s.strip()]
DISCOVERY_COUNT          = int(os.getenv("DISCOVERY_COUNT", "100"))           # كم سهم من كل شاشة
DISCOVERY_PRICE_MIN      = float(os.getenv("DISCOVERY_PRICE_MIN", "0.3"))
DISCOVERY_PRICE_MAX      = float(os.getenv("DISCOVERY_PRICE_MAX", "15"))
DISCOVERY_MIN_CHANGE     = float(os.getenv("DISCOVERY_MIN_CHANGE", "3"))      # % يومي أدنى لدخول القائمة الساخنة
HOT_MAX_AGE_SEC          = int(os.getenv("HOT_MAX_AGE_SEC", "1200"))          # سهم ما أُعيد تأكيده خلال 20 دقيقة = يُحذف (ميت)
HOT_MAX_ITEMS            = 120
RANK_MAX_CANDLE_FETCH    = int(os.getenv("RANK_MAX_CANDLE_FETCH", "12"))      # كم سهم نجلب شموعه في كل دورة ترتيب
RANK_MIN_AI_SCORE        = int(os.getenv("RANK_MIN_AI_SCORE", "60"))
AI_RETRY_COOLDOWN        = int(os.getenv("AI_RETRY_COOLDOWN", "1200"))        # لا نعيد تحليل نفس السهم قبل 20 دقيقة
ALERT_SYMBOL_COOLDOWN    = int(os.getenv("ALERT_SYMBOL_COOLDOWN", "2700"))    # كولداون موحّد لكل المصادر (45 دقيقة)
ALERT_CONTINUATION_PCT   = float(os.getenv("ALERT_CONTINUATION_PCT", "8"))    # +8% فوق سعر آخر تنبيه = موجة جديدة → يُسمح بتنبيه ثاني
ALERT_CONTINUATION_MIN_GAP = 300
EXT_HOURS_MAX_SYMBOLS    = int(os.getenv("EXT_HOURS_MAX_SYMBOLS", "25"))      # عدد أسهم Pre/After اللي نفحص شموعها كل دورة
QUICK_JUMP_MAX_CHECK     = int(os.getenv("QUICK_JUMP_MAX_CHECK", "12"))
QUICK_JUMP_MIN_GAIN_3M   = float(os.getenv("QUICK_JUMP_MIN_GAIN_3M", "7"))    # قفزة 7% خلال 3 شموع 1د تكفي (بجانب 15% في شمعة واحدة)

# ================= AI PROCESS SYMBOL SETTINGS =================
AI_PROCESS_ENABLED = True
AI_CONFIDENCE_THRESHOLD = 70
AI_MAX_ANALYSIS_PER_MINUTE = 10
AI_MODEL = os.getenv("AI_MODEL", "openai/gpt-oss-20b")
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
    now_ts = time.time()
    with state_lock:
        hot_count = len(state.get("hot_watchlist", []))
        universe_count = len(state.get("tickers", []))
    return jsonify({
        "status": "ok", "bot_enabled": bool(bot), "has_chat_id": bool(CHAT_ID),
        "hot_watchlist": hot_count, "universe": universe_count,
        "scanner_age_sec": {k: int(now_ts - v) for k, v in dict(_heartbeats).items()},
        "discovery": {"errors": _discovery_stats.get("errors", 0), "last_ok": _discovery_stats.get("last_ok", False),
                      "last_poll_age_sec": int(now_ts - _discovery_stats["last_poll"]) if _discovery_stats.get("last_poll") else None},
    })

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
    "hot_watchlist_timestamp": 0,
    "alert_gate": {},
    "in_play": {},
    "explosive_setups": {},
    "setup_tracker": {},
    "setup_history": []
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
    if not state.get("tickers"): state["tickers"] = ["AAPL", "TSLA", "NVDA", "AMD", "MSFT", "META", "GOOGL", "AMZN", "NFLX", "PYPL"]
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
        state.setdefault("watchlist", [])
        state.setdefault("price_alerts", {})
        state.setdefault("real_trades", {})
        state.setdefault("ai_candidates", [])
        state.setdefault("alert_gate", {})
        state.setdefault("in_play", {})

        # ── hot_watchlist: أي سهم ما أُعيد تأكيده خلال HOT_MAX_AGE_SEC = ميّت (كانت تبقى أيام وترجع بعد كل إعادة تشغيل) ──
        _now_ts = time.time()
        state["hot_watchlist"] = [
            x for x in state.get("hot_watchlist", [])
            if isinstance(x, dict) and x.get("symbol")
            and _now_ts - _safe_float(x.get("last_seen", x.get("timestamp"))) <= HOT_MAX_AGE_SEC
        ]
        state["ai_candidates"] = []
        state["alert_gate"] = {k: v for k, v in state.get("alert_gate", {}).items()
                               if isinstance(v, dict) and _now_ts - _safe_float(v.get("ts")) < 86400}
        state["in_play"] = {k: v for k, v in state.get("in_play", {}).items()
                            if isinstance(v, dict) and _now_ts - _safe_float(v.get("last_seen")) < 172800}

        # ── Explosive Setup Engine: العرض الحي يُبنى من جديد كل تشغيل؛ المتابَعة (setup_tracker) تُنظّف بعمرها ──
        state.setdefault("explosive_setups", {})
        state.setdefault("setup_tracker", {})
        state["explosive_setups"] = {}
        state["setup_tracker"] = {
            k: v for k, v in state.get("setup_tracker", {}).items()
            if isinstance(v, dict)
            and _now_ts - _safe_float(v.get("last_seen", v.get("first_seen")))
                < (SETUP_TRACK_TTL_MIN * 60 if "BROKE" in v.get("alerted", []) else SETUP_ARM_TTL_MIN * 60)
        }
        state.setdefault("setup_history", [])   # أرشيف /esestats — يبقى محفوظاً عبر إعادة التشغيل عمداً
        if len(state["setup_history"]) > 500:
            state["setup_history"] = state["setup_history"][-500:]


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
            snapshot = {k: v for k, v in state.items() if k != "full_tickers"}  # قائمة 6700 سهم تضخّم الملف وتبطّئ كل حفظ
            tmp_path = STATE_FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, default=str)
            os.replace(tmp_path, STATE_FILE)  # كتابة ذرّية: الملف ما يتلف لو انقطع التشغيل أثناء الحفظ
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
            if not state.get("tickers"): state["tickers"] = ["AAPL", "TSLA", "NVDA", "AMD", "MSFT", "META", "GOOGL", "AMZN", "NFLX", "PYPL"]
            logger.info("State loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
    else:
        ensure_state_schema()
        if not state.get("tickers"): state["tickers"] = ["AAPL", "TSLA", "NVDA", "AMD", "MSFT", "META", "GOOGL", "AMZN", "NFLX", "PYPL"]


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
                    elif "parse entities" in err_str.lower():
                        # Markdown مكسور (رمز _ أو * غير مغلق) — كانت الرسالة تضيع بصمت؛ نعيد إرسالها كنص عادي
                        try:
                            if photo:
                                try:
                                    photo.seek(0)
                                except Exception:
                                    pass
                                bot.send_photo(CHAT_ID, photo, caption=message)
                            else:
                                bot.send_message(CHAT_ID, message)
                            _last_telegram_time = time.time()
                            logger.warning("Telegram Markdown parse failed — resent as plain text")
                        except Exception as resend_error:
                            logger.error(f"Telegram plain resend failed: {resend_error}")
                        break
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



def _load_full_ticker_list():
    """قائمة كل الأسهم من GitHub — تُستخدم فقط لو فعّلت FULL_ROTATION_ENABLED. لا تُحفظ في ملف الحالة."""
    tickers = set()
    sources = [
        "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_tickers.txt",
        "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_tickers.txt",
        "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_tickers.txt",
    ]
    for url in sources:
        try:
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if resp.status_code == 200:
                for line in resp.text.split("\n"):
                    sym = line.strip().upper()
                    if sym and sym.isalpha() and 2 <= len(sym) <= 5:
                        tickers.add(sym)
        except Exception as error:
            logger.debug(f"Full ticker list source failed: {error}")
    if tickers:
        with state_lock:
            state["full_tickers"] = sorted(tickers)
    return len(tickers)


def update_all_tickers():
    """
    يبني قائمة المراقبة من شاشات Yahoo الحيّة مباشرة.
    (النسخة القديمة كانت تخلط ~6700 سهم وتأخذ أول 100 عشوائي مرة وحدة عند التشغيل — فيصير البوت يراقب أسهم عشوائية طول اليوم.)
    من هنا وطالع، top_gainers_scanner يحدّث القائمة كل ~45 ثانية.
    """
    try:
        n = _discovery_poll_once()
        with state_lock:
            save_state()
        logger.info(f"✅ Universe built from live screeners: {n} hot candidates")
    except Exception as error:
        logger.warning(f"Universe build failed (discovery loop will retry): {error}")
    try:
        if os.getenv("FULL_ROTATION_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on"):
            logger.info(f"📚 Full ticker list loaded: {_load_full_ticker_list()} symbols")
    except Exception as error:
        logger.debug(f"Full ticker list skipped: {error}")
    with state_lock:
        return list(state.get("tickers", []))


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
    @bot.message_handler(commands=['start', 'help'])
    def cmd_start(message):
        logger.info(f"📨 Received /start from {message.chat.id}")
        if not ensure_authorized(message): return
        phase = get_market_phase()
        msg = f"👋 Trading Bot v33!\n📊 {len(state.get('tickers', []))} stocks\n🕐 {PHASE_SETTINGS[phase]['description']}\n\n📋 *الأوامر المتاحة:*\n\n📊 *الحالة والتقارير*\n/status - الأداء العام\n/positions - الصفقات المفتوحة\n/recommend SYMBOL - توصية Paper موحدة\n/recommendations - سجل التوصيات\n/signals - آخر الإشارات\n\n🔎 *التحليل والماسحات*\n/scan SYMBOL - تحليل سهم\n/sr SYMBOL - دعم ومقاومة\n/news SYMBOL - أخبار سهم\n/top - أفضل المرشحين\n/movers - أسباب ارتفاع 100%+\n/gainers - أكبر الرابحين\n/losers - أكبر الخاسرين\n/vwap SYMBOL - تحليل VWAP\n/candle SYMBOL - تحليل الشموع\n/adv SYMBOL - تحليل EMA/Fibonacci متقدم\n/fundamental SYMBOL - تحليل أساسي\n/deep SYMBOL - تحليل شامل (AI)\n\n👀 *قائمة المراقبة والتنبيهات*\n/watchlist - عرض القائمة\n/add SYMBOL - إضافة سهم\n/remove SYMBOL - حذف سهم\n/alert SYMBOL PRICE - تنبيه سعري\n/alerts - عرض التنبيهات\n/delalert SYMBOL - حذف تنبيه\n\n🛡️ *Paper Trading والحماية*\n/b SYMBOL QUANTITY - شراء Paper\n/s SYMBOL - بيع Paper\n/close SYMBOL - إغلاق Paper\n/halt - الأسهم الموقوفة\n/clearhalt SYMBOL - إزالة إيقاف قديم\n\n⚠️ كل العمليات Paper Trading فقط وليست توصية مالية"
        send_telegram(msg)

    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        if not ensure_authorized(message): return
        with state_lock:
            perf = state["performance"]
            total = perf["wins"] + perf["losses"]
            wr = (perf["wins"] / total * 100) if total > 0 else 0
            msg = f"📊 *Status*\n✅ Wins: {perf['wins']}\n❌ Losses: {perf['losses']}\n📈 WR: {wr:.1f}%\n💵 PnL: ${perf['total_pnl']:+.2f}\n📦 Open: {len(state['open_trades'])}\n🌐 Universe: {len(state['tickers'])}"
        send_telegram(msg)

    @bot.message_handler(commands=['positions'])
    def cmd_positions(message):
        if not ensure_authorized(message): return
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
        if not ensure_authorized(message): return
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
        except Exception as e: send_telegram(f"❌ Error: {e}")

    @bot.message_handler(commands=['scan'])
    def cmd_scan(message):
        if not ensure_authorized(message): return
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
        except Exception as e: send_telegram(f"❌ Error: {e}")

    @bot.message_handler(commands=['sr'])
    def cmd_support_resistance(message):
        if not ensure_authorized(message): return
        try:
            args = message.text.split()
            if len(args) != 2:
                send_telegram("Usage: /sr <SYMBOL>")
                return
            symbol = args[1].upper().strip()
            send_telegram(f"🔎 جاري تحليل الدعم والمقاومة لـ *{symbol}*...")
            df = cached_download(symbol, period="1d", interval="1m", force_refresh=True)
            if df.empty or len(df) < 35:
                df = cached_download(symbol, period="5d", interval="5m", force_refresh=True)
            if df.empty or len(df) < 35:
                send_telegram(f"❌ لا توجد بيانات كافية لحساب الدعم والمقاومة لـ *{symbol}*")
                return
            analysis = calculate_support_resistance(df)
            if not analysis:
                send_telegram(f"❌ لم تظهر مستويات دعم/مقاومة واضحة لـ *{symbol}*")
                return
            send_telegram(format_support_resistance_message(symbol, analysis))
        except Exception as e: send_telegram(f"❌ خطأ: {e}")

    @bot.message_handler(commands=['b'])
    def cmd_buy(message):
        if not ensure_authorized(message): return
        threading.Thread(target=handle_manual_buy, args=(message,), daemon=True).start()

    def handle_manual_buy(message):
        try:
            args = message.text.split()
            if len(args) != 3:
                send_telegram("⚠️ الاستخدام الصحيح:\n`/b SYMBOL QUANTITY`\nمثال: `/b SMX 50`")
                return
            symbol, quantity = args[1].upper().strip(), int(args[2])
            with state_lock:
                if symbol in state["open_trades"]:
                    send_telegram(f"⚠️ *{symbol}* مفتوح مسبقاً!")
                    return
                if len(state["open_trades"]) >= MAX_OPEN_TRADES:
                    send_telegram(f"❌ وصلت الحد الأقصى للصفقات المفتوحة ({MAX_OPEN_TRADES})")
                    return
            df = cached_download(symbol, period="1d", interval="5m")
            if df.empty:
                send_telegram(f"❌ لم أستطع جلب بيانات *{symbol}*")
                return
            df.columns = [c.lower() for c in df.columns]
            price = float(df['close'].iloc[-1])
            tp, sl, tp_pct = get_trade_levels(price, 1.0)
            with state_lock:
                state["open_trades"][symbol] = {"entry": price, "tp": tp, "sl": sl, "size": quantity, "time": time.time(), "phase": get_market_phase(), "manual": True, "paper": PAPER_TRADING}
                save_state()
            send_telegram(f"✅ *Paper Entry* — {symbol} بسعر *${price:.2f}*")
        except Exception as e: send_telegram(f"❌ خطأ: {e}")

    @bot.message_handler(commands=['s'])
    def cmd_sell(message):
        if not ensure_authorized(message): return
        try:
            args = message.text.split()
            if len(args) != 2:
                send_telegram("⚠️ الاستخدام الصحيح:\n`/s SYMBOL`\nمثال: `/s SMX`")
                return
            symbol = args[1].upper().strip()
            with state_lock:
                if symbol not in state["open_trades"]:
                    send_telegram(f"❌ *{symbol}* غير موجود")
                    return
                trade = state["open_trades"][symbol]
            df = cached_download(symbol, period='1d', interval='5m')
            if df.empty:
                send_telegram(f"❌ تعذر جلب السعر لـ *{symbol}*")
                return
            exit_price = float(df['close'].iloc[-1])
            close_trade(symbol, exit_price, "Manual Sell /s")
            pnl = (exit_price - trade["entry"]) * trade["size"]
            send_telegram(f"✅ تم إغلاق *{symbol}* بسعر ${exit_price:.2f} | PnL: ${pnl:+.2f}")
        except Exception as e: send_telegram(f"❌ خطأ: {e}")

    @bot.message_handler(commands=['news'])
    def cmd_news(message):
        if not ensure_authorized(message): return
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
            msg = f"📰 *أخبار {symbol}*\n\n"
            for item in news[:5]:
                msg += f"• {item.get('title', 'No title')}\n🔗 [رابط الخبر]({item.get('link', '#')})\n\n"
            send_telegram(msg)
        except Exception as e: send_telegram(f"❌ خطأ: {e}")

    @bot.message_handler(commands=['movers'])
    def cmd_movers(message):
        if not ensure_authorized(message): return
        msg = "🚀 *أسباب الارتفاع الكبير (محفزات):*\n\n1️⃣ قرار FDA 💊\n2️⃣ استحواذ أو شراكة 🤝\n3️⃣ شراء الداخليين 🐋\n4️⃣ Short Squeeze 🔥\n5️⃣ إيقاف تداول 🔓\n\nالبوت يراقب كل هذا آلياً!"
        send_telegram(msg)

    @bot.message_handler(commands=['recommendations', 'signals'])
    def cmd_recommendations(message):
        if not ensure_authorized(message): return
        with state_lock:
            history = list(state.get("recommendation_history", []))
        if not history:
            send_telegram("📭 لا توجد توصيات مسجلة.")
            return
        msg = "📜 *آخر التوصيات (AI Consensus):*\n━━━━━━━━━━━━━━━━\n"
        for rec in history[-10:]:
            emoji = "🟢" if "BUY" in rec.get('final_decision', '') else ("🟡" if "WATCH" in rec.get('final_decision', '') else "🔴")
            msg += f"{emoji} *{rec['symbol']}* | {rec.get('final_decision', 'N/A')} | ثقة: {rec.get('confidence', 0)}%\n"
        send_telegram(msg)

    @bot.message_handler(commands=['recommend'])
    def cmd_recommend(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram("Usage: /recommend SYMBOL")
            return
        symbol = args[1].upper().strip()
        send_telegram(f"🔍 جاري تحليل *{symbol}*...")
        final_trade_recommendation(symbol, send_alert=True)

    @bot.message_handler(commands=['top'])
    def cmd_top(message):
        if not ensure_authorized(message): return
        with state_lock:
            top = list(state.get("ai_candidates", []))
            hot = list(state.get("hot_watchlist", []))
        
        if not top and not hot:
            send_telegram("📭 لا يوجد مرشحين حالياً. البوت لا يزال في مرحلة المسح الأولي.")
            return
            
        msg = "🏆 *أفضل المرشحين (AI Candidates):*\n"
        if top:
            msg += "\n".join([f"• *{c['symbol']}* | سكور: {c['score']} | ${c['price']:.2f}" for c in top])
        else:
            msg += "⏳ لا توجد ترشيحات AI مكتملة بعد.\n"
            
        if hot:
            msg += "\n\n🔥 *قائمة المراقبة اللحظية (Hot):*\n"
            msg += ", ".join([str(x.get('symbol')) for x in hot[:10]])
            
        send_telegram(msg)

    @bot.message_handler(commands=['setups'])
    def cmd_setups(message):
        if not ensure_authorized(message): return
        with state_lock:
            setups = dict(state.get("explosive_setups", {}))
        if not setups:
            send_telegram("📭 لا توجد إعدادات انفجارية مرصودة الآن.")
            return
        rows = sorted(setups.items(), key=lambda kv: kv[1].get("score", 0), reverse=True)[:10]
        lines = []
        for sym, s in rows:
            icon = {"ARMED": "🟡", "BROKE": "🚀", "FORMING": "⚪"}.get(s.get("state"), "⚪")
            lines.append(f"{icon} *{sym}* | {s.get('state')} | سكور {s.get('score')} | "
                         f"${s.get('price', 0):.2f} → ${s.get('trigger', 0):.2f}")
        send_telegram("💥 *Explosive Setups*\n" + "\n".join(lines))

    @bot.message_handler(commands=['esestats'])
    def cmd_esestats(message):
        if not ensure_authorized(message): return
        with state_lock:
            history = list(state.get("setup_history", []))
            live_tr = dict(state.get("setup_tracker", {}))
        live_records = []
        for sym, tr in live_tr.items():
            if not tr.get("armed_ts"):
                continue
            alerted = set(tr.get("alerted", []))
            locked_plan = tr.get("locked_plan") or {}
            armed_price = _safe_float(tr.get("armed_price"))
            max_price = _safe_float(tr.get("max_price"), armed_price)
            trigger = _safe_float(tr.get("locked_trigger", tr.get("trigger")))
            live_records.append({
                "armed_ts": tr.get("armed_ts"), "broke_ts": tr.get("broke_ts"),
                "t1_pct": (locked_plan.get("t1") or {}).get("pct"),
                "max_run_pct": ((max_price - trigger) / trigger * 100.0) if trigger > 0 else None,
                "broke": "BROKE" in alerted, "hit_t1": "T1" in alerted, "hit_t2": "T2" in alerted,
                "hit_t3": "T3" in alerted, "hit_stop": "STOP" in alerted,
            })
        all_records = history + live_records
        stats = _compute_ese_stats(all_records)
        send_telegram(_format_ese_stats(stats, len(all_records)))

    @bot.message_handler(commands=['replay'])
    def cmd_replay(message):
        if not ensure_authorized(message): return
        parts = (message.text or "").split()
        if len(parts) < 2:
            send_telegram("الاستخدام: /replay SYMBOL — مثال: /replay IMCC")
            return
        symbol = parts[1].upper().strip()
        send_telegram(f"⏳ أعيد تشغيل اليوم على {symbol}...")
        try:
            df = cached_download(symbol, period="5d", interval="1m", prepost=True, force_refresh=True)
            report = _replay_symbol_today(symbol, df)
        except Exception as error:
            report = f"تعذر تشغيل الإعادة على {symbol}: {error}"
        send_telegram(report)

    @bot.message_handler(commands=['gainers'])
    def cmd_gainers(message):
        if not ensure_authorized(message): return
        try:
            send_telegram("🔍 جاري جلب أكبر الرابحين من Yahoo...")
            url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_gainers&count=10&formatted=false"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            resp = requests.get(url, headers=headers, impersonate="chrome120", timeout=15)
            data = resp.json()
            quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
            if not quotes:
                send_telegram("📭 لم يتم العثور على بيانات رابحين حالياً.")
                return
            msg = "📈 *أكبر الرابحين اليوم:*\n━━━━━━━━━━━━━━━━\n"
            for i, q in enumerate(quotes[:10], 1):
                sym = q.get("symbol", "???")
                prc = q.get("regularMarketPrice", 0)
                chg = q.get("regularMarketChangePercent", 0)
                msg += f"{i}. *{sym}* — ${prc:.2f} ({chg:+.1f}%)\n"
            send_telegram(msg)
        except Exception as e:
            logger.error(f"Gainers command error: {e}")
            send_telegram(f"❌ خطأ في جلب الرابحين: {e}")

    @bot.message_handler(commands=['losers'])
    def cmd_losers(message):
        if not ensure_authorized(message): return
        try:
            send_telegram("🔍 جاري جلب أكبر الخاسرين من Yahoo...")
            url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_losers&count=10&formatted=false"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            resp = requests.get(url, headers=headers, impersonate="chrome120", timeout=15)
            data = resp.json()
            quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
            if not quotes:
                send_telegram("📭 لم يتم العثور على بيانات خاسرين حالياً.")
                return
            msg = "📉 *أكبر الخاسرين اليوم:*\n━━━━━━━━━━━━━━━━\n"
            for i, q in enumerate(quotes[:10], 1):
                sym = q.get("symbol", "???")
                prc = q.get("regularMarketPrice", 0)
                chg = q.get("regularMarketChangePercent", 0)
                msg += f"{i}. *{sym}* — ${prc:.2f} ({chg:+.1f}%)\n"
            send_telegram(msg)
        except Exception as e:
            logger.error(f"Losers command error: {e}")
            send_telegram(f"❌ خطأ في جلب الخاسرين: {e}")

    @bot.message_handler(commands=['vwap'])
    def cmd_vwap(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram("Usage: /vwap SYMBOL")
            return
        symbol = args[1].upper().strip()
        res = analyze_vwap_bounce(symbol)
        if res: send_telegram(_format_unified_vwap(symbol, res))
        else: send_telegram(f"❌ لا توجد إشارة VWAP لـ {symbol}")

    @bot.message_handler(commands=['candle'])
    def cmd_candle(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram("Usage: /candle SYMBOL")
            return
        symbol = args[1].upper().strip()
        df = cached_download(symbol, period="5d", interval="5m")
        if not df.empty:
            df.columns = [c.lower() for c in df.columns]
            patterns = analyze_candle_patterns(df)
            if patterns:
                msg = f"🕯️ *أنماط الشموع لـ {symbol}:*\n" + "\n".join([f"• {p['type']}" for p in patterns])
                send_telegram(msg)
                return
        send_telegram(f"🟡 لا توجد أنماط واضحة لـ {symbol}")

    @bot.message_handler(commands=['adv'])
    def cmd_adv(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram("Usage: /adv SYMBOL")
            return
        symbol = args[1].upper().strip()
        df = cached_download(symbol, period="120d", interval="1d")
        if not df.empty:
            df.columns = [c.lower() for c in df.columns]
            ichimoku = calculate_ichimoku(df)
            supertrend = calculate_supertrend(df)
            msg = f"📐 *تحليل متقدم: {symbol}*\n"
            if ichimoku: msg += f"☁️ Ichimoku: {ichimoku['cloud_position']}\n"
            if supertrend: msg += f"📈 SuperTrend: {supertrend['direction']}\n"
            send_telegram(msg)
        else: send_telegram(f"❌ لا توجد بيانات لـ {symbol}")

    @bot.message_handler(commands=['fundamental'])
    def cmd_fundamental(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram("Usage: /fundamental SYMBOL")
            return
        symbol = args[1].upper().strip()
        ticker = yf.Ticker(symbol)
        info = ticker.info
        msg = f"📊 *أساسيات {symbol}:*\nCap: ${info.get('marketCap', 0):,.0f}\nFloat: {info.get('floatShares', 0):,.0f}\nShort: {info.get('shortPercentOfFloat', 0)*100:.1f}%"
        send_telegram(msg)

    @bot.message_handler(commands=['deep'])
    def cmd_deep(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) != 2:
            send_telegram("Usage: /deep SYMBOL")
            return
        symbol = args[1].upper().strip()
        send_telegram(f"🔍 جاري التحليل العميق لـ {symbol} (AI)...")
        res = ai_fundamental_analysis(symbol)
        if res: send_telegram(f"🧠 *Deep Analysis: {symbol}*\n\n{res[:4000]}")
        else: send_telegram("❌ فشل التحليل")

    @bot.message_handler(commands=['watchlist'])
    def cmd_watchlist(message):
        if not ensure_authorized(message): return
        with state_lock: wl = list(state.get("watchlist", []))
        if wl: send_telegram("👀 *Watchlist:*\n" + "\n".join([f"• {s}" for s in wl]))
        else: send_telegram("📭 القائمة فارغة")

    @bot.message_handler(commands=['add'])
    def cmd_add(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) == 2:
            s = args[1].upper()
            with state_lock:
                wl = state.setdefault("watchlist", [])
                if s not in wl: wl.append(s); save_state()
            send_telegram(f"✅ تمت إضافة {s}")

    @bot.message_handler(commands=['remove'])
    def cmd_remove(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) == 2:
            s = args[1].upper()
            with state_lock:
                wl = state.get("watchlist", [])
                if s in wl: wl.remove(s); save_state()
            send_telegram(f"✅ تم حذف {s}")

    @bot.message_handler(commands=['alert'])
    def cmd_alert(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) == 3:
            s, p = args[1].upper(), float(args[2])
            with state_lock:
                state.setdefault("price_alerts", {})[s] = {"price": p, "triggered": False}
                save_state()
            send_telegram(f"🔔 تنبيه لـ {s} عند {p}")

    @bot.message_handler(commands=['alerts'])
    def cmd_alerts(message):
        if not ensure_authorized(message): return
        with state_lock: al = state.get("price_alerts", {})
        if al: send_telegram("🔔 *Alerts:*\n" + "\n".join([f"• {s}: {d['price']}" for s, d in al.items() if not d['triggered']]))
        else: send_telegram("📭 لا توجد تنبيهات")

    @bot.message_handler(commands=['delalert'])
    def cmd_delalert(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) == 2:
            s = args[1].upper()
            with state_lock:
                al = state.get("price_alerts", {})
                if s in al: del al[s]; save_state()
            send_telegram(f"✅ تم حذف تنبيه {s}")

    @bot.message_handler(commands=['halt'])
    def cmd_halt(message):
        if not ensure_authorized(message): return
        with state_lock: h = list(state.get("halted_stocks", {}).keys())
        if h: send_telegram("⛔ *Halted:*\n" + "\n".join([f"• {x.split('_')[0]}" for x in h[:20]]))
        else: send_telegram("✅ لا توجد إيقافات")

    @bot.message_handler(commands=['clearhalt'])
    def cmd_clearhalt(message):
        if not ensure_authorized(message): return
        args = message.text.split()
        if len(args) == 2:
            s = args[1].upper()
            with state_lock:
                halted = state.get("halted_stocks", {})
                to_del = [k for k in halted if k.startswith(s)]
                for k in to_del: del halted[k]
                save_state()
            send_telegram(f"✅ تم تنظيف {s}")

    @bot.message_handler(commands=['realbuy'])
    def cmd_realbuy(message):
        if not ensure_authorized(message): return
        send_telegram("🎯 تم استلام طلب الشراء الحقيقي (Paper Mode).")

    @bot.message_handler(commands=['realsell'])
    def cmd_realsell(message):
        if not ensure_authorized(message): return
        send_telegram("🎯 تم استلام طلب البيع الحقيقي (Paper Mode).")

# ================= NEWS CATALYST SCANNER =================





# ================= GROQ SAFE COMPLETION =================
GROQ_MODEL_FALLBACKS = [AI_MODEL, 'openai/gpt-oss-120b', 'qwen/qwen3.6-27b']
_groq_model_lock = threading.Lock()
_groq_active_model = AI_MODEL
_groq_disabled_until = 0.0
_groq_last_404_log = 0.0
GROQ_MODEL_COOLDOWN = 600



def groq_chat_completion(**kwargs):
    """استدعاء Groq مع تبديل تلقائي للنموذج عند 404 فقط."""
    global _groq_active_model, _groq_disabled_until, _groq_last_404_log
    if time.time() < _groq_disabled_until:
        raise RuntimeError('Groq temporarily disabled after model-not-found responses')
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
    # All candidates returned 404: pause Groq for ten minutes and let callers use local fallback.
    if last_error is not None:
        _groq_disabled_until = time.time() + GROQ_MODEL_COOLDOWN
        if time.time() - _groq_last_404_log > GROQ_MODEL_COOLDOWN:
            _groq_last_404_log = time.time()
            logger.error('Groq temporarily disabled for %ss: no configured model was accepted', GROQ_MODEL_COOLDOWN)
    raise last_error or RuntimeError('No Groq model available')


# ================================================================
# 🧠 OPTIONAL MULTI-AI CONSENSUS ENGINE
# Gemini primary → Mistral devil's advocate → OpenRouter tie-breaker.
# Disabled automatically when keys are absent; Paper Trading only.
# ================================================================
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '').strip()
MISTRAL_API_KEY = os.getenv('MISTRAL_API_KEY', '').strip()
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY', '').strip()
GEMINI_ENABLED = bool(GEMINI_API_KEY)
MISTRAL_ENABLED = bool(MISTRAL_API_KEY)
OPENROUTER_ENABLED = bool(OPENROUTER_API_KEY)
GEMINI_RPM_LIMIT = 15
MISTRAL_RPM_LIMIT = 10
OPENROUTER_DAILY_LIMIT = 50
_ai_request_log = {
    'gemini': {'count': 0, 'reset': time.time()},
    'mistral': {'count': 0, 'reset': time.time()},
    'openrouter': {'count': 0, 'reset': time.time(), 'daily': 0, 'daily_reset': time.time()},
}
_ai_request_lock = threading.Lock()



def can_use_ai(service):
    with _ai_request_lock:
        now = time.time()
        log = _ai_request_log.get(service)
        if not log:
            return False
        if now - log['reset'] >= 60:
            log['count'] = 0
            log['reset'] = now
        if service == 'openrouter' and now - log.get('daily_reset', 0) >= 86400:
            log['daily'] = 0
            log['daily_reset'] = now
        if service == 'openrouter' and log.get('daily', 0) >= OPENROUTER_DAILY_LIMIT:
            return False
        limits = {'gemini': GEMINI_RPM_LIMIT, 'mistral': MISTRAL_RPM_LIMIT, 'openrouter': 999}
        if log['count'] >= limits.get(service, 0):
            return False
        log['count'] += 1
        if service == 'openrouter':
            log['daily'] += 1
        return True



def _parse_ai_json(raw):
    try:
        value = raw if isinstance(raw, dict) else json.loads(str(raw or '').strip())
        return value if isinstance(value, dict) else None
    except Exception:
        return None



def call_gemini(prompt, max_tokens=300):
    if not GEMINI_ENABLED or not can_use_ai('gemini'):
        return None
    try:
        from google import genai
        model = genai.Client(api_key=GEMINI_API_KEY)
        response = model.models.generate_content(model=os.getenv('GEMINI_MODEL', 'gemini-3.6-flash'), contents=prompt)
        return getattr(response, 'text', None)
    except Exception as error:
        logger.warning(f'[AI Consensus] Gemini error: {error}')
        return None



def call_mistral(prompt, max_tokens=300):
    if not MISTRAL_ENABLED or not can_use_ai('mistral'):
        return None
    try:
        response = requests.post(
            'https://api.mistral.ai/v1/chat/completions',
            headers={'Authorization': f'Bearer {MISTRAL_API_KEY}', 'Content-Type': 'application/json'},
            json={'model': os.getenv('MISTRAL_MODEL', 'mistral-small-latest'), 'messages': [{'role': 'user', 'content': prompt}], 'max_tokens': max_tokens, 'temperature': 0.1},
            timeout=10,
        )
        if response.status_code != 200:
            logger.warning(f'[AI Consensus] Mistral status={response.status_code}')
            return None
        return response.json().get('choices', [{}])[0].get('message', {}).get('content', '')
    except Exception as error:
        logger.warning(f'[AI Consensus] Mistral error: {error}')
        return None



def call_openrouter(prompt, max_tokens=300):
    if not OPENROUTER_ENABLED or not can_use_ai('openrouter'):
        return None
    try:
        response = requests.post(
            'https://openrouter.ai/api/v1/chat/completions',
            headers={'Authorization': f'Bearer {OPENROUTER_API_KEY}', 'Content-Type': 'application/json', 'HTTP-Referer': 'https://railway.app', 'X-Title': 'Penny Hunter'},
            json={'model': os.getenv('OPENROUTER_MODEL', 'openai/gpt-oss-20b:free'), 'messages': [{'role': 'user', 'content': prompt}], 'max_tokens': max_tokens},
            timeout=10,
        )
        if response.status_code != 200:
            logger.warning(f'[AI Consensus] OpenRouter status={response.status_code}')
            return None
        return response.json().get('choices', [{}])[0].get('message', {}).get('content', '')
    except Exception as error:
        logger.warning(f'[AI Consensus] OpenRouter error: {error}')
        return None



def ai_consensus(symbol, signal_data):
    if signal_data.get('score', 0) < 65 or signal_data.get('rvol', 0) < 2:
        return None
    if not (GEMINI_ENABLED or MISTRAL_ENABLED or OPENROUTER_ENABLED):
        return None
    prompt = f"""Analyze {symbol} conservatively. Paper trading only. Return JSON only.
Price ${signal_data.get('price', 0):.4f}; Change {signal_data.get('change', 0):+.1f}%; RVOL {signal_data.get('rvol', 0):.1f}x; RSI {signal_data.get('rsi', 50):.0f}; Above VWAP {signal_data.get('above_vwap', False)}; EMA bullish {signal_data.get('ema_bullish', False)}; Breakout {signal_data.get('breakout', False)}; Score {signal_data.get('score', 0)}/100.
Return {{\"decision\":\"BUY|WATCH|AVOID\",\"confidence\":0,\"reason_ar\":\"short Arabic reason\"}}"""
    gemini = _parse_ai_json(call_gemini(prompt)) if GEMINI_ENABLED else None
    if not gemini and not MISTRAL_ENABLED and not OPENROUTER_ENABLED:
        return None
    gemini_decision = str((gemini or {}).get('decision', 'WATCH')).upper()
    gemini_confidence = max(0, min(int((gemini or {}).get('confidence', 50) or 50), 100))
    if gemini_decision == 'AVOID' and gemini:
        return {'final': 'AVOID', 'confidence': gemini_confidence, 'reason': (gemini or {}).get('reason_ar', 'Gemini رفض الإشارة'), 'gemini_decision': gemini_decision, 'gemini_confidence': gemini_confidence, 'mistral_pass': False, 'openrouter_used': False}

    mistral = None
    mistral_pass = True
    mistral_confidence = 100
    if MISTRAL_ENABLED:
        critic_prompt = f"""You are a devil's advocate for {symbol}. Paper trading only. Gemini decision: {gemini_decision} ({gemini_confidence}). RVOL {signal_data.get('rvol', 0):.1f}x, score {signal_data.get('score', 0)}/100. Return JSON only: {{\"decision\":\"PASS|FAIL\",\"confidence\":0,\"reason_ar\":\"سبب مختصر\"}}"""
        mistral = _parse_ai_json(call_mistral(critic_prompt))
        if mistral:
            mistral_pass = str(mistral.get('decision', 'PASS')).upper() == 'PASS'
            mistral_confidence = max(0, min(int(mistral.get('confidence', 100) or 100), 100))
        if not mistral_pass or mistral_confidence < 40:
            return {'final': 'AVOID', 'confidence': min(gemini_confidence, mistral_confidence), 'reason': (mistral or {}).get('reason_ar', 'Mistral رفض الإشارة'), 'gemini_decision': gemini_decision, 'gemini_confidence': gemini_confidence, 'mistral_pass': False, 'openrouter_used': False}

    openrouter = None
    openrouter_confidence = 0
    if OPENROUTER_ENABLED and (abs(gemini_confidence - mistral_confidence) > 20 or gemini_confidence < 70):
        openrouter = _parse_ai_json(call_openrouter(f"""Final risk check for {symbol}. Gemini: {gemini_decision} {gemini_confidence}; Mistral: {'PASS' if mistral_pass else 'FAIL'}. Return JSON only: {{\"decision\":\"BUY|WATCH|AVOID\",\"confidence\":0,\"reason_ar\":\"سبب مختصر\"}}"""))
        if openrouter:
            openrouter_confidence = max(0, min(int(openrouter.get('confidence', 0) or 0), 100))

    final_score = signal_data.get('score', 0) * 0.40
    final_score += (gemini_confidence if gemini_decision == 'BUY' else gemini_confidence * 0.5) * 0.25
    final_score += (80 if mistral_pass else 20) * 0.20
    if openrouter:
        decision = str(openrouter.get('decision', 'WATCH')).upper()
        final_score += (openrouter_confidence if decision == 'BUY' else openrouter_confidence * 0.5) * 0.15
    final_score = min(100, final_score)
    final = 'BUY' if final_score >= 85 else 'WATCH' if final_score >= 70 else 'AVOID'
    return {'final': final, 'confidence': round(final_score, 1), 'reason': (openrouter or gemini or {}).get('reason_ar', 'تحليل إجماع AI'), 'gemini_decision': gemini_decision, 'gemini_confidence': gemini_confidence, 'mistral_pass': mistral_pass, 'openrouter_used': bool(openrouter)}


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
        if "timed out" in str(e).lower():
            logger.debug(f"RSS timeout {url}")
        else:
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
YAHOO_MIN_REQUEST_GAP = float(os.getenv("YAHOO_MIN_REQUEST_GAP", "1.2"))
YAHOO_OUTAGE_UNTIL = 0.0
YAHOO_INVALID_JSON_COUNT = 0
YAHOO_OUTAGE_LOCK = threading.Lock()
YAHOO_OUTAGE_COOLDOWN = 300

# القائمة اليدوية فُرّغت: كانت تحتوي أسهم حيّة (INTC, ZM, GE, NOW, RDDT, QUBT, CLSK, ASST, XXII ...) وتمنع البوت يشوفها نهائياً.
KNOWN_DELISTED = set()

BAD_TICKER_TTL = 3600  # ساعة واحدة، وفقط لأخطاء 404 (الرمز غير موجود) — مو لأي timeout أو خطأ مؤقت



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
    global YAHOO_LAST_REQUEST, YAHOO_INVALID_JSON_COUNT, YAHOO_OUTAGE_UNTIL
    if get_market_phase() in ("PRE", "AFTER"):
        prepost = True
    # ── circuit breaker: لا نكرر الطلبات أثناء حجب Yahoo أو إرجاع HTML فارغ ──
    with YAHOO_OUTAGE_LOCK:
        if time.time() < YAHOO_OUTAGE_UNTIL:
            return pd.DataFrame()
    # ── تحقق سريع: هل الرمز في القائمة السوداء؟ ──
    symbol = str(symbol).upper().strip()
    if symbol in KNOWN_DELISTED or _is_bad_ticker(symbol):
        return pd.DataFrame()

    try:
        days_map = {"1d": 1, "2d": 2, "5d": 5, "10d": 10, "1mo": 30, "60d": 60, "3mo": 90, "90d": 90, "120d": 120, "6mo": 180, "1y": 365}  # كانت 60d/90d/120d تنجلب كـ 5 أيام فقط!
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
                        logger.debug(f"⛔ {symbol} → 404 (added to blacklist for 1h)")
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
                        with YAHOO_OUTAGE_LOCK:
                            YAHOO_INVALID_JSON_COUNT += 1
                            invalid_count = YAHOO_INVALID_JSON_COUNT
                            if invalid_count >= 3:
                                YAHOO_OUTAGE_UNTIL = time.time() + YAHOO_OUTAGE_COOLDOWN
                                YAHOO_INVALID_JSON_COUNT = 0
                        if invalid_count <= 3:
                            logger.warning(f"Yahoo returned empty/invalid JSON for {symbol}; retry {attempt + 1}/3")
                        if invalid_count >= 3:
                            return pd.DataFrame()
                        time.sleep(2 ** attempt + random.uniform(0.5, 1.5))
                        continue
                    break
                except Exception as request_error:
                    if attempt == 2:
                        raise
                    time.sleep(2 ** attempt + random.uniform(0.5, 1.5))

        if not isinstance(data, dict):
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
        logger.warning(f"Yahoo download skipped for {symbol}: {e}")  # خطأ مؤقت: لا نحظر السهم
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
    """
    Extended-Hours Discovery v2 (Pre-Market + After-Hours).
    الشاشات ما تعكس Pre/After بشكل موثوق، فنفحص شموع الأسهم اللي كانت "بالحدث" (in_play) — مرتّبة بالحرارة —
    بدل 60 سهم عشوائي. يغذي hot_watchlist فقط (بدون Telegram).
    """
    interval = 120
    while True:
        try:
            phase = get_market_phase()
            if phase not in ("PRE", "AFTER"):
                time.sleep(120)
                continue
            scanner_heartbeat("ext_hours")
            now = now_est()
            with state_lock:
                in_play = dict(state.get("in_play", {}))
            ranked = sorted(in_play.items(),
                            key=lambda kv: (_safe_float(kv[1].get("heat")), _safe_float(kv[1].get("last_seen"))),
                            reverse=True)
            symbols = [s for s, _ in ranked if s and s not in KNOWN_DELISTED][:EXT_HOURS_MAX_SYMBOLS]
            discovered = 0
            for symbol in symbols:
                try:
                    df = cached_download(symbol, period="5d", interval="5m", prepost=True)
                    if df is None or df.empty or len(df) < 8:
                        continue
                    d = df.copy()
                    d.columns = [str(c).lower() for c in d.columns]
                    if not {"close", "volume"}.issubset(d.columns):
                        continue
                    d["_s"] = [_classify_session(ts) for ts in d.index]
                    today = now.date()
                    ext = d[(d.index.date == today) & (d["_s"] == phase)]
                    if phase == "PRE":
                        ref = d[(d["_s"] == "REGULAR") & (d.index.date < today)]
                    else:
                        ref = d[(d["_s"] == "REGULAR") & (d.index.date <= today)]
                    if ext.empty or ref.empty:
                        continue
                    price = float(d["close"].iloc[-1])
                    ref_close = float(ref["close"].iloc[-1])
                    if ref_close <= 0:
                        continue
                    change = (price - ref_close) / ref_close * 100
                    ext_volume = float(ext["volume"].sum() or 0)
                    if not (DISCOVERY_PRICE_MIN <= price <= MAX_PRICE and change >= DISCOVERY_MIN_CHANGE and ext_volume >= 10000):
                        continue
                    fm = fresh_metrics(d.drop(columns=["_s"]), phase)
                    if not fm.get("ok") or fm.get("dead"):
                        continue
                    candidate = {
                        "symbol": symbol, "price": price, "change": change, "volume": ext_volume,
                        "rvol": float(get_unified_rvol(d.drop(columns=["_s"]), phase=phase)),
                        "momentum_5m": fm["move_5m"], "vol_rate_ratio": fm["vol_accel"],
                        "phase": phase, "source": "EXT_HOURS",
                    }
                    _merge_into_hot([candidate], time.time())
                    discovered += 1
                except Exception as error:
                    logger.debug(f"[ExtHoursDiscovery] {symbol}: {error}")
            with state_lock:
                save_state()
            logger.info(f"[ExtHoursDiscovery] {phase}: checked={len(symbols)} | alive candidates={discovered}")
            time.sleep(interval)
        except Exception as error:
            logger.error(f"[ExtHoursDiscovery] error: {error}")
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
    # Yahoo batch downloads are opt-in; Top Gainers remains the live Discovery source by default.
    batch_discovery_enabled = os.getenv('FULL_ROTATION_ENABLED', 'false').lower() in ('1', 'true', 'yes', 'on')
    BATCH_SIZE = 40
    MIN_GAIN = 3.0
    BATCH_SLEEP = 1.5
    SYMBOL_COOLDOWN = 300
    last_seen = {}

    while True:
        try:
            if not batch_discovery_enabled:
                time.sleep(300)
                continue
            phase = get_market_phase()
            with state_lock:
                all_tickers = list(state.get('full_tickers', []))
                if not all_tickers:
                    all_tickers = list(state.get('tickers', []))
            all_tickers = list(dict.fromkeys(str(x).upper().strip() for x in all_tickers if x))
            if not all_tickers:
                logger.warning('[Discovery] No tickers available; waiting for universe loader')
                time.sleep(300)
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

# ================================================================
# 🔎 DISCOVERY ENGINE v2 — اكتشاف حيّ بدون عينات عشوائية
#   • يقرأ شاشات Yahoo الحيّة (Gainers + Most Active + Small-Cap Gainers) كل ~45 ثانية = 3 طلبات فقط للدورة.
#   • يقارن كل قراءة بالتي قبلها → زخم لحظي حقيقي (5 دقايق) + تسارع حجم، بدون أي طلب شموع إضافي.
#   • يحذف تلقائياً أي سهم ما أُعيد تأكيده (ميّت) ويرتّب حسب الزخم مو حسب % اليوم.
#   • DISCOVERY_EXTRA_PROVIDERS: مكان جاهز لمصدر ثاني (مثل Webull) بنفس صيغة _normalize_quote.
# ================================================================
from collections import deque

_heartbeats = {}
_price_hist = {}
_price_hist_lock = threading.Lock()
_discovery_stats = {"last_poll": 0.0, "last_count": 0, "errors": 0, "last_ok": False}
DISCOVERY_EXTRA_PROVIDERS = []


def scanner_heartbeat(name):
    """نبضة لكل ماسح — تظهر في /health عشان تعرف مين متأخر أو واقف."""
    _heartbeats[name] = time.time()


# ---------------- كولداون موحّد لكل مصادر التنبيه ----------------
def _alert_gate_allow(symbol, price=0.0, source=""):
    """نفس السهم ما يوصلك مرتين (من أي ماسح) خلال ALERT_SYMBOL_COOLDOWN، إلا لو كمل صعوده +8% بعد 5 دقايق على الأقل."""
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        return False
    now_ts = time.time()
    with state_lock:
        rec = state.get("alert_gate", {}).get(symbol)
    if not rec:
        return True
    age = now_ts - _safe_float(rec.get("ts"))
    if age >= ALERT_SYMBOL_COOLDOWN:
        return True
    last_price = _safe_float(rec.get("price"))
    if (last_price > 0 and _safe_float(price) >= last_price * (1 + ALERT_CONTINUATION_PCT / 100.0)
            and age >= ALERT_CONTINUATION_MIN_GAP):
        return True
    return False


def _alert_gate_mark(symbol, price=0.0, source=""):
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        return
    now_ts = time.time()
    with state_lock:
        gate = state.setdefault("alert_gate", {})
        gate[symbol] = {"ts": now_ts, "price": _safe_float(price), "source": source}
        if len(gate) > 300:
            cutoff = now_ts - 86400
            for k in [k for k, v in gate.items() if _safe_float(v.get("ts")) < cutoff]:
                gate.pop(k, None)


# ---------------- مقاييس الحيوية من شموع 5 دقايق ----------------
def fresh_metrics(df, phase=None):
    """
    يفرّق بين سهم يتحرك الآن وسهم ميّت صعد قبل ساعات:
      move_15m / move_30m : تغيّر السعر آخر 3 / 6 شموع (5د)
      vol_accel           : متوسط حجم آخر 3 شموع ÷ الوسيط الطبيعي قبلها
      off_high_pct        : بعده عن قمة اليوم
      stale               : آخر شمعة قديمة (السهم واقف أو موقوف)
      dead                : ميّت = واقف/قديم أو تراجع عن القمة أو ما فيه أي نشاط لحظي
    """
    out = {"ok": False}
    try:
        if df is None or df.empty or len(df) < 8:
            return out
        d = df.copy()
        d.columns = [str(c).lower() for c in d.columns]
        if not {"close", "high", "volume"}.issubset(d.columns):
            return out
        close = d["close"].astype(float)
        high = d["high"].astype(float)
        vol = d["volume"].astype(float)
        price = float(close.iloc[-1])
        if price <= 0:
            return out
        phase = phase or get_market_phase()

        try:
            age_min = (now_est() - d.index[-1]).total_seconds() / 60.0
        except Exception:
            age_min = 0.0

        def _mv(n):
            if len(close) > n and float(close.iloc[-1 - n]) > 0:
                return (price / float(close.iloc[-1 - n]) - 1.0) * 100.0
            return 0.0

        move_5m, move_15m, move_30m = _mv(1), _mv(3), _mv(6)

        # حجم آخر 3 شموع (ناخذ الأعلى بين "مع الشمعة الجارية" و"بدونها" لأن الشمعة الجارية ناقصة)
        r_with = float(vol.tail(3).mean()) if len(vol) >= 3 else 0.0
        r_without = float(vol.iloc[-4:-1].mean()) if len(vol) >= 4 else 0.0
        recent_avg = max(r_with, r_without)
        base = vol.iloc[-23:-3]
        base = base[base > 0]
        if len(base) >= 5:
            base_avg = float(base.median())
        else:
            base_avg = float(base.mean()) if len(base) else 0.0
        vol_accel = (recent_avg / base_avg) if base_avg > 0 else 0.0

        try:
            today = now_est().date()
            today_mask = (d.index.date == today)
            high_today = high[today_mask]
            day_high = float(high_today.max()) if len(high_today) else float(high.tail(78).max())
        except Exception:
            day_high = float(high.tail(78).max())
        off_high_pct = ((day_high - price) / day_high * 100.0) if day_high > 0 else 0.0
        prior_high = float(high.iloc[-11:-1].max()) if len(high) > 11 else float(high.iloc[:-1].max())
        breakout = price > prior_high
        new_high_recent = float(high.tail(6).max()) >= day_high * 0.998

        stale = age_min > (15.0 if phase == "REGULAR" else 30.0)
        active_signal = ((move_15m >= 2.0 and vol_accel >= 1.2) or vol_accel >= 2.5
                         or (breakout and vol_accel >= 1.2))
        faded = off_high_pct >= 12.0 and move_15m <= 0.5
        dead = bool(stale or faded or not active_signal)

        out.update({
            "ok": True, "price": price, "age_min": round(age_min, 1),
            "move_5m": round(move_5m, 2), "move_15m": round(move_15m, 2), "move_30m": round(move_30m, 2),
            "vol_accel": round(vol_accel, 2), "day_high": day_high, "off_high_pct": round(off_high_pct, 2),
            "breakout": bool(breakout), "new_high_recent": bool(new_high_recent),
            "stale": bool(stale), "faded": bool(faded), "dead": dead,
        })
        return out
    except Exception as error:
        logger.debug(f"fresh_metrics error: {error}")
        return {"ok": False}


def score_setup(df, candidate=None, phase=None):
    """سكور فني حقيقي (0-100) من شموع 5 دقايق فقط + الحيوية اللحظية. يرجع None لو البيانات ما تكفي (ما فيه أي قيم مفترضة)."""
    candidate = candidate or {}
    phase = phase or get_market_phase()
    try:
        d = df.copy()
        d.columns = [str(c).lower() for c in d.columns]
        if len(d) < 20:
            return None
        d = compute_indicators(d)
        fm = fresh_metrics(d, phase)
        if not fm.get("ok"):
            return None
        price = fm["price"]
        rvol = float(get_unified_rvol(d, phase=phase))
        rsi = _safe_float(d["rsi"].iloc[-1], 50.0)
        vwap = _safe_float(d["vwap"].iloc[-1], price)
        ema9 = _safe_float(d["ema9"].iloc[-1], price)
        ema21 = _safe_float(d["ema21"].iloc[-1], price)
        above_vwap = price >= vwap
        ema_bullish = ema9 > ema21
        change = _safe_float(candidate.get("change"))

        score, factors = 0, []
        if rvol >= 4:
            score += 22; factors.append(f"RVOL {rvol:.1f}x")
        elif rvol >= 2.5:
            score += 16; factors.append(f"RVOL {rvol:.1f}x")
        elif rvol >= 1.5:
            score += 8
        if fm["vol_accel"] >= 3:
            score += 18; factors.append(f"تسارع حجم {fm['vol_accel']:.1f}x")
        elif fm["vol_accel"] >= 1.8:
            score += 10; factors.append(f"تسارع حجم {fm['vol_accel']:.1f}x")
        if fm["move_15m"] >= 6:
            score += 18; factors.append(f"+{fm['move_15m']:.1f}% آخر 15د")
        elif fm["move_15m"] >= 2.5:
            score += 12; factors.append(f"+{fm['move_15m']:.1f}% آخر 15د")
        elif fm["move_15m"] >= 1:
            score += 5
        elif fm["move_15m"] <= -1:
            score -= 10
        if above_vwap:
            score += 10; factors.append("فوق VWAP")
        if ema_bullish:
            score += 5; factors.append("EMA Bullish")
        if 45 <= rsi <= 72:
            score += 5; factors.append(f"RSI {rsi:.0f}")
        elif rsi > 90:
            score -= 5
        if fm["breakout"]:
            score += 10; factors.append("اختراق قمة قريبة")
        if fm["new_high_recent"]:
            score += 5
        if change >= 20:
            score += 8; factors.append(f"+{change:.1f}% اليوم")
        elif change >= 8:
            score += 4
        if fm["off_high_pct"] >= 10:
            score -= 25
        elif fm["off_high_pct"] >= 5:
            score -= 10
        if fm["dead"]:
            score = min(score, 25)
        score = max(0, min(100, int(round(score))))

        vol_now = _safe_float(d["volume"].iloc[-1])
        vol_avg = max(_safe_float(d["volume"].tail(20).mean()), 1.0)
        return {
            "price": price, "rvol": rvol, "rsi": rsi, "vwap": vwap, "above_vwap": above_vwap,
            "ema_bullish": ema_bullish, "volume_spike": vol_now >= vol_avg * 1.5,
            "breakout": fm["breakout"], "score": score, "factors": factors, "fm": fm, "dead": fm["dead"],
        }
    except Exception as error:
        logger.debug(f"score_setup error: {error}")
        return None


# ---------------- ترتيب الأولوية (الزخم أولاً، مو % اليوم) ----------------
def discovery_priority(item, now_ts=None):
    now_ts = now_ts or time.time()

    def f(key):
        return _safe_float(item.get(key))

    score = 0.0
    score += min(max(f("momentum_5m"), 0.0), 25.0) * 2.0
    score += min(max(f("momentum_1m"), 0.0), 20.0) * 2.0
    score += min(max(f("vol_rate_ratio"), 0.0), 15.0) * 2.5
    score += min(max(f("rvol"), 0.0), 15.0) * 0.8           # RVOL التراكمي يبقى عالي طول اليوم للأسهم الميتة، فوزنه صغير
    score += min(max(f("change"), 0.0), 40.0) * 0.3          # % اليوم صار مجرد كاسر تعادل
    qj = f("qj_ts")
    if qj > 0 and now_ts - qj < 300:
        score += 25.0                                          # قفزة مؤكدة بشمعة 1د قبل أقل من 5 دقايق
    if "EXT_HOURS" in str(item.get("source", "")).upper():
        score += 10.0
    score -= min(max(f("off_high_pct"), 0.0), 40.0) * 0.8     # بعيد عن قمة اليوم = تراجع = أقل أولوية
    last_seen = f("last_seen") or f("timestamp") or now_ts
    score -= max(0.0, (now_ts - last_seen) / 60.0) * 0.7      # كل دقيقة بدون تأكيد تنقص الأولوية
    return score


# ---------------- سجل الأسعار بين القراءات ----------------
def _update_price_history(symbol, ts, price, volume):
    with _price_hist_lock:
        dq = _price_hist.get(symbol)
        if dq is None:
            dq = deque(maxlen=60)
            _price_hist[symbol] = dq
        if dq and ts - dq[-1][0] < 5:
            dq[-1] = (ts, price, volume)
        else:
            dq.append((ts, price, volume))
        if len(_price_hist) > 2000:
            for k in list(_price_hist.keys())[:500]:
                if k != symbol:
                    _price_hist.pop(k, None)
        return list(dq)


def _ref_sample(hist, window):
    """أحدث عينة عمرها ≥ 80% من النافذة، وإلا أقدم عينة لو الفترة ≥ نصف النافذة."""
    if len(hist) < 2:
        return None
    ts_n = hist[-1][0]
    best = None
    for s in hist[:-1]:
        if ts_n - s[0] >= window * 0.8:
            best = s
    if best is None:
        oldest = hist[0]
        if ts_n - oldest[0] >= window * 0.5:
            best = oldest
    return best


def _momentum_from_history(hist, avg_volume, volume_valid):
    """يرجع (تغيّر 5 دقايق %, تغيّر ~1 دقيقة %, نسبة تسارع الحجم) من قراءات الشاشات فقط."""
    if len(hist) < 2:
        return 0.0, 0.0, 0.0
    ts_n, p_n, v_n = hist[-1]
    r5 = _ref_sample(hist, 300)
    r1 = _ref_sample(hist, 60)
    r3 = _ref_sample(hist, 180)
    move_5m = ((p_n / r5[1] - 1.0) * 100.0) if (r5 and r5[1] > 0) else 0.0
    move_1m = ((p_n / r1[1] - 1.0) * 100.0) if (r1 and r1[1] > 0) else 0.0
    vol_ratio = 0.0
    if volume_valid and r3 and avg_volume > 0:
        dt_min = (ts_n - r3[0]) / 60.0
        dv = v_n - r3[2]
        if dt_min > 0 and dv >= 0:
            vol_ratio = (dv / dt_min) / max(avg_volume / 390.0, 1.0)
    return move_5m, move_1m, vol_ratio


def _expected_volume_fraction(now=None):
    """كم نسبة من حجم اليوم المعتاد المفروض يكون تداول لين الآن (منحنى مبسّط: الافتتاح أثقل)."""
    now = now or now_est()
    mins = now.hour * 60 + now.minute - (9 * 60 + 30)
    if mins <= 0:
        return 0.0
    mins = min(mins, 390)
    return min(1.0, max(0.02, (mins / 390.0) ** 0.6))


# ---------------- مصدر Yahoo (شاشات جاهزة) ----------------
def _http_get_json(url, timeout=10):
    """محاولة 1: بصمة متصفح (impersonate) — محاولة 2: الأسلوب القديم بـ User-Agent عادي. أي واحدة تنجح تكفي."""
    last_error = None
    attempts = (
        {"impersonate": "chrome120"},
        {"headers": {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                     "Accept": "application/json"}},
    )
    for kwargs in attempts:
        try:
            resp = requests.get(url, timeout=timeout, **kwargs)
            if resp.status_code == 200:
                return resp.json()
            last_error = RuntimeError(f"HTTP {resp.status_code}")
            if resp.status_code == 429:
                break      # محظورين مؤقتاً: لا نضاعف الطلبات
        except Exception as error:
            last_error = error
    raise last_error if last_error else RuntimeError("request failed")


def _yahoo_screener_quotes(screener_id, count=100):
    url = ("https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
           f"?scrIds={screener_id}&count={int(count)}&formatted=false&includePrePost=true&lang=en-US&region=US")
    data = _http_get_json(url, timeout=10)
    result = (data.get("finance", {}) or {}).get("result", []) or []
    return (result[0].get("quotes", []) if result else []) or []


def _normalize_quote(q, phase):
    """يحوّل quote من Yahoo لصيغة موحدة. يرجع None للـETF والرموز الغريبة والـwarrants/rights/units."""
    try:
        sym = str(q.get("symbol", "")).upper().strip()
        if not sym or not sym.isalpha() or len(sym) > 5:
            return None
        if len(sym) == 5 and sym[-1] in ("W", "R", "U"):
            return None
        qtype = str(q.get("quoteType") or "EQUITY").upper()
        if qtype != "EQUITY":
            return None
        price = _safe_float(q.get("regularMarketPrice"))
        change = _safe_float(q.get("regularMarketChangePercent"))
        volume = _safe_float(q.get("regularMarketVolume"))
        ext_live = False
        if phase == "PRE" and _safe_float(q.get("preMarketPrice")) > 0:
            price = _safe_float(q.get("preMarketPrice"))
            change = _safe_float(q.get("preMarketChangePercent"), change)
            ext_live = True
        elif phase == "AFTER" and _safe_float(q.get("postMarketPrice")) > 0:
            price = _safe_float(q.get("postMarketPrice"))
            change = _safe_float(q.get("postMarketChangePercent"), change)
            ext_live = True
        if price <= 0:
            return None
        avg_vol = _safe_float(q.get("averageDailyVolume3Month")) or _safe_float(q.get("averageDailyVolume10Day"))
        day_high = _safe_float(q.get("regularMarketDayHigh"))
        off_high = ((day_high - price) / day_high * 100.0) if (phase == "REGULAR" and day_high > 0 and price < day_high) else 0.0
        return {"symbol": sym, "price": price, "change": change, "volume": volume,
                "avg_volume": avg_vol, "market_cap": _safe_float(q.get("marketCap")), "ext_live": ext_live,
                "off_high_pct": round(off_high, 2)}
    except Exception:
        return None


def _merge_into_hot(cands, now_ts=None):
    """يدمج المرشحين في hot_watchlist (يحافظ على first_seen) ويحذف أي سهم ما أُعيد تأكيده خلال HOT_MAX_AGE_SEC."""
    now_ts = now_ts or time.time()
    with state_lock:
        existing = {}
        for x in state.get("hot_watchlist", []):
            if isinstance(x, dict) and x.get("symbol"):
                existing[str(x["symbol"]).upper().strip()] = dict(x)
        for c in cands:
            sym = str(c.get("symbol", "")).upper().strip()
            if not sym:
                continue
            old = existing.get(sym, {})
            merged = dict(old)
            merged.update(c)
            merged["symbol"] = sym
            merged["first_seen"] = old.get("first_seen", now_ts)
            merged["last_seen"] = now_ts
            merged["timestamp"] = now_ts
            existing[sym] = merged
        fresh = [v for v in existing.values()
                 if now_ts - _safe_float(v.get("last_seen", v.get("timestamp"))) <= HOT_MAX_AGE_SEC]
        fresh.sort(key=lambda it: discovery_priority(it, now_ts), reverse=True)
        state["hot_watchlist"] = fresh[:HOT_MAX_ITEMS]
        state["hot_watchlist_timestamp"] = now_ts


def _update_in_play(cands, now_ts=None):
    """يحفظ الأسهم 'اللي كانت بالحدث' (ذاكرة 48 ساعة) — تُستخدم لفحص Pre/After بدل عينة عشوائية."""
    now_ts = now_ts or time.time()
    with state_lock:
        ip = state.setdefault("in_play", {})
        for c in cands:
            heat = (max(_safe_float(c.get("change")), 0.0) + 5.0 * max(_safe_float(c.get("rvol")), 0.0)
                    + 2.0 * max(_safe_float(c.get("momentum_5m")), 0.0))
            old = ip.get(c["symbol"], {})
            ip[c["symbol"]] = {"last_seen": now_ts, "price": c["price"], "change": c.get("change", 0.0),
                               "volume": c.get("volume", 0.0), "heat": max(heat, _safe_float(old.get("heat")) * 0.9)}
        cutoff = now_ts - 48 * 3600
        for s in [k for k, v in ip.items() if _safe_float(v.get("last_seen")) < cutoff]:
            ip.pop(s, None)
        if len(ip) > 400:
            keep = sorted(ip.items(), key=lambda kv: _safe_float(kv[1].get("heat")), reverse=True)[:400]
            state["in_play"] = dict(keep)


def _discovery_poll_once():
    """قراءة واحدة لكل الشاشات. يرجع عدد المرشحين اللي دخلوا القائمة الساخنة."""
    phase = get_market_phase()
    now_ts = time.time()
    merged = {}
    ok_screeners = 0
    for scr in DISCOVERY_SCREENERS:
        try:
            quotes = _yahoo_screener_quotes(scr, DISCOVERY_COUNT)
            ok_screeners += 1
        except Exception as error:
            _discovery_stats["errors"] += 1
            logger.warning(f"[Discovery] screener {scr} failed: {error}")
            continue
        for q in quotes:
            n = _normalize_quote(q, phase)
            if not n:
                continue
            cur = merged.get(n["symbol"])
            if cur is None:
                n["screeners"] = [scr]
                merged[n["symbol"]] = n
            else:
                cur["screeners"].append(scr)
        time.sleep(0.3)
    for provider in list(DISCOVERY_EXTRA_PROVIDERS):
        try:
            for n in (provider() or []):
                if isinstance(n, dict) and n.get("symbol") and _safe_float(n.get("price")) > 0 and n["symbol"] not in merged:
                    n.setdefault("screeners", ["EXTRA"])
                    n.setdefault("volume", 0.0)
                    n.setdefault("change", 0.0)
                    n.setdefault("avg_volume", 0.0)
                    merged[n["symbol"]] = n
                    ok_screeners += 1
        except Exception as error:
            logger.warning(f"[Discovery] extra provider failed: {error}")
    _discovery_stats["last_ok"] = ok_screeners > 0
    if not merged:
        return 0

    hot_cands, play_cands = [], []
    for sym, n in merged.items():
        price = n["price"]
        if sym in KNOWN_DELISTED or not (DISCOVERY_PRICE_MIN <= price <= DISCOVERY_PRICE_MAX):
            continue
        hist = _update_price_history(sym, now_ts, price, n["volume"])
        vol_valid = (phase == "REGULAR")
        m5, m1, vr = _momentum_from_history(hist, n.get("avg_volume", 0.0), vol_valid)
        est_rvol = 0.0
        if vol_valid and n.get("avg_volume", 0.0) > 0:
            frac = _expected_volume_fraction()
            if frac > 0:
                est_rvol = n["volume"] / (n["avg_volume"] * frac)
        n.update({"momentum_5m": round(m5, 2), "momentum_1m": round(m1, 2),
                  "vol_rate_ratio": round(vr, 2), "rvol": round(est_rvol, 2)})
        change = n["change"]
        live = vol_valid or n.get("ext_live", False)
        if (live and n["volume"] >= MIN_VOLUME
                and (change >= DISCOVERY_MIN_CHANGE or m5 >= 2.0 or vr >= 2.5 or est_rvol >= 2.0)):
            cand = dict(n)
            cand["source"] = "YF:" + ",".join(n.get("screeners", []))
            cand["phase"] = phase
            hot_cands.append(cand)
        if change >= 8.0 or est_rvol >= 3.0 or vr >= 3.0 or m5 >= 5.0:
            play_cands.append(n)

    _merge_into_hot(hot_cands, now_ts)
    _update_in_play(play_cands, now_ts)
    with state_lock:
        ordered = sorted(hot_cands, key=lambda c: discovery_priority(c, now_ts), reverse=True)
        universe = [c["symbol"] for c in ordered][:MAX_TICKERS_TO_SCAN]
        if len(universe) < 30:
            ip = state.get("in_play", {})
            for s, _ in sorted(ip.items(), key=lambda kv: _safe_float(kv[1].get("heat")), reverse=True):
                if s not in universe:
                    universe.append(s)
                if len(universe) >= 60:
                    break
        if universe:
            state["tickers"] = universe
        state["last_ticker_update"] = now_ts
    return len(hot_cands)


def top_gainers_scanner():
    """Discovery Engine v2 (اسم الدالة محفوظ للتوافق): يقرأ شاشات Yahoo كل DISCOVERY_INTERVAL_SEC ويغذي hot_watchlist."""
    fail_streak = 0
    while True:
        try:
            scanner_heartbeat("discovery")
            if get_market_phase() == "CLOSED":
                time.sleep(120)
                continue
            t0 = time.time()
            n = _discovery_poll_once()
            _discovery_stats.update(last_poll=time.time(), last_count=n)
            fail_streak = 0 if _discovery_stats.get("last_ok") else fail_streak + 1
            logger.info(f"[Discovery] hot candidates={n} | took={time.time() - t0:.1f}s | errors={_discovery_stats['errors']}")
            with state_lock:
                save_state()
        except Exception as error:
            fail_streak += 1
            logger.error(f"[Discovery] error: {error}")
        time.sleep(min(300, DISCOVERY_INTERVAL_SEC * (2 ** min(fail_streak, 3))))


def quick_jump_scanner():
    """
    QUICK DISCOVERY ENGINE v2
    بدل مسح 100-400 سهم عشوائي (بطيء ويفوّت الحركة): يفحص فقط الأسهم اللي تحرّكت لتوها حسب قراءات الشاشات
    (زخم 5 دقايق أو تسارع حجم)، ويؤكد القفزة بشمعة 1 دقيقة. لا يرسل Telegram — يضيف/يرفع الأولوية في hot_watchlist.
    """
    _seen_jumps = {}
    while True:
        try:
            scanner_heartbeat("quick_jump")
            phase = get_market_phase()
            if phase not in ("REGULAR", "PRE", "AFTER"):
                time.sleep(60)
                continue
            now_ts = time.time()
            with state_lock:
                hot = [dict(x) for x in state.get("hot_watchlist", []) if isinstance(x, dict)]
            spiking = [h for h in hot
                       if now_ts - _safe_float(h.get("last_seen", h.get("timestamp"))) <= 300
                       and (_safe_float(h.get("momentum_5m")) >= 3.0
                            or _safe_float(h.get("momentum_1m")) >= 2.0
                            or _safe_float(h.get("vol_rate_ratio")) >= 3.0)]
            spiking.sort(key=lambda h: discovery_priority(h, now_ts), reverse=True)
            for cand in spiking[:QUICK_JUMP_MAX_CHECK]:
                symbol = str(cand.get("symbol", "")).upper().strip()
                try:
                    if not symbol or now_ts - _seen_jumps.get(symbol, 0) < 180:
                        continue
                    df = cached_download(symbol, period="1d", interval="1m")
                    if df is None or df.empty or len(df) < 4:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    if not {"open", "close", "volume"}.issubset(df.columns):
                        continue
                    df = df.dropna(subset=["open", "close", "volume"])
                    if len(df) < 4:
                        continue
                    price = float(df["close"].iloc[-1])
                    open_1m = float(df["open"].iloc[-1])
                    close_2 = float(df["close"].iloc[-2])
                    close_4 = float(df["close"].iloc[-4])
                    vol_1m = float(df["volume"].iloc[-1])
                    vol_3m = float(df["volume"].tail(3).sum())
                    if not (QUICK_JUMP_PRICE_MIN <= price <= QUICK_JUMP_PRICE_MAX):
                        continue
                    if max(vol_1m, vol_3m / 3.0) < QUICK_JUMP_MIN_VOL:
                        continue
                    jump_incandle = (price - open_1m) / max(open_1m, 0.0001) * 100
                    jump_2candles = (price - close_2) / max(close_2, 0.0001) * 100
                    jump_3candles = (price - close_4) / max(close_4, 0.0001) * 100
                    best_jump = max(jump_incandle, jump_2candles)
                    if best_jump < QUICK_JUMP_MIN_GAIN_1M and jump_3candles < QUICK_JUMP_MIN_GAIN_3M:
                        continue
                    avg_vol = float(df["volume"].tail(20).mean()) if len(df) >= 20 else vol_1m
                    rvol = (vol_1m / max(avg_vol, 1))
                    today_df = df[df.index.date == now_est().date()]
                    day_high = float(today_df["high"].max()) if (not today_df.empty and "high" in today_df) else price
                    update = {
                        "symbol": symbol, "price": price, "volume": vol_1m,
                        "momentum_1m": max(best_jump, jump_3candles), "day_high": day_high,
                        "near_hod": (price >= day_high * 0.98), "phase": phase,
                        "qj_ts": now_ts, "qj_rvol": rvol,
                    }
                    _merge_into_hot([update], now_ts)
                    _seen_jumps[symbol] = now_ts
                    logger.info("[QuickJump] %s +%.1f%% (1m) / +%.1f%% (3m) | RVOL(1m)=%.1fx → Ranking",
                                symbol, best_jump, jump_3candles, rvol)
                except Exception as symbol_error:
                    logger.debug("[QuickJump] %s: %s", symbol, symbol_error)
                    continue
                time.sleep(0.1)
        except Exception as error:
            logger.error("[QuickJump] scanner error: %s", error)
        time.sleep(QUICK_JUMP_SCAN_INTERVAL)


# ================================================================
# 💥 EXPLOSIVE SETUP ENGINE v1 — يصيد "القاعدة قبل الانفجار"
#
# الفكرة (نمط IMCC): سهم عليه اهتمام حقيقي → اندفاعة قوية بحجم عالي (Thrust) → تصحيح 30-65% →
# قاعدة ضيقة يجف فيها الحجم وتضغط على مقاومة (Base) → اختراق بحجم = موجة ثانية.
# المحرك يبني مستويات الخطة من البنية نفسها (مو نسب ثابتة): الدخول فوق قمة القاعدة، الوقف تحت قاعها،
# والأهداف من "المساحة المتاحة" (قمم سابقة، فيبو التصحيح، مسافة العمود، أرقام نفسية).
# ================================================================
import math

SETUP_ENGINE_ENABLED     = os.getenv("SETUP_ENGINE_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
SETUP_ARM_MIN            = int(os.getenv("SETUP_ARM_MIN", "62"))              # أدنى سكور لتسليح الإعداد
SETUP_THRUST_MIN_PCT     = float(os.getenv("SETUP_THRUST_MIN_PCT", "30"))     # أدنى اندفاعة (%) لتعتبر السهم "له اهتمام"
SETUP_BASE_MIN_MIN       = int(os.getenv("SETUP_BASE_MIN_MIN", "10"))         # أقل مدة للقاعدة (دقائق)
SETUP_BASE_MAX_RANGE     = float(os.getenv("SETUP_BASE_MAX_RANGE", "0.18"))   # أقصى مدى (ذيول) للقاعدة
SETUP_BASE_MAX_CLOSE_RANGE = 0.12                                             # أقصى مدى للإغلاقات داخل القاعدة
SETUP_MAX_TRIGGER_DIST   = float(os.getenv("SETUP_MAX_TRIGGER_DIST", "6"))    # % أقصى بُعد عن الاختراق لنسلّح
SETUP_T1_MIN_PCT         = float(os.getenv("SETUP_T1_MIN_PCT", "15"))         # الهدف الأول لا يقل عن 15% لو فيه مساحة
SETUP_MAX_ACTIVE         = int(os.getenv("SETUP_MAX_ACTIVE", "6"))
SETUP_ARM_TTL_MIN        = int(os.getenv("SETUP_ARM_TTL_MIN", "120"))         # إعداد مسلّح بدون اختراق يُلغى بعد ساعتين
SETUP_TRACK_TTL_MIN      = int(os.getenv("SETUP_TRACK_TTL_MIN", "180"))       # متابعة الصفقة بعد الاختراق (لقياس أقصى صعود)
SETUP_SCAN_INTERVAL      = int(os.getenv("SETUP_SCAN_INTERVAL", "60"))        # كل كم ثانية تُفحص القائمة
SETUP_MAX_PER_CYCLE      = int(os.getenv("SETUP_MAX_PER_CYCLE", "14"))        # سقف شموع 1د لكل دورة (المتابَعة + جدد)
SETUP_MAX_ALERTS_PER_DAY = int(os.getenv("SETUP_MAX_ALERTS_PER_DAY", "15"))


def _tick_for(price):
    return 0.0001 if price < 1.0 else 0.01


def _fmt_px(price):
    return f"{price:.4f}" if price < 1.0 else f"{price:.2f}"


def _session_start(now=None):
    now = now or now_est()
    return now.replace(hour=4, minute=0, second=0, microsecond=0)


def _to_grid(df, start=None):
    """شموع دقيقة منتظمة: الدقيقة بدون صفقات = شمعة مسطّحة (حجم صفر) بدل ما تختفي — عشان الأسهم الضعيفة السيولة."""
    try:
        d = df.copy()
        d.columns = [str(c).lower() for c in d.columns]
        if not {"open", "high", "low", "close", "volume"}.issubset(d.columns):
            return None
        d = d[["open", "high", "low", "close", "volume"]].astype(float)
        d = d.dropna(subset=["close"])
        if start is not None:
            d = d[d.index >= start]
        if d.empty:
            return None
        d = d[~d.index.duplicated(keep="last")].sort_index()
        full = pd.date_range(d.index[0], d.index[-1], freq="1min")
        d = d.reindex(full)
        d["close"] = d["close"].ffill()
        for c in ("open", "high", "low"):
            d[c] = d[c].fillna(d["close"])
        d["volume"] = d["volume"].fillna(0.0)
        return d
    except Exception as error:
        logger.debug(f"_to_grid error: {error}")
        return None


def _zigzag(highs, lows, thr):
    """قمم/قيعان مؤكدة بانعكاس ≥ thr. يرجع [(index, price, 'H'|'L', confirmed)] والأخيرة غير مؤكدة."""
    n = len(highs)
    pivots = []
    if n < 2:
        return pivots
    up_i, up_p = 0, float(highs[0])
    dn_i, dn_p = 0, float(lows[0])
    direction = 0
    for i in range(1, n):
        h, l = float(highs[i]), float(lows[i])
        if direction >= 0 and h >= up_p:
            up_i, up_p = i, h
        if direction <= 0 and l <= dn_p:
            dn_i, dn_p = i, l
        if direction == 0:
            if up_p > 0 and (up_p - l) / up_p >= thr and up_i >= dn_i:
                if dn_p > 0 and up_p / dn_p - 1.0 >= thr and dn_i < up_i:
                    pivots.append((dn_i, dn_p, "L", True))
                pivots.append((up_i, up_p, "H", True))
                direction = -1
                dn_i, dn_p = i, l
            elif dn_p > 0 and (h - dn_p) / dn_p >= thr and dn_i >= up_i:
                pivots.append((dn_i, dn_p, "L", True))
                direction = 1
                up_i, up_p = i, h
        elif direction == 1:
            if up_p > 0 and (up_p - l) / up_p >= thr:
                pivots.append((up_i, up_p, "H", True))
                direction = -1
                dn_i, dn_p = i, l
        else:
            if dn_p > 0 and (h - dn_p) / dn_p >= thr:
                pivots.append((dn_i, dn_p, "L", True))
                direction = 1
                up_i, up_p = i, h
    if direction == 1:
        pivots.append((up_i, up_p, "H", False))
    elif direction == -1:
        pivots.append((dn_i, dn_p, "L", False))
    return pivots


def _trim_recent_advance(hi, lo, cl, vo, i_h, n):
    """
    يفحص آخر ست شموع: هل تمثّل اندفاعة محلية جديدة (سعر يتصاعد باستمرار + حجم أعلى من المعتاد) بدل جزء من القاعدة؟
    بدون هذا الفحص: تقدّم تدريجي على عدة شموع (لا قفزة واحدة حادة) قد يظل ضمن سماحية مدى القاعدة فتبتلعه نافذة
    القاعدة نفسها، فيرتفع سعر الدخول المُكتشف ليقترب من السعر الحالي بدل مقاومة القاعدة الحقيقية (دخول متأخر).
    يرجع عدد الشموع الواجب استبعادها من آخر النافذة قبل البحث عن القاعدة.
    """
    max_check = min(6, n - 1 - i_h - SETUP_BASE_MIN_MIN)
    if max_check <= 0:
        return 0
    calm_end = n - 1 - max_check
    calm_start = max(i_h + 1, calm_end - 20)
    if calm_end <= calm_start:
        return 0
    ref_price = float(np.median(cl[calm_start: calm_end + 1]))
    active = vo[calm_start: calm_end + 1]
    active = active[active > 0]
    ref_vol = float(np.median(active)) if len(active) else 0.0
    if ref_price <= 0:
        return 0
    trim = 0
    for back in range(1, max_check + 1):
        k = n - back
        rise = (float(cl[k]) - ref_price) / ref_price
        vol_ok = ref_vol <= 0 or float(vo[k]) >= ref_vol * 2.0
        held = float(cl[k]) >= float(cl[k - 1]) * 0.997 if k - 1 >= 0 else True
        if rise >= 0.03 and vol_ok and held:
            trim = back
        else:
            break
    return trim


def _find_base(hi, lo, cl, start_min_i, end_i):
    """
    أطول نافذة تنتهي عند end_i مدى إغلاقاتها ≤ 12% ومدى ذيولها ≤ 18% (مسح للخلف).
    مربوطة بمرجع القمم لآخر SETUP_BASE_MIN_MIN شمعة: لو هبوط الاندفاعة يتلاشى تدريجياً (ذيل الهبوط ما ينقطع بحدة)،
    بدون هذا الربط كان المسح للخلف يبتلع جزءاً من الهبوط نفسه فيرفع قمة القاعدة (ودخول وهمي أعلى من الحقيقي).
    يرجع (start_i, high, low) أو None.
    """
    if end_i <= start_min_i:
        return None
    core = max(start_min_i, end_i - SETUP_BASE_MIN_MIN + 1)
    ref_high = float(max(hi[core: end_i + 1]))
    b_hi, b_lo = float(hi[end_i]), float(lo[end_i])
    c_hi, c_lo = float(cl[end_i]), float(cl[end_i])
    start = end_i
    for j in range(end_i - 1, start_min_i - 1, -1):
        if float(hi[j]) > ref_high * 1.05:
            break                                   # ما زلنا في ذيل هبوط الاندفاعة، مو في القاعدة
        nb_hi, nb_lo = max(b_hi, float(hi[j])), min(b_lo, float(lo[j]))
        nc_hi, nc_lo = max(c_hi, float(cl[j])), min(c_lo, float(cl[j]))
        if nb_lo <= 0 or nc_lo <= 0:
            break
        if (nb_hi - nb_lo) / nb_lo > SETUP_BASE_MAX_RANGE or (nc_hi - nc_lo) / nc_lo > SETUP_BASE_MAX_CLOSE_RANGE:
            break
        b_hi, b_lo, c_hi, c_lo, start = nb_hi, nb_lo, nc_hi, nc_lo, j
    return start, b_hi, b_lo


def _mean_tr_pct(hi, lo, cl, vo, a, b):
    """متوسط المدى % للدقائق النشطة في [a, b)."""
    if b - a <= 0:
        return 0.0
    rng = [(hi[k] - lo[k]) / cl[k] for k in range(a, b) if cl[k] > 0 and vo[k] > 0]
    if not rng:
        rng = [(hi[k] - lo[k]) / cl[k] for k in range(a, b) if cl[k] > 0]
    return float(sum(rng) / len(rng)) if rng else 0.0


# ---------------- الأهداف من المساحة المتاحة ----------------
def build_room_targets(df, entry, stop, h1=None, l0=None, l2=None, t1_min_pct=None):
    """
    يبني الأهداف من بنية السهم بدل نسب ثابتة:
      قمم سابقة + فيبو تصحيح (H1→L2) + مسافة العمود (Measured Move) + امتدادات + أرقام نفسية.
    T1 = أول مستوى ≥ t1_min_pct (15%) و≥ 1.5R، T2/T3 = مستويات رئيسية أبعد، Runner = أعلى امتداد.
    """
    t1_min = (SETUP_T1_MIN_PCT if t1_min_pct is None else t1_min_pct) / 100.0
    entry, stop = float(entry), float(stop)
    risk = entry - stop
    if entry <= 0 or risk <= 0:
        return None
    levels = []  # (price, weight, label)
    try:
        if df is not None and len(df) >= 10:
            d = df.copy()
            d.columns = [str(c).lower() for c in d.columns]
            highs, lows = d["high"].astype(float).values, d["low"].astype(float).values
            for (_, p, kind, _) in _zigzag(highs, lows, 0.08):
                if kind == "H" and p > entry * 1.01:
                    levels.append((p, 3, "قمة سابقة"))
            if h1 is None:
                h1 = float(np.nanmax(highs))
    except Exception as error:
        logger.debug(f"room targets swings error: {error}")
    if h1 and h1 > entry * 1.01:
        levels.append((float(h1), 4, "قمة الاندفاعة"))
    if h1 and l2 and h1 > l2:
        for frac, w in ((0.382, 2), (0.5, 2), (0.618, 3), (0.786, 2)):
            lvl = l2 + (h1 - l2) * frac
            if lvl > entry * 1.01:
                levels.append((lvl, w, f"فيبو {frac:g}"))
    if h1 and l0 and h1 > l0:
        pole = h1 - l0
        if entry + pole > entry * 1.05:
            levels.append((entry + pole, 3, "مسافة العمود"))
        for ext in (1.272, 1.618):
            lvl = l0 + pole * ext
            if lvl > entry * 1.05:
                levels.append((lvl, 2, f"امتداد {ext:g}"))
    step = 0.25 if entry < 2 else (0.5 if entry < 10 else 1.0)
    lvl, added = math.ceil(entry * 1.02 / step) * step, 0
    while lvl <= entry * 2.2 and added < 8:
        levels.append((lvl, 1, "رقم نفسي"))
        lvl += step
        added += 1

    levels.sort(key=lambda x: x[0])
    merged = []
    for p, w, lab in levels:
        if merged and p <= merged[-1][0] * 1.015:
            if w > merged[-1][1]:
                merged[-1] = (max(p, merged[-1][0]) if w >= merged[-1][1] else merged[-1][0], w, lab)
            continue
        merged.append((p, w, lab))

    def pct(x):
        return (x - entry) / entry * 100.0

    cands = [m for m in merged if m[0] >= entry * (1 + t1_min) and (m[0] - entry) / risk >= 1.5]
    if cands:
        t1 = cands[0]
    else:
        t1 = (entry + max(2.0 * risk, entry * t1_min), 0, "مسافة R:R")
    t2 = next((m for m in merged if m[0] >= t1[0] * 1.06 and m[1] >= 2), None)
    if t2 is None:
        t2 = next((m for m in merged if m[0] >= t1[0] * 1.06), (t1[0] * 1.15, 0, "مسافة"))
    t3 = next((m for m in merged if m[0] >= t2[0] * 1.06 and m[1] >= 3), None)
    if t3 is None:
        t3 = next((m for m in merged if m[0] >= t2[0] * 1.06), (t2[0] * 1.15, 0, "مسافة"))
    runner = None
    far = [m for m in merged if m[0] >= t3[0] * 1.10 and m[1] >= 2 and m[0] <= entry * 3.5]
    if far:
        runner = far[-1]
    strong = [m for m in merged if m[1] >= 3]
    room = pct(strong[0][0]) if strong else pct(t2[0])

    def pack(t):
        return None if t is None else {"price": float(t[0]), "pct": pct(t[0]), "label": t[2],
                                        "rr": (t[0] - entry) / risk}
    return {"t1": pack(t1), "t2": pack(t2), "t3": pack(t3), "runner": pack(runner), "room_pct": room,
            "levels": [(round(m[0], 4), m[1], m[2]) for m in merged[:14]]}


# ---------------- كشف الإعداد ----------------
def detect_setup(df, ctx=None, now=None):
    """
    يفحص شموع الجلسة (دقيقة) ويرجع إعداد 'قاعدة قبل الانفجار' أو None.
    ctx (اختياري): float_shares, market_cap, catalyst_age_min, prev_day_change, symbol.
    """
    ctx = ctx or {}
    now = now or now_est()
    g = _to_grid(df, _session_start(now))
    if g is None or len(g) < 25:
        return None
    hi, lo, cl, vo = g["high"].values, g["low"].values, g["close"].values, g["volume"].values
    idx = g.index
    n = len(g)
    price = float(cl[-1])
    if not (0.3 <= price <= max(MAX_PRICE, 20.0)):
        return None

    # 1) الاندفاعة (Thrust): قمة الجلسة + بداية آخر موجة صاعدة إليها
    i_h = n - 1 - int(np.argmax(hi[::-1]))
    h1 = float(hi[i_h])
    if n - 1 - i_h < SETUP_BASE_MIN_MIN + 2:
        return None                              # القمة حديثة جداً: ما فيه تصحيح/قاعدة بعد
    piv = _zigzag(hi[: i_h + 1], lo[: i_h + 1], 0.07)
    lows_before = [p for p in piv if p[2] == "L" and p[0] < i_h]
    if lows_before:
        i_l0, l0 = lows_before[-1][0], float(lows_before[-1][1])
    else:
        i_l0 = int(np.argmin(lo[: i_h + 1]))
        l0 = float(lo[i_l0])
    if l0 <= 0 or h1 / l0 - 1.0 < SETUP_THRUST_MIN_PCT / 100.0:
        return None
    thrust_gain = (h1 / l0 - 1.0) * 100.0
    thrust_min = max(1, i_h - i_l0)
    thrust_vpm = float(vo[i_l0: i_h + 1].mean()) if i_h >= i_l0 else 0.0
    pre = vo[max(0, i_l0 - 60): i_l0]
    pre_vpm = float(np.median(pre[pre > 0])) if (len(pre) and (pre > 0).any()) else 1000.0
    vol_mult = thrust_vpm / max(pre_vpm, 1.0)

    # 2) القاعدة: أطول نافذة ضيقة بعد القمة. نستبعد أولاً أي اندفاعة محلية حديثة (pre_trim)، ثم نجرب
    #    تجاهل 0-3 شموع إضافية فوقها (لاحتمال اختراق حديث) ونأخذ الأطول.
    pre_trim = _trim_recent_advance(hi, lo, cl, vo, i_h, n)
    candidates = []
    for skip in (0, 1, 2, 3):
        end_i = n - 1 - pre_trim - skip
        if end_i <= i_h + SETUP_BASE_MIN_MIN:
            continue
        fb = _find_base(hi, lo, cl, i_h + 1, end_i)
        if fb:
            b_start, bh, bl = fb
            length = end_i - b_start + 1
            if length >= SETUP_BASE_MIN_MIN:
                candidates.append((length, skip, end_i, b_start, bh, bl))
    if not candidates:
        return None
    candidates.sort(key=lambda c: (-c[0], c[1]))
    _, skip, end_i, b_start, b_hi, b_lo = candidates[0]
    total_skip = pre_trim + skip
    base_min = end_i - b_start + 1
    active = float((vo[b_start: end_i + 1] > 0).mean())
    if active < 0.30:
        return None                                # سيولة شبه معدومة: حركة عشوائية مو قاعدة
    tick = _tick_for(price)
    broke = total_skip > 0 and float(hi[end_i + 1:].max()) >= b_hi + tick
    if total_skip > 0 and not broke:
        zero = next((c for c in candidates if c[1] == 0), None)   # احتياط: لا اختراق فعلي فوق الأطول → رجوع لتفسير skip=0
        if zero:
            _, _, end_i, b_start, b_hi, b_lo = zero
        broke = False
    l2 = float(lo[i_h + 1:].min())
    pullback = (h1 - b_lo) / h1
    retrace = (h1 - b_lo) / max(h1 - l0, 1e-9)
    if retrace > 0.90:
        return None                                # الاندفاعة انمحت تقريباً: فشل مو استمرار
    base_range = (b_hi - b_lo) / b_lo
    if base_range > 0.6 * max(pullback, 0.02) and pullback > 0.10:
        return None                                # القاعدة أوسع من نص التصحيح: مو ضغط

    # 3) خصائص القاعدة
    base_vpm = float(vo[b_start: end_i + 1].mean())
    dryup = base_vpm / max(thrust_vpm, 1.0)
    third = max(1, base_min // 3)
    tr_first = _mean_tr_pct(hi, lo, cl, vo, b_start, b_start + third)
    tr_last = _mean_tr_pct(hi, lo, cl, vo, end_i + 1 - third, end_i + 1)
    contraction = (tr_last / tr_first) if tr_first > 0 else 1.0
    half = b_start + base_min // 2
    higher_lows = float(lo[half: end_i + 1].min()) >= float(lo[b_start: half].min()) * 0.997
    touches, last_touch = 0, -10
    for k in range(b_start, end_i + 1):
        if hi[k] >= b_hi * 0.985 and k - last_touch >= 2:
            touches += 1
        if hi[k] >= b_hi * 0.985:
            last_touch = k
    close_pos = (float(cl[end_i]) - b_lo) / max(b_hi - b_lo, 1e-9)
    typical = (hi + lo + cl) / 3.0
    vsum = float(vo.sum())
    vwap = float((typical * vo).sum() / vsum) if vsum > 0 else price
    above_vwap = float(cl[end_i]) >= vwap
    last3 = float(vo[max(b_start, end_i - 2): end_i + 1].mean())
    uptick = last3 / max(base_vpm, 1.0)
    dist_pct = (b_hi - float(cl[end_i])) / max(float(cl[end_i]), 1e-9) * 100.0
    trigger = round(b_hi + tick, 4 if price < 1.0 else 2)
    buffer = max(tick, b_lo * 0.01)
    stop = round(b_lo - buffer, 4 if price < 1.0 else 2)
    minutes_since_high = n - 1 - i_h

    # 4) السكور (0-100)
    score, why = 0, []
    if thrust_gain >= 100: score += 12
    elif thrust_gain >= 60: score += 9
    elif thrust_gain >= 35: score += 6
    else: score += 3
    why.append(f"اندفاعة +{thrust_gain:.0f}% خلال {thrust_min}د")
    if thrust_min <= 30: score += 4
    elif thrust_min <= 90: score += 2
    if vol_mult >= 10: score += 4; why.append(f"حجم الاندفاعة {vol_mult:.0f}× الطبيعي")
    elif vol_mult >= 4: score += 2
    if 0.35 <= retrace <= 0.68: score += 10
    elif 0.20 <= retrace <= 0.78: score += 7
    elif retrace < 0.20: score += 6; why.append("Flag عالي قرب القمة")
    else: score += 3
    if base_min >= 30: score += 8
    elif base_min >= 15: score += 6
    else: score += 4
    why.append(f"قاعدة {base_min}د ضيقة {base_range * 100:.1f}%")
    if base_range <= 0.06: score += 6
    elif base_range <= 0.10: score += 5
    elif base_range <= 0.15: score += 3
    if contraction <= 0.7: score += 5; why.append("تقلّص التذبذب")
    elif contraction <= 0.95: score += 3
    if dryup <= 0.25: score += 5; why.append(f"الحجم جف ({dryup * 100:.0f}% من الاندفاعة)")
    elif dryup <= 0.45: score += 3
    if higher_lows: score += 3; why.append("قيعان صاعدة")
    if contraction <= 0.85 and touches >= 3: score += 3; why.append(f"ضغط على المقاومة ({touches} لمسات)")
    elif contraction <= 0.85 and touches == 2: score += 2
    if dist_pct <= 0.5: score += 8
    elif dist_pct <= 1.5: score += 6
    elif dist_pct <= 3.0: score += 3
    if uptick >= 1.8: score += 4; why.append(f"الحجم يبدأ يرتفع {uptick:.1f}×")
    elif uptick >= 1.3: score += 2
    if close_pos >= 0.7: score += 3
    if above_vwap: score += 5; why.append("فوق VWAP")
    fl = _safe_float(ctx.get("float_shares"))
    if fl > 0:
        if fl <= 10e6: score += 7; why.append(f"Float صغير جداً ({fl / 1e6:.1f}M)")
        elif fl <= 25e6: score += 4; why.append(f"Float صغير ({fl / 1e6:.1f}M)")
        elif fl <= 50e6: score += 2
    if _safe_float(ctx.get("catalyst_age_min"), 1e9) <= 24 * 60:
        score += 6; why.append("خبر/محفّز حديث")
    mins_to_open = 9 * 60 + 30 - (now.hour * 60 + now.minute)
    if -45 <= mins_to_open <= 30:
        score += 4; why.append("قرب الافتتاح")
    if 1.5 <= price <= 10.0:
        score += 3
    if minutes_since_high > 300:
        score -= 6                                  # الاندفاعة قديمة (أكثر من 5 ساعات)
    score = int(max(0, min(100, score)))

    # 5) الحالة
    if broke:
        state = "BROKE"
    elif dist_pct <= SETUP_MAX_TRIGGER_DIST:
        state = "ARMED"
    else:
        state = "FORMING"
    plan = build_room_targets(g, trigger, stop, h1=h1, l0=l0, l2=l2)
    return {
        "symbol": str(ctx.get("symbol", "")).upper(), "state": state, "score": score, "reasons": why,
        "price": price, "trigger": trigger, "stop": stop, "base_high": b_hi, "base_low": b_lo,
        "base_min": base_min, "base_range_pct": base_range * 100.0, "dist_pct": dist_pct,
        "thrust": {"l0": l0, "h1": h1, "gain_pct": thrust_gain, "minutes": thrust_min, "vol_mult": vol_mult,
                   "h1_ts": str(idx[i_h]), "l0_ts": str(idx[i_l0])},
        "retrace": retrace, "plan": plan, "vwap": vwap,
        "features": {"dryup": dryup, "contraction": contraction, "higher_lows": bool(higher_lows),
                     "touches": touches, "uptick": uptick, "close_pos": close_pos, "avg_vol_base": base_vpm,
                     "active_ratio": active, "above_vwap": bool(above_vwap), "minutes_since_high": minutes_since_high},
        "asof": str(idx[-1]),
    }
_setup_info_cache = {}
_setup_info_lock = threading.Lock()


def _setup_float_context(symbol, prelim_score):
    """Float/Market Cap فقط للمرشحين الواعدين (سكور أولي قريب من عتبة التسليح) — كاش 24 ساعة يوفّر أغلب طلبات .info."""
    now_ts = time.time()
    with _setup_info_lock:
        cached = _setup_info_cache.get(symbol)
    if cached and now_ts - cached[0] < 86400:
        return cached[1]
    if prelim_score < SETUP_ARM_MIN - 12:
        return {}
    ctx = {}
    try:
        info = yf.Ticker(symbol).info
        fl = _safe_float(info.get("floatShares")) or _safe_float(info.get("sharesOutstanding"))
        if fl > 0:
            ctx["float_shares"] = fl
        mc = _safe_float(info.get("marketCap"))
        if mc > 0:
            ctx["market_cap"] = mc
    except Exception as error:
        logger.debug(f"[SetupEngine] info fetch failed for {symbol}: {error}")
    with _setup_info_lock:
        _setup_info_cache[symbol] = (now_ts, ctx)
        if len(_setup_info_cache) > 400:
            for k in sorted(_setup_info_cache, key=lambda s: _setup_info_cache[s][0])[:150]:
                _setup_info_cache.pop(k, None)
    return ctx


def _setup_catalyst_age_min(symbol):
    """يعيد استخدام أرشيف rss_news_scanner (pre_breakout_news_impact) بدل جلب أخبار مستقل."""
    with state_lock:
        item = state.get("pre_breakout_news_impact", {}).get(symbol)
    if not item:
        return 1e9
    return max(0.0, (time.time() - _safe_float(item.get("time"))) / 60.0)


def _setup_candidates():
    """المتابَعة دائماً + بقية الحصة من in_play/hot_watchlist (الأحدث زخماً أولاً)."""
    with state_lock:
        tracked = list(state.get("setup_tracker", {}).keys())
        in_play = dict(state.get("in_play", {}))
        hot = [h.get("symbol") for h in state.get("hot_watchlist", []) if isinstance(h, dict)]
    ranked_play = [s for s, _ in sorted(in_play.items(), key=lambda kv: _safe_float(kv[1].get("heat")), reverse=True)]
    pool = [s for s in dict.fromkeys(ranked_play + [s for s in hot if s]) if s and s not in KNOWN_DELISTED]
    ordered = tracked + [s for s in pool if s not in tracked]
    return ordered[:SETUP_MAX_PER_CYCLE]


def _latest_close(df):
    """سعر احتياطي مباشر من آخر إغلاق — يبقي تتبّع صفقة مؤكدة شغّالاً حتى لو تعذّر إعادة تحليل النمط الكامل مؤقتاً."""
    try:
        d = df.copy()
        d.columns = [str(c).lower() for c in d.columns]
        return float(d["close"].dropna().iloc[-1])
    except Exception:
        return None


def _format_setup_alert(kind, setup):
    symbol, price = setup["symbol"], setup["price"]
    trigger, stop = setup["trigger"], setup["stop"]
    plan = setup.get("plan") or {}
    risk_pct = (trigger - stop) / trigger * 100.0 if trigger > 0 else 0.0
    t1, t2, t3, runner = plan.get("t1"), plan.get("t2"), plan.get("t3"), plan.get("runner")

    def tline(tag, t):
        return f"   {tag}: *${_fmt_px(t['price'])}* (+{t['pct']:.0f}%, {t['label']})\n" if t else ""

    targets_block = tline("T1", t1) + tline("T2", t2) + tline("T3", t3)
    if runner:
        targets_block += f"   🏃 Runner: ${_fmt_px(runner['price'])} (+{runner['pct']:.0f}%)\n"

    if kind == "ARMED":
        head = f"🟡 *ESE — قاعدة تتشكل: {symbol}*"
        body = (f"السعر الآن: ${_fmt_px(price)} | السكور: *{setup['score']}/100*\n"
                f"🎯 محفّز الدخول (اختراق): *${_fmt_px(trigger)}*\n"
                f"🛑 الوقف المقترح: *${_fmt_px(stop)}* (-{risk_pct:.1f}%)\n\n"
                f"📦 الأهداف عند الاختراق:\n{targets_block}\n"
                f"📝 *لماذا:*\n" + "\n".join(f"   • {r}" for r in setup["reasons"][:6])
                + "\n\n⏳ ننتظر تأكيد الاختراق بحجم — لا دخول قبل تجاوز السعر أعلاه.")
    elif kind == "BROKE":
        rr1 = t1["rr"] if t1 else 0.0
        head = f"🚀 *ESE — اختراق مؤكد: {symbol}*"
        body = (f"السعر: *${_fmt_px(price)}* (تجاوز ${_fmt_px(trigger)}) | السكور: *{setup['score']}/100*\n"
                f"🛑 الوقف: *${_fmt_px(stop)}* (-{risk_pct:.1f}%)\n\n"
                f"📦 الأهداف:\n{targets_block}\n"
                f"⚖️ R:R للهدف الأول: *{rr1:.1f}:1*\n\n"
                f"📝 *لماذا:*\n" + "\n".join(f"   • {r}" for r in setup["reasons"][:6]))
    elif kind in ("T1", "T2", "T3"):
        t = plan.get(kind.lower())
        nxt = {"T1": t2, "T2": t3, "T3": runner}.get(kind)
        trail = {"T1": trigger, "T2": (t1["price"] if t1 else trigger), "T3": (t2["price"] if t2 else trigger)}.get(kind)
        head = f"✅ *ESE — {kind} تحقق: {symbol}*"
        body = f"السعر: *${_fmt_px(price)}*" + (f" | الهدف كان +{t['pct']:.0f}%\n" if t else "\n")
        body += (f"➡️ الهدف التالي: ${_fmt_px(nxt['price'])} (+{nxt['pct']:.0f}%)\n" if nxt else "🏁 آخر هدف مرصود — إدارة يدوية من هنا.\n")
        body += f"🔒 التوصية: ارفع الوقف إلى *${_fmt_px(trail)}* على الأقل (تعادل/قفل ربح)."
    else:  # STOP
        head = f"🔻 *ESE — كسر الوقف: {symbol}*"
        body = f"السعر: *${_fmt_px(price)}* كسر الوقف ${_fmt_px(stop)}. الإعداد فشل — خروج وفق الخطة، بدون تعديل."
    return head + "\n" + body + "\n\n⚠️ تحليل آلي حسب بنية السعر فقط، وليس توصية مالية."


def _locked_view(symbol, tr, price):
    """يبني نسخة 'setup' من القيم المقفلة وقت الاختراق — تُستخدم لتنسيق تنبيهات T1/T2/T3/STOP بدل إعادة حساب الخطة."""
    return {"symbol": symbol, "price": price, "trigger": tr.get("locked_trigger", tr.get("trigger")),
            "stop": tr.get("locked_stop", tr.get("stop")), "plan": tr.get("locked_plan"),
            "score": tr.get("score", 0), "reasons": tr.get("locked_reasons", [])}


def _archive_setup_outcome(symbol, tr, reason):
    """
    يحفظ خلاصة الإعداد عند خروجه من المتابعة الحيّة (تجاهل، انتهاء صلاحية، أو إفساح مكان لغيره) — بدون هذا
    الأرشيف، أي تقييم لاحق لأداء المحرك (/esestats) يفقد كل إعداد سبق وخرج من setup_tracker. يتجاهل ما لم
    يصل ARMED قط (لا قيمة إحصائية لمرشح لم يُصنَّف كإعداد حقيقي أصلاً).
    """
    if not tr or not tr.get("armed_ts"):
        return
    alerted = set(tr.get("alerted", []))
    locked_plan = tr.get("locked_plan") or {}
    armed_price = _safe_float(tr.get("armed_price"))
    max_price = _safe_float(tr.get("max_price"), armed_price)
    trigger = _safe_float(tr.get("locked_trigger", tr.get("trigger")))
    rec = {
        "symbol": symbol, "armed_ts": tr.get("armed_ts"), "armed_price": armed_price,
        "broke_ts": tr.get("broke_ts"), "broke_price": tr.get("broke_price"),
        "trigger": trigger, "stop": _safe_float(tr.get("locked_stop", tr.get("stop"))),
        "t1_pct": (locked_plan.get("t1") or {}).get("pct"), "max_price": max_price,
        "max_run_pct": ((max_price - trigger) / trigger * 100.0) if trigger > 0 else None,
        "broke": "BROKE" in alerted, "hit_t1": "T1" in alerted, "hit_t2": "T2" in alerted,
        "hit_t3": "T3" in alerted, "hit_stop": "STOP" in alerted,
        "closed_ts": time.time(), "closed_reason": reason,
    }
    with state_lock:
        hist = state.setdefault("setup_history", [])
        hist.append(rec)
        if len(hist) > 500:
            del hist[: len(hist) - 500]


def _compute_ese_stats(records):
    """
    دالة صرفة (بلا حالة) تحوّل قائمة سجلات (مؤرشفة + الحيّة المحوّلة لنفس الشكل) إلى جدول المقاييس المطلوب:
    ARMED/BROKE/T1/T2/T3/False Break/STOP/Avg T1/Avg Max Run/Detection (دقائق من ARMED إلى BROKE).
    """
    armed = [r for r in records if r.get("armed_ts")]
    broke = [r for r in armed if r.get("broke")]
    false_break = [r for r in broke if r.get("hit_stop") and not r.get("hit_t1")]
    leads = [(r["broke_ts"] - r["armed_ts"]) / 60.0 for r in broke if r.get("broke_ts") and r.get("armed_ts")]
    t1_pcts = [r["t1_pct"] for r in broke if r.get("t1_pct") is not None]
    max_runs = [r["max_run_pct"] for r in broke if r.get("max_run_pct") is not None]

    def pct(n, d):
        return round(n / d * 100.0, 1) if d else 0.0

    def avg(xs):
        return round(sum(xs) / len(xs), 1) if xs else None

    n_broke = len(broke)
    return {
        "armed": len(armed), "broke": n_broke,
        "t1": sum(1 for r in broke if r.get("hit_t1")), "t2": sum(1 for r in broke if r.get("hit_t2")),
        "t3": sum(1 for r in broke if r.get("hit_t3")), "stop": sum(1 for r in broke if r.get("hit_stop")),
        "false_break": len(false_break),
        "t1_rate": pct(sum(1 for r in broke if r.get("hit_t1")), n_broke),
        "false_break_rate": pct(len(false_break), n_broke),
        "avg_t1_target_pct": avg(t1_pcts), "avg_max_run_pct": avg(max_runs),
        "max_run_best_pct": max(max_runs) if max_runs else None,
        "avg_detection_min": avg(leads),
    }


def _format_ese_stats(stats, n_records):
    if stats["armed"] == 0:
        return "📊 *ESE Stats*\nما فيه بيانات كافية بعد — خلّه يشتغل شوي ثم جرّب /esestats."
    lines = [
        "📊 *ESE Stats* (من تشغيل حي — مو باكتست)",
        f"عدد الإعدادات المرصودة: {n_records}",
        f"🟡 ARMED: *{stats['armed']}*",
        f"🚀 BROKE: *{stats['broke']}* (من أصل ARMED)",
        f"✅ T1: *{stats['t1']}*" + (f" ({stats['t1_rate']}% من BROKE)" if stats["broke"] else ""),
        f"✅ T2: *{stats['t2']}*",
        f"✅ T3: *{stats['t3']}*",
        f"🔻 STOP: *{stats['stop']}*",
        f"⚠️ False Break (وقف قبل T1): *{stats['false_break']}* ({stats['false_break_rate']}%)",
    ]
    if stats["avg_t1_target_pct"] is not None:
        lines.append(f"🎯 متوسط هدف T1 المحسوب: *{stats['avg_t1_target_pct']}%*")
    if stats["avg_max_run_pct"] is not None:
        lines.append(f"📈 متوسط أقصى صعود بعد الاختراق: *{stats['avg_max_run_pct']}%* (أفضل حالة: {stats['max_run_best_pct']}%)")
    if stats["avg_detection_min"] is not None:
        lines.append(f"⏱ متوسط مدة التشكّل (ARMED→BROKE): *{stats['avg_detection_min']} دقيقة*")
    lines.append("\n⚠️ هذي أرقام حيّة تراكمية من تشغيل البوت الفعلي، مو اختبار تاريخي على بيانات قديمة.")
    return "\n".join(lines)


def setup_scanner():
    """
    Explosive Setup Engine — طبقة منفصلة تمامًا عن الترتيب والصياد الآلي (ما تلمس BASE_TP_PCT ولا فلاتر /recommend).
    تبحث تحديدًا عن نمط 'اندفاعة قوية → قاعدة ضيقة يجف فيها الحجم → اختراق' (نمط IMCC)، وتبني خطة كاملة
    (دخول/وقف/أهداف) من بنية السهم نفسها، مع تتبّع بعد الدخول (T1/T2/T3 وتحريك الوقف، أو كسر الوقف).
    بعد الاختراق: الخطة تُقفل من لحظة التأكيد ولا تتغيّر مع كل دورة — التحقق من الأهداف بعدها يعتمد على
    السعر الفعلي فقط، بغض النظر عمّا يقوله تصنيف النمط الطازج في تلك اللحظة (بدونه: إعادة اكتشاف القاعدة
    كل دورة قد تُزحزح الخطة قليلاً بعد الدخول، فيفسد قياس "هل تحقق الهدف؟").
    """
    if not SETUP_ENGINE_ENABLED:
        logger.info("[SetupEngine] disabled via SETUP_ENGINE_ENABLED")
        return
    daily_count, daily_day = 0, None
    while True:
        try:
            scanner_heartbeat("setup_engine")
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(180)
                continue
            today = now_est().date()
            if daily_day != today:
                daily_day, daily_count = today, 0
            now_ts = time.time()
            symbols = _setup_candidates()
            armed_n = broke_n = 0
            for symbol in symbols:
                try:
                    df = cached_download(symbol, period="1d", interval="1m", prepost=True)
                    if df is None or df.empty:
                        continue
                    with state_lock:
                        tr = dict(state.get("setup_tracker", {}).get(symbol, {}))
                    already_broke = "BROKE" in set(tr.get("alerted", []))

                    # ── صفقة مؤكدة بالفعل: تتبّع نتيجة فقط، بغض النظر عن تصنيف اللحظة الطازج ──
                    if already_broke:
                        prelim = detect_setup(df, {"symbol": symbol}, now_est())
                        price = prelim["price"] if prelim else _latest_close(df)
                        if price is None:
                            continue
                        alerted = set(tr.get("alerted", []))
                        if now_ts - _safe_float(tr.get("broke_ts", tr.get("first_seen"))) > SETUP_TRACK_TTL_MIN * 60:
                            _archive_setup_outcome(symbol, tr, "tracking_ttl")
                            with state_lock:
                                state.get("setup_tracker", {}).pop(symbol, None)
                            continue
                        tr["max_price"] = max(_safe_float(tr.get("max_price"), price), price)

                        def _mark_and_send(event, msg):
                            nonlocal daily_count
                            if daily_count >= SETUP_MAX_ALERTS_PER_DAY:
                                return False
                            send_telegram(msg)
                            _alert_gate_mark(symbol, price, "SETUP")
                            daily_count += 1
                            alerted.add(event)
                            return True

                        stop_v = tr.get("locked_stop", tr.get("stop"))
                        if stop_v is not None and price <= stop_v and "STOP" not in alerted:
                            _mark_and_send("STOP", _format_setup_alert("STOP", _locked_view(symbol, tr, price)))
                        else:
                            for tk in ("t1", "t2", "t3"):
                                tgt = (tr.get("locked_plan") or {}).get(tk)
                                if tgt and price >= tgt["price"] and tk.upper() not in alerted:
                                    _mark_and_send(tk.upper(), _format_setup_alert(tk.upper(), _locked_view(symbol, tr, price)))
                        tr["alerted"] = sorted(alerted)
                        tr["last_seen"] = now_ts
                        with state_lock:
                            state.setdefault("setup_tracker", {})[symbol] = tr
                            state.setdefault("explosive_setups", {})[symbol] = {
                                "state": "BROKE", "score": tr.get("score", 0), "price": price,
                                "trigger": tr.get("locked_trigger", tr.get("trigger")), "stop": stop_v,
                                "dist_pct": 0.0, "asof": str(now_est()),
                            }
                        continue

                    # ── لم يخترق بعد: اكتشاف/تحديث إعداد جديد كما هو معتاد ──
                    prelim = detect_setup(df, {"symbol": symbol}, now_est())
                    if not prelim:
                        if tr:
                            _archive_setup_outcome(symbol, tr, "lost_pattern")
                            with state_lock:
                                state.get("setup_tracker", {}).pop(symbol, None)
                        continue
                    ctx = {"symbol": symbol, "catalyst_age_min": _setup_catalyst_age_min(symbol)}
                    ctx.update(_setup_float_context(symbol, prelim["score"]))
                    setup = detect_setup(df, ctx, now_est()) or prelim
                    with state_lock:
                        state.setdefault("explosive_setups", {})[symbol] = {
                            k: setup[k] for k in ("state", "score", "price", "trigger", "stop", "dist_pct", "asof")
                        }

                    first_seen = tr.get("first_seen", now_ts)
                    alerted = set(tr.get("alerted", []))
                    if now_ts - first_seen > SETUP_ARM_TTL_MIN * 60:
                        _archive_setup_outcome(symbol, tr, "expired_no_break")
                        with state_lock:
                            state.get("setup_tracker", {}).pop(symbol, None)
                        continue
                    if setup["score"] < SETUP_ARM_MIN or setup["state"] == "FORMING":
                        if not tr:
                            continue
                        with state_lock:
                            state.setdefault("setup_tracker", {})[symbol] = tr
                        continue

                    price = setup["price"]
                    armed_ts = tr.get("armed_ts")
                    armed_price = tr.get("armed_price")

                    def _mark_and_send(event, msg):
                        nonlocal daily_count
                        if daily_count >= SETUP_MAX_ALERTS_PER_DAY:
                            return False
                        send_telegram(msg)
                        _alert_gate_mark(symbol, price, "SETUP")
                        daily_count += 1
                        alerted.add(event)
                        return True

                    locked_trigger = tr.get("locked_trigger")
                    locked_stop = tr.get("locked_stop")
                    locked_plan = tr.get("locked_plan")
                    locked_reasons = tr.get("locked_reasons")

                    if setup["state"] == "ARMED" and "ARMED" not in alerted:
                        if _mark_and_send("ARMED", _format_setup_alert("ARMED", setup)):
                            armed_n += 1
                            armed_ts, armed_price = now_ts, price
                    elif setup["state"] == "BROKE" and "BROKE" not in alerted:
                        if armed_ts is None:
                            armed_ts, armed_price = now_ts, price   # اكتُشف وهو يخترق مباشرة (بدون تنبيه ARMED سابق)
                        if _mark_and_send("BROKE", _format_setup_alert("BROKE", setup)):
                            broke_n += 1
                            locked_trigger, locked_stop = setup["trigger"], setup["stop"]
                            locked_plan, locked_reasons = setup["plan"], setup["reasons"]
                            tr["broke_ts"], tr["broke_price"] = now_ts, price

                    max_price = max(_safe_float(tr.get("max_price"), price), price)
                    new_tr = dict(tr, symbol=symbol, first_seen=first_seen, last_seen=now_ts,
                                  trigger=setup["trigger"], stop=setup["stop"], score=setup["score"],
                                  alerted=sorted(alerted), armed_ts=armed_ts, armed_price=armed_price,
                                  max_price=max_price)
                    if locked_trigger is not None:
                        new_tr.update(locked_trigger=locked_trigger, locked_stop=locked_stop,
                                     locked_plan=locked_plan, locked_reasons=locked_reasons)
                    with state_lock:
                        state.setdefault("setup_tracker", {})[symbol] = new_tr
                except Exception as symbol_error:
                    logger.debug(f"[SetupEngine] {symbol}: {symbol_error}")
                time.sleep(0.15)
            with state_lock:
                tr_all = state.get("setup_tracker", {})
                if len(tr_all) > SETUP_MAX_ACTIVE:
                    ranked = sorted(tr_all.items(), key=lambda kv: _safe_float(kv[1].get("score")), reverse=True)
                    for sym, dropped in ranked[SETUP_MAX_ACTIVE:]:
                        _archive_setup_outcome(sym, dropped, "capacity_trim")
                    state["setup_tracker"] = dict(ranked[:SETUP_MAX_ACTIVE])
                save_state()
            logger.info(f"[SetupEngine] checked={len(symbols)} armed_new={armed_n} broke_new={broke_n} "
                        f"tracked={len(state.get('setup_tracker', {}))} alerts_today={daily_count}")
            time.sleep(SETUP_SCAN_INTERVAL)
        except Exception as error:
            logger.error(f"[SetupEngine] error: {error}")
            time.sleep(60)


def _replay_symbol_today(symbol, df, session_date=None):
    """
    يعيد تشغيل يوم تداول واحد لسهم واحد دقيقة بدقيقة، كأن ESE كان يراقبه لحظياً: كل قرار عند الدقيقة i
    يُبنى فقط من الشموع حتى تلك الدقيقة (بدون أي اطلاع على المستقبل). يستخدم نفس detect_setup الحيّة تماماً
    — مو نسخة منفصلة — فالنتيجة تطابق ما كان سيصدره البوت فعلاً لو كان يراقب ذلك اليوم بالضبط.
    يتتبّع دورة واحدة فقط (أول ARMED ← أول BROKE ← نتيجتها) وليس كل الدورات المحتملة في نفس اليوم.
    """
    if df is None or df.empty:
        return f"📭 {symbol}: ما فيه بيانات."
    d = df.copy()
    d.columns = [str(c).lower() for c in d.columns]
    if not {"open", "high", "low", "close", "volume"}.issubset(d.columns):
        return f"📭 {symbol}: صيغة بيانات غير متوقعة."
    d = d[~d.index.duplicated(keep="last")].sort_index()
    session_date = session_date or d.index[-1].date()
    day_df = d[d.index.date == session_date]
    if len(day_df) < 21:
        return f"📭 {symbol}: بيانات {session_date} غير كافية ({len(day_df)} شمعة) — جرّب لاحقاً باليوم."

    armed = broke = None
    for i in range(20, len(day_df)):
        window = day_df.iloc[: i + 1]
        ts = window.index[-1]
        setup = detect_setup(window, {"symbol": symbol}, ts)
        if not setup or setup["score"] < SETUP_ARM_MIN:
            continue
        if armed is None:
            armed = {"ts": ts, "price": setup["price"], "score": setup["score"]}
        if setup["state"] == "BROKE":
            broke = {"ts": ts, "price": setup["price"], "trigger": setup["trigger"], "stop": setup["stop"],
                     "plan": setup["plan"], "score": setup["score"]}
            break

    lines = [f"🔁 *Replay — {symbol}* ({session_date})"]
    if armed is None:
        lines.append("ما اكتشف ESE أي إعداد مؤهل هذا اليوم لهذا السهم — إما ما فيه نمط اندفاعة+قاعدة واضح، "
                     "أو سعره خارج نطاق البوت.")
        return "\n".join(lines)
    lines.append(f"🟡 ARMED الساعة {armed['ts'].strftime('%H:%M')} عند ${_fmt_px(armed['price'])} (سكور {armed['score']})")
    if broke is None:
        lines.append("ما صار اختراق مؤكد بعدها بنفس اليوم (بقي عند ARMED/تراجع قبل التأكيد).")
        return "\n".join(lines)

    lead_min = (broke["ts"] - armed["ts"]).total_seconds() / 60.0
    lines.append(f"🚀 BROKE الساعة {broke['ts'].strftime('%H:%M')} عند ${_fmt_px(broke['price'])} "
                 f"(بعد {lead_min:.0f} دقيقة من ARMED)")
    trigger, stop, plan = broke["trigger"], broke["stop"], broke["plan"]
    lines.append(f"🎯 الدخول: ${_fmt_px(trigger)} | 🛑 الوقف: ${_fmt_px(stop)}")

    rest = day_df[day_df.index > broke["ts"]]
    max_price = max(float(rest["high"].max()) if not rest.empty else broke["price"], broke["price"])
    hit_ts = {}
    for tk in ("t1", "t2", "t3"):
        tgt = plan.get(tk)
        if not tgt:
            continue
        after = rest[rest["high"] >= tgt["price"]]
        hit_ts[tk] = after.index[0] if not after.empty else None
    stop_after = rest[rest["low"] <= stop]
    stop_ts = stop_after.index[0] if not stop_after.empty else None

    for tk in ("t1", "t2", "t3"):
        tgt = plan.get(tk)
        if not tgt:
            continue
        if hit_ts.get(tk) is not None:
            lines.append(f"✅ {tk.upper()} (+{tgt['pct']:.0f}%, ${_fmt_px(tgt['price'])}) تحقق الساعة {hit_ts[tk].strftime('%H:%M')}")
        else:
            lines.append(f"◻️ {tk.upper()} (+{tgt['pct']:.0f}%, ${_fmt_px(tgt['price'])}) لم يتحقق")
    if stop_ts is not None:
        false_break = hit_ts.get("t1") is None
        lines.append(f"🔻 كسر الوقف الساعة {stop_ts.strftime('%H:%M')}" + (" (قبل أي هدف — اختراق فاشل)" if false_break else " (بعد تحقيق أهداف)"))
    max_run = (max_price - trigger) / trigger * 100.0 if trigger > 0 else 0.0
    lines.append(f"📈 أقصى سعر بعد الاختراق: ${_fmt_px(max_price)} (+{max_run:.0f}% من الدخول)")
    lines.append("\n⚠️ إعادة تشغيل بمنطق ESE الفعلي — دورة واحدة فقط، وليست توصية مالية.")
    return "\n".join(lines)

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


def _safe_number(val, default=0.0):
    try:
        if val is None or pd.isna(val): return default
        return float(val)
    except: return default


def _candle_values(row):
    return (float(row['open']), float(row['high']), float(row['low']), float(row['close']))


def detect_doji(df, body_threshold=0.10):
    if df is None or df.empty: return None
    o, h, l, c = _candle_values(df.iloc[-1])
    span = h - l
    if span <= 0 or abs(c - o) / span >= body_threshold: return None
    upper, lower = h - max(o, c), min(o, c) - l
    return {'pattern': 'DOJI', 'type': '🕯️ Doji — تردد', 'price': c, 'bullish': lower > upper * 2, 'bearish': upper > lower * 2}


def detect_hammer(df, inverted=False):
    if df is None or df.empty: return None
    o, h, l, c = _candle_values(df.iloc[-1])
    body = abs(c - o)
    if body <= 0: return None
    upper, lower = h - max(o, c), min(o, c) - l
    if not inverted and lower > body * 2 and upper < body * 0.5:
        return {'pattern': 'HAMMER', 'type': '🔨 Hammer — انعكاس صاعد محتمل', 'price': c, 'bullish': True}
    if inverted and upper > body * 2 and lower < body * 0.5:
        return {'pattern': 'INVERTED_HAMMER', 'type': '🔨 Inverted Hammer — يحتاج تأكيدًا', 'price': c, 'bullish': True}
    return None


def detect_shooting_star(df):
    if df is None or df.empty: return None
    o, h, l, c = _candle_values(df.iloc[-1])
    body = abs(c - o)
    if body <= 0: return None
    upper, lower = h - max(o, c), min(o, c) - l
    if upper > body * 2 and lower < body * 0.5:
        return {'pattern': 'SHOOTING_STAR', 'type': '🌠 Shooting Star — انعكاس هابط محتمل', 'price': c, 'bearish': True}
    return None


def detect_engulfing(df):
    if df is None or len(df) < 2: return None
    p, x = df.iloc[-2], df.iloc[-1]
    po, pc = float(p['open']), float(p['close'])
    xo, xc = float(x['open']), float(x['close'])
    if pc < po and xc > xo and xo <= pc and xc >= po:
        return {'pattern': 'ENGULFING_BULLISH', 'type': '🔄 Bullish Engulfing', 'price': xc, 'bullish': True}
    if pc > po and xc < xo and xo >= pc and xc <= po:
        return {'pattern': 'ENGULFING_BEARISH', 'type': '🔄 Bearish Engulfing', 'price': xc, 'bearish': True}
    return None


def detect_morning_evening_star(df):
    if df is None or len(df) < 3: return None
    a, b, c = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    ao, ac = float(a['open']), float(a['close'])
    bo, bc = float(b['open']), float(b['close'])
    co, cc = float(c['open']), float(c['close'])
    a_body, b_body = abs(ac - ao), abs(bc - bo)
    if a_body <= 0 or b_body > a_body * 0.3: return None
    if ac < ao and cc > co and cc > (ao + ac) / 2:
        return {'pattern': 'MORNING_STAR', 'type': '⭐ Morning Star', 'price': cc, 'bullish': True}
    if ac > ao and cc < co and cc < (ao + ac) / 2:
        return {'pattern': 'EVENING_STAR', 'type': '⭐ Evening Star', 'price': cc, 'bearish': True}
    return None


def detect_harami(df):
    if df is None or len(df) < 2: return None
    p, x = df.iloc[-2], df.iloc[-1]
    po, pc = float(p['open']), float(p['close'])
    xo, xc = float(x['open']), float(x['close'])
    pbody, xbody = abs(pc - po), abs(xc - xo)
    if pbody <= 0 or xbody >= pbody * 0.5: return None
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
        if result: patterns.append(result)
    return patterns


def _local_extrema(df, lookback=50):
    if df is None or len(df) < lookback: return [], []
    highs, lows = df['high'].astype(float).to_numpy(), df['low'].astype(float).to_numpy()
    peaks, troughs = [], []
    for i in range(2, len(df) - 2):
        if highs[i] > max(highs[i-2:i]) and highs[i] > max(highs[i+1:i+3]): peaks.append((i, highs[i]))
        if lows[i] < min(lows[i-2:i]) and lows[i] < min(lows[i+1:i+3]): troughs.append((i, lows[i]))
    return peaks, troughs


def detect_double_top(df):
    peaks, troughs = _local_extrema(df)
    if len(peaks) < 2: return None
    a, b = peaks[-2], peaks[-1]
    if abs(a[1] - b[1]) / max(a[1], 0.01) <= 0.02 and any(a[0] < t[0] < b[0] for t in troughs):
        neckline = min(t[1] for t in troughs if a[0] < t[0] < b[0])
        return {'pattern': 'DOUBLE_TOP', 'type': '🔺 Double Top — هابط محتمل', 'price': float(df['close'].iloc[-1]), 'bearish': True, 'neckline': neckline}
    return None


def detect_double_bottom(df):
    peaks, troughs = _local_extrema(df)
    if len(troughs) < 2: return None
    a, b = troughs[-2], troughs[-1]
    if abs(a[1] - b[1]) / max(a[1], 0.01) <= 0.02 and any(a[0] < p[0] < b[0] for p in peaks):
        neckline = max(p[1] for p in peaks if a[0] < p[0] < b[0])
        return {'pattern': 'DOUBLE_BOTTOM', 'type': '🔻 Double Bottom — صاعد محتمل', 'price': float(df['close'].iloc[-1]), 'bullish': True, 'neckline': neckline}
    return None


def detect_head_shoulders(df):
    peaks, troughs = _local_extrema(df)
    if len(peaks) < 3: return None
    a, head, b = peaks[-3:]
    if head[1] > a[1] and head[1] > b[1] and abs(a[1] - b[1]) / max(a[1], 0.01) <= 0.03:
        between = [t[1] for t in troughs if a[0] < t[0] < b[0]]
        if len(between) >= 2:
            neckline = sum(between[-2:]) / 2
            return {'pattern': 'HEAD_SHOULDERS', 'type': '👤 Head & Shoulders — هابط محتمل', 'price': float(df['close'].iloc[-1]), 'bearish': True, 'neckline': neckline}
    return None


def detect_inverse_head_shoulders(df):
    peaks, troughs = _local_extrema(df)
    if len(troughs) < 3: return None
    a, head, b = troughs[-3:]
    if head[1] < a[1] and head[1] < b[1] and abs(a[1] - b[1]) / max(a[1], 0.01) <= 0.03:
        between = [p[1] for p in peaks if a[0] < p[0] < b[0]]
        if len(between) >= 2:
            neckline = sum(between[-2:]) / 2
            return {'pattern': 'INVERSE_HEAD_SHOULDERS', 'type': '👤 Inverse H&S — صاعد محتمل', 'price': float(df['close'].iloc[-1]), 'bullish': True, 'neckline': neckline}
    return None


def detect_cup_handle(df):
    if df is None or len(df) < 50: return None
    close = df['close'].astype(float).to_numpy()[-50:]
    bottom = int(close.argmin())
    if bottom < 10 or bottom > 40: return None
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
                if time.time() - _sent_candle_patterns.get(symbol, 0) < CANDLE_PATTERNS_COOLDOWN: continue
                try:
                    df = cached_download(symbol, period='5d', interval='5m')
                    if df is None or df.empty or len(df) < 20: continue
                    df.columns = [str(c).lower() for c in df.columns]
                    price = float(df['close'].iloc[-1])
                    if not (0.5 <= price <= 30): continue
                    patterns = analyze_candle_patterns(df)
                    if not patterns: continue
                    key = f'candle_patterns_{symbol}_{now_est().date()}'
                    with state_lock:
                        if key in state.get('seen_signals', {}): continue
                    send_telegram(f"🕯️ *CANDLE PATTERN: {symbol}*\nالسعر: ${price:.4f}\n" + '\n'.join(f"• {p['type']}" for p in patterns[:5]) + '\n⚠️ Paper فقط وليست توصية')
                    _sent_candle_patterns[symbol] = time.time()
                    with state_lock:
                        state.setdefault('seen_signals', {})[key] = time.time(); save_state()
                except Exception as error: logger.debug(f'[Candle Patterns] {symbol}: {error}')
            time.sleep(CANDLE_PATTERNS_INTERVAL)
        except Exception as error: logger.error(f'[Candle Patterns] scanner error: {error}'); time.sleep(60)


def chart_pattern_scanner():
    while True:
        try:
            scanner_heartbeat('chart_patterns') if 'scanner_heartbeat' in globals() else None
            if not CHART_PATTERNS_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(CHART_PATTERNS_INTERVAL); continue
            with state_lock:
                tickers = list(state.get('tickers', []))[:200]
            for symbol in tickers:
                if time.time() - _sent_chart_patterns.get(symbol, 0) < CHART_PATTERNS_COOLDOWN: continue
                try:
                    df = cached_download(symbol, period='90d', interval='1d')
                    if df is None or df.empty or len(df) < 50: continue
                    df.columns = [str(c).lower() for c in df.columns]
                    price = float(df['close'].iloc[-1])
                    if not (0.5 <= price <= 50): continue
                    patterns = chart_patterns(df)
                    if not patterns: continue
                    key = f'chart_patterns_{symbol}_{now_est().date()}'
                    with state_lock:
                        if key in state.get('seen_signals', {}): continue
                    details = '\n'.join(f"• {p['type']}" for p in patterns[:4])
                    send_telegram(f"📐 *CHART PATTERN: {symbol}*\nالسعر: ${price:.4f}\n{details}\n⚠️ يحتاج تأكيد حجم وإغلاق؛ Paper فقط")
                    _sent_chart_patterns[symbol] = time.time()
                    with state_lock:
                        state.setdefault('seen_signals', {})[key] = time.time(); save_state()
                except Exception as error: logger.debug(f'[Chart Patterns] {symbol}: {error}')
            time.sleep(CHART_PATTERNS_INTERVAL)
        except Exception as error: logger.error(f'[Chart Patterns] scanner error: {error}'); time.sleep(60)


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


def calculate_ichimoku(df, tenkan_period=9, kijun_period=26, senkou_period=52):
    try:
        work = df.copy()
        work.columns = [str(c).lower() for c in work.columns]
        if not {'high', 'low', 'close'}.issubset(work.columns) or len(work) < senkou_period: return None
        tenkan = (work['high'].rolling(tenkan_period).max() + work['low'].rolling(tenkan_period).min()) / 2
        kijun = (work['high'].rolling(kijun_period).max() + work['low'].rolling(kijun_period).min()) / 2
        span_a = ((tenkan + kijun) / 2).shift(kijun_period)
        span_b = ((work['high'].rolling(senkou_period).max() + work['low'].rolling(senkou_period).min()) / 2).shift(kijun_period)
        close = float(work['close'].iloc[-1])
        a = float(span_a.iloc[-1]) if pd.notna(span_a.iloc[-1]) else close
        b = float(span_b.iloc[-1]) if pd.notna(span_b.iloc[-1]) else close
        cloud_position = 'ABOVE' if close > max(a, b) else 'BELOW' if close < min(a, b) else 'INSIDE'
        tk_bullish = bool(pd.notna(tenkan.iloc[-1]) and pd.notna(kijun.iloc[-1]) and tenkan.iloc[-1] > kijun.iloc[-1])
        return {'tenkan_sen': float(tenkan.iloc[-1]), 'kijun_sen': float(kijun.iloc[-1]), 'senkou_span_a': a, 'senkou_span_b': b,
                'cloud_bullish': a > b, 'cloud_position': cloud_position, 'tenkan_kijun_cross': tk_bullish,
                'signal': 'BULLISH' if cloud_position == 'ABOVE' and tk_bullish else 'BEARISH' if cloud_position == 'BELOW' else 'NEUTRAL'}
    except Exception as error: logger.debug(f'[Ichimoku] {error}'); return None


def calculate_supertrend(df, period=10, multiplier=3.0):
    try:
        work = df.copy()
        work.columns = [str(c).lower() for c in work.columns]
        if not {'high', 'low', 'close'}.issubset(work.columns) or len(work) < period + 2: return None
        high, low, close = work['high'].astype(float), work['low'].astype(float), work['close'].astype(float)
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        hl2 = (high + low) / 2
        upper, lower = hl2 + multiplier * atr, hl2 - multiplier * atr
        direction = pd.Series(1, index=work.index, dtype=int)
        line = pd.Series(np.nan, index=work.index, dtype=float)
        for i in range(period, len(work)):
            prev = i - 1
            if close.iloc[i] > upper.iloc[prev]: direction.iloc[i] = 1
            elif close.iloc[i] < lower.iloc[prev]: direction.iloc[i] = -1
            else:
                direction.iloc[i] = direction.iloc[prev]
                if direction.iloc[i] == 1: lower.iloc[i] = max(lower.iloc[i], lower.iloc[prev])
                else: upper.iloc[i] = min(upper.iloc[i], upper.iloc[prev])
            line.iloc[i] = lower.iloc[i] if direction.iloc[i] == 1 else upper.iloc[i]
        last_dir, prev_dir = int(direction.iloc[-1]), int(direction.iloc[-2])
        last_line = float(line.iloc[-1]) if pd.notna(line.iloc[-1]) else float(close.iloc[-1])
        return {'supertrend': last_line, 'direction': 'UPTREND' if last_dir == 1 else 'DOWNTREND', 'color': 'GREEN' if last_dir == 1 else 'RED', 'signal': 'BUY' if last_dir == 1 and prev_dir == -1 else 'SELL' if last_dir == -1 and prev_dir == 1 else 'HOLD', 'price': float(close.iloc[-1])}
    except Exception as error: logger.debug(f'[SuperTrend] {error}'); return None


def calculate_fibonacci(df, lookback=50):
    try:
        work = df.copy()
        work.columns = [str(c).lower() for c in work.columns]
        if not {'high', 'low', 'close'}.issubset(work.columns) or len(work) < min(lookback, 20): return None
        high, low, current = float(work['high'].tail(lookback).max()), float(work['low'].tail(lookback).min()), float(work['close'].iloc[-1])
        if high <= low or current <= 0: return None
        diff = high - low
        levels = {'0%': high, '23.6%': high - diff * .236, '38.2%': high - diff * .382, '50%': high - diff * .5, '61.8%': high - diff * .618, '78.6%': high - diff * .786, '100%': low}
        closest = min(levels.items(), key=lambda item: abs(item[1] - current))
        near_618 = abs(current - levels['61.8%']) / current <= .01
        return {'high': high, 'low': low, 'current': current, 'levels': levels, 'closest_level': closest[0], 'closest_price': closest[1], 'near_618': near_618, 'signal': 'SUPPORT' if near_618 and current >= levels['61.8%'] else 'RESISTANCE' if near_618 else 'NEUTRAL'}
    except Exception as error: logger.debug(f'[Fibonacci] {error}'); return None


def calculate_keltner_channels(df, period=20, multiplier=2.0):
    try:
        work = df.copy()
        work.columns = [str(c).lower() for c in work.columns]
        if not {'high', 'low', 'close'}.issubset(work.columns) or len(work) < period + 2: return None
        high, low, close = work['high'].astype(float), work['low'].astype(float), work['close'].astype(float)
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        middle = close.ewm(span=period, adjust=False).mean()
        upper, lower = middle + multiplier * atr, middle - multiplier * atr
        width = float((upper - lower).iloc[-1])
        avg_width = float((upper - lower).tail(period).mean())
        price = float(close.iloc[-1])
        position = 'ABOVE_UPPER' if price > upper.iloc[-1] else 'BELOW_LOWER' if price < lower.iloc[-1] else 'ABOVE_MIDDLE' if price >= middle.iloc[-1] else 'BELOW_MIDDLE'
        return {'upper': float(upper.iloc[-1]), 'middle': float(middle.iloc[-1]), 'lower': float(lower.iloc[-1]), 'width': width, 'squeeze': bool(avg_width > 0 and width < avg_width * .7), 'position': position, 'signal': 'OVERBOUGHT' if position == 'ABOVE_UPPER' else 'OVERSOLD' if position == 'BELOW_LOWER' else 'NEUTRAL'}
    except Exception as error: logger.debug(f'[Keltner] {error}'); return None


def calculate_parabolic_sar(df, step=.02, max_step=.2):
    try:
        work = df.copy()
        work.columns = [str(c).lower() for c in work.columns]
        if not {'high', 'low', 'close'}.issubset(work.columns) or len(work) < 5: return None
        high, low, close = work['high'].astype(float).to_numpy(), work['low'].astype(float).to_numpy(), work['close'].astype(float).to_numpy()
        sar, af, extreme, up = float(low[0]), step, float(high[0]), True
        series = [sar]
        for i in range(1, len(close)):
            candidate = sar + af * (extreme - sar)
            if up:
                candidate = min(candidate, low[i - 1], low[i - 2] if i > 1 else low[i - 1])
                if low[i] < candidate: up, sar, extreme, af = False, extreme, low[i], step
                else:
                    sar = candidate
                    if high[i] > extreme: extreme, af = high[i], min(max_step, af + step)
            else:
                candidate = max(candidate, high[i - 1], high[i - 2] if i > 1 else high[i - 1])
                if high[i] > candidate: up, sar, extreme, af = True, extreme, high[i], step
                else:
                    sar = candidate
                    if low[i] < extreme: extreme, af = low[i], min(max_step, af + step)
            series.append(sar)
        price = float(close[-1])
        return {'sar': float(series[-1]), 'direction': 'UPTREND' if price > series[-1] else 'DOWNTREND', 'reversal': False, 'signal': 'BUY' if price > series[-1] else 'SELL', 'price': price}
    except Exception as error: logger.debug(f'[ParabolicSAR] {error}'); return None


def analyze_advanced_indicators(symbol, df):
    if df is None or df.empty or len(df) < 60: return None
    work = df.copy()
    work.columns = [str(c).lower() for c in work.columns]
    results = {'symbol': symbol, 'price': float(work['close'].iloc[-1]), 'timestamp': time.time()}
    results['ichimoku'] = calculate_ichimoku(work)
    results['supertrend'] = calculate_supertrend(work)
    results['fibonacci'] = calculate_fibonacci(work)
    results['keltner'] = calculate_keltner_channels(work)
    results['parabolic_sar'] = calculate_parabolic_sar(work)
    score, signals = 0, []
    ichi, st, fib, kel, psar = results['ichimoku'], results['supertrend'], results['fibonacci'], results['keltner'], results['parabolic_sar']
    if ichi and ichi['signal'] == 'BULLISH': score += 25; signals.append('Ichimoku bullish')
    if st and st['direction'] == 'UPTREND': score += 20; signals.append('SuperTrend up')
    if fib and fib['signal'] == 'SUPPORT': score += 15; signals.append('Fib support')
    if kel and kel['squeeze']: score += 15; signals.append('Keltner squeeze')
    if psar and psar['direction'] == 'UPTREND': score += 15; signals.append('Parabolic SAR up')
    results['score'] = min(100, score)
    results['signals'] = signals
    results['signal'] = 'BULLISH' if score >= 55 else 'BEARISH' if (ichi and ichi['signal'] == 'BEARISH') else 'NEUTRAL'
    return results

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
    if high <= low: return {}
    diff = high - low
    return {'0%': high, '23.6%': high - diff * 0.236, '38.2%': high - diff * 0.382, '50.0%': high - diff * 0.500, '61.8%': high - diff * 0.618, '78.6%': high - diff * 0.786, '100%': low}


def detect_peaks_troughs(df, lookback=60):
    if df is None or len(df) < lookback: return [], []
    highs, lows = df['high'].astype(float).to_numpy(), df['low'].astype(float).to_numpy()
    peaks, troughs = [], []
    for i in range(5, len(df) - 5):
        if highs[i] > max(highs[i-5:i]) and highs[i] > max(highs[i+1:i+6]): peaks.append(float(highs[i]))
        if lows[i] < min(lows[i-5:i]) and lows[i] < min(lows[i+1:i+6]): troughs.append(float(lows[i]))
    return peaks, troughs


def advanced_analysis(symbol):
    try:
        df = cached_download(symbol, period='120d', interval='1d')
        if df is None or df.empty or len(df) < 60: return None
        df.columns = [str(c).lower() for c in df.columns]
        required = {'close', 'high', 'low'}
        if not required.issubset(df.columns): return None
        close = df['close'].astype(float)
        price = float(close.iloc[-1])
        ema20, ema50, ema100 = float(close.ewm(span=20, adjust=False).mean().iloc[-1]), float(close.ewm(span=50, adjust=False).mean().iloc[-1]), float(close.ewm(span=100, adjust=False).mean().iloc[-1])
        high_60, low_60 = float(df['high'].tail(60).max()), float(df['low'].tail(60).min())
        fib = calculate_fibonacci_levels(high_60, low_60)
        if not fib: return None
        peaks, troughs = detect_peaks_troughs(df)
        resistance_candidates = sorted({x for x in peaks if x > price})
        support_candidates = sorted({x for x in troughs if x < price}, reverse=True)
        resistance, support = resistance_candidates[0] if resistance_candidates else fib.get('23.6%'), support_candidates[0] if support_candidates else fib.get('78.6%')
        stop_loss = min(float(support), price * 0.92) if support and support < price else price * 0.92
        risk = max(price - stop_loss, price * 0.01)
        targets = [float(level) for level in (fib.get('23.6%'), fib.get('38.2%'), fib.get('50.0%'), fib.get('61.8%')) if level and level > price]
        if not targets: targets = [x for x in resistance_candidates if x > price]
        targets = sorted(set(round(x, 4) for x in targets))[:4]
        rr = round((targets[0] - price) / risk, 2) if targets and targets[0] > price else 0.0
        advanced_indicators = analyze_advanced_indicators(symbol, df)
        return {'symbol': symbol, 'price': round(price, 4), 'ema20': round(ema20, 4), 'ema50': round(ema50, 4), 'ema100': round(ema100, 4), 'support': round(float(support), 4) if support else None, 'resistance': round(float(resistance), 4) if resistance else None, 'targets': targets, 'stop_loss': round(stop_loss, 4), 'rr': rr, 'fib': fib, 'above_ema20': price >= ema20, 'advanced_indicators': advanced_indicators}
    except Exception as error: logger.debug(f'[AdvancedAnalysis] {symbol}: {error}'); return None


def _format_advanced_analysis(result):
    price = result['price']
    msg = f"📊 *تحليل متقدم: {result['symbol']}*\n━━━━━━━━━━━━━━━━\n💰 السعر الحالي: *${price:.4f}*\n\n📊 *المتوسطات:*\n"
    for key in ('ema20', 'ema50', 'ema100'): msg += f"• {key.upper()}: ${result[key]:.4f} {'✅' if price >= result[key] else '❌'}\n"
    msg += f"\n📍 *المستويات:*\n• الدعم: ${result['support']:.4f}\n• المقاومة: ${result['resistance']:.4f}\n• وقف Paper: ${result['stop_loss']:.4f}\n"
    if result['targets']:
        msg += '\n🎯 *الأهداف:*\n'
        for index, target in enumerate(result['targets'], 1): msg += f"• T{index}: ${target:.4f} ({(target - price) / max(price, 1e-9) * 100:+.1f}%)\n"
    msg += f"\n📐 *R:R: {result['rr']:.2f}:1*" + (' — ممتازة' if result['rr'] >= 3 else (' — جيدة' if result['rr'] >= 2 else ' — أقل من الحد المفضل')) + '\n⚠️ تحليل Paper وليس توصية مالية'
    return msg


def advanced_analysis_scanner():
    while True:
        try:
            if not ADVANCED_ANALYSIS_ENABLED or get_market_phase() == 'CLOSED':
                time.sleep(ADVANCED_ANALYSIS_INTERVAL); continue
            with state_lock: tickers = list(state.get('tickers', []))[:ADVANCED_ANALYSIS_MAX_SYMBOLS]
            for symbol in tickers:
                if time.time() - _sent_advanced_analysis.get(symbol, 0) < ADVANCED_ANALYSIS_COOLDOWN: continue
                result = advanced_analysis(symbol)
                if not result or not result['targets'] or result['rr'] < 1.5 or result['price'] < result['ema20'] * 0.95: continue
                key = f"advanced_{symbol}_{now_est().date()}"
                with state_lock:
                    if key in state.get('seen_signals', {}): continue
                send_telegram(_format_advanced_analysis(result))
                _sent_advanced_analysis[symbol] = time.time()
                with state_lock: state.setdefault('seen_signals', {})[key] = time.time(); save_state()
                time.sleep(1.0)
            gc.collect(); time.sleep(ADVANCED_ANALYSIS_INTERVAL)
        except Exception as error: logger.error(f'[AdvancedAnalysis] scanner error: {error}'); time.sleep(60)


def ai_process_symbol(symbol, news_impact=0):
    """
    تحليل AI عميق لسهم معين.
    يستخدم Groq لتقييم المخاطر والمحفزات بناءً على البيانات الفنية والأخبار.
    """
    try:
        symbol = str(symbol).upper().strip()
        df = cached_download(symbol, period="5d", interval="5m")
        if df is None or df.empty or len(df) < 20: return
        df.columns = [str(c).lower() for c in df.columns]
        df = compute_indicators(df)
        price = float(df['close'].iloc[-1])
        rvol = float(get_unified_rvol(df) if 'get_unified_rvol' in globals() else calculate_rvol(df))
        atr = float(calculate_atr(df))
        final_trade_recommendation(symbol, news_impact=news_impact, setup="AI_SCANNER", send_alert=True)
    except Exception as e: logger.error(f"ai_process_symbol error {symbol}: {e}")



def ai_fundamental_analysis(symbol):
    """إرسال النموذج إلى AI للحصول على تحليل شامل"""
    if not client:
        return None
    
    prompt = f"""
حلل سهم {symbol} المدرج في ناسداك تحليلًا احترافيًا ومحدثًا، وابحث في جميع ملفات SEC وآخر الأخبار والقوائم المالية، ثم أجب بالتفصيل عن النقاط التالية:

1- ملخص سريع
2- آخر الأخبار المؤثرة
3- آخر إفصاحات SEC
4- التمويل والطروحات
5- تقييم مخاطر التخفيف (Dilution) من 10
6- تقييم مخاطر الشطب من 10
7- احتمالية التقسيم العكسي (Reverse Split) من 10
8- التحليل المالي (نقد، ديون، إيرادات، Cash Burn, Runway)
9- هيكل السهم (Market Cap, Float, Short Interest)
10- التحليل الفني (دعم، مقاومة، أهداف)
11- تقييم الإدارة
12- أهم عوامل الخطر
13- التقييم النهائي من 10 لـ: الوضع المالي، حرق النقد، مخاطر التخفيف، مخاطر الشطب، التقسيم العكسي، قوة الخبر، قوة الزخم، جودة الإدارة، جودة الاستثمار، جودة المضاربة
14- القرار النهائي: 🟢 مناسب للمضاربة / 🟡 مناسب بحذر / 🟠 عالي المخاطر / 🔴 يفضل تجنبه

أجب بتنسيق منظم وواضح، مع شرح مختصر لكل نقطة.
"""
    
    try:
        response = client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "أنت محلل مالي محترف متخصص في الأسهم الأمريكية. أجب بتنسيق منظم وواضح باللغة العربية."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=2000,
            timeout=45
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"AI fundamental analysis error for {symbol}: {e}")
        return None


def _maybe_send_recommendation(base, price, send_alert, auto):
    """يرسل التوصية. في الوضع التلقائي: لا نرسل AVOID (ضجيج — البوت نفسه يقول لا تدخل) ونمرّر على الكولداون الموحّد."""
    decision = base.get("final_decision", "NO_TRADE")
    allowed = ("BUY", "WATCH") if auto else ("BUY", "WATCH", "AVOID")
    if not send_alert or decision not in allowed:
        return False
    symbol = base.get("symbol", "")
    if auto and not _alert_gate_allow(symbol, price, "REC"):
        logger.info(f"[Recommendation] {symbol} {decision} skipped (unified cooldown)")
        return False
    send_telegram(_format_recommendation_message(base))
    if auto:
        _alert_gate_mark(symbol, price, "REC")
    return True


def final_trade_recommendation(symbol, news_impact=0, setup="NONE", pump_risk=False, send_alert=True, auto=False):
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
        consensus_result = None
        if technical_score >= 60 and rvol >= 2.0:
            signal_data = {
                'price': price, 'change': 0.0, 'rvol': rvol,
                'above_vwap': above_vwap, 'ema_bullish': ema_bullish,
                'rsi': rsi, 'breakout': breakout, 'volume_spike': volume_spike,
                'day_high': high_10, 'phase': get_market_phase(), 'score': technical_score,
            }
            consensus_result = ai_consensus(symbol, signal_data)
        if consensus_result:
            final_decision = consensus_result['final']
            base.update({
                'ai_decision': consensus_result.get('gemini_decision', 'CONSENSUS'),
                'confidence': consensus_result.get('confidence', 0),
                'final_decision': final_decision,
                'reason_ar': consensus_result.get('reason', 'تحليل إجماع AI'),
                'reasons': ['Gemini + Mistral + OpenRouter consensus'],
            })
            _record_recommendation({**base, 'time': time.time(), 'technical_score': technical_score})
            _maybe_send_recommendation(base, price, send_alert, auto)
            return base
        if not can_send_ai_request() or not OPENAI_AVAILABLE or client is None:
            if auto:
                # تلقائي وبدون AI: نرسل فقط لو نجحت كل الفلاتر المحلية (مو مجرد سكور 55)
                base["final_decision"] = "WATCH" if (technical_score >= RECOMMENDATION_MIN_TECH_SCORE and not base["invalidations"] and not pump_risk) else "NO_TRADE"
            else:
                base["final_decision"] = "WATCH" if technical_score >= 55 and not pump_risk else "NO_TRADE"
            base["reason_ar"] = "تعذر تحليل AI أو تم بلوغ حد الطلبات؛ القرار المحلي تحفظي"
            _record_recommendation({**base, "time": time.time(), "technical_score": technical_score})
            _maybe_send_recommendation(base, price, send_alert, auto)
            return base
        
        prompt = f"""Analyze {symbol} trading data: Price {price:.2f}, RVOL {rvol:.1f}, RSI {rsi:.0f}, Score {technical_score}/100.
        Return ONLY valid JSON:
        {{
        "decision": "BUY",
        "confidence": 85,
        "setup": "Breakout",
        "risk_level": "Medium",
        "reasons": ["RVOL spike", "Above VWAP"],
        "invalidations": ["Low volume"],
        "reason_ar": "تحليل فني قوي"
        }}"""

        response = groq_chat_completion(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "Conservative analyst. JSON ONLY. NO MARKDOWN."},
                {"role": "user", "content": prompt},
            ], response_format={"type": "json_object"}, temperature=0.1, max_tokens=1000, timeout=30)
        ai = json.loads(response.choices[0].message.content)
        ai_decision = str(ai.get("decision", "WATCH")).upper()
        confidence = max(0, min(int(ai.get("confidence", 0) or 0), 100))
        local_ok = (technical_score >= RECOMMENDATION_MIN_TECH_SCORE and
                    rvol >= RECOMMENDATION_MIN_RVOL and above_vwap and
                    risk_reward >= RECOMMENDATION_MIN_RR and not pump_risk and
                    not base["invalidations"])
        
        is_early_setup = (setup in ("PRE_BREAKOUT", "TOP_GAINER_RANKED", "QUICK_JUMP", "NEWS_MOMENTUM"))
        early_momentum = (rvol >= 3.0 and (breakout or volume_spike or above_vwap))
        
        if ai_decision == "AVOID":
            final_decision = "AVOID"
        elif ai_decision == "BUY" and confidence >= RECOMMENDATION_MIN_CONFIDENCE and local_ok:
            final_decision = "BUY"
        elif (ai_decision in ("BUY", "WATCH") or technical_score >= 60) and not pump_risk:
            final_decision = "WATCH"
        else:
            final_decision = "NO_TRADE"
            
        base.update({"ai_decision": ai_decision, "confidence": confidence,
                     "final_decision": final_decision, "setup": ai.get("setup", setup),
                     "risk_level": ai.get("risk_level", "HIGH"),
                     "reasons": ai.get("reasons", []), "reason_ar": ai.get("reason_ar", "")})
        base["invalidations"].extend(ai.get("invalidations", [])[:4])
        _record_recommendation({**base, "time": time.time(), "technical_score": technical_score})
        _maybe_send_recommendation(base, price, send_alert, auto)
        return base
    except Exception as error:
        logger.error(f"Final recommendation error {symbol}: {error}")
        base["reason_ar"] = "حدث خطأ؛ تم إلغاء التوصية حفاظًا على رأس المال"
        return base


def hot_watchlist_scanner():
    """
    Ranking Engine v2: Discovery → ترتيب بالحيوية (شموع حقيقية فقط) → أفضل 3 إلى AI → Telegram.
    التغييرات عن النسخة القديمة:
      • ما فيه بيانات مزيفة: لو فشل جلب الشموع السهم يُتجاهل (ما نخترع above_vwap/breakout).
      • الميّت يُرفض: سهم واقف، أو تراجع عن قمته، أو ما فيه نشاط حجم/سعر آخر 15-30 دقيقة.
      • الأولوية للزخم اللحظي مو نسبة الصعود اليومية.
      • كولداون موحّد مع بقية الماسحات (نفس السهم ما يتكرر).
    """
    last_ai_time = {}
    dead_cache = {}      # سهم حكمنا عليه ميّت → ما نضيّع عليه طلبات لمدة 5 دقايق (إلا لو رجع تحرك)
    nodata_cache = {}    # سهم فشل جلب شموعه → نعيد المحاولة بعد دقيقتين
    while True:
        try:
            scanner_heartbeat("ranking_engine")
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(120)
                continue
            now_ts = time.time()
            with state_lock:
                candidates = [dict(x) for x in state.get("hot_watchlist", []) if isinstance(x, dict)]
            unique = {}
            for item in candidates:
                symbol = str(item.get("symbol", "")).upper().strip()
                if not symbol or symbol in KNOWN_DELISTED:
                    continue
                if now_ts - _safe_float(item.get("last_seen", item.get("timestamp"))) > HOT_MAX_AGE_SEC:
                    continue
                unique[symbol] = item
            if not unique:
                logger.info("[Ranking] candidates=0 | waiting for Discovery")
                time.sleep(15)
                continue

            def _skip(item):
                sym = str(item.get("symbol", "")).upper().strip()
                if now_ts - nodata_cache.get(sym, 0) < 120:
                    return True
                if now_ts - dead_cache.get(sym, 0) < 300:
                    revived = (_safe_float(item.get("momentum_5m")) >= 3.0
                               or (_safe_float(item.get("qj_ts")) > 0 and now_ts - _safe_float(item.get("qj_ts")) < 300))
                    return not revived
                return False

            ordered = [it for it in sorted(unique.values(), key=lambda it: discovery_priority(it, now_ts), reverse=True)
                       if not _skip(it)][:RANK_MAX_CANDLE_FETCH]
            ranked, rejected, dead_n, nodata = [], 0, 0, 0
            for candidate in ordered:
                symbol = str(candidate.get("symbol", "")).upper().strip()
                try:
                    df = cached_download(symbol, period="5d", interval="5m", prepost=phase in ("PRE", "AFTER"))   # 5d (مو 2d): الاثنين الصبح 2d ما تغطي جمعة = شموع ناقصة
                    if df is None or df.empty or len(df) < 20:
                        nodata += 1
                        nodata_cache[symbol] = now_ts
                        continue
                    res = score_setup(df, candidate, phase)
                    if not res:
                        nodata += 1
                        nodata_cache[symbol] = now_ts
                        continue
                    if not (0.30 <= res["price"] <= MAX_PRICE):
                        rejected += 1
                        continue
                    if res["dead"]:
                        dead_n += 1
                        dead_cache[symbol] = now_ts
                        continue
                    ranked.append({
                        "symbol": symbol, "price": res["price"], "change": _safe_float(candidate.get("change")),
                        "rvol": res["rvol"], "rsi": res["rsi"], "vwap": res["vwap"],
                        "above_vwap": res["above_vwap"], "ema_bullish": res["ema_bullish"],
                        "volume_spike": res["volume_spike"], "breakout": res["breakout"],
                        "score": res["score"], "advanced_score": 0, "advanced_indicators": None,
                        "factors": res["factors"], "move_15m": res["fm"]["move_15m"],
                        "vol_accel": res["fm"]["vol_accel"], "timestamp": time.time(),
                        "data_source": "YAHOO_5M",
                    })
                except Exception as error:
                    rejected += 1
                    logger.warning(f"[Ranking] {symbol}: {error}")

            ranked.sort(key=lambda it: int(it.get("score", 0)), reverse=True)
            top5 = ranked[:5]
            with state_lock:
                state["ai_candidates"] = [dict(item) for item in top5]
                state["ranking_timestamp"] = time.time()
                save_state()

            ai_count = 0
            for candidate in top5:
                if ai_count >= 3:
                    break
                if int(candidate.get("score", 0)) < RANK_MIN_AI_SCORE:
                    continue
                symbol = candidate["symbol"]
                if time.time() - last_ai_time.get(symbol, 0) < AI_RETRY_COOLDOWN:
                    continue
                if not _alert_gate_allow(symbol, candidate.get("price", 0), "RANK"):
                    continue
                last_ai_time[symbol] = time.time()
                if AI_PROCESS_ENABLED:
                    threading.Thread(
                        target=final_trade_recommendation,
                        args=(symbol, 0, "TOP_GAINER_RANKED", False, True, True),
                        daemon=True, name=f"TopRankAI-{symbol}",
                    ).start()
                    ai_count += 1
                    time.sleep(1)
            logger.info(f"[Ranking] pool={len(unique)} checked={len(ordered)} alive={len(ranked)} "
                        f"dead={dead_n} no_data={nodata} rejected={rejected} ai_sent={ai_count}")
            time.sleep(20)
        except Exception as error:
            logger.error(f"[Ranking] error: {error}")
            time.sleep(30)


def _record_recommendation(rec):
    with state_lock:
        history = state.setdefault("recommendation_history", [])
        history.append(rec)
        if len(history) > 100: history.pop(0)
        stats = state.setdefault("recommendation_stats", {})
        decision = rec.get("final_decision", "NO_TRADE")
        stats[decision] = stats.get(decision, 0) + 1
        save_state()


def _format_recommendation_message(base):
    symbol = base.get("symbol", "???")
    decision = base.get("final_decision", "WATCH")
    conf = base.get("confidence", 0)
    price = base.get("price", 0.0)
    rvol = base.get("rvol", 0.0)
    setup = base.get("setup", "NONE")
    risk = base.get("risk_level", "HIGH")
    entry_low = base.get("entry_zone_low", 0.0)
    entry_high = base.get("entry_zone_high", 0.0)
    confirm = base.get("confirmation_price", 0.0)
    sl = base.get("stop_loss", 0.0)
    t1 = base.get("target_1", 0.0)
    t2 = base.get("target_2", 0.0)
    rr = base.get("risk_reward", 0.0)
    reason_ar = base.get("reason_ar", "")
    reasons = base.get("reasons", [])
    msg = f"🧠 *FINAL RECOMMENDATION: {symbol}*\n━━━━━━━━━━━━━━━━\n"
    msg += f"🤖 قرار AI: {base.get('ai_decision', 'N/A')} | الثقة: {conf}/100\n"
    msg += f"🛡️ القرار النهائي: *{decision}*\n"
    msg += f"📌 Setup: {setup} | المخاطرة: {risk}\n"
    msg += f"💰 السعر: ${price:.4f} | RVOL: {rvol:.1f}x\n"
    msg += f"🎯 منطقة الدخول: ${entry_low:.4f} - ${entry_high:.4f}\n"
    msg += f"✅ تأكيد: ${confirm:.4f}\n"
    msg += f"🛑 الوقف: ${sl:.4f}\n"
    msg += f"🎯 الأهداف: ${t1:.4f} / ${t2:.4f}\n"
    msg += f"📐 R:R: {rr:.2f} | الصلاحية: 30 دقيقة\n━━━━━━━━━━━━━━━━\n"
    if reasons:
        msg += "🔍 العوامل:\n"
        for r in reasons: msg += f"  • {r}\n"
    if base.get("invalidations"):
        msg += "🚫 يلغى القرار عند: " + ", ".join(base["invalidations"]) + "\n"
    if reason_ar:
        msg += f"💡 {reason_ar}\n"
    msg += "⚠️ توصية تحليلية Paper Trading — لا تنفيذ تلقائي"
    return msg


def has_dilution_risk(text):
    if not text: return False
    keywords = ['dilution', 'offering', 'warrants', 'shelf', 'convertible', 'direct offering', 'public offering']
    text_lower = text.lower()
    return any(k in text_lower for k in keywords)


def _unified_should_send(store, symbol, cooldown=1800):
    return time.time() - store.get(symbol, 0) >= cooldown


def _format_unified_vwap(symbol, result):
    price = result.get('price', 0)
    targets = result.get('targets', [])
    msg = f"🔗 *VWAP/SR Unified: {symbol}*\n━━━━━━━━━━━━━━━━\n💰 السعر: ${price:.4f}\n"
    msg += f"📏 VWAP: ${result.get('vwap', 0):.4f}\n📍 الدعم: ${result.get('support', 0):.4f}\n📍 المقاومة: ${result.get('resistance', 0):.4f}\n"
    msg += f"🔥 RVOL: {result.get('rvol', 0):.1f}x | R:R: {result.get('rr', 0):.2f}:1\n"
    if targets: msg += '🎯 الأهداف: ' + ' → '.join(f'${float(x):.4f}' for x in targets[:3]) + '\n'
    msg += '⚠️ تحليل Paper ومراقبة فنية فقط'
    return msg


def analyze_vwap_bounce(symbol, df_1m=None, df_5m=None, df_daily=None):
    try:
        if df_5m is None: df_5m = cached_download(symbol, period='5d', interval='5m')
        if df_5m is None or df_5m.empty or len(df_5m) < 20: return None
        df_5m = df_5m.copy()
        df_5m.columns = [str(c).lower() for c in df_5m.columns]
        df_5m = compute_indicators(df_5m)
        price = float(df_5m['close'].iloc[-1])
        if not (0.5 <= price <= 50.0): return None
        volume = float(df_5m['volume'].iloc[-1] or 0)
        avg_volume = float(df_5m['volume'].tail(20).mean() or 0)
        rvol = volume / max(avg_volume, 1.0)
        if rvol < 1.5: return None
        typical = (df_5m['high'] + df_5m['low'] + df_5m['close']) / 3
        vwap_series = (typical * df_5m['volume']).cumsum() / df_5m['volume'].cumsum().replace(0, np.nan)
        vwap = float(vwap_series.iloc[-1]) if pd.notna(vwap_series.iloc[-1]) else price
        std = float(df_5m['close'].rolling(20).std().iloc[-1] or 0)
        lower_band = vwap - std * 1.5
        support = min(float(df_5m['low'].tail(20).min()), lower_band)
        resistance = float(df_5m['high'].tail(20).max())
        bounce = price > support * 1.01 and price > float(df_5m['close'].iloc[-2]) and price >= vwap * 0.98
        if not (price < lower_band and bounce): return None
        entry = max(price * 1.002, resistance * 1.002)
        stop = min(support * 0.98, price * 0.94)
        targets = [round(entry * p, 4) for p in [1.08, 1.15, 1.25]]
        rr = (targets[0] - entry) / max(entry - stop, 0.0001)
        score = 0
        if rvol >= 3: score += 40
        if bounce: score += 30
        if rr >= 3: score += 30
        return {'symbol': symbol, 'price': round(price, 4), 'entry': round(entry, 4), 'stop': round(stop, 4), 'targets': targets, 'support': round(support, 4), 'resistance': round(resistance, 4), 'vwap': round(vwap, 4), 'rvol': round(rvol, 2), 'rr': round(rr, 2), 'score': min(100, score), 'timestamp': time.time()}
    except Exception as error: logger.debug(f'[VWAP Bounce] {symbol}: {error}'); return None



# ================================================================
# 🤖 AUTO HUNTER — يصطاد الأسهم ويحللها آلياً بدون تدخل يدوي
# ================================================================
AUTO_HUNTER_ENABLED = True
AUTO_HUNTER_INTERVAL = 120
AUTO_HUNTER_MAX_CANDIDATES = 5
AUTO_HUNTER_MIN_SCORE = 80
_sent_auto_hunter = {}


def price_alert_monitor():
    """يراقب التنبيهات السعرية ويرسل إشعارات"""
    while True:
        try:
            with state_lock:
                alerts = dict(state.get("price_alerts", {}))
            if not alerts:
                time.sleep(60); continue
                
            for symbol, data in list(alerts.items()):
                try:
                    if isinstance(data, dict) and data.get("triggered", False):
                        continue
                    target = data.get("price", 0) if isinstance(data, dict) else data
                    if target == 0: continue
                    
                    df = cached_download(symbol, period="1d", interval="1m")
                    if df.empty: continue
                    
                    current = float(df['close'].iloc[-1])
                    # Trigger if price hits or exceeds target (upward or downward)
                    triggered = False
                    if current >= target and target > 0: # Target hit from below
                         triggered = True
                    
                    if triggered:
                        msg = f"🔔 *تنبيه سعري!*\n━━━━━━━━━━━━━━━━\n📊 *{symbol}*\n💰 السعر الحالي: *${current:.2f}*\n🎯 المستهدف: *${abs(target):.2f}*\n✅ تم الوصول للسعر المستهدف!"
                        send_telegram(msg)
                        logger.info(f"🔔 PRICE ALERT: {symbol} reached ${current:.2f}")
                        with state_lock:
                            if symbol in state.get("price_alerts", {}):
                                state["price_alerts"][symbol]["triggered"] = True
                                save_state()
                except Exception as e:
                    logger.debug(f"Alert monitor error {symbol}: {e}")
            time.sleep(60)
        except Exception as e:
            logger.error(f"Price alert monitor error: {e}")
            time.sleep(60)


def auto_hunter_scan():
    """
    الصياد الآلي v2: يشتغل على الأسهم الحيّة فقط (نفس معيار الحيوية في الـRanking) ويضيف فحص أساسي (Short/Cap)
    للأفضل 3 فقط بدل 30 طلب .info بطيء. كولداون موحّد مع بقية الماسحات، وبدون سطور "فريق الذكاء الاصطناعي"
    المزيفة اللي كانت تنطبع بدون ما يُستدعى أي AI فعلياً.
    """
    while True:
        try:
            scanner_heartbeat("auto_hunter")
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(300)
                continue
            now_ts = time.time()
            with state_lock:
                candidates = [dict(x) for x in state.get("hot_watchlist", []) if isinstance(x, dict)]
            candidates = [c for c in candidates
                          if c.get("symbol") and now_ts - _safe_float(c.get("last_seen", c.get("timestamp"))) <= HOT_MAX_AGE_SEC]
            if not candidates:
                time.sleep(30)
                continue
            candidates.sort(key=lambda it: discovery_priority(it, now_ts), reverse=True)

            ranked = []
            for c in candidates[:15]:
                symbol = str(c.get("symbol", "")).upper().strip()
                if not symbol or not _alert_gate_allow(symbol, c.get("price", 0), "HUNTER"):
                    continue
                df = cached_download(symbol, period="5d", interval="5m", prepost=phase in ("PRE", "AFTER"))   # نفس مفتاح الكاش مع الـRanking والتوصية = طلب واحد يخدم الثلاثة
                if df is None or df.empty or len(df) < 20:
                    continue
                res = score_setup(df, c, phase)
                if not res or res["dead"] or res["price"] > 15.0:
                    continue
                ranked.append((symbol, res, c))
            ranked.sort(key=lambda t: t[1]["score"], reverse=True)

            info_calls = 0
            for symbol, res, c in ranked[:AUTO_HUNTER_MAX_CANDIDATES]:
                score = int(res["score"])
                reasons = list(res["factors"])
                risks = []
                short_pct = 0.0
                market_cap = _safe_float(c.get("market_cap"))
                if score >= 65 and info_calls < 3:
                    info_calls += 1
                    try:
                        time.sleep(3.0)  # نتفادى 429 من Yahoo
                        info = yf.Ticker(symbol).info
                        sp = info.get("shortPercentOfFloat")
                        short_pct = float(sp) * 100 if sp is not None else 0.0
                        mc = _safe_float(info.get("marketCap"))
                        market_cap = mc or market_cap
                        cash = _safe_float(info.get("totalCash"))
                        debt = _safe_float(info.get("totalDebt"))
                        profit_margin = _safe_float(info.get("profitMargins")) * 100
                        if short_pct > 20:
                            score += 10; reasons.append(f"📉 Short {short_pct:.1f}%")
                        if 0 < market_cap < 500_000_000:
                            score += 5; reasons.append("🎯 Small Cap")
                        if cash < 50_000_000 and debt > cash:
                            risks.append("💸 نقد منخفض"); score -= 10
                        if profit_margin < 0:
                            risks.append("📉 غير مربحة"); score -= 5
                    except Exception as error:
                        logger.debug(f"AutoHunter info fetch error for {symbol}: {error}")
                score = max(0, min(100, score))
                if score < AUTO_HUNTER_MIN_SCORE:
                    continue
                if not _alert_gate_allow(symbol, res["price"], "HUNTER"):
                    continue

                cap_str = f"${market_cap:,.0f}" if market_cap > 0 else "N/A"
                short_str = f"{short_pct:.1f}%" if short_pct > 0 else "N/A"
                fm = res["fm"]
                msg = f"🎯 *AUTO HUNTER: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                msg += f"📊 السكور: *{score}/100*\n💰 السعر: *${res['price']:.4f}* | RVOL: *{res['rvol']:.1f}x*\n"
                msg += f"⚡ آخر 15د: *{fm['move_15m']:+.1f}%* | تسارع حجم: *{fm['vol_accel']:.1f}x*\n"
                msg += f"📉 الشورت: *{short_str}* | Cap: {cap_str}\n"
                if reasons:
                    msg += "\n✅ *الإيجابيات:*\n" + "\n".join(f"   • {r}" for r in reasons[:5]) + "\n"
                if risks:
                    msg += "\n⚠️ *المخاطر:*\n" + "\n".join(f"   • {r}" for r in risks[:3]) + "\n"
                try:
                    df_daily = cached_download(symbol, period="120d", interval="1d")
                    if df_daily is not None and not df_daily.empty:
                        df_daily.columns = [str(col).lower() for col in df_daily.columns]
                        ichimoku = calculate_ichimoku(df_daily)
                        supertrend = calculate_supertrend(df_daily)
                        fib = calculate_fibonacci(df_daily)
                        if ichimoku:
                            pos = "✅ صاعد" if ichimoku["cloud_position"] == "ABOVE" else ("❌ هابط" if ichimoku["cloud_position"] == "BELOW" else "⏳ تذبذب")
                            msg += f"\n☁️ *Ichimoku:* {pos} | Cross: {'✅' if ichimoku['tenkan_kijun_cross'] else '❌'}\n"
                        if supertrend:
                            msg += f"📈 *SuperTrend:* {supertrend['direction']} | 🛑 Stop: ${supertrend['supertrend']:.4f}\n"
                        if fib and fib["near_618"]:
                            msg += f"📐 *Fibonacci:* ⭐ قرب 61.8% (${fib['levels']['61.8%']:.4f})\n"
                except Exception as error:
                    logger.debug(f"AutoHunter daily indicators error for {symbol}: {error}")

                decision = "🟢 مناسب للمضاربة" if score >= 85 else ("🟡 مناسب بحذر" if score >= 75 else "🟠 عالي المخاطر")
                msg += f"\n━━━━━━━━━━━━━━━━\n📌 *القرار النهائي:* {decision}\n"
                msg += (f"\n🎯 *خطة التداول:*\n   🎯 هدف 1: ${res['price'] * 1.08:.4f} (+8%)\n"
                        f"   🎯 هدف 2: ${res['price'] * 1.15:.4f} (+15%)\n   🛑 وقف: ${res['price'] * 0.95:.4f} (-5%)\n")
                msg += "\n⚠️ تحليل آلي وليست توصية مالية"

                send_telegram(msg)
                _alert_gate_mark(symbol, res["price"], "HUNTER")
                _sent_auto_hunter[symbol] = time.time()
                time.sleep(1)

            gc.collect()
            time.sleep(AUTO_HUNTER_INTERVAL)
        except Exception as error:
            logger.error(f"[AutoHunter] scanner error: {error}")
            time.sleep(60)



if __name__ == "__main__":
    load_state()
    ensure_state_schema()
    if not state.get("tickers"): state["tickers"] = ["AAPL", "TSLA", "NVDA", "AMD", "MSFT", "META", "GOOGL", "AMZN", "NFLX", "PYPL"]

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

    # 🔥 ماسحات صيد البيني
    # 🔍 PRIMARY DISCOVERY ENGINE
    threading.Thread(target=full_sweep_scanner, daemon=True, name='FullRotationDiscovery').start()
    logger.info('🔍 Full Rotation Discovery started')
    threading.Thread(target=after_hours_discovery_scanner, daemon=True, name='AfterHoursDiscovery').start()
    logger.info('🌙 After-Hours Discovery started')

    # 🏆 TOP GAINERS SCANNER
    threading.Thread(target=top_gainers_scanner, daemon=True, name='TopGainersDiscovery').start()
    logger.info('🏆 Top Gainers Discovery started')
    
    # 🚨 QUICK JUMP SCANNER
    threading.Thread(target=quick_jump_scanner, daemon=True, name="QuickJumpDiscovery").start()
    logger.info('🚨 Quick Jump Discovery started')

    # 🎯 RANKING ENGINE
    threading.Thread(target=hot_watchlist_scanner, daemon=True).start()
    threading.Thread(target=setup_scanner, daemon=True, name="ExplosiveSetupEngine").start()
    logger.info('🎯 Hybrid pipeline started: Discovery → Ranking → AI → Telegram')

    # 📰 أخبار RSS والمحفزات
    threading.Thread(target=rss_news_scanner, daemon=True).start()
    start_safe_finnhub()
    threading.Thread(target=auto_hunter_scan, daemon=True, name="AutoHunter").start()
    threading.Thread(target=price_alert_monitor, daemon=True, name="PriceAlertMonitor").start()
    logger.info("🤖 Auto Hunter started")
    
    logger.info("=" * 50)
    logger.info("🚀 PENNY HUNTER BOT STARTED!")
    logger.info("📊 Target: $0.3 - $30 penny stocks")
    logger.info("=" * 50)
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)

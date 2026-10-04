# =====================================================================
# PENNY HUNTER — main.py — النسخة الموحّدة (دمج main-robot_fixed.py + 4_5909128868987411489.py)
# ---------------------------------------------------------------------
# بوت تنبيهات أسهم البيني (Paper Trading فقط — لا يوجد تنفيذ حقيقي) يعمل على تيليجرام + Flask (/health).
#
# خط الأنابيب:  Discovery (شاشات Yahoo + Nasdaq + Webull + بث WebSocket)
#               → Ranking (حيوية لحظية، مو % اليوم) → AI (Groq) → Telegram
# طبقات التنبيه المستقلة: ESE (Explosive Setup Engine) · EARLY (قبل الانفجار + قائمة A+ + بوابة VWAP)
#               · Mega Mover · Gap Hunter (+ SURGE ومتابعة الفجوة) · Pump&Dump · Accumulation · High Breakout
#               · Doji بعد هبوط (+ قاع مزدوج / ابتلاع شرائي) · Weekly Swing · Anomaly · Insider (Form 4) · RSS/Finnhub
# الروبوت الذاتي (SLE): تعلّم وتطوّر ذاتي + مشرف يعيد تشغيل أي خيط يموت + Move Registry + Signal Journal
#               + نسخ احتياطي للحالة على GitHub (اختياري).
# متعلّم الأنماط 🧬 (من الشموع، قبل الحركة): نماذج +25%/ساعة و+25/50/80/100/170%/يوم، وسلّم قياس لـ +230/500/1200% (يُعرض بـ /patterns).
# متعلّم الشارت اليومي 📅 (دعم/انضغاط، 3 سنوات): قائمة المرشحين بـ /picks.
# المدرّب الأوفلاين 🌙: والسوق مقفل (نهاية الأسبوع/الليل/العطل) يسحب شموع 5د لآلاف الأسهم القديمة، يعيد تشغيل الأيام الماضية
#               على إعدادات الروبوت، ويغذّي تطوّر الجينوم (مع حجز 30% للتحقق ضد فرط التعلّم). الأوامر: /offline /offlineon /offlineoff
#
# مصدر هذه النسخة: الأساس main-robot_fixed.py (الأحدث)، ومعه من الملف الثاني حزم FIX-13..18
#   (تغطية EARLY كاملة، بوابة VWAP، قائمة A+، تشديد الدوجي، أنماط القاع المزدوج والابتلاع، SETUP_THRUST_MIN_PCT=15).
#   وأُزيلت الدوال الميتة (ماسحات ما تنبدأ أبداً، دوال بلا استدعاء، ثوابت بلا قراءة) — نسخة منها بملف الأرشيف.
#
# كل الإعدادات تتعدّل من متغيرات البيئة (Environment) بدون لمس الكود.
# مصدر بيانات إضافي: أضف دالة إلى DISCOVERY_EXTRA_PROVIDERS ترجع قائمة dicts فيها symbol/price/change/volume.
# =====================================================================
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
from datetime import datetime, timedelta, timezone as _timezone
import random
import gc
import base64
import struct
import math
from collections import deque
try:
    import psutil
except ImportError:
    psutil = None
try:
    import pandas_ta as ta
except ImportError:
    try:
        # pandas-ta الأصلي غير متاح حاليًا لبعض إصدارات Python؛
        # pandas-ta-classic يحافظ على واجهة المؤشرات الأساسية.
        import pandas_ta_classic as ta
    except ImportError:
        ta = None

import pandas as pd
import numpy as np
# [MemFix] gymnasium + stable_baselines3 يسحبون torch كاملة (~300-450MB RSS) بمجرد الاستيراد.
# صار الاستيراد "كسول": ما يتحمّل إلا لحظة تدريب PPO الفعلي (وبعد التأكد من توفر الذاكرة)،
# فالتدريب والتعلّم الأوفلاين يشتغلون عادي وبدون ما نحرق الذاكرة بالتشغيل.
gym = None
spaces = None
PPO = None
_RL_STATE = {"tried": False, "ok": False}
_RL_LOCK = threading.Lock()


def _rl_lazy_import():
    """يحمّل gymnasium/stable-baselines3 مرة وحدة عند أول حاجة. يرجع True لو جاهزة."""
    global gym, spaces, PPO
    with _RL_LOCK:
        if _RL_STATE["tried"]:
            return _RL_STATE["ok"]
        _RL_STATE["tried"] = True
        try:
            import gymnasium as _gym
            from gymnasium import spaces as _spaces
            from stable_baselines3 import PPO as _PPO
            gym, spaces, PPO = _gym, _spaces, _PPO
            _RL_STATE["ok"] = True
        except Exception as _rl_err:
            logger.warning(f"[PPO] lazy import failed: {_rl_err}")
        return _RL_STATE["ok"]
import pytz
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, jsonify
import telebot
from curl_cffi import requests
import yfinance as yf

# Runtime safety: AI/RL components are advisory/shadow by default; Paper Trading is default.
# Keep these safeguards enabled until a long out-of-sample and paper evaluation passes.
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
# Railway Volume: في مشروعك المجلد هو /data. نستخدم متغير Railway إن وُجد،
# ثم /data إن كان موجودًا، وإلا المجلد الحالي للتشغيل المحلي.
_VOLUME_ROOT = os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or ("/data" if os.path.isdir("/data") else ".")
STATE_FILE = os.path.join(_VOLUME_ROOT, "state_penny_hunter.json")

# GitHub backup/restore: Volume هو المصدر المحلي السريع، وGitHub نسخة خارجية
# تحمي الذاكرة والتجارب من انتهاء/إعادة إنشاء خدمة Railway.
GITHUB_SYNC_ENABLED = os.getenv("GITHUB_SYNC_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPO = os.getenv("GITHUB_REPO", "").strip()          # مثال: username/private-repo
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip() or "main"
GITHUB_STATE_PATH = os.getenv("GITHUB_STATE_PATH", "backups/state_penny_hunter.json.gz").strip().lstrip("/")
GITHUB_SYNC_INTERVAL_SEC = max(300, int(os.getenv("GITHUB_SYNC_INTERVAL_SEC", "1800")))
GITHUB_COMMIT_NAME = os.getenv("GITHUB_COMMIT_NAME", "Penny Hunter Robot").strip()
GITHUB_COMMIT_EMAIL = os.getenv("GITHUB_COMMIT_EMAIL", "penny-hunter@users.noreply.github.com").strip()
GITHUB_API_ROOT = "https://api.github.com"
_GITHUB_SYNC_LOCK = threading.Lock()
_GITHUB_LAST_UPLOAD_HASH = None

# حماية أوامر Telegram: يجب ضبط AUTHORIZED_CHAT_ID صراحةً.
AUTHORIZED_CHAT_ID = os.getenv("AUTHORIZED_CHAT_ID", "").strip()

# محرك التوصية الموحد: AI يفسر، والقواعد المحلية تملك حق الرفض.
RECOMMENDATION_MIN_CONFIDENCE = 80
RECOMMENDATION_MIN_TECH_SCORE = 75
RECOMMENDATION_MIN_RVOL = 2.0
RECOMMENDATION_MIN_RR = 2.0
RECOMMENDATION_EXPIRY_MINUTES = 30

# ================= MERGED LEGACY SAFETY + JOURNAL CONFIG =================
# Retained from main-15: extended-move dedupe and measurable signal journal.
BIGMOVE_UPDATE_DELTA = float(os.getenv("BIGMOVE_UPDATE_DELTA", "20"))
BIGMOVE_UPDATE_COOLDOWN_SEC = int(os.getenv("BIGMOVE_UPDATE_COOLDOWN_SEC", "600"))
BIGMOVE_STRONG_DELTA = float(os.getenv("BIGMOVE_STRONG_DELTA", "40"))
BIGMOVE_DISTRIBUTION_PCT = float(os.getenv("BIGMOVE_DISTRIBUTION_PCT", "12"))
BIGMOVE_DUMP_PCT = float(os.getenv("BIGMOVE_DUMP_PCT", "20"))
BIGMOVE_NEWHIGH_MARGIN = float(os.getenv("BIGMOVE_NEWHIGH_MARGIN", "0.05"))
_REGIME_RANK = {"ACTIVE": 0, "DISTRIBUTION": 1, "DUMP": 2}
_BIGMOVE_TIERS = (20.0, 50.0, 100.0, 200.0)
BEARISH_AI_DECISIONS = ("SELL", "AVOID", "SHORT", "BEARISH", "STRONG_SELL")
JOURNAL_ENABLED = os.getenv("JOURNAL_ENABLED", "true").strip().lower() == "true"
JOURNAL_MAX = int(os.getenv("JOURNAL_MAX", "10000"))
JOURNAL_HORIZONS_MIN = (15, 30, 60, 180)
JOURNAL_CHECK_INTERVAL = int(os.getenv("JOURNAL_CHECK_INTERVAL", "300"))
JOURNAL_MAX_CHECKS_PER_CYCLE = int(os.getenv("JOURNAL_MAX_CHECKS_PER_CYCLE", "40"))


# Strategy Parameters
BASE_TP_PCT = 1.06
# هدف الربح المستهدف للصفقات الفعلية. هذا هدف وليس ضمانًا؛ لا يمكن معرفة وقت
# الوصول إليه مسبقًا. غيّره من Railway Variables إذا أردت ملفًا مختلفًا.
TARGET_PROFIT_PCT = max(1.0, float(os.getenv("TARGET_PROFIT_PCT", "25")))
TARGET_PROFIT_MULTIPLIER = 1.0 + (TARGET_PROFIT_PCT / 100.0)
# وضع تجريبي حصري: لا تُرسل تنبيهات السوق إلا للمحركات المحددة هنا.
# اتركه فارغًا لتعطيل الحصرية وإعادة كل المحركات.
ACTIVE_ENGINES_ONLY = {
    x.strip().upper() for x in os.getenv("ACTIVE_ENGINES_ONLY", "").split(",") if x.strip()
}
SL_PCT = 0.97
TRAIL_TO_BREAKEVEN_TRIGGER = 1.04
TRAIL_TO_LOCK_PROFIT_TRIGGER = 1.10
TRAIL_LOCK_PROFIT_SL_PCT = 1.05
NEAR_TARGET_ALERT_RATIO = 0.80
MIDDAY_MIN_SCORE = 70
DAILY_REPORT_HOUR = 16
DAILY_REPORT_MINUTE = 15

PENNY_MIN_GAIN = 15.0
PENNY_MIN_RVOL = 4.0

# Scanner General Settings
MIN_PRICE = 0.2
MAX_PRICE = 15.0
MIN_VOLUME = 10000
MAX_TICKERS_TO_SCAN = 500   # 500 سهم فقط - الأكثر نشاطاً (Explosive Mode)
MIN_RVOL_BY_PHASE = {"PRE": 2.0, "REGULAR": 1.5, "AFTER": 2.5}
TELEGRAM_DELAY = float(os.getenv("TELEGRAM_DELAY", "1.2"))  # كانت 3 ثواني تحت قفل عام → التنبيهات تتراكم وتتأخر
TRADE_MONITOR_INTERVAL = 30
SIGNAL_COOLDOWN = 3600         # ساعة واحدة كولداون (كان ساعتين — خُفّف)
MAX_DAILY_SIGNALS = 150       # رُفع من 50 لأيام الحركة القوية

# ================= أسماء الجلسات (للرسائل) =================
_PHASE_LABEL_AR = {
    "PRE": "قبل الافتتاح", "REGULAR": "الجلسة العادية",
    "AFTER": "بعد الإغلاق", "CLOSED": "السوق مغلق",
}

# ================= DISCOVERY / FRESHNESS SETTINGS (v2) =================
# كل القيم تنعدّل من متغيرات البيئة (Environment) بدون لمس الكود.
DISCOVERY_INTERVAL_SEC   = int(os.getenv("DISCOVERY_INTERVAL_SEC", "45"))     # كل كم ثانية نقرأ شاشات Yahoo
DISCOVERY_SCREENERS      = [s.strip() for s in os.getenv("DISCOVERY_SCREENERS", "day_gainers,most_actives,small_cap_gainers,most_shorted_stocks,aggressive_small_caps").split(",") if s.strip()]
# تصحيح: most_actives_penny_stocks اللي حطيتها قبل كانت تخمين غلط — تأكدت الحين
# من كود yfinance الرسمي نفسه إن معرّفات ياهو الحقيقية المتاحة هي فقط:
# day_gainers, day_losers, most_actives, most_shorted_stocks, small_cap_gainers,
# aggressive_small_caps, growth_technology_stocks, undervalued_growth_stocks,
# undervalued_large_caps (+ صناديق/ETF). استبدلتها بـmost_shorted_stocks (فرز
# حسب نسبة الشورت من الأسهم القائمة — مرتبط مباشرة بمنطق short squeeze) و
# aggressive_small_caps (سيولة عالية + نمو أرباح ضعيف = تغطية أوسع للمضاربة).
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
AI_MAX_ANALYSIS_PER_MINUTE = 10
AI_MODEL = os.getenv("AI_MODEL", "openai/gpt-oss-20b")


# ================= QUICK JUMP SCANNER (15% في دقيقة أو نص دقيقة) =================
QUICK_JUMP_SCAN_INTERVAL = 30     # فحص كل 30 ثانية
QUICK_JUMP_MIN_GAIN_1M   = 15.0   # قفزة 15% خلال آخر شمعة 1 دقيقة
QUICK_JUMP_MIN_VOL       = 30000  # حجم أدنى للشمعة
QUICK_JUMP_PRICE_MIN     = 0.3
QUICK_JUMP_PRICE_MAX     = 50.0


SR_MIN_RVOL = 2.5                  # رُفع من 1.8 → 2.5 لتقليل الإشارات الضعيفة
SR_PROXIMITY_PCT = 1.0             # تنبيه عند الاقتراب من الدعم/المقاومة ضمن 1%
SR_BREAKOUT_BUFFER_PCT = 0.35      # هامش اختراق فوق المقاومة لتقليل الإشارات الكاذبة
SR_MIN_TOUCHES = 3                 # رُفع من 2 → 3 لمسات لاعتماد المستوى


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
    now_ts = time.time()
    with state_lock:
        hot_count = len(state.get("hot_watchlist", []))
        universe_count = len(state.get("tickers", []))
        sl = dict(state.get("self_learning") or {})
    # [SLE] حالة الروبوت تظهر في /health مباشرة (جيل، جينوم حيّ، محركات مكتومة)
    sle_view = {
        "enabled": bool(sl.get("enabled", False)),
        "generation": sl.get("generation"),
        "active_genome": sl.get("active_genome_id"),
        "champion": sl.get("champion_id"),
        "signals_seen": sl.get("signals_seen_total"),
        "suppressed_total": sl.get("suppressed_total"),
        "health_score": (sl.get("health") or {}).get("health_score"),
        "engine_trust": {k: v.get("trust") for k, v in (sl.get("engine_trust") or {}).items()},
    }
    return jsonify({
        "status": "ok", "bot_enabled": bool(bot), "has_chat_id": bool(CHAT_ID),
        "hot_watchlist": hot_count, "universe": universe_count,
        "scanner_age_sec": {k: int(now_ts - v) for k, v in dict(_heartbeats).items()},
        "freshness": _data_freshness_snapshot(),
        "self_learning": sle_view,
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
    return now_saudi().strftime("%I:%M:%S %p")
def get_trade_levels(price, rvol=1.0):
    """تحديد الهدف والوقف للصفقة الفعلية؛ الهدف الافتراضي +25% وليس ضمانًا."""
    tp_pct = TARGET_PROFIT_MULTIPLIER
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
        state.setdefault("mega_mover_sent", {})
        state.setdefault("doji_watch", {})
        state.setdefault("high_breakout_sent", {})
        state.setdefault("gap_hunter_sent", {})
        state.setdefault("gap_followups", {})
        state.setdefault("surge_alerts", {})
        state.setdefault("accumulation_watch", {})
        state.setdefault("pump_dump_watch", {})
        state.setdefault("swing_watch", {})
        state.setdefault("anomaly_sent", {})
        state.setdefault("insider_buy_sent", {})
        state.setdefault("price_cache", {})  # {symbol: [(ts, o, h, l, c, v), ...]} آخر 4 ساعات
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
        state.setdefault("event_gate", {})
        state.setdefault("in_play", {})
        # [SLE] ذاكرة الروبوت: الجينوم، الأجيال، الدروس، ثقة المحركات، الأهداف، صحة النظام
        state.setdefault("self_learning", {})
        state.setdefault("dqn_agent", {})
        state.setdefault("finrl_env", {})

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
        state["event_gate"] = {k: v for k, v in state.get("event_gate", {}).items()
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


try:
    import pandas_market_calendars as mcal
    _NYSE_CAL = mcal.get_calendar("NYSE")
except Exception:
    _NYSE_CAL = None

_holiday_cache = {"date": None, "is_holiday": False}


def _is_market_holiday(now_et):
    """
    يتأكد لو اليوم عطلة رسمية بالبورصة (عيد الشكر، الكريسماس...) — get_market_phase
    كانت تتأكد من يوم الأسبوع بس (weekday<5)، فتعتبر أي يوم اثنين-جمعة "تداول عادي"
    حتى لو عطلة رسمية، وتحاول تفحص سوق مقفل أصلاً طول اليوم. فكرة استخدام
    pandas_market_calendars من مشروع مفتوح المصدر (stock-scanner). مخزّن مؤقتًا
    (يوم واحد بس) — ما يحسبها كل مرة تُستدعى get_market_phase.
    """
    if _NYSE_CAL is None:
        return False
    today_str = now_et.strftime("%Y-%m-%d")
    if _holiday_cache["date"] == today_str:
        return _holiday_cache["is_holiday"]
    try:
        sched = _NYSE_CAL.schedule(start_date=today_str, end_date=today_str)
        is_holiday = sched.empty
    except Exception:
        is_holiday = False
    _holiday_cache["date"] = today_str
    _holiday_cache["is_holiday"] = is_holiday
    return is_holiday


def get_market_phase():
    now = now_est()
    weekday = now.weekday()
    minutes = now.hour * 60 + now.minute
    if weekday >= 5 or _is_market_holiday(now):
        return "CLOSED"
    if 4 * 60 <= minutes < 9 * 60 + 30:
        return "PRE"
    if 9 * 60 + 30 <= minutes < 16 * 60:
        return "REGULAR"
    if 16 * 60 <= minutes < 20 * 60:
        return "AFTER"
    return "CLOSED"


_state_dirty = False
_state_dirty_lock = threading.Lock()
_STATE_FLUSH_INTERVAL = float(os.getenv("STATE_FLUSH_INTERVAL", "5"))


def _save_state_now():
    """Write a consistent atomic snapshot. Callers should normally use save_state()."""
    with state_lock:
        try:
            os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
            snapshot = {k: v for k, v in state.items() if k != "full_tickers"}
            tmp_path = STATE_FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, default=str)
            os.replace(tmp_path, STATE_FILE)
            return True
        except Exception as e:
            logger.error(f"Save state error: {e}")
            return False


def save_state(immediate=False):
    """Mark state dirty; batch frequent writes and optionally flush immediately."""
    global _state_dirty
    with _state_dirty_lock:
        _state_dirty = True
    if immediate:
        _save_state_now()
        with _state_dirty_lock:
            _state_dirty = False


def state_flusher():
    """Flush state periodically so journal/scanner updates do not rewrite JSON per event."""
    global _state_dirty
    while True:
        time.sleep(_STATE_FLUSH_INTERVAL)
        with _state_dirty_lock:
            dirty = _state_dirty
        if dirty and _save_state_now():
            with _state_dirty_lock:
                _state_dirty = False


def _github_configured():
    """لا يرسل شيئًا إلا إذا ضبط المستخدم التوكن والمستودع صراحةً."""
    return bool(GITHUB_SYNC_ENABLED and GITHUB_TOKEN and GITHUB_REPO and GITHUB_STATE_PATH)


def _github_headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": GITHUB_COMMIT_NAME or "Penny-Hunter-Robot",
    }


def _github_contents_url():
    from urllib.parse import quote
    repo = quote(GITHUB_REPO, safe="/")
    path = quote(GITHUB_STATE_PATH, safe="/")
    return f"{GITHUB_API_ROOT}/repos/{repo}/contents/{path}"


def _github_download_state():
    """يستعيد آخر نسخة فقط إذا لم توجد نسخة محلية على Volume."""
    if not _github_configured() or os.path.exists(STATE_FILE):
        return False
    try:
        response = requests.get(_github_contents_url(), headers=_github_headers(),
                                params={"ref": GITHUB_BRANCH}, timeout=20)
        if response.status_code == 404:
            logger.info("[GitHub] لا توجد نسخة احتياطية بعد؛ سيبدأ ملف Volume جديدًا")
            return False
        response.raise_for_status()
        payload = response.json()
        raw = base64.b64decode(payload["content"].replace("\n", ""))
        if GITHUB_STATE_PATH.endswith((".gz", ".gzip")):
            import gzip
            raw = gzip.decompress(raw)
        json.loads(raw.decode("utf-8"))  # تحقق قبل الكتابة
        os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
        tmp = STATE_FILE + ".github.tmp"
        with open(tmp, "wb") as f:
            f.write(raw)
        os.replace(tmp, STATE_FILE)
        logger.info("[GitHub] تم استعادة ذاكرة الروبوت إلى %s", STATE_FILE)
        return True
    except Exception as e:
        logger.warning("[GitHub] فشل الاستعادة (سيستمر البوت محليًا): %s", e)
        return False


def _github_upload_state(force=False):
    """يرفع snapshot مضغوطًا إلى GitHub عبر Contents API مع commit واحد."""
    global _GITHUB_LAST_UPLOAD_HASH
    if not _github_configured() or not os.path.exists(STATE_FILE):
        return False
    with _GITHUB_SYNC_LOCK:
        try:
            import gzip
            with open(STATE_FILE, "rb") as f:
                raw = f.read()
            # تحقق من JSON قبل نشره؛ لا نرفع ملفًا ناقصًا أو فاسدًا.
            json.loads(raw.decode("utf-8"))
            # ضغط ثابت؛ يمنع إنشاء commit جديد إذا لم تتغير الحالة.
            packed = gzip.compress(raw, compresslevel=6, mtime=0)
            digest = __import__("hashlib").sha256(packed).hexdigest()
            if not force and digest == _GITHUB_LAST_UPLOAD_HASH:
                return True
            encoded = base64.b64encode(packed).decode("ascii")
            current = requests.get(_github_contents_url(), headers=_github_headers(),
                                   params={"ref": GITHUB_BRANCH}, timeout=20)
            sha = current.json().get("sha") if current.status_code == 200 else None
            body = {
                "message": "chore: backup Penny Hunter robot state",
                "content": encoded,
                "branch": GITHUB_BRANCH,
                "committer": {"name": GITHUB_COMMIT_NAME, "email": GITHUB_COMMIT_EMAIL},
            }
            if sha:
                body["sha"] = sha
            response = requests.put(_github_contents_url(), headers=_github_headers(),
                                    json=body, timeout=30)
            response.raise_for_status()
            _GITHUB_LAST_UPLOAD_HASH = digest
            logger.info("[GitHub] تم حفظ ذاكرة الروبوت (%d KB مضغوطة)", len(packed) // 1024)
            return True
        except Exception as e:
            logger.warning("[GitHub] فشل رفع النسخة (سيستمر البوت محليًا): %s", e)
            return False


def github_state_sync_loop():
    """مزامنة دورية منخفضة التكرار؛ Volume يبقى المصدر الأساسي أثناء التشغيل."""
    if not _github_configured():
        logger.info("[GitHub] المزامنة معطّلة: اضبط GITHUB_TOKEN وGITHUB_REPO")
        return
    # أعطِ state_flusher فرصة لكتابة أول snapshot.
    time.sleep(min(30, GITHUB_SYNC_INTERVAL_SEC))
    while True:
        _github_upload_state()
        time.sleep(GITHUB_SYNC_INTERVAL_SEC)


def load_state():
    global state
    # عند فقدان Volume بعد إعادة إنشاء خدمة Railway، استرجع من GitHub أولًا.
    _github_download_state()
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


# ================= [FIX-9..11] الحزمة الثالثة: وقت السعودية · حماية الحظر · حداثة الأسعار =================
#  [FIX-9]  كل رسالة تنتهي بـ"وقت التنبيه" بتوقيت السعودية (UTC+3 ثابت — السعودية بدون توقيت صيفي، فلا نعتمد على tzdata).
#  [FIX-10] قاطع ياهو كان يشتغل فقط عند JSON فاسد؛ الـ429 كانت تُعاد 3 مرات لكل سهم بدون تبريد، وطلبات الشاشات
#           (كل ~45ث) ما لها أي قاطع أصلًا. الحين: 429 ⇒ قاطع أُسّي (2 دقيقة، 4، 8 … حتى 15 دقيقة) يوقف كل طلبات ياهو
#           (شاشات + شموع) بدل ما يضاعفها أثناء الحجب.
#  [FIX-11] حداثة الأسعار صارت قابلة للقياس: /status و/health يعرضان حالة بث ياهو المباشر وعمر آخر قراءة.

KSA_TZ = _timezone(timedelta(hours=3), "KSA")
ALERT_TIME_FOOTER_ENABLED = os.getenv("ALERT_TIME_FOOTER_ENABLED", "true").strip().lower() == "true"
ALERT_TIME_LABEL = "وقت التنبيه"
YAHOO_429_BASE_COOLDOWN = int(os.getenv("YAHOO_429_BASE_COOLDOWN", "120"))
YAHOO_429_MAX_COOLDOWN = int(os.getenv("YAHOO_429_MAX_COOLDOWN", "900"))
_yahoo_429_state = {"level": 0, "last_trip": 0.0, "trips": 0}


def _ksa_time_text(now_ts=None):
    """07:19:32 ص — 12 ساعة + ص/م + ثواني، بتوقيت السعودية (UTC+3)."""
    dt = datetime.fromtimestamp(float(now_ts) if now_ts else time.time(), KSA_TZ)
    return f"{dt.hour % 12 or 12:02d}:{dt.minute:02d}:{dt.second:02d} {'ص' if dt.hour < 12 else 'م'}"


def _ksa_hm(now_ts=None):
    """07:19 ص (بدون ثواني) للتقارير المختصرة."""
    dt = datetime.fromtimestamp(float(now_ts) if now_ts else time.time(), KSA_TZ)
    return f"{dt.hour % 12 or 12:02d}:{dt.minute:02d} {'ص' if dt.hour < 12 else 'م'}"


def _with_alert_time(message, limit=4000, now_ts=None):
    """
    [FIX-9] يضيف بنهاية الرسالة: «🕐 وقت التنبيه: 07:19:32 ص (توقيت السعودية)».
    لا يكرر لو موجود، ولا يكسر ماركداون تيليجرام (بدون _ * ` [). لو الرسالة قريبة من الحد (كابشن الصورة 1024)
    ينزل للصيغة المختصرة، ولو ما تكفي يتركها بدون تعديل بدل ما يفشل الإرسال.
    """
    if not ALERT_TIME_FOOTER_ENABLED or not isinstance(message, str) or not message.strip():
        return message
    if ALERT_TIME_LABEL in message:
        return message
    footer = f"\n\n🕐 {ALERT_TIME_LABEL}: {_ksa_time_text(now_ts)} (توقيت السعودية)"
    if limit and len(message) + len(footer) > limit:
        footer = f"\n🕐 {_ksa_time_text(now_ts)} KSA"
        if len(message) + len(footer) > limit:
            return message
    return message + footer


def _yahoo_cooldown_remaining(now_ts=None):
    """كم ثانية باقية من تبريد ياهو (0 = عادي)."""
    return max(0.0, YAHOO_OUTAGE_UNTIL - (float(now_ts) if now_ts else time.time()))


def _yahoo_trip_breaker(reason="429", now_ts=None):
    """
    [FIX-10] يفعّل نفس قاطع ياهو الموجود (YAHOO_OUTAGE_UNTIL) بتبريد أُسّي: 120ث، 240، 480 … حتى 900ث.
    المستوى ينزل لصفر بعد ساعة بدون حجب. يرجع مدة التبريد (ثواني).
    """
    global YAHOO_OUTAGE_UNTIL
    now = float(now_ts) if now_ts else time.time()
    with YAHOO_OUTAGE_LOCK:
        if now - _yahoo_429_state["last_trip"] > 3600:
            _yahoo_429_state["level"] = 0
        level = _yahoo_429_state["level"]
        cooldown = min(YAHOO_429_MAX_COOLDOWN, YAHOO_429_BASE_COOLDOWN * (2 ** level))
        _yahoo_429_state.update(level=min(level + 1, 6), last_trip=now, trips=_yahoo_429_state["trips"] + 1)
        YAHOO_OUTAGE_UNTIL = max(YAHOO_OUTAGE_UNTIL, now + cooldown)
    logger.warning(f"[YahooGuard] {reason} — ياهو رفض الطلبات: نوقف كل طلباته {cooldown:.0f}ث (المستوى {level + 1}) عشان ما يطول الحجب")
    return cooldown


def _data_freshness_snapshot(now_ts=None):
    """[FIX-11] لقطة حداثة البيانات (للأمر /status ولمسار /health)."""
    now = float(now_ts) if now_ts else time.time()
    with _live_prices_lock:
        ages = [max(0.0, now - _safe_float(v.get("ts"))) for v in _live_prices.values() if isinstance(v, dict)]
    fresh = sorted(a for a in ages if a <= YAHOO_STREAM_STALE_SEC)
    last_poll = _safe_float(_discovery_stats.get("last_poll"))
    with state_lock:
        hot_n = len(state.get("hot_watchlist", []))
    with _early_pool_lock:
        pool_n = len(_early_pool["rows"]) if now - _safe_float(_early_pool["ts"]) <= EARLY_POOL_MAX_AGE_SEC else 0
    return {
        "hot_list": hot_n,
        "early_pool": pool_n,
        "stream_enabled": bool(YAHOO_STREAM_ENABLED),
        "stream_symbols": len(ages),
        "stream_fresh": len(fresh),
        "stream_last_tick_age_sec": int(min(ages)) if ages else None,
        "stream_median_age_sec": int(fresh[len(fresh) // 2]) if fresh else None,
        "discovery_last_poll_age_sec": int(now - last_poll) if last_poll > 0 else None,
        "finnhub": bool(FINNHUB_ENABLED and _finnhub_client is not None),
        "yahoo_cooldown_sec": int(_yahoo_cooldown_remaining(now)),
        "yahoo_breaker_trips": _yahoo_429_state["trips"],
    }


def _freshness_text(now_ts=None):
    """[FIX-11] نص عربي قصير لحداثة الأسعار يُلحق بـ/status."""
    snap = _data_freshness_snapshot(now_ts)
    lines = ["📡 *حداثة الأسعار*"]
    if not snap["stream_enabled"]:
        lines.append("• بث ياهو المباشر: معطّل")
    elif snap["stream_fresh"] > 0:
        lines.append(f"• بث ياهو المباشر: {snap['stream_fresh']} سهم حي (آخر نبضة قبل {snap['stream_last_tick_age_sec']}ث، "
                     f"الوسيط {snap['stream_median_age_sec']}ث)")
    elif snap["stream_symbols"] > 0:
        lines.append(f"• بث ياهو المباشر: ⚠️ متوقف (آخر نبضة قبل {snap['stream_last_tick_age_sec']}ث)")
    else:
        lines.append("• بث ياهو المباشر: ⚠️ ما وصلت أي نبضة (غير متصل، أو السوق مغلق)")
    if snap["discovery_last_poll_age_sec"] is not None:
        lines.append(f"• قراءة شاشات ياهو: قبل {snap['discovery_last_poll_age_sec']}ث (كل ~{DISCOVERY_INTERVAL_SEC}ث)")
    if snap["hot_list"] or snap["early_pool"]:
        lines.append(f"• نطاق المراقبة: {snap['early_pool']} مرشح بآخر دورة | القائمة الساخنة {snap['hot_list']} من {HOT_MAX_ITEMS} (EARLY يفحص الاثنين)")
    lines.append(f"• Finnhub: {'مفعّل' if snap['finnhub'] else 'غير مفعّل'}")
    if snap["yahoo_cooldown_sec"] > 0:
        lines.append(f"• حماية ياهو: ⛔ تبريد {snap['yahoo_cooldown_sec']}ث بعد رفض الطلبات (429)")
    else:
        lines.append("• حماية ياهو: عادي (بدون حجب)")
    return "\n".join(lines)


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
    message = _with_alert_time(message, limit=(1000 if photo else 4000))     # [FIX-9] وقت التنبيه بتوقيت السعودية بنهاية كل رسالة
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


# ================================================================
# 🚀 POWER RUNNER SCANNER — مصمم لصيد الأسهم المتفجرة مثل HTCO
# ================================================================


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
            "entry_time_saudi": saudi_time_str(),
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
        f"⏰ وقت الدخول: *{saudi_time_str()} 🇸🇦*\n"
        f"💰 Entry: ${price:.2f}\n"
        f"🎯 TP: ${tp:.2f} (+{target_move_pct:.0f}%) — هدف مستهدف\n"
        f"🛑 SL: ${sl:.2f}\n"
        f"📤 البيع: عند وصول الهدف أو الوقف فقط؛ وقت البيع لا يمكن تحديده مسبقًا\n"
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
    exit_saudi = saudi_time_str()
    entry_saudi = trade.get("entry_time_saudi", "غير مسجل")
    send_telegram(
        f"{emoji} *CLOSED: {symbol}*\n"
        f"📝 السبب: {reason}\n"
        f"⏰ الدخول: {entry_saudi} 🇸🇦\n"
        f"⏰ البيع/الخروج: {exit_saudi} 🇸🇦\n"
        f"💵 Exit: ${price:.2f}\n"
        f"📈 PnL: ${pnl:+.2f} ({pnl_pct:+.2f}%)"
    )

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
        
        # لا يوجد فلتر SMA200 عمداً: نبي أسهم البيني المتفجرة حتى لو كانت تحت المتوسط الطويل.
        is_accumulating, acc_score = detect_accumulation(df)
        is_pre_breakout = detect_pre_breakout(df)

        # 🕵️ فحص التجميع الصامت (Silent Accumulation)
        is_silent_acc, acc_ratio = detect_silent_accumulation(df)
        
        # 🔥 فحص الانفجار - إذا ما في انفجار ولا تجميع صامت نوقف هنا
        explosion, explosion_score = detect_explosion(df)
        
        # لا بوابة صارمة على الانفجار/التجميع الصامت عمداً: نفحص كل الأسهم ونترك التسجيل (score) يقرر.

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
        logger.exception("process_symbol failed for %s: %s", symbol, e)


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
    
    # حلقة إعادة تشغيل دائمة: تيليجرام أحياناً يرجّع 429 / 502 / Read timed out بشكل مؤقت.
    # سابقاً كان bot.polling() يفشل مرة وحدة، الاستثناء يتسجّل، والدالة ترجع — فيموت خيط
    # البوت نهائياً بصمت (بدون أي كراش يظهر بالسجلات) بينما بقية التطبيق (Flask + السكانرز)
    # يستمر شغّال طبيعي. هذه الحلقة تعيد تشغيل الـ polling تلقائياً بدل ما يموت.
    error_streak = 0
    while True:
        try:
            # polling بسيط بدون threading معقد — يمنع تعارض النسخ
            bot.polling(non_stop=True, interval=1, timeout=20)
            # لو رجعت polling() بدون استثناء (توقف طبيعي)، نعتبرها حالة غير متوقعة ونعيد المحاولة
            logger.warning("⚠️ Telegram polling stopped unexpectedly (no exception) — restarting...")
            error_streak = 0
            time.sleep(2)
        except Exception as e:
            error_streak += 1
            wait = min(60, 5 * error_streak)
            logger.error(f"Telebot polling error: {e} — restarting in {wait}s (streak={error_streak})")
            try:
                bot.remove_webhook()
            except Exception:
                pass
            time.sleep(wait)


if bot:
    @bot.message_handler(commands=['start', 'help'])
    def cmd_start(message):
        logger.info(f"📨 Received /start from {message.chat.id}")
        if not ensure_authorized(message): return
        phase = get_market_phase()
        msg = f"""👋 Trading Bot v33!
📊 {len(state.get('tickers', []))} stocks
🕐 {PHASE_SETTINGS[phase]['description']}

📋 *الأوامر المتاحة:*

📊 *الحالة والتقارير*
/status - الأداء العام
/positions - الصفقات المفتوحة
/recommend SYMBOL - توصية Paper موحدة
/recommendations - سجل التوصيات
/signals - آخر الإشارات

🔎 *التحليل والماسحات*
/scan SYMBOL - تحليل سهم
/sr SYMBOL - دعم ومقاومة
/news SYMBOL - أخبار سهم
/top - أفضل المرشحين
/movers - أسباب ارتفاع 100%+
/gainers - أكبر الرابحين
/losers - أكبر الخاسرين
/vwap SYMBOL - تحليل VWAP
/candle SYMBOL - تحليل الشموع
/adv SYMBOL - تحليل EMA/Fibonacci متقدم
/fundamental SYMBOL - تحليل أساسي
/deep SYMBOL - تحليل شامل (AI)

🎯 *ESE - Explosive Setup*
/setups - المرشحين النشطين الحين (ARMED/BROKE)
/esestats - إحصائيات أداء ESE
/replay SYMBOL - إعادة تشغيل نمط اليوم (مثال: IMCC)

👀 *قائمة المراقبة والتنبيهات*
/gap - فجوات اليوم (≥20% عن إغلاق أمس)
/journal - قياس أداء الإشارات بعد 15/30/60/180 دقيقة
/acc - أسهم تحت مراقبة التجميع حالياً
/pump - بمبات ≥50% تحت متابعة خروج
/early - إشارات EARLY (بداية الحركة قبل الانفجار) وأداؤها بعد الإشارة
/swing - مراكز السوينق الأسبوعية الحالية
🧠 *الروبوت — تعلّم وتطوّر ذاتي*
/brain - عقل الروبوت: الجيل الحالي، أداء كل محرك، أهدافه، وآخر ما تعلّمه
/genome - الإعدادات التي عدّلها الروبوت بنفسه مقابل الأصل
/trust - ثقة الروبوت بكل محرك (من نتائج حقيقية)
/goals - أهدافه وخطته للوصول لها
/lessons - سجل دروسه (كتم/إحياء/إصلاح/تطوّر)
/evolve - خطوة تطوّر يدوية الآن
/selfcheck - تشخيص الروبوت لنفسه (خيوط، بيانات، صحة)
/roboton · /robotoff - تشغيل/إطفاء التعلّم الذاتي
/offline - تدريب الروبوت والسوق مقفل (تقدّم الأسهم، نتائج الأساس مقابل البطل)
/offlinetrades - سجل الصفقات الوهمية (شراء/هدف/وقف/بيع/ربح) + ملف CSV. للتخصيص: /offlinetrades 25 6 (هدف ووقف)
/offlineon · /offlineoff - تشغيل/إطفاء التدريب الأوفلاين
/picks - أقوى الأسهم الحين بنمط الدعم/الانضغاط (قائمة الاثنين)
/patterns - ما تعلّمه الروبوت عن أنماط ما قبل الانفجار (دقة بالتحقق + أنماط مكتشفة)
/patternscore SYMBOL - يقيّم سهماً (أو عدة أسهم) بنماذج الأنماط الآن
/watchlist - عرض القائمة
/add SYMBOL - إضافة سهم
/remove SYMBOL - حذف سهم
/alert SYMBOL PRICE - تنبيه سعري
/alerts - عرض التنبيهات
/delalert SYMBOL - حذف تنبيه

🛡️ *Paper Trading*
/b SYMBOL QUANTITY - شراء Paper
/s SYMBOL - بيع Paper
/close SYMBOL - إغلاق Paper
/halt - الأسهم الموقوفة
/clearhalt SYMBOL - إزالة إيقاف قديم

🚀 *تنبيهات تلقائية (بدون أمر منك)*
EARLY (بداية الحركة قبل الانفجار)، Ranking+AI، AUTO HUNTER المُثرى (سكور 80+)، Mega Mover، Gap Hunter (فجوة ≥20%)، دوجي بعد هبوط، اختراق قمة 20 يوم، تجميع مبكر (Accumulation)، Pump & Dump Hunter، Swing أسبوعي (كل 6 ساعات)، كاشف شذوذ إحصائي تجريبي (Isolation Forest)، وشراء داخليين (SEC Form 4 عبر OpenInsider) — وأخبار Finnhub (AI الآن لأحدث خبر) مرفقة تلقائيًا بأي تنبيه كبير. الأسعار الحين تجي من بث ياهو المباشر أول (WebSocket لحظي) قبل Finnhub وYahoo العادي. وكل تنبيه ينتهي بوقته بتوقيت السعودية.

⚠️ كل العمليات Paper Trading ولا تعتبر توصية مالية"""
        send_telegram(msg)

    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        if not ensure_authorized(message): return
        with state_lock:
            perf = state["performance"]
            total = perf["wins"] + perf["losses"]
            wr = (perf["wins"] / total * 100) if total > 0 else 0
            msg = f"📊 *Status*\n✅ Wins: {perf['wins']}\n❌ Losses: {perf['losses']}\n📈 WR: {wr:.1f}%\n💵 PnL: ${perf['total_pnl']:+.2f}\n📦 Open: {len(state['open_trades'])}\n🌐 Universe: {len(state['tickers'])}"
        send_telegram(msg + "\n\n" + _freshness_text())         # [FIX-11] حداثة الأسعار + حالة بث ياهو + حماية الحظر

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
        handle_manual_sell(message, cmd_label="/s")

    def handle_manual_sell(message, cmd_label="/s"):
        try:
            args = message.text.split()
            if len(args) != 2:
                send_telegram(f"⚠️ الاستخدام الصحيح:\n`{cmd_label} SYMBOL`\nمثال: `{cmd_label} SMX`")
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
            close_trade(symbol, exit_price, f"Manual Sell {cmd_label}")
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
        result = final_trade_recommendation(symbol, send_alert=True)
        if isinstance(result, dict) and result.get("final_decision") not in ("BUY", "WATCH", "AVOID"):
            # [FIX-4] كانت NO_TRADE (بما فيها "سهم ممتد") تنتهي بصمت — الحين تعرف السبب
            why = "؛ ".join(str(x) for x in (result.get("invalidations") or [])[:3]) or result.get("reason_ar") or "الشروط غير متحققة"
            send_telegram(f"⛔ *{symbol}*: لا توصية دخول الحين — {why}")

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
        # بطلب المستخدم 2026-09-24: يبقى Paper بالكامل — هو يشتري السهم الحقيقي بنفسه،
        # البوت إشارات بس. نفس منطق /b تمامًا (بدون أي تنفيذ حقيقي أو وسيط).
        if not ensure_authorized(message): return
        threading.Thread(target=handle_manual_buy, args=(message,), daemon=True).start()

    @bot.message_handler(commands=['realsell'])
    def cmd_realsell(message):
        # نفس /s تمامًا — Paper بس.
        if not ensure_authorized(message): return
        handle_manual_sell(message, cmd_label="/realsell")

    @bot.message_handler(commands=['gap', 'gaps'])
    def cmd_gap(message):
        if not ensure_authorized(message): return
        with state_lock:
            hot = [dict(x) for x in state.get("hot_watchlist", []) if isinstance(x, dict)]
        gappers = [c for c in hot if _safe_float(c.get("change")) >= GAP_HUNTER_MIN_CHANGE]
        if not gappers:
            send_telegram(f"📭 لا توجد فجوات ≥{GAP_HUNTER_MIN_CHANGE:.0f}% حالياً. Gap Hunter يراقب تلقائياً ويرسل أول ما توصل فجوة.")
            return
        gappers.sort(key=lambda c: _safe_float(c.get("change")), reverse=True)
        msg = f"🕳️ *فجوات اليوم (≥{GAP_HUNTER_MIN_CHANGE:.0f}% عن إغلاق أمس):*\n━━━━━━━━━━━━━━━━\n"
        for c in gappers[:15]:
            change = _safe_float(c.get("change"))
            tag = "🔥🔥" if change >= GAP_HUNTER_STRONG_CHANGE else "🔥"
            msg += f"{tag} *{c.get('symbol')}* — ${_safe_float(c.get('price')):.2f} (+{change:.1f}%) | حجم {int(_safe_float(c.get('volume'))):,}\n"
        send_telegram(msg)

    @bot.message_handler(commands=['journal', 'stats'])
    def cmd_journal(message):
        if not ensure_authorized(message): return
        with state_lock:
            journal = [dict(e) for e in state.get("signal_journal", [])]
        if not journal:
            send_telegram("📭 سجل الإشارات فاضي لسه — أي تنبيه مؤهل يُسجّل ويُقاس بعد 15/30/60/180 دقيقة.")
            return
        parts = []
        for hz, label in (("30", "بعد 30 دقيقة"), ("60", "بعد ساعة"), ("180", "بعد 3 ساعات")):
            txt = _journal_summary_text(journal, horizon=hz)
            if txt:
                parts.append(f"⏱️ *{label}:*\n{txt}")
        done = sum(1 for e in journal if e.get("done"))
        head = f"📒 *سجل الإشارات ({len(journal)} إشارة، {done} مكتملة القياس)*\n━━━━━━━━━━━━━━━━\n"
        body = "\n\n".join(parts) if parts else "لسه ما اكتملت أي مرحلة قياس (أول مرحلة بعد 15 دقيقة من التنبيه)."
        foot = ("\n\nإيجابي = نسبة الإشارات التي كان سعرها فوق سعر التنبيه عند المرحلة. "
                "أعلى ربح/أسوأ هبوط = متوسط أعلى وأدنى سعر بعد التنبيه.")
        send_telegram(head + body + foot)

    # ═══════════════ 🧠 أوامر الروبوت (SLE — التعلّم والتطوّر الذاتي) ═══════════════
    @bot.message_handler(commands=['brain', 'learn', 'robot'])
    def cmd_brain(message):
        if not ensure_authorized(message): return
        send_telegram(sle_brain_report())

    @bot.message_handler(commands=['genome'])
    def cmd_genome(message):
        if not ensure_authorized(message): return
        send_telegram(sle_genome_report())

    @bot.message_handler(commands=['lessons'])
    def cmd_lessons(message):
        if not ensure_authorized(message): return
        send_telegram(sle_lessons_report())

    @bot.message_handler(commands=['selfcheck'])
    def cmd_selfcheck(message):
        if not ensure_authorized(message): return
        send_telegram(sle_selfcheck_report())

    @bot.message_handler(commands=['goals'])
    def cmd_goals(message):
        if not ensure_authorized(message): return
        send_telegram(sle_goals_report())

    @bot.message_handler(commands=['trust'])
    def cmd_trust(message):
        if not ensure_authorized(message): return
        send_telegram(sle_trust_report())

    @bot.message_handler(commands=['evolve'])
    def cmd_evolve(message):
        if not ensure_authorized(message): return
        send_telegram("🧬 جاري تشغيل خطوة تطوّر يدوية… النتيجة توصلك بعد لحظات.")

        def _run_evolve():
            result = sle_evolve(force=True, reason="طلب يدوي من Telegram") or {}
            send_telegram(result.get("summary") or f"⚠️ ما قدرت أطوّر الحين: {result.get('reason', 'غير معروف')}")
        threading.Thread(target=_run_evolve, daemon=True, name="SLE_ManualEvolve").start()

    @bot.message_handler(commands=['roboton', 'robotoff'])
    def cmd_robot_toggle(message):
        if not ensure_authorized(message): return
        enable = str(message.text or "").strip().lower().endswith("on")
        state_now = sle_runtime_toggle(enable)
        send_telegram(f"🧠 التعلّم والتطوّر الذاتي: *{'مفعّل' if state_now else 'مطفّى'}*")

    @bot.message_handler(commands=['offline'])
    def cmd_offline(message):
        if not ensure_authorized(message): return
        send_telegram(_offline_status_text())

    @bot.message_handler(commands=['offlinetrades'])
    def cmd_offline_trades(message):
        if not ensure_authorized(message): return
        # الاستخدام: /offlinetrades  أو  /offlinetrades 25 6  (هدف% ووقف%)
        tp = sl = None
        try:
            parts = str(message.text or "").split()[1:]
            if len(parts) >= 1: tp = min(1000.0, max(0.5, float(parts[0])))
            if len(parts) >= 2: sl = min(50.0, max(0.5, float(parts[1])))
        except Exception:
            tp = sl = None
        send_telegram("📒 جاري بناء سجل الصفقات الوهمية… يوصلك بعد لحظات.")
        threading.Thread(target=_offline_trades_send, args=(tp, sl), daemon=True, name="OfflineTrades").start()

    @bot.message_handler(commands=['offlineon', 'offlineoff'])
    def cmd_offline_toggle(message):
        global OFFLINE_ENABLED
        if not ensure_authorized(message): return
        OFFLINE_ENABLED = str(message.text or "").strip().lower().split("@")[0].endswith("on")
        send_telegram(f"🌙 التدريب الأوفلاين (والسوق مقفل): *{'مفعّل' if OFFLINE_ENABLED else 'مطفّى'}*")

    @bot.message_handler(commands=['patterns'])
    def cmd_patterns(message):
        if not ensure_authorized(message): return
        for _txt in _pl_all_reports():
            send_telegram(_txt)

    @bot.message_handler(commands=['picks'])
    def cmd_picks(message):
        if not ensure_authorized(message): return
        send_telegram(_dl_setups_text(10))

    @bot.message_handler(commands=['patternscore'])
    def cmd_patternscore(message):
        if not ensure_authorized(message): return
        syms = [s.upper() for s in str(message.text or "").split()[1:6] if s.strip().isalpha()]
        if not syms:
            send_telegram("الاستخدام: /patternscore ABCD  أو  /patternscore ABCD EFGH")
            return
        send_telegram("🧬 جاري تقييم النمط الحالي… يوصلك بعد لحظات.")
        threading.Thread(target=_pl_score_send, args=(syms,), daemon=True, name="PatternScore").start()

    @bot.message_handler(commands=['acc', 'accumulation'])
    def cmd_accumulation(message):
        if not ensure_authorized(message): return
        with state_lock:
            watch = dict(state.get("accumulation_watch", {}))
        active = {s: e for s, e in watch.items() if isinstance(e, dict) and not e.get("confirmed")}
        if not active:
            send_telegram("📭 لا يوجد تجميع مرصود حالياً. Accumulation Tracker يراقب تلقائياً ويرسل أول ما يلقى شي.")
            return
        rows = sorted(active.items(), key=lambda kv: _safe_float(kv[1].get("armed_ts")), reverse=True)
        msg = "🐋 *أسهم تحت المراقبة (تجميع مرصود):*\n━━━━━━━━━━━━━━━━\n"
        for symbol, entry in rows[:15]:
            hrs = (time.time() - _safe_float(entry.get("armed_ts"))) / 3600.0
            msg += f"• *{symbol}* — رُصد عند ${_safe_float(entry.get('armed_price')):.4f} (قبل {hrs:.1f} ساعة) | إشارات: {entry.get('signals', 0)}/3\n"
        send_telegram(msg)

    @bot.message_handler(commands=['pump'])
    def cmd_pump(message):
        if not ensure_authorized(message): return
        with state_lock:
            watch = dict(state.get("pump_dump_watch", {}))
        active = {s: e for s, e in watch.items() if isinstance(e, dict) and _pd_stage(e) == "PUMP_ACTIVE"}
        if not active:
            send_telegram(f"📭 لا توجد بمبات ≥{PUMP_DUMP_MIN_PUMP_PCT:.0f}% متابَعة حالياً. Pump & Dump Hunter يراقب تلقائياً.")
            return
        rows = sorted(active.items(), key=lambda kv: _safe_float(kv[1].get("armed_ts")), reverse=True)
        msg = f"💊 *بمبات تحت المتابعة (بحثًا عن دامب):*\n━━━━━━━━━━━━━━━━\n"
        for symbol, entry in rows[:15]:
            hrs = (time.time() - _safe_float(entry.get("armed_ts"))) / 3600.0
            msg += f"• *{symbol}* — قمة ${_safe_float(entry.get('peak_price')):.4f} (قبل {hrs:.1f} ساعة)\n"
        send_telegram(msg)

    @bot.message_handler(commands=['early'])
    def cmd_early(message):
        if not ensure_authorized(message): return
        with state_lock:
            alerts = [dict(a) for a in state.get("early_alerts", []) if isinstance(a, dict)]
        send_telegram(_early_report_text(_early_track_update(alerts, time.time()), time.time()))

    @bot.message_handler(commands=['swing'])
    def cmd_swing(message):
        if not ensure_authorized(message): return
        with state_lock:
            watch = dict(state.get("swing_watch", {}))
        if not watch:
            send_telegram("📭 لا توجد مراكز سوينق مختارة حالياً. Weekly Swing يفحص كل 6 ساعات ويختار أفضل المرشحين تلقائيًا.")
            return
        rows = sorted(watch.items(), key=lambda kv: _safe_float(kv[1].get("picked_ts")), reverse=True)
        msg = "📅 *مراكز السوينق الأسبوعية الحالية:*\n━━━━━━━━━━━━━━━━\n"
        for symbol, entry in rows[:15]:
            days = (time.time() - _safe_float(entry.get("picked_ts"))) / 86400.0
            msg += (f"• *{symbol}* — دخول ${_safe_float(entry.get('entry_price')):.4f} | "
                    f"هدف ${_safe_float(entry.get('target')):.4f} | وقف ${_safe_float(entry.get('stop')):.4f} "
                    f"| عمره {days:.1f} يوم\n")
        send_telegram(msg)


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
    gemini_norm, _gemini_bearish = _normalize_ai_decision(gemini_decision)   # [FIX-1] SELL/SHORT = رفض (البوت long-only)
    gemini_confidence = max(0, min(int((gemini or {}).get('confidence', 50) or 50), 100))
    if gemini_norm == 'AVOID' and gemini:
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
    final_score += (gemini_confidence if gemini_norm == 'BUY' else gemini_confidence * 0.5) * 0.25
    final_score += (80 if mistral_pass else 20) * 0.20
    if openrouter:
        decision = _normalize_ai_decision(openrouter.get('decision', 'WATCH'))[0]
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


def _finnhub_local_score(headline, summary, use_ai=False):
    """
    كانت تعتمد بس على قائمة كلمات مفتاحية صغيرة (12 كلمة) → سكور شبه ثنائي
    (0.55 دائمًا إلا لو طابق كلمة حرفية من القائمة → 0.75)، وما تستخدم
    ai_analyze_news الموجودة بالكود أصلًا (تحليل AI فعلي لمعنى الخبر، مو
    مطابقة نص). خبر مثل "SYMBOL secures new distribution deal" ما يطابق
    ولا كلمة بالقائمة فيرجع 0.55 المحايدة ويترفض رغم وضوح إيجابيته — هذا
    سبب رئيسي وراء "ما توصل أخبار إيجابية حقيقية".
    use_ai=True (تُستدعى مرة وحدة بس لأحدث خبر لكل سهم — راجع
    get_finnhub_news_safe) يفعّل ai_analyze_news (Groq) فوق الفلتر الرخيص،
    بنفس سقف can_send_ai_request المشترك مع قرارات الصفقات — ما يزيد الحمل
    ولا يتجاوز أي حد، يتراجع تلقائيًا للكلمات المفتاحية لو الحصة مشغولة.
    """
    text = f'{headline} {summary}'.lower()
    impact = min(50, sum(points for word, points in _FINNHUB_IMPACT_KEYWORDS.items() if word in text))
    negative = any(word in text for word in _FINNHUB_NEGATIVE_KEYWORDS)
    basic = analyze_news_sentiment_basic(headline, summary) if 'analyze_news_sentiment_basic' in globals() else {}
    sentiment = 0.75 if impact >= 10 else 0.55
    if basic.get('sentiment') == 'positive':
        sentiment = max(sentiment, 0.70)
    if negative or basic.get('sentiment') == 'negative':
        sentiment = min(sentiment, 0.25)
    if use_ai and not negative:
        try:
            ai_result = ai_analyze_news(headline, summary) or {}
        except Exception as error:
            logger.debug(f"[NewsAI] {error}")
            ai_result = {}
        ai_sentiment = str(ai_result.get('sentiment', '')).lower()
        if ai_sentiment == 'negative' or ai_result.get('dilution_risk') or ai_result.get('offering_risk'):
            negative = True
            sentiment = min(sentiment, 0.20)
        elif ai_sentiment == 'positive':
            ai_impact = max(impact, _safe_float(ai_result.get('impact_score', 0)))
            graded = 0.65 + min(0.30, ai_impact / 100.0 * 0.30)
            if ai_result.get('is_catalyst'):
                graded = min(0.97, graded + 0.05)
            if ai_result.get('likely_priced_in'):
                graded -= 0.10
            sentiment = max(sentiment, graded)
            impact = int(max(impact, ai_impact))
    return round(max(0.0, min(1.0, sentiment)), 3), impact, negative


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
        for idx, item in enumerate(raw_news[:FINNHUB_MAX_NEWS_PER_SYMBOL]):
            headline = str(item.get('headline', '')).strip()
            summary = str(item.get('summary', '')).strip()
            if not headline:
                continue
            # AI (Groq) بس لأحدث خبر (idx==0) — ما تنحشى الحصة المشتركة بقرارات الصفقات
            sentiment, impact, negative = _finnhub_local_score(headline, summary, use_ai=(idx == 0))
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


def _translate_headline_ar(text):
    """
    ترجمة سريعة لعنوان الخبر لعربي طبيعي — نفس عميل Groq المستخدم بكل مكان
    بالبوت (ai_analyze_news مثلاً)، فما يحتاج مفتاح أو حساب جديد. فشل الترجمة
    (أو استنفاد حصة can_send_ai_request المشتركة) يرجّع النص الإنجليزي
    الأصلي بدل ما يوقف التنبيه بالكامل.
    """
    text = str(text or "").strip()
    if not text:
        return text
    if not OPENAI_AVAILABLE or client is None or not can_send_ai_request():
        return text
    try:
        response = groq_chat_completion(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "Translate the given financial news headline into natural, concise Arabic. Reply with ONLY the Arabic translation, no quotes, no English, no extra commentary."},
                {"role": "user", "content": text[:250]},
            ],
            temperature=0.2,
            max_tokens=120,
            timeout=6,
        )
        translated = str(response.choices[0].message.content or "").strip().strip('"').strip()
        return translated if translated else text
    except Exception as error:
        logger.debug(f"[Translate] {error}")
        return text


def get_news_context_line(symbol):
    """
    سطر خبر مختصر يُضاف لأي تنبيه قوي (Ranking المُثرى / Mega Mover / Doji) —
    'تأكد من الأخبار' يصير خطوة تلقائية بكل تنبيه كبير، مو ماسح RSS منفصل بس.
    يرجع None لو ما فيه خبر إيجابي حديث (حتى ما تنحشى الرسالة بسطر فاضي).
    العنوان يترجم للعربي تلقائيًا (كان يوصل بالإنجليزي زي ما Finnhub يرجعه).
    """
    try:
        items = get_finnhub_news_safe(symbol)
    except Exception as error:
        logger.debug(f"[NewsContext] {symbol}: {error}")
        items = []
    if not items:
        return None
    best = max(items, key=lambda item: (item.get("impact_score", 0), item.get("sentiment", 0)))
    if best.get("sentiment", 0) < 0.65:
        return None
    headline_ar = _translate_headline_ar(str(best.get("headline", "")))[:110]
    return f"📰 خبر مؤكد: {headline_ar} ({best.get('sentiment', 0) * 100:.0f}% إيجابي)"


GAP_HUNTER_MAX_PRICE_AGE_SEC = int(os.getenv("GAP_HUNTER_MAX_PRICE_AGE_SEC", "120"))
GAP_HUNTER_PRICE_MISMATCH_PCT = float(os.getenv("GAP_HUNTER_PRICE_MISMATCH_PCT", "1.5"))
_last_price_source = {}


def _gap_live_quote(symbol, candidate, now_ts=None):
    """
    يرجع (price, source, age_sec) ويمنع الخلط بين سعر قديم ونسبة حديثة.
    الأولوية للبث الحي، لكن إذا تعارض البث مع مرشح hot_watchlist حديث بأكثر من
    GAP_HUNTER_PRICE_MISMATCH_PCT نستخدم المرشح الحديث؛ وإذا لم يوجد سعر حديث
    نرجع 0 حتى لا يرسل GAP Hunter سعراً ثابتاً مضللاً.
    """
    now_ts = now_ts or time.time()
    symbol = str(symbol or '').upper().strip()
    cand_price = _safe_float(candidate.get('price')) if isinstance(candidate, dict) else 0.0
    cand_ts = _safe_float(candidate.get('last_seen', candidate.get('timestamp', 0))) if isinstance(candidate, dict) else 0.0
    cand_age = now_ts - cand_ts if cand_ts > 0 else 999999.0
    live_price = _live_price(symbol)
    live_age = 999999.0
    with _live_prices_lock:
        live_entry = dict(_live_prices.get(symbol) or {})
    if live_entry:
        live_age = max(0.0, now_ts - _safe_float(live_entry.get('ts')))
    if live_price and live_price > 0 and live_age <= YAHOO_STREAM_STALE_SEC:
        if cand_price > 0 and cand_age <= GAP_HUNTER_MAX_PRICE_AGE_SEC:
            mismatch = abs(live_price / cand_price - 1.0) * 100.0
            if mismatch >= GAP_HUNTER_PRICE_MISMATCH_PCT:
                logger.warning(f"[GapHunter] {symbol}: live/provider mismatch {live_price:.4f} vs hot {cand_price:.4f} ({mismatch:.1f}%) — using hot quote")
                _last_price_source[symbol] = ('hot_watchlist', cand_ts)
                return cand_price, 'hot_watchlist', cand_age
        _last_price_source[symbol] = ('Yahoo WebSocket', now_ts - live_age)
        return float(live_price), 'Yahoo WebSocket', live_age
    if cand_price > 0 and cand_age <= GAP_HUNTER_MAX_PRICE_AGE_SEC:
        _last_price_source[symbol] = ('hot_watchlist', cand_ts)
        return cand_price, 'hot_watchlist', cand_age
    # Deliberately do not fall back to a cached 5m candle for GAP Hunter.
    logger.info(f"[GapHunter] {symbol}: no fresh quote (stream_age={live_age:.0f}s hot_age={cand_age:.0f}s), skipped")
    return 0.0, 'stale/none', min(live_age, cand_age)


def _finnhub_current_price(symbol):
    """
    كانت تقرأ Yahoo فقط رغم اسمها (نفس تأخير كل مكان ثاني). الحين ترتيب
    السرعة: (1) بث ياهو المباشر (WebSocket، لحظي فعليًا لو السهم مشترك فيه)
    → (2) Finnhub الحقيقي (مجاني، شبه لحظي) → (3) Yahoo المؤجّل كحل أخير.
    """
    live = _live_price(symbol)
    if live and live > 0:
        return live
    if FINNHUB_ENABLED and _finnhub_client is not None:
        try:
            q = _finnhub_client.quote(symbol)
            c = _safe_float(q.get("c")) if q else 0.0
            if c > 0:
                return c
        except Exception as error:
            logger.debug(f'[Finnhub price] {symbol}: {error}')
    try:
        df = cached_download(symbol, period='1d', interval='5m')
        if df is not None and not df.empty:
            return float(df['close'].iloc[-1])
    except Exception as error:
        logger.debug(f'[Finnhub price fallback] {symbol}: {error}')
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
        state["seen_catalyst"] = {k: v for k, v in state.get("seen_catalyst", {}).items() if isinstance(v, (int, float)) and now - v < expiry}
        
        # تنظيف ملفات SEC
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
        days_map = {"1d": 1, "2d": 2, "5d": 5, "10d": 10, "1mo": 30, "55d": 55, "60d": 60, "3mo": 90, "90d": 90, "120d": 120, "6mo": 180, "1y": 365, "2y": 730, "3y": 1095, "5y": 1825}  # كانت 60d/90d/120d تنجلب كـ 5 أيام فقط!
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
                    if response.status_code == 429:
                        _yahoo_trip_breaker(f"chart HTTP 429 ({symbol})")             # [FIX-10] كانت تُعاد 3 مرات لكل سهم أثناء الحجب
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


# ════════════════════════════════════════════════════════════
#          PENNY STOCK POWER SCANNER — صياد البيني ستوك
# ════════════════════════════════════════════════════════════
# السعر: $0.10 - $5.00
# يشتغل كل 10 دقائق طوال وقت التداول الرسمي
# شروط: +15% يومي + حجم 3x + زخم صاعد + سيولة كافية


# ================================================================
# 🌐 FULL MARKET SWEEP — يمسح كل 6700+ سهم دفعة بدفعة (50 سهم × طلب)
#    يصطاد ZTG / FAP / MTEN / CCTG / INHD وكل الأسهم المجهولة اللي تنفجر
#    الميزة: لا يعتمد على الحجم التاريخي — يفحص الكل كل 3 دقائق
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

_heartbeats = {}
_price_hist = {}
_price_hist_lock = threading.Lock()
_discovery_stats = {"last_poll": 0.0, "last_count": 0, "errors": 0, "last_ok": False}
DISCOVERY_EXTRA_PROVIDERS = []

# ================================================================
# 🌐 مصدر اكتشاف إضافي: شاشة Nasdaq العامة المجانية (بدون مفتاح/حساب) — فكرة
# مأخوذة من مشروع مفتوح المصدر (penny-scout). النقطة المهمة: شاشات ياهو
# الثلاث (day_gainers/most_actives/small_cap_gainers) تغطي فقط اللي ياهو نفسه
# يصنّفه ضمن هذي الفئات المحددة — سهم زي MSGY ممكن ما يظهر بأي وحدة منها
# مبكرًا. هذا المصدر يجيب كل الأسهم المدرجة بـNASDAQ/NYSE/AMEX (3 طلبات فقط،
# نداء واحد لكل بورصة) ونطبق فلترتنا احنا (نفس عتبات DISCOVERY الموجودة) بدل
# ما نعتمد على تصنيف ياهو المسبق — تغطية أوسع بكثير بنفس المجانية.
# مخزّن مؤقتًا NASDAQ_UNIVERSE_INTERVAL (افتراضي 5 دقايق) لأن الاستجابة كبيرة
# (آلاف الأسطر) ومن الأدب عدم قصف Nasdaq كل دورة اكتشاف (~45 ثانية).
# ================================================================
NASDAQ_UNIVERSE_ENABLED = os.getenv("NASDAQ_UNIVERSE_ENABLED", "true").strip().lower() == "true"
NASDAQ_UNIVERSE_INTERVAL = int(os.getenv("NASDAQ_UNIVERSE_INTERVAL", "300"))
_nasdaq_universe_cache = {"ts": 0.0, "rows": []}
_nasdaq_universe_lock = threading.Lock()


def _nasdaq_screener_provider():
    """يرجع قائمة dicts (symbol/price/change/volume) من شاشة Nasdaq العامة —
    بنفس صيغة DISCOVERY_EXTRA_PROVIDERS المتوقعة. مخزّن مؤقتًا، فلا يضرب
    Nasdaq إلا كل NASDAQ_UNIVERSE_INTERVAL ثانية بغض النظر عن تكرار استدعائه."""
    if not NASDAQ_UNIVERSE_ENABLED:
        return []
    now_ts = time.time()
    with _nasdaq_universe_lock:
        if now_ts - _nasdaq_universe_cache["ts"] < NASDAQ_UNIVERSE_INTERVAL and _nasdaq_universe_cache["rows"]:
            return _nasdaq_universe_cache["rows"]
    out = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json"}
    for exch in ("NASDAQ", "NYSE", "AMEX"):
        try:
            url = f"https://api.nasdaq.com/api/screener/stocks?tableonly=false&download=true&exchange={exch}"
            resp = requests.get(url, headers=headers, timeout=15)
            rows = ((resp.json() or {}).get("data", {}) or {}).get("rows") or []
            for r in rows:
                try:
                    sym = str(r.get("symbol", "")).strip().upper()
                    price = _safe_float(str(r.get("lastsale", "")).replace("$", "").replace(",", ""))
                    change = _safe_float(str(r.get("pctchange", "")).replace("%", "").replace(",", ""))
                    volume = _safe_float(str(r.get("volume", "")).replace(",", ""))
                    if not sym or not (DISCOVERY_PRICE_MIN <= price <= DISCOVERY_PRICE_MAX):
                        continue
                    if change < DISCOVERY_MIN_CHANGE or volume < 100_000:
                        continue
                    out.append({"symbol": sym, "price": price, "change": change, "volume": volume, "avg_volume": 0.0})
                except Exception:
                    continue
        except Exception as error:
            logger.debug(f"[NasdaqUniverse] {exch}: {error}")
        time.sleep(0.5)
    with _nasdaq_universe_lock:
        _nasdaq_universe_cache["ts"] = now_ts
        _nasdaq_universe_cache["rows"] = out
    logger.info(f"[NasdaqUniverse] fetched {len(out)} candidates across NASDAQ/NYSE/AMEX")
    return out


DISCOVERY_EXTRA_PROVIDERS.append(_nasdaq_screener_provider)


# ================================================================
# 🧪 تجربة: أعلى الرابحين من Webull (بري ماركت / افتر / اليوم) — endpoint عام
# غير رسمي يقرأه موقع Webull نفسه لأي زائر بدون تسجيل دخول. ما فيه حساب ولا
# API key ولا أي بيانات دخول (لا تداول، لا حساب، قراءة بيانات سوق فقط) —
# استخرجت العنوان وشكل الطلب من مكتبة webull غير الرسمية، بدون استخدام المكتبة
# نفسها (تعتمد على إصدارات قديمة تكسر بيئتك).
# ⚠️ تجريبي بصراحة: ما قدرت أتحقق حيًا من شكل الاستجابة (ما عندي إنترنت
# بالبيئة)، فالقراءة دفاعية وتطبع عيّنة خام باللوق [WebullMovers] sample.
# أي شك بصحة الأرقام = يتجاهل الدفعة بدل ما يرسل تنبيه خاطئ.
# ================================================================
WEBULL_MOVERS_ENABLED = os.getenv("WEBULL_MOVERS_ENABLED", "true").strip().lower() == "true"
WEBULL_MOVERS_INTERVAL = int(os.getenv("WEBULL_MOVERS_INTERVAL", "120"))
WEBULL_MOVERS_PER_LIST = int(os.getenv("WEBULL_MOVERS_PER_LIST", "40"))
_webull_movers_cache = {"ts": 0.0, "phase": None, "rows": []}
_webull_movers_lock = threading.Lock()
_webull_sample_logged = set()
_WEBULL_DID = os.urandom(16).hex()
_WEBULL_HEADERS = {
    "Accept": "*/*", "Accept-Language": "en-US,en;q=0.5", "Content-Type": "application/json",
    "platform": "web", "hl": "en", "os": "web", "app": "global", "appid": "webull-webapp",
    "ver": "3.39.18", "lzone": "dc_core_r001", "locale": "eng", "device-type": "Web",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:99.0) Gecko/20100101 Firefox/99.0",
    "did": _WEBULL_DID,
}


def _wb_flatten(obj, out=None, depth=0):
    """يدمج القواميس المتداخلة بقاموس واحد (أول قيمة لكل مفتاح تفوز) — شكل الاستجابة غير موثق."""
    if out is None:
        out = {}
    if isinstance(obj, dict) and depth < 4:
        for k, v in obj.items():
            if isinstance(v, dict):
                _wb_flatten(v, out, depth + 1)
            elif not isinstance(v, (list, tuple)):
                out.setdefault(k, v)
    return out


def _wb_num(flat, *names):
    for n in names:
        if n in flat and flat[n] not in (None, ""):
            try:
                return float(str(flat[n]).replace(",", "").replace("%", ""))
            except ValueError:
                continue
    return None


def _webull_movers_provider():
    """أعلى الرابحين من Webull حسب المرحلة: PRE→preMarket، REGULAR→1d، AFTER→afterMarket.
    يرجع dicts بصيغة DISCOVERY_EXTRA_PROVIDERS. مخزّن مؤقتًا، ويرجع [] بأمان لو فشل شي."""
    if not WEBULL_MOVERS_ENABLED:
        return []
    phase = get_market_phase()
    rank_types = {"PRE": ["preMarket"], "REGULAR": ["1d"], "AFTER": ["afterMarket"]}.get(phase)
    if not rank_types:
        return []
    now_ts = time.time()
    with _webull_movers_lock:
        if (_webull_movers_cache["phase"] == phase and _webull_movers_cache["rows"] is not None
                and now_ts - _webull_movers_cache["ts"] < WEBULL_MOVERS_INTERVAL):
            return _webull_movers_cache["rows"]
    out = []
    for rank_type in rank_types:
        extended = rank_type in ("preMarket", "afterMarket")
        try:
            url = ("https://quotes-gw.webullfintech.com/api/wlas/ranking/topGainers"
                   f"?regionId=6&rankType={rank_type}&pageIndex=1&pageSize={WEBULL_MOVERS_PER_LIST}")
            resp = requests.get(url, headers=_WEBULL_HEADERS, timeout=15)
            js = resp.json() or {}
            data = js.get("data") if isinstance(js, dict) else js
            if isinstance(data, dict):
                data = next((v for v in data.values() if isinstance(v, list)), [])
            items = data if isinstance(data, list) else []
            if items and rank_type not in _webull_sample_logged:
                _webull_sample_logged.add(rank_type)
                logger.info(f"[WebullMovers] sample {rank_type} (raw item 0): {str(items[0])[:500]}")
            parsed = []
            for it in items:
                try:
                    flat = _wb_flatten(it)
                    sym = str(flat.get("disSymbol") or flat.get("symbol") or "").upper().strip()
                    price = (_wb_num(flat, "pPrice", "close", "price", "lastPrice") if extended
                             else _wb_num(flat, "close", "price", "lastPrice"))
                    ratio = (_wb_num(flat, "pChRatio", "changeRatio") if extended
                             else _wb_num(flat, "changeRatio", "pChRatio"))
                    vol = _wb_num(flat, "volume", "vol") or 0.0
                    if not sym or price is None or ratio is None:
                        continue
                    change = ratio * 100.0  # Webull يرجّع نسبة كسرية (0.35 = 35%) — وحارس الدفعة تحت يغطي لو غلط
                    if not extended:
                        prev = _wb_num(flat, "preClose", "prevClose", "previousClose")
                        if prev and prev > 0:
                            price_change = (price - prev) / prev * 100.0
                            if abs(price_change - change) > max(5.0, abs(change) * 0.25):
                                continue  # تناقض بين النسبة والسعر: نتجاهل الصف بدل ما نثق بأحدهما
                    parsed.append({"symbol": sym, "price": price, "change": change, "volume": vol})
                except Exception:
                    continue
            # حارس شكل/مقياس: وسيط أعلى 10 رابحين فوق 300% غالبًا يعني إن النسبة % مو كسر (تضخّم ×100)
            top = sorted((p["change"] for p in parsed), reverse=True)[:10]
            if top and sorted(top)[len(top) // 2] > 300:
                logger.warning(f"[WebullMovers] {rank_type}: مقياس النسبة مشكوك فيه (وسيط أعلى 10 = "
                               f"{sorted(top)[len(top) // 2]:.0f}%) — تجاهلت الدفعة. راجع العيّنة الخام باللوق.")
                continue
            min_vol = 10_000 if extended else 100_000
            for p in parsed:
                if (DISCOVERY_PRICE_MIN <= p["price"] <= DISCOVERY_PRICE_MAX
                        and DISCOVERY_MIN_CHANGE <= p["change"] <= 1500
                        and p["volume"] >= min_vol):
                    out.append({**p, "avg_volume": 0.0})
            logger.info(f"[WebullMovers] {rank_type}: items={len(items)} parsed={len(parsed)} accepted={len(out)}")
        except Exception as error:
            logger.debug(f"[WebullMovers] {rank_type}: {error}")
    with _webull_movers_lock:
        _webull_movers_cache.update({"ts": now_ts, "phase": phase, "rows": out})
    return out


DISCOVERY_EXTRA_PROVIDERS.append(_webull_movers_provider)


# ================================================================
# 🔴 بث حي مباشر من WebSocket ياهو الرسمي — فكرة من مشروع مفتوح المصدر
# yliveticker. نفس القناة اللي موقع ياهو نفسه يستخدمها لتحديث صفحة السهم
# لحظيًا لأي زائر (بدون تسجيل دخول، بدون مفتاح، بدون حساب). هذا أسرع بكثير
# من الاستطلاع الدوري (polling) كل 45 ثانية — تحديثات تجي فور حدوثها.
# نفك تشفير protobuf يدويًا للحقول اللي نحتاجها بس (بدون مكتبة protobuf
# الكاملة، تفاديًا لحساسية توافق الإصدارات) — فقط websocket-client مطلوبة.
# ================================================================
YAHOO_STREAM_ENABLED = os.getenv("YAHOO_STREAM_ENABLED", "true").strip().lower() == "true"
YAHOO_STREAM_RESUB_INTERVAL = int(os.getenv("YAHOO_STREAM_RESUB_INTERVAL", "45"))
YAHOO_STREAM_MAX_SYMBOLS = int(os.getenv("YAHOO_STREAM_MAX_SYMBOLS", "120"))
YAHOO_STREAM_STALE_SEC = int(os.getenv("YAHOO_STREAM_STALE_SEC", "90"))
_live_prices = {}
_live_prices_lock = threading.Lock()


def _parse_yaticker(raw_bytes):
    """
    فك تشفير يدوي لرسالة yaticker (protobuf) من بث ياهو — الحقول المطلوبة بس:
    id=1(string) price=2(float) changePercent=8(float) dayVolume=9(sint64 zigzag)
    change=12(float). Wire format قياسي: varint tag ثم القيمة حسب نوعها؛ أي حقل
    ثاني نتجاوزه بأمان (نقرأ طوله الصحيح حتى لو ما نستخدمه) عشان الترقيم يبقى صح.
    """
    result = {}
    i, n = 0, len(raw_bytes)

    def read_varint(pos):
        val, shift = 0, 0
        while True:
            b = raw_bytes[pos]
            val |= (b & 0x7F) << shift
            pos += 1
            if not (b & 0x80):
                return val, pos
            shift += 7

    while i < n:
        tag, i = read_varint(i)
        field_num, wire_type = tag >> 3, tag & 0x7
        if wire_type == 0:
            val, i = read_varint(i)
            if field_num == 9:
                result['dayVolume'] = (val >> 1) ^ -(val & 1)
        elif wire_type == 1:
            i += 8
        elif wire_type == 2:
            length, i = read_varint(i)
            chunk = raw_bytes[i:i + length]
            i += length
            if field_num == 1:
                try:
                    result['id'] = chunk.decode('utf-8')
                except Exception:
                    pass
        elif wire_type == 5:
            chunk = raw_bytes[i:i + 4]
            i += 4
            if field_num in (2, 8, 12) and len(chunk) == 4:
                try:
                    val = struct.unpack('<f', chunk)[0]
                    if field_num == 2:
                        result['price'] = val
                    elif field_num == 8:
                        result['changePercent'] = val
                    elif field_num == 12:
                        result['change'] = val
                except Exception:
                    pass
        else:
            break
    return result


def _live_price(symbol):
    """سعر لحظي من بث ياهو المباشر لو متوفر وحديث (أقل من YAHOO_STREAM_STALE_SEC)، وإلا None."""
    with _live_prices_lock:
        entry = _live_prices.get(symbol)
    if entry and (time.time() - entry.get("ts", 0)) < YAHOO_STREAM_STALE_SEC:
        return entry.get("price")
    return None


def _yahoo_stream_current_symbols():
    with state_lock:
        symbols = set()
        for x in state.get("hot_watchlist", []):
            if isinstance(x, dict) and x.get("symbol"):
                symbols.add(str(x["symbol"]).upper().strip())
    return list(symbols)[:YAHOO_STREAM_MAX_SYMBOLS]


def yahoo_stream_worker():
    """خيط دائم يحافظ على اتصال WebSocket مع ياهو، يشترك بأسهم hot_watchlist
    الحالية، ويحدّث الاشتراك كل YAHOO_STREAM_RESUB_INTERVAL ثانية. أي انقطاع
    يعيد الاتصال تلقائيًا (حلقة خارجية + إعادة تشغيل الخيط الفرعي)."""
    if not YAHOO_STREAM_ENABLED:
        logger.info("ℹ️ Yahoo live stream disabled (YAHOO_STREAM_ENABLED=false)")
        return
    try:
        import websocket
    except ImportError:
        logger.warning("[YahooStream] websocket-client غير مثبت — البث المباشر معطّل. أضفه لـrequirements.txt")
        return

    def on_message(ws, message):
        try:
            parsed = _parse_yaticker(base64.b64decode(message))
            sym, price = parsed.get('id'), parsed.get('price')
            if sym and price:
                with _live_prices_lock:
                    _live_prices[sym] = {"price": float(price), "ts": time.time(),
                                          "change_pct": float(parsed.get('changePercent', 0.0)),
                                          "volume": int(parsed.get('dayVolume', 0))}
        except Exception as error:
            logger.debug(f"[YahooStream] parse error: {error}")

    def on_error(ws, error):
        logger.debug(f"[YahooStream] ws error: {error}")

    def on_open(ws):
        symbols = _yahoo_stream_current_symbols()
        if symbols:
            try:
                ws.send(json.dumps({"subscribe": symbols}))
                logger.info(f"[YahooStream] connected, subscribed {len(symbols)} symbols")
            except Exception as error:
                logger.debug(f"[YahooStream] subscribe error: {error}")

    resub_state = {"ws": None, "started": False}

    def resubscribe_loop():
        while True:
            time.sleep(YAHOO_STREAM_RESUB_INTERVAL)
            ws = resub_state.get("ws")
            if ws is None:
                continue
            try:
                symbols = _yahoo_stream_current_symbols()
                if symbols:
                    ws.send(json.dumps({"subscribe": symbols}))
            except Exception as error:
                logger.debug(f"[YahooStream] resub error: {error}")

    while True:
        try:
            ws = websocket.WebSocketApp("wss://streamer.finance.yahoo.com/",
                                         on_message=on_message, on_error=on_error, on_open=on_open)
            resub_state["ws"] = ws
            if not resub_state["started"]:
                threading.Thread(target=resubscribe_loop, daemon=True, name="YahooStreamResub").start()
                resub_state["started"] = True
            ws.run_forever(ping_interval=15, ping_timeout=10)
        except Exception as error:
            logger.error(f"[YahooStream] worker error: {error}")
        time.sleep(5)  # قبل إعادة محاولة الاتصال


def scanner_heartbeat(name):
    """نبضة لكل ماسح — تظهر في /health عشان تعرف مين متأخر أو واقف."""
    _heartbeats[name] = time.time()


# ---------------- مدير أحداث موحّد + كولداونات منفصلة حسب نوع الحدث ----------------
# ================= UNIFIED EVENT MANAGER =================
# كل محركات التنبيه تمر من حالة واحدة، لكن الكولداون منفصل حسب نوع الحدث.
EVENT_COOLDOWNS = {
    "DISCOVERY": int(os.getenv("EVENT_COOLDOWN_DISCOVERY", "900")),
    "EARLY": int(os.getenv("EVENT_COOLDOWN_EARLY", "300")),
    "ENTRY": int(os.getenv("EVENT_COOLDOWN_ENTRY", "1800")),
    "SETUP": int(os.getenv("EVENT_COOLDOWN_SETUP", "1800")),
    "MOMENTUM": int(os.getenv("EVENT_COOLDOWN_MOMENTUM", "300")),
    "GAP": int(os.getenv("EVENT_COOLDOWN_GAP", "900")),
    "MEGA": int(os.getenv("EVENT_COOLDOWN_MEGA", "1800")),
    "PUMP": int(os.getenv("EVENT_COOLDOWN_PUMP", "900")),
    "SURGE": int(os.getenv("EVENT_COOLDOWN_SURGE", "600")),
    "TARGET": int(os.getenv("EVENT_COOLDOWN_TARGET", "60")),
    "STOP": int(os.getenv("EVENT_COOLDOWN_STOP", "60")),
    "EXIT": int(os.getenv("EVENT_COOLDOWN_EXIT", "120")),
    "DUMP": int(os.getenv("EVENT_COOLDOWN_DUMP", "120")),
    "DEFAULT": int(os.getenv("ALERT_SYMBOL_COOLDOWN", "2700")),
}
EVENT_CONTINUATION_PCT = float(os.getenv("EVENT_CONTINUATION_PCT", "8"))
EVENT_CONTINUATION_MIN_GAP = int(os.getenv("EVENT_CONTINUATION_MIN_GAP", "300"))

def _event_type(source, explicit=None):
    if explicit:
        return str(explicit).upper().strip()
    src = str(source or "").upper().strip()
    return {"RANK":"DISCOVERY", "REC":"ENTRY", "EARLY":"EARLY", "SETUP":"SETUP",
            "MOMENTUM":"MOMENTUM", "GAP":"GAP", "MEGA":"MEGA", "PUMP":"PUMP",
            "SURGE":"SURGE", "TARGET":"TARGET", "STOP":"STOP", "EXIT":"EXIT", "DUMP":"DUMP"}.get(src, src or "DEFAULT")

def _event_gate_allow(symbol, price=0.0, event_type="DEFAULT"):
    symbol = str(symbol or "").upper().strip()
    event = _event_type("", event_type)
    if not symbol:
        return False
    now_ts = time.time()
    with state_lock:
        rec = state.get("event_gate", {}).get(f"{symbol}|{event}")
    if not rec:
        return True
    age = now_ts - _safe_float(rec.get("ts"))
    cooldown = max(0, int(EVENT_COOLDOWNS.get(event, EVENT_COOLDOWNS["DEFAULT"])))
    if age >= cooldown:
        return True
    last_price = _safe_float(rec.get("price"))
    return bool(last_price > 0 and _safe_float(price) >= last_price * (1 + EVENT_CONTINUATION_PCT / 100.0)
                and age >= EVENT_CONTINUATION_MIN_GAP)

def _event_gate_mark(symbol, price=0.0, event_type="DEFAULT", source=""):
    symbol = str(symbol or "").upper().strip()
    event = _event_type(source, event_type)
    if not symbol:
        return
    now_ts = time.time()
    with state_lock:
        gate = state.setdefault("event_gate", {})
        gate[f"{symbol}|{event}"] = {"ts": now_ts, "price": _safe_float(price), "source": source or event}
        if len(gate) > 1500:
            cutoff = now_ts - 86400
            gate = {k:v for k,v in gate.items() if _safe_float(v.get("ts")) >= cutoff}
            if len(gate) > 1500:
                gate = dict(sorted(gate.items(), key=lambda kv:_safe_float(kv[1].get("ts")), reverse=True)[:1500])
            state["event_gate"] = gate

def _alert_gate_allow(symbol, price=0.0, source=""):
    return _event_gate_allow(symbol, price, _event_type(source))

def _alert_gate_mark(symbol, price=0.0, source=""):
    _event_gate_mark(symbol, price, _event_type(source), source)
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        return
    now_ts = time.time()
    with state_lock:
        gate = state.setdefault("alert_gate", {})
        gate[symbol] = {"ts": now_ts, "price": _safe_float(price), "source": source}
        if len(gate) > 300:
            cutoff = now_ts - 86400
            for k in [k for k,v in gate.items() if _safe_float(v.get("ts")) < cutoff]:
                gate.pop(k, None)

def _gap_resend_threshold(prev_change, phase=None):
    """20→22 لا يكفي، 20→25 ممكن، 20→30 واضح؛ ويتدرج مع حجم الحركة السابقة."""
    base = max(1.0, float(GAP_HUNTER_RESEND_DELTA))
    prev = max(0.0, _safe_float(prev_change))
    dynamic = prev * float(os.getenv("GAP_HUNTER_RESEND_PCT", "0.15"))
    if phase in ("PRE", "AFTER"):
        dynamic *= 1.15
    return max(base, min(12.0, dynamic))


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
        # قمة جديدة ليست دخولاً تلقائياً: إذا جاءت مع اندفاعة حادة نخفضها كي لا تصل للـAI كأفضل مرشح.
        if fm["new_high_recent"] and fm["move_15m"] >= PEAK_FAST_MOVE_15M_PCT and rsi >= PEAK_RSI_LIMIT:
            score -= 22; factors.append("تحذير: قرب القمة بعد اندفاعة — ليس دخولاً")
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
    if "yahoo.com" in url and _yahoo_cooldown_remaining() > 0:
        raise RuntimeError(f"yahoo cooldown {_yahoo_cooldown_remaining():.0f}s")      # [FIX-10] ياهو رافض الطلبات: لا نضاعفها
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
                if "yahoo.com" in url:
                    _yahoo_trip_breaker("screener HTTP 429")                          # [FIX-10] تبريد أُسّي لكل طلبات ياهو
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
        if _yahoo_cooldown_remaining() > 0:
            break      # [FIX-10] ياهو رافض الطلبات: ما نضاعفها
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
    # [FIX-5] كل مزوّدات Discovery الإضافية (Nasdaq / Webull / أي مزوّد قادم) تمر بنفس فلتر الرموز (warrants/rights/units)
    _extra_ok, _extra_rejected = _ingest_extra_providers(merged, list(DISCOVERY_EXTRA_PROVIDERS))
    ok_screeners += _extra_ok
    _note_extra_rejects(_extra_rejected)
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
    _publish_early_pool(hot_cands, now_ts)        # EARLY يشوف كل المرشحين مو بس أول HOT_MAX_ITEMS
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
            # Discovery is a rolling feed; a temporarily sparse screener must not
            # shrink the whole universe to only 2-3 symbols.
            existing = [str(x).upper().strip() for x in state.get("tickers", []) if isinstance(x, str)]
            in_play = list(state.get("in_play", {}).keys())
            merged_universe = list(dict.fromkeys(universe + existing + in_play))
            state["tickers"] = merged_universe[:MAX_TICKERS_TO_SCAN]
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

SETUP_ENGINE_ENABLED     = os.getenv("SETUP_ENGINE_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
SETUP_ARM_MIN            = int(os.getenv("SETUP_ARM_MIN", "62"))              # أدنى سكور لتسليح الإعداد
SETUP_THRUST_MIN_PCT     = float(os.getenv("SETUP_THRUST_MIN_PCT", "15"))     # أدنى اندفاعة (%) لتعتبر السهم "له اهتمام" — [FIX-17] كانت 30: أعلام الساق 10–25% ما تنكشف
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
    stamp = _session_stamp()
    source = _price_src_label(symbol)
    return head + "\n" + body + ("\n" + stamp if stamp else "") + (f"\n💰 مصدر السعر{source}" if source else "") + "\n\n⚠️ تحليل آلي حسب بنية السعر فقط، وليس توصية مالية."


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
                            ev_type = "STOP" if str(event).upper() == "STOP" else ("TARGET" if str(event).upper() in ("T1", "T2", "T3") else "SETUP")
                            if not _event_gate_allow(symbol, price, ev_type):
                                return False
                            send_telegram(msg)
                            _event_gate_mark(symbol, price, ev_type, "SETUP")
                            try:
                                sle_journal_signal(symbol, "SETUP", price)
                            except Exception:
                                pass
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
                        ev_type = "STOP" if str(event).upper() == "STOP" else ("TARGET" if str(event).upper() in ("T1", "T2", "T3") else "SETUP")
                        if not _event_gate_allow(symbol, price, ev_type):
                            return False
                        send_telegram(msg)
                        _event_gate_mark(symbol, price, ev_type, "SETUP")
                        try:
                            sle_journal_signal(symbol, "SETUP", price)
                        except Exception:
                            pass
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
                        overshoot_pct = ((price - setup["trigger"]) / max(setup["trigger"], 1e-9)) * 100.0
                        if overshoot_pct > SETUP_MAX_BROKE_OVERSHOOT_PCT:
                            # لا نشتري بعد أن تجاوز السعر محفز الاختراق بأكثر من الهامش؛ ننتظر قاعدة جديدة.
                            if "LATE" not in alerted:
                                logger.info(f"[SetupEngine] {symbol} late breakout ignored: +{overshoot_pct:.1f}% over trigger")
                                alerted.add("LATE")
                            setup["state"] = "BROKE_LATE"
                        else:
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


# ================================================================
# 🧠 RAILWAY MEMORY MONITOR — حماية من تجاوز الذاكرة
# ================================================================
MEMORY_LIMIT_MB = int(os.getenv("MEMORY_LIMIT_MB", "450"))   # [MemFix] الافتراضي نفسه؛ قابل للتعديل من Railway بدون لمس الكود
MEMORY_CHECK_INTERVAL = 60
MEMORY_LOG_EVERY_SEC = int(os.getenv("MEMORY_LOG_EVERY_SEC", "600"))   # سطر [Memory] دوري بأحجام المستهلكات


def memory_monitor():
    """يراقب RSS؛ يعيد التشغيل عبر Railway عند تجاوز الحد الآمن فقط."""
    if psutil is None:
        logger.warning('⚠️ psutil unavailable; memory monitor disabled')
        return
    process = psutil.Process(os.getpid())
    _first_sample = True
    _last_mem_log = time.time()
    while True:
        try:
            memory_mb = process.memory_info().rss / (1024 * 1024)
            if _first_sample:
                _first_sample = False
                logger.info(f'[Memory] startup RSS={memory_mb:.1f} MB (limit {MEMORY_LIMIT_MB} MB)')
            if memory_mb > MEMORY_LIMIT_MB:
                _heavy = [m for m in ('torch', 'stable_baselines3', 'numba', 'matplotlib', 'sklearn', 'scipy') if m in sys.modules]
                logger.error(f'🚨 Memory usage {memory_mb:.1f} MB exceeds {MEMORY_LIMIT_MB} MB; requesting Railway restart | threads={threading.active_count()} | heavy_modules={_heavy}')
                os._exit(1)
            if time.time() - _last_mem_log >= MEMORY_LOG_EVERY_SEC:
                _last_mem_log = time.time()
                logger.info(f'[Memory] RSS={memory_mb:.0f}MB/{MEMORY_LIMIT_MB} | {_mem_summary()}')
        except Exception as error:
            logger.warning(f'[Memory] monitor error: {error}')
        time.sleep(MEMORY_CHECK_INTERVAL)


# ================================================================
# 🕯️ CANDLESTICK + CHART PATTERN DETECTORS
# ================================================================


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


# ================================================================
# 📊 ADVANCED ANALYSIS SCANNER — EMA + Fibonacci + Support/Resistance
# ================================================================


def ai_process_symbol(symbol, news_impact=0):
    """
    تحليل AI عميق لسهم معين.
    يستخدم Groq لتقييم المخاطر والمحفزات بناءً على البيانات الفنية والأخبار.
    """
    try:
        symbol = str(symbol).upper().strip()
        df = cached_download(symbol, period="5d", interval="5m")
        if df is None or df.empty or len(df) < 20: return
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
    # [FIX-14] شغّل المدقق النهائي فعلياً قبل الإرسال، وليس كدالة غير مستخدمة.
    base = _finalize_recommendation_plan(base)
    decision = base.get("final_decision", "NO_TRADE")
    allowed = ("BUY", "WATCH") if auto else ("BUY", "WATCH", "AVOID")
    if not send_alert or decision not in allowed:
        return False
    symbol = base.get("symbol", "")
    if auto and not _alert_gate_allow(symbol, price, "REC"):
        logger.info(f"[Recommendation] {symbol} {decision} skipped (unified cooldown)")
        return False
    send_telegram(_format_recommendation_message(base))
    _journal_record(symbol, "REC", price, {"score": base.get("technical_score"), "rvol": base.get("rvol")})
    if auto:
        _alert_gate_mark(symbol, price, "REC")
    return True


# ================= [FIX-1][FIX-2] سلامة خطة التوصية: الاتجاه + منطقة الدخول + التحقق الصلب =================
# الأسباب الجذرية (الشرح الكامل في FIX_REPORT.md):
#  [FIX-1] قرار الـAI نص حر: كانت SELL/HOLD تسقط على فرع WATCH (لأن الدرجة الفنية ≥ 60) فتنطبع "SELL"
#          مع خطة شراء (وقف تحت السعر + أهداف فوقه). البوت long-only ⇒ أي حكم هابط = AVOID وبدون خطة أصلًا.
#  [FIX-2] منطقة الدخول max(price-0.35ATR, vwap*0.995) كانت تنعكس (low > high) لو السعر تحت VWAP.
#  [FIX-1] ما كان فيه تحقق نهائي من ترتيب أرقام الخطة؛ _validate_long_plan الحين بوابة صلبة قبل الحفظ/الإرسال.
_AI_DECISION_BEARISH = {"SELL", "STRONG_SELL", "STRONGSELL", "SHORT", "BEARISH", "REDUCE", "EXIT", "TRIM"}
_AI_DECISION_AVOID = {"AVOID", "NO_TRADE", "NOTRADE", "SKIP", "REJECT"}
_AI_DECISION_BUY = {"BUY", "STRONG_BUY", "STRONGBUY", "LONG", "BULLISH", "ACCUMULATE"}


def _normalize_ai_decision(raw):
    """
    يوحّد قرار الـAI (نص حر) → (decision, bearish):
      decision ∈ {"BUY", "WATCH", "AVOID"}
      bearish  = True لو الحكم هابط صريح (SELL/SHORT/...) — يُعامَل AVOID لأن البوت للشراء فقط.
    HOLD/NEUTRAL/فاضي/غير معروف → WATCH (نفس السلوك القديم)، وما يصير BUY أبدًا إلا بكلمة شراء صريحة.
    """
    key = re.sub(r"[\s\-]+", "_", str(raw or "").strip().upper())
    if key in _AI_DECISION_BEARISH:
        return "AVOID", True
    if key in _AI_DECISION_AVOID:
        return "AVOID", False
    if key in _AI_DECISION_BUY:
        return "BUY", False
    return "WATCH", False


def _build_long_entry_zone(price, atr, vwap, above_vwap):
    """
    منطقة دخول LONG مرتبة دائمًا (low <= high).
    الخطأ القديم: entry_low = max(price - 0.35*ATR, vwap*0.995). لو السعر تحت VWAP يصير vwap*0.995 أعلى من
    entry_high = price + 0.15*ATR فتنطبع المنطقة معكوسة (مثال VG: 13.1500 - 12.9888).
    الحل: أرضية VWAP تنطبق فقط لو السعر فوق VWAP (وقتها VWAP تحت السعر فما تتجاوز الحد الأعلى)؛ وتحت VWAP نبني
    المنطقة من السعر وATR فقط (والتوصية أصلًا معلَّمة "السعر تحت VWAP"). والترتيب الدفاعي أخيرًا يضمن low <= high.
    """
    low = price - atr * 0.35
    high = price + atr * 0.15
    if above_vwap and vwap > 0:
        low = max(low, vwap * 0.995)
    if low > high:
        low, high = high, low
    return low, high


def _validate_long_plan(price, entry_low, entry_high, confirmation, stop, target_1, target_2):
    """
    بوابة صلبة لخطة LONG قبل الحفظ/الإرسال → (ok, reason). الترتيب المطلوب:
        stop < entry_low <= entry_high <= confirmation      و      entry_high < target_1 < target_2
        وأيضًا  stop < price < target_1
    أي خرق ⇒ التوصية تُلغى بسبب واضح بدل ما تنطبع أرقام متناقضة (وقف فوق الدخول، أهداف تحت السعر، منطقة معكوسة...).
    """
    vals = {"price": price, "entry_low": entry_low, "entry_high": entry_high,
            "confirmation": confirmation, "stop": stop, "target_1": target_1, "target_2": target_2}
    for name, value in vals.items():
        try:
            number = float(value)
        except (TypeError, ValueError):
            return False, f"قيمة غير رقمية ({name})"
        if not math.isfinite(number) or number <= 0:
            return False, f"قيمة غير صالحة ({name})"
    if not stop < entry_low:
        return False, "الوقف ليس تحت منطقة الدخول"
    if not entry_low <= entry_high:
        return False, "منطقة الدخول معكوسة"
    if not entry_high <= confirmation:
        return False, "سعر التأكيد تحت منطقة الدخول"
    if not entry_high < target_1 < target_2:
        return False, "الأهداف ليست فوق منطقة الدخول بترتيب تصاعدي"
    if not stop < price < target_1:
        return False, "السعر خارج نطاق الوقف/الهدف الأول"
    return True, ""


# ================= [FIX-4..8] الحزمة الثانية: الحركة الممتدة · Warrants · دعم Swing · R:R · تكرار التنبيهات =================
#  [FIX-4] الـAI كان أعمى عن نسبة صعود اليوم ('change': 0.0 + برومبت Groq بدون تغيّر) ⇒ MSGY طلع BUY عند 8.26 وهو +309% قبل الانهيار.
#          الحين نحسب التغيّر اليومي (إغلاق الجلسة العادية السابقة، وإلا hot_watchlist مُعاد حسابه بالسعر الحي) ونمرره للـAI،
#          وبوابة محلية: صعود ≥ RECOMMENDATION_MAX_DAY_CHANGE_PCT (50% = نفس تعريف PUMP بالبوت) ⇒ سهم ممتد ⇒ NO_TRADE بدون AI.
#  [FIX-5] فلتر warrants/rights/units كان داخل _normalize_quote فقط؛ مزوّدات Discovery الإضافية (Nasdaq / Webull) تتجاوزه
#          (GRMLW / SHMDW / USDEW / SATLW). الحين نقطة فلترة واحدة لكل المزوّدات الحالية والقادمة.
#  [FIX-6] calculate_support_resistance يرجّع 'support' قاموسًا {level,...} وSwing كان يحوّله float ⇒ "دعم $0.0000" دائمًا.
#  [FIX-7] R:R يطلع 1.9999999999999 بسبب الفاصلة العائمة (≈24% من الحالات) ⇒ يُرفض BUY ويُطبع "R:R أقل من 2.0" جنب "2.00".
#  [FIX-8] MEGA MOVER + GAP HUNTER + PUMP كانت ترسل 3 رسائل لنفس الحركة (نفس عتبات 50% / 300K). الحين PUMP يملك الحركة.
RECOMMENDATION_MAX_DAY_CHANGE_PCT = float(os.getenv("RECOMMENDATION_MAX_DAY_CHANGE_PCT", "50"))   # صعود اليوم = ممتد ⇒ لا توصية دخول
# [FIX-13] بوابة توقيت الدخول: تمنع شراء السهم ملاصقاً لقمة يومية بعد اندفاعة
PEAK_ENTRY_GUARD_ENABLED = os.getenv("PEAK_ENTRY_GUARD_ENABLED", "true").strip().lower() == "true"
PEAK_NEAR_DAY_HIGH_PCT = float(os.getenv("PEAK_NEAR_DAY_HIGH_PCT", "1.5"))
PEAK_NEAR_RECENT_HIGH_PCT = float(os.getenv("PEAK_NEAR_RECENT_HIGH_PCT", "0.8"))
PEAK_MIN_DAY_CHANGE_PCT = float(os.getenv("PEAK_MIN_DAY_CHANGE_PCT", "12"))
PEAK_FAST_MOVE_15M_PCT = float(os.getenv("PEAK_FAST_MOVE_15M_PCT", "5"))
PEAK_RSI_LIMIT = float(os.getenv("PEAK_RSI_LIMIT", "68"))
PEAK_VWAP_DEVIATION_PCT = float(os.getenv("PEAK_VWAP_DEVIATION_PCT", "6"))
SETUP_MAX_BROKE_OVERSHOOT_PCT = float(os.getenv("SETUP_MAX_BROKE_OVERSHOOT_PCT", "4"))
RECOMMENDATION_RR_TOLERANCE = 1e-6                                                                 # [FIX-7] هامش الفاصلة العائمة
EVENT_DEDUPE_ENABLED = os.getenv("EVENT_DEDUPE_ENABLED", "true").strip().lower() == "true"          # [FIX-8] false = السلوك القديم (3 رسائل)
_extra_reject_log = {"ts": 0.0}


def _prev_regular_close(df):
    """
    [FIX-4] إغلاق آخر جلسة عادية (9:30–16:00 نيويورك) قبل يوم آخر شمعة بالبيانات. يتجاهل شموع البري/الافتر لأنها
    تتحرك بعد الإغلاق الرسمي. يرجع 0.0 لو تعذّر (فهرس غير زمني، أو يوم واحد فقط بالبيانات).
    """
    try:
        idx = df.index
        if not isinstance(idx, pd.DatetimeIndex) or len(idx) < 2:
            return 0.0
        idx = idx.tz_localize("UTC") if idx.tz is None else idx
        idx = idx.tz_convert(EASTERN_TZ)
        days = [ts.strftime("%Y-%m-%d") for ts in idx]
        last_day = days[-1]
        closes = df["close"]
        for i in range(len(idx) - 2, -1, -1):
            if days[i] < last_day:
                minute_of_day = idx[i].hour * 60 + idx[i].minute
                if 570 <= minute_of_day < 960:
                    return _safe_number(closes.iloc[i])
        return 0.0
    except Exception:
        return 0.0


def _day_change_from_df(df, price):
    """[FIX-4] % التغيّر عن إغلاق الجلسة السابقة من الشموع؛ None لو غير معروف."""
    prev_close = _prev_regular_close(df)
    if prev_close > 0 and price > 0:
        return (price / prev_close - 1.0) * 100.0
    return None


def _hot_change_live(symbol, price):
    """[FIX-4] بديل لما تكون الشموع بدون يوم سابق: change من hot_watchlist مُعاد حسابه بالسعر الحي (_pd_live_change)."""
    try:
        with state_lock:
            record = next((dict(x) for x in state.get("hot_watchlist", [])
                           if isinstance(x, dict) and str(x.get("symbol", "")).upper().strip() == symbol), None)
        if record:
            return _pd_live_change(record.get("price"), record.get("change"), price)
    except Exception as error:
        logger.debug(f"[Recommendation] hot change {symbol}: {error}")
    return None


def _is_plain_common_symbol(symbol):
    """
    [FIX-5] رمز سهم عادي؟ يرفض: غير حرفي (BRK.B، ^)، أطول من 5 أحرف، وخمس أحرف تنتهي بـW/R/U (Warrants / Rights / Units).
    نفس قاعدة _normalize_quote بالضبط (اللي كانت تحمي شاشات ياهو فقط).
    """
    s = str(symbol or "").upper().strip()
    if not s or not s.isalpha() or len(s) > 5:
        return False
    if len(s) == 5 and s[-1] in ("W", "R", "U"):
        return False
    return True


def _ingest_extra_providers(merged, providers):
    """
    [FIX-5] يدمج صفوف مزوّدات Discovery الإضافية (Nasdaq / Webull / أي مزوّد مستقبلي) داخل merged بعد فلتر الرموز.
    → (عدد المقبول، قائمة الرموز المرفوضة). فشل مزوّد واحد ما يوقف الباقي.
    """
    accepted, rejected = 0, []
    for provider in providers:
        try:
            for n in (provider() or []):
                if isinstance(n, dict) and n.get("symbol") and _safe_float(n.get("price")) > 0 and n["symbol"] not in merged:
                    if not _is_plain_common_symbol(n["symbol"]):
                        rejected.append(str(n["symbol"]).upper().strip())
                        continue
                    n.setdefault("screeners", ["EXTRA"])
                    n.setdefault("volume", 0.0)
                    n.setdefault("change", 0.0)
                    n.setdefault("avg_volume", 0.0)
                    merged[n["symbol"]] = n
                    accepted += 1
        except Exception as error:
            logger.warning(f"[Discovery] extra provider failed: {error}")
    return accepted, rejected


def _note_extra_rejects(rejected):
    """[FIX-5] سطر لوق (كل 10 دقايق كحد أقصى) يبيّن كم رمز غير عادي انحجب — للتحقق بعد النشر."""
    if not rejected:
        return
    now = time.time()
    if now - _extra_reject_log["ts"] >= 600:
        _extra_reject_log["ts"] = now
        logger.info(f"[Discovery] extra providers: dropped {len(rejected)} non-common symbols "
                    f"(warrants/rights/units), e.g. {', '.join(sorted(set(rejected))[:6])}")


def _sr_level(value):
    """[FIX-6] سعر مستوى الدعم/المقاومة سواء رجع رقم أو قاموس {'level': ...} (calculate_support_resistance يرجّع قاموسًا)."""
    if isinstance(value, dict):
        value = value.get("level")
    return _safe_float(value)


def _pump_alert_owns_move(change, volume):
    """
    [FIX-8] هل تنبيه PUMP يغطي هذي الحركة؟ (نفس عتبات PUMP_DUMP: 50% / 300K افتراضيًا). لو نعم، MEGA MOVER وGAP HUNTER
    يسكتون عنها (PUMP فيه السعر والتغيّر والحجم والرادار والخبر + متابعة الدامب)، فتوصلك رسالة وحدة بدل ثلاث.
    """
    return bool(EVENT_DEDUPE_ENABLED and PUMP_DUMP_ENABLED
                and change >= PUMP_DUMP_MIN_PUMP_PCT and volume >= PUMP_DUMP_MIN_VOLUME)


logger.info("[FixPack] unified event manager + separate cooldowns + dynamic GAP resend + plan-direction + entry-zone + pump/dump state-machine + extended-move gate + "
            "warrant filter + swing support + R:R tolerance + event dedupe + KSA alert time + yahoo 429 guard + EARLY engine: ACTIVE")


def _peak_entry_guard(df, price, day_change=None, rsi=50.0, vwap=None):
    """[FIX-13] يرفض توقيت الدخول المتأخر من القمة، مع إبقاء ARMED/EARLY متاحين.
    لا يعتمد على نسبة اليوم وحدها: يقيس قرب السعر من قمة اليوم/آخر ساعة، سرعة الاندفاعة،
    تشبع RSI وانحراف السعر عن VWAP. يرجع (late, reason, metrics).
    """
    metrics = {"day_high": 0.0, "recent_high": 0.0, "off_day_high_pct": 0.0,
               "off_recent_high_pct": 0.0, "move_15m": 0.0, "vwap_dev_pct": 0.0}
    if not PEAK_ENTRY_GUARD_ENABLED or df is None or getattr(df, "empty", True):
        return False, "", metrics
    try:
        d = df.copy(); d.columns = [str(c).lower() for c in d.columns]
        if not {"close", "high"}.issubset(d.columns) or len(d) < 6:
            return False, "", metrics
        close, high = d["close"].astype(float), d["high"].astype(float)
        price = float(price)
        if price <= 0: return False, "", metrics
        try:
            today = now_est().date(); mask = (d.index.date == today)
            today_high = high[mask]
            day_high = float(today_high.max()) if len(today_high) else float(high.tail(78).max())
        except Exception:
            day_high = float(high.tail(78).max())
        recent_high = float(high.tail(12).max())
        off_day = max(0.0, (day_high - price) / max(day_high, 1e-9) * 100.0)
        off_recent = max(0.0, (recent_high - price) / max(recent_high, 1e-9) * 100.0)
        move_15m = (price / float(close.iloc[-4]) - 1.0) * 100.0 if len(close) >= 5 and close.iloc[-4] > 0 else 0.0
        vwap_dev = ((price - float(vwap)) / float(vwap) * 100.0) if vwap and float(vwap) > 0 else 0.0
        metrics.update(day_high=day_high, recent_high=recent_high, off_day_high_pct=off_day,
                      off_recent_high_pct=off_recent, move_15m=move_15m, vwap_dev_pct=vwap_dev)
        near_day = off_day <= PEAK_NEAR_DAY_HIGH_PCT
        near_recent = off_recent <= PEAK_NEAR_RECENT_HIGH_PCT
        fast_and_hot = move_15m >= PEAK_FAST_MOVE_15M_PCT and (float(rsi) >= PEAK_RSI_LIMIT or vwap_dev >= PEAK_VWAP_DEVIATION_PCT)
        # الحالات التالية تعني أن التوصية ستصل بعد معظم الحركة، وليست بداية قاعدة:
        if (day_change is not None and float(day_change) >= PEAK_MIN_DAY_CHANGE_PCT and near_day and fast_and_hot):
            return True, "السعر ملاصق لقمة اليوم بعد اندفاعة سريعة/تشبع", metrics
        if (day_change is not None and float(day_change) >= 20.0 and off_day <= 3.0):
            return True, "السهم ممتد وقريب جداً من قمة اليوم", metrics
        if near_recent and move_15m >= PEAK_FAST_MOVE_15M_PCT and float(rsi) >= PEAK_RSI_LIMIT:
            return True, "دخول متأخر قرب قمة آخر ساعة", metrics
        if float(rsi) >= 78.0 and vwap_dev >= PEAK_VWAP_DEVIATION_PCT and near_day:
            return True, "RSI وانحراف VWAP يؤكدان مطاردة القمة", metrics
        return False, "", metrics
    except Exception as error:
        logger.debug(f"[PeakGuard] {error}")
        return False, "", metrics


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
        # [FIX-4] التغيّر اليومي: من الشموع (إغلاق الجلسة العادية السابقة)، وإلا من hot_watchlist مُعاد حسابه بالسعر الحي
        day_change = _day_change_from_df(df, price)
        if day_change is None:
            day_change = _hot_change_live(symbol, price)
        day_change_known = day_change is not None
        day_change = float(day_change) if day_change_known else 0.0
        base["day_change"] = day_change if day_change_known else None
        peak_late, peak_reason, peak_metrics = _peak_entry_guard(df, price, day_change if day_change_known else None, rsi, vwap)
        base["peak_guard"] = peak_metrics
        if peak_late:
            base["invalidations"].append(f"دخول متأخر من القمة: {peak_reason}")
            pump_risk = True
        extended = day_change_known and day_change >= RECOMMENDATION_MAX_DAY_CHANGE_PCT
        if extended:
            pump_risk = True
        if price < MIN_PRICE or volume < MIN_VOLUME:
            base["invalidations"].append("سيولة أو سعر غير مناسب")
        if pump_risk:
            base["invalidations"].append(f"سهم ممتد: صعد +{day_change:.0f}% اليوم" if extended else "خطر تلاعب محتمل")
        if rvol < RECOMMENDATION_MIN_RVOL:
            base["invalidations"].append(f"RVOL أقل من {RECOMMENDATION_MIN_RVOL}x")
        if not above_vwap:
            base["invalidations"].append("السعر تحت VWAP")
        entry_low, entry_high = _build_long_entry_zone(price, atr, vwap, above_vwap)   # [FIX-2] مرتبة دائمًا low <= high
        confirmation = max(high_10, price + atr * 0.25)
        stop_loss = max(price - atr * 1.2, price * 0.94)
        target_1 = price + atr * 2.4
        target_2 = price + atr * 3.6
        risk_reward = (target_1 - price) / max(price - stop_loss, 1e-9)
        base.update({"price": price, "rvol": rvol, "entry_zone_low": entry_low,
                     "entry_zone_high": entry_high, "confirmation_price": confirmation,
                     "stop_loss": stop_loss, "target_1": target_1, "target_2": target_2,
                     "risk_reward": risk_reward})
        # [FIX-1] بوابة صلبة: ترتيب أرقام الخطة لازم يكون منطقي، وإلا تُلغى التوصية بسبب واضح
        plan_ok, plan_reason = _validate_long_plan(price, entry_low, entry_high, confirmation,
                                                   stop_loss, target_1, target_2)
        if not plan_ok:
            base["invalidations"].append(f"خطة غير منطقية: {plan_reason}")
            logger.warning(f"[Recommendation] {symbol} plan rejected: {plan_reason} "
                           f"(price={price:.4f} atr={atr:.4f} vwap={vwap:.4f})")
        if technical_score < RECOMMENDATION_MIN_TECH_SCORE:
            base["invalidations"].append(f"الدرجة الفنية {technical_score} أقل من {RECOMMENDATION_MIN_TECH_SCORE}")
        if risk_reward < RECOMMENDATION_MIN_RR - RECOMMENDATION_RR_TOLERANCE:      # [FIX-7] هامش الفاصلة العائمة
            base["invalidations"].append(f"R:R أقل من {RECOMMENDATION_MIN_RR}")
        if peak_late:
            logger.info(f"[Recommendation] {symbol} skipped: peak-entry guard ({peak_reason}) ")
            base.update({"final_decision": "NO_TRADE", "ai_decision": "SKIPPED_PEAK",
                         "reason_ar": f"تم منع الدخول من القمة: {peak_reason} — ننتظر قاعدة/تصحيح أو إشارة EARLY"})
            return base
        if extended:
            # [FIX-4] سهم ممتد: ما نصرف طلب AI ولا نطبع خطة — القرار ثابت (متابعته وظيفة PUMP/DUMP)
            logger.info(f"[Recommendation] {symbol} skipped: extended +{day_change:.0f}% today "
                        f"(limit {RECOMMENDATION_MAX_DAY_CHANGE_PCT:.0f}%)")
            base.update({"final_decision": "NO_TRADE", "ai_decision": "SKIPPED",
                         "reason_ar": f"سهم ممتد: صعد +{day_change:.0f}% اليوم — لا نوصي بمطاردته (متابعته عبر PUMP/DUMP)"})
            return base
        consensus_result = None
        if technical_score >= 60 and rvol >= 2.0:
            signal_data = {
                'price': price, 'change': day_change, 'rvol': rvol,
                'above_vwap': above_vwap, 'ema_bullish': ema_bullish,
                'rsi': rsi, 'breakout': breakout, 'volume_spike': volume_spike,
                'day_high': high_10, 'phase': get_market_phase(), 'score': technical_score,
            }
            consensus_result = ai_consensus(symbol, signal_data)
        if consensus_result:
            final_decision = consensus_result['final']
            if final_decision in ("BUY", "WATCH") and not plan_ok:
                final_decision = "NO_TRADE"      # [FIX-1] خطة متناقضة الأرقام ما تُرسل أبدًا
            if final_decision in ("BUY", "WATCH") and pump_risk:
                final_decision = "NO_TRADE"      # [FIX-4] مسار الإجماع كان يتجاهل pump_risk
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
                base["final_decision"] = "WATCH" if technical_score >= 55 and not pump_risk and plan_ok else "NO_TRADE"
            base["reason_ar"] = "تعذر تحليل AI أو تم بلوغ حد الطلبات؛ القرار المحلي تحفظي"
            _record_recommendation({**base, "time": time.time(), "technical_score": technical_score})
            _maybe_send_recommendation(base, price, send_alert, auto)
            return base
        
        change_txt = f", Change today {day_change:+.1f}%" if day_change_known else ""     # [FIX-4] الـAI كان أعمى عن الصعود اليومي
        prompt = f"""Analyze {symbol} trading data: Price {price:.2f}{change_txt}, RVOL {rvol:.1f}, RSI {rsi:.0f}, Score {technical_score}/100.
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
        ai_decision_raw = str(ai.get("decision", "WATCH")).upper()
        # [FIX-1] توحيد مفردات القرار: SELL/SHORT ⇒ AVOID (البوت long-only)، HOLD ⇒ WATCH.
        # قبل: SELL كانت تنزل لفرع WATCH (سكور فني ≥ 60) وتنطبع بخطة شراء.
        ai_decision, ai_bearish = _normalize_ai_decision(ai_decision_raw)
        confidence = max(0, min(int(ai.get("confidence", 0) or 0), 100))
        local_ok = (technical_score >= RECOMMENDATION_MIN_TECH_SCORE and
                    rvol >= RECOMMENDATION_MIN_RVOL and above_vwap and
                    risk_reward >= RECOMMENDATION_MIN_RR - RECOMMENDATION_RR_TOLERANCE and not pump_risk and
                    not base["invalidations"])
        
        if ai_decision == "AVOID":
            final_decision = "AVOID"
        elif ai_decision == "BUY" and confidence >= RECOMMENDATION_MIN_CONFIDENCE and local_ok:
            final_decision = "BUY"
        elif (ai_decision in ("BUY", "WATCH") or technical_score >= 60) and not pump_risk:
            final_decision = "WATCH"
        else:
            final_decision = "NO_TRADE"
            
        if final_decision in ("BUY", "WATCH") and not plan_ok:
            final_decision = "NO_TRADE"          # [FIX-1] بوابة الخطة الصلبة
        base.update({"ai_decision": ai_decision_raw, "ai_bearish": ai_bearish, "confidence": confidence,
                     "final_decision": final_decision, "setup": ai.get("setup", setup),
                     "risk_level": ai.get("risk_level", "HIGH"),
                     "reasons": ai.get("reasons", []), "reason_ar": ai.get("reason_ar", "")})
        base["invalidations"].extend(ai.get("invalidations", [])[:4])
        if ai_bearish:
            base["invalidations"].insert(0, "AI حكم هابط — البوت شراء فقط، لا توجد خطة دخول")
        _record_recommendation({**base, "time": time.time(), "technical_score": technical_score})
        _maybe_send_recommendation(base, price, send_alert, auto)
        return base
    except Exception as error:
        logger.error(f"Final recommendation error {symbol}: {error}")
        base["reason_ar"] = "حدث خطأ؛ تم إلغاء التوصية حفاظًا على رأس المال"
        return base


def _duration_and_speed_line(symbol, candidate, now_ts):
    """
    سطر لمضارب لحظي: من متى السهم على الرادار + بأي سرعة يتحرك الحين — يفرق بين
    فرصة بس بدأت وفرصة خلصت شوطها. first_seen موجود أصلًا بكل عنصر hot_watchlist،
    والسرعة من _price_hist (يتحدث كل دورة Discovery أصلًا، بدون طلب Yahoo إضافي).
    """
    first_seen = _safe_float(candidate.get("first_seen", now_ts))
    duration_min = max(0.0, (now_ts - first_seen) / 60.0)
    if duration_min < 1:
        since_str = "لحظات (أول رصد)"
    elif duration_min < 60:
        since_str = f"{duration_min:.0f} دقيقة"
    else:
        since_str = f"{duration_min / 60.0:.1f} ساعة"
    speed_str = ""
    try:
        with _price_hist_lock:
            hist = list(_price_hist.get(symbol, []))
        if len(hist) >= 2:
            ref = _ref_sample(hist, 900)  # ~15 دقيقة
            if ref:
                ref_price = _safe_float(ref[1])
                now_price = _safe_float(hist[-1][1])
                if ref_price > 0:
                    pct = (now_price - ref_price) / ref_price * 100.0
                    speed_str = f" | آخر ~15د: {pct:+.1f}%"
    except Exception as error:
        logger.debug(f"[DurationLine] {symbol}: {error}")
    return f"⏱️ على الرادار من: {since_str}{speed_str}"


def _send_hunter_style_alert(symbol, res, candidate, info_calls_box):
    """
    نسخة مدمجة من AUTO HUNTER القديم: تستخدم res الجاهز من حلقة Ranking (بدون
    تحميل أو score_setup إضافي)، تضيف فحص Short/Cap للسكور العالي فقط (حد 3
    طلبات .info بالدورة)، وترسل بنفس alert_gate الموحّد "RANK" — يعني بدون
    كولداون منفصل يخلي نفس السهم يوصل مرتين من ماسحين مختلفين.
    """
    score = int(res["score"])
    if score < 65:
        return False
    reasons = list(res["factors"])
    risks = []
    short_pct = 0.0
    float_shares = 0.0
    market_cap = _safe_float(candidate.get("market_cap"))
    if info_calls_box[0] < 3:
        info_calls_box[0] += 1
        try:
            time.sleep(1.0)
            info = yf.Ticker(symbol).info
            sp = info.get("shortPercentOfFloat")
            short_pct = float(sp) * 100 if sp is not None else 0.0
            mc = _safe_float(info.get("marketCap"))
            market_cap = mc or market_cap
            float_shares = _safe_float(info.get("floatShares"))
            cash = _safe_float(info.get("totalCash"))
            debt = _safe_float(info.get("totalDebt"))
            profit_margin = _safe_float(info.get("profitMargins")) * 100
            if short_pct > 20:
                score += 10; reasons.append(f"📉 Short {short_pct:.1f}%")
            if 0 < market_cap < 500_000_000:
                score += 5; reasons.append("🎯 Small Cap")
            # فلوت منخفض جدًا (زي MSGY: أسهم قليلة جدًا متاحة للتداول) = أي ضغط
            # شراء يحرك السعر بشكل غير متناسب — نمط سكوييز عنيف معروف تاريخيًا،
            # مو ضمان تكرر، بس يستاهل وزن إضافي بالسكور.
            if 0 < float_shares < 15_000_000:
                score += 12; reasons.append(f"🔥 فلوت منخفض جدًا ({float_shares/1e6:.1f}M سهم)")
            elif 0 < float_shares < 30_000_000:
                score += 6; reasons.append(f"⚡ فلوت منخفض ({float_shares/1e6:.1f}M سهم)")
            if cash < 50_000_000 and debt > cash:
                risks.append("💸 نقد منخفض"); score -= 10
            if profit_margin < 0:
                risks.append("📉 غير مربحة"); score -= 5
        except Exception as error:
            logger.debug(f"Hunter-alert info fetch error for {symbol}: {error}")
    score = max(0, min(100, score))
    if score < AUTO_HUNTER_MIN_SCORE:
        return False
    if not _alert_gate_allow(symbol, res["price"], "RANK"):
        return False

    fresh_price = _finnhub_current_price(symbol) or res["price"]

    cap_str = f"${market_cap:,.0f}" if market_cap > 0 else "N/A"
    short_str = f"{short_pct:.1f}%" if short_pct > 0 else "N/A"
    fm = res["fm"]
    msg = f"🎯 *AUTO HUNTER: {symbol}*\n━━━━━━━━━━━━━━━━\n"
    msg += f"📊 السكور: *{score}/100*\n💰 السعر: *${fresh_price:.4f}* | RVOL: *{res['rvol']:.1f}x*\n"
    msg += f"⚡ آخر 15د: *{fm['move_15m']:+.1f}%* | تسارع حجم: *{fm['vol_accel']:.1f}x*\n"
    msg += f"📉 الشورت: *{short_str}* | Cap: {cap_str}\n"
    msg += f"{_duration_and_speed_line(symbol, candidate, time.time())}\n"
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
        logger.debug(f"Hunter-alert daily indicators error for {symbol}: {error}")

    news_line = get_news_context_line(symbol)
    if news_line:
        msg += f"\n{news_line}\n"

    decision = "🟢 مناسب للمضاربة" if score >= 85 else ("🟡 مناسب بحذر" if score >= 75 else "🟠 عالي المخاطر")
    msg += f"\n━━━━━━━━━━━━━━━━\n📌 *القرار النهائي:* {decision}\n"

    # الأهداف الحين مبنية على التذبذب الفعلي للسهم (ATR اليومي، من نفس df_daily
    # المجلوب أصلًا فوق لـIchimoku/SuperTrend — بدون أي طلب إضافي)، مو نسبة
    # ثابتة نفسها لكل سهم. خبر مؤكد إيجابي (news_line) يوسّع هدف 2 أكثر —
    # حركة مدعومة بخبر تميل تستمر/تمتد أبعد من حركة فنية بحتة.
    try:
        atr = calculate_atr(df_daily) if df_daily is not None and not df_daily.empty else 0.0
    except Exception:
        atr = 0.0
    if atr and atr > 0:
        t2_mult = 6.0 if news_line else 5.0
        t1 = fresh_price + atr * 3.0
        t2 = fresh_price + atr * t2_mult
        stop_p = fresh_price - atr * 2.0
        pct1 = (t1 / fresh_price - 1) * 100
        pct2 = (t2 / fresh_price - 1) * 100
        pct_stop = (stop_p / fresh_price - 1) * 100
        basis = "تحليل + خبر مؤكد" if news_line else "تحليل فني (ATR)"
        msg += (f"\n🎯 *خطة التداول* (مبنية على {basis}):\n"
                f"   🎯 هدف 1: ${t1:.4f} ({pct1:+.0f}%)\n"
                f"   🎯 هدف 2: ${t2:.4f} ({pct2:+.0f}%)\n"
                f"   🛑 وقف: ${stop_p:.4f} ({pct_stop:+.0f}%)\n")
    else:
        msg += "\n⚠️ ما قدرت أحسب هدف مبني على تذبذب حقيقي لهذا السهم (بيانات يومية غير كافية) — راجع الشارت يدويًا قبل أي قرار.\n"
    msg += "\n⚠️ تحليل آلي وليست توصية مالية"

    send_telegram(msg)
    _journal_record(symbol, "RANK", fresh_price, {"score": score, "rvol": res.get("rvol")})
    _alert_gate_mark(symbol, res["price"], "RANK")
    return True


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
            info_calls_box = [0]  # حد 3 طلبات .info بالدورة، نفس حد Auto Hunter القديم
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
                    if int(res["score"]) >= 65:
                        try:
                            _send_hunter_style_alert(symbol, res, candidate, info_calls_box)
                        except Exception as hunter_error:
                            logger.debug(f"[Ranking] hunter-alert {symbol}: {hunter_error}")
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


def mega_mover_scanner():
    """
    Mega Mover: تنبيه توعية فقط — يمسك الأسهم اللي فعليًا فجّرت % كبير اليوم
    حتى لو فلتر الحيوية بالـRanking حكم عليها dead (وقفت/تراجعت شوي بعد
    الانفجار، زي WHLR/JAGX/GRML). بدون نقطة دخول ولا وقف، مرة وحدة باليوم
    لكل سهم، كولداون خاص بيه (مب alert_gate العادي) عشان ما يتعارض مع صرامة
    الدخول الفعلي بالـESE/Ranking. يقرأ من hot_watchlist الجاهز — بدون
    تحميل Yahoo إضافي للفحص الأساسي.
    """
    if not MEGA_MOVER_ENABLED:
        logger.info("ℹ️ Mega Mover disabled (MEGA_MOVER_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("mega_mover")
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(300)
                continue
            today_key = datetime.now().strftime("%Y-%m-%d")
            with state_lock:
                candidates = [dict(x) for x in state.get("hot_watchlist", []) if isinstance(x, dict)]
                sent_today = dict(state.get("mega_mover_sent", {}))
            sent_count = 0
            for c in candidates:
                symbol = str(c.get("symbol", "")).upper().strip()
                if not symbol:
                    continue
                change = _safe_float(c.get("change"))
                volume = _safe_float(c.get("volume"))
                if change < MEGA_MOVER_MIN_CHANGE or volume < MEGA_MOVER_MIN_VOLUME:
                    continue
                if _pump_alert_owns_move(change, volume):
                    continue        # [FIX-8] "PUMP مؤكد" يغطي هذي الحركة — رسالة وحدة بدل ثلاث
                gate_key = f"{symbol}_{today_key}"
                if sent_today.get(gate_key):
                    continue
                price = _finnhub_current_price(symbol) or _safe_float(c.get("price"))
                change = _fresh_change(c, price)
                if change < MEGA_MOVER_MIN_CHANGE:
                    continue
                if not _event_gate_allow(symbol, price, "MEGA"):
                    continue
                allowed_move, move_reason, move_regime, _drawdown = _bigmove_gate(
                    symbol, "MEGA", change, price, seed_peak=_seed_peak_for(symbol, c))
                if not allowed_move:
                    logger.info(f"[MegaMover] {symbol} suppressed by Move Registry: {move_reason}/{move_regime}")
                    continue
                msg = (f"🚀 *MEGA MOVER: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                       f"💰 السعر الحالي: *${price:.4f}{_price_src_label(symbol)}*\n📈 التغيّر اليوم: *+{change:.1f}%*\n"
                       f"📊 الحجم: {volume:,.0f}\n"
                       f"{_duration_and_speed_line(symbol, c, time.time())}\n")
                news_line = get_news_context_line(symbol)
                if news_line:
                    msg += f"\n{news_line}\n"
                stamp = _session_stamp(volume_note=True)
                if stamp:
                    msg += "\n" + stamp + "\n"
                msg += ("\n⚠️ *تنبيه معرفة فقط — السهم مكشوف/ممتد فعلًا، بدون نقطة دخول أو وقف محدد.*\n"
                        "قد يكون فاتك جزء من الحركة أو بدأ يتراجع — تأكد بنفسك قبل أي قرار.")
                send_telegram(msg)
                _event_gate_mark(symbol, price, "MEGA", "MEGA")
                with state_lock:
                    state.setdefault("mega_mover_sent", {})[gate_key] = time.time()
                    dw = state["mega_mover_sent"]
                    if len(dw) > 500:
                        keep = sorted(dw.keys(), key=lambda k: dw[k], reverse=True)[:500]
                        state["mega_mover_sent"] = {k: dw[k] for k in keep}
                    save_state()
                sent_today[gate_key] = time.time()
                sent_count += 1
                if sent_count >= 8:
                    break
                time.sleep(1)
            logger.info(f"[MegaMover] pool={len(candidates)} sent={sent_count}")
            time.sleep(MEGA_MOVER_INTERVAL)
        except Exception as error:
            logger.error(f"[MegaMover] error: {error}")
            time.sleep(60)


def _doji_support(df, doji_low, tol_pct=None):
    """
    [FIX-16] دعم تاريخي ملاصق لشمعة الدوجي: SMA50 / SMA200 (إغلاق يومي) أو أدنى قاع بآخر 60 يوم (بدون آخر 3 شموع).
    → (النوع، المستوى) لأقرب دعم يبعد عن قاع الدوجي أقل من tol_pct%، أو None لو الدوجي بمنتصف المسار السعري.
    """
    tol = (DOJI_SUPPORT_TOL_PCT if tol_pct is None else tol_pct) / 100.0
    closes = pd.to_numeric(df["close"], errors="coerce")
    lows = pd.to_numeric(df["low"], errors="coerce")
    levels = []
    if len(closes) >= 50:
        levels.append(("SMA50", float(closes.tail(50).mean())))
    if len(closes) >= 200:
        levels.append(("SMA200", float(closes.tail(200).mean())))
    prior = lows.iloc[-63:-3]
    if len(prior) >= 10:
        levels.append(("قاع 60 يوم", float(prior.min())))
    best = None
    for kind, level in levels:
        if level > 0 and doji_low > 0:
            gap = abs(doji_low / level - 1.0)
            if gap <= tol and (best is None or gap < best[2]):
                best = (kind, level, gap)
    return (best[0], best[1]) if best else None


def _doji_rvol(df):
    """
    [FIX-16] حجم شمعة اليوم الحالية مقابل متوسط 20 يوم، معدّل بنسبة الحجم المتوقعة لين الآن (منحنى _expected_volume_fraction)
    عشان شمعة نص النهار ما تظهر "ضعيفة" بس لأنها ما اكتملت. None لو البيانات ناقصة أو قبل الافتتاح (ما نحكم).
    """
    volumes = pd.to_numeric(df["volume"], errors="coerce")
    if len(volumes) < 22:
        return None
    average = float(volumes.iloc[-21:-1].mean())
    today_volume = float(volumes.iloc[-1])
    fraction = _expected_volume_fraction()
    if not (average > 0) or fraction <= 0 or not math.isfinite(today_volume):
        return None
    return today_volume / (average * fraction)


# ================= [FIX-18] أنماط يومية من نصيحة الـAI الثاني: القاع المزدوج + الابتلاع الشرائي عند دعم =================
# كانت ناقصة بالتسليم السابق (نفّذت الدوجي/A+/VWAP/العلم فقط). الاثنين يمشون على نفس شموع اليوم اللي يجلبها ماسح الدوجي
# (سنة يومية للمرشحين) — صفر طلبات شبكة جديدة. ما ينرسل شي إلا بعد اكتمال النمط (اختراق خط العنق / شمعة الابتلاع).
def _rsi_series(close, period=14):
    """RSI بطريقة Wilder (متوسط أُسّي alpha=1/period). NaN لأول period شمعة."""
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.where(avg_loss != 0)
    rsi = 100.0 - 100.0 / (1.0 + rs)
    return rsi.where(~((avg_loss == 0) & avg_gain.notna()), 100.0)


def _swing_lows(lows, k):
    """فهارس القيعان المحلية: الأدنى بنافذة ±k شمعة (وأقل من الشمعة اللي قبله — أول نقطة بالقاع المسطّح)."""
    values = [float(v) for v in lows]
    out = []
    for i in range(k, len(values) - k):
        window = values[i - k:i + k + 1]
        if values[i] <= min(window) and values[i] < values[i - 1]:
            out.append(i)
    return out


def _detect_double_bottom(df):
    """
    قاع مزدوج (W) مؤكد باختراق خط العنق، على شموع يومية (أعمدة صغيرة: open/high/low/close/volume).
    الشروط (كلها لازم):
      • قاعان بينهما DBOTTOM_MIN_GAP..DBOTTOM_MAX_GAP شمعة، والثاني ضمن ±DBOTTOM_TOL_PCT% من الأول.
      • خط العنق (أعلى قمة بينهما) فوق القاعين بعمق ≥ DBOTTOM_MIN_DEPTH_PCT%، وقبل القاع الأول بـ15 شمعة كان السهم أعلى بـDBOTTOM_MIN_PRIOR_DROP_PCT% (هبوط سابق).
      • دايفرجنس إيجابي: RSI(14) عند القاع الثاني أعلى من الأول بـ≥ DBOTTOM_MIN_RSI_DIV نقطة.
      • القاع الثاني ما انكسر بعدها، وأول إغلاق فوق خط العنق هو الشمعة الأخيرة (اختراق طازج) وما تعدّى +10% (ما نطارد).
      • حجم الاختراق ≥ DBOTTOM_MIN_RVOL × المعتاد (معدّل بالوقت). → dict أو None.
    الهدف = قياس عمق النموذج فوق خط العنق. الوقف = منتصف النموذج (لا أقل من أدنى قاع ×0.99): وقف تحت القاع كله يعطي R:R أقل
    من 1 دايمًا (الهدف والوقف كلاهما على بعد ارتفاع النموذج تقريبًا)؛ وكسر القاع نفسه = إلغاء النموذج.
    """
    try:
        d = df.tail(DBOTTOM_LOOKBACK).reset_index(drop=True)
        if len(d) < 50:
            return None
        high, low, close = (pd.to_numeric(d[c], errors="coerce") for c in ("high", "low", "close"))
        if high.isna().any() or low.isna().any() or close.isna().any():
            return None
        rsi = _rsi_series(close)
        m = len(d)
        pivots = _swing_lows(low, DBOTTOM_PIVOT_K)
        last_close = float(close.iloc[-1])
        for b in reversed(pivots):
            for a in pivots:
                gap = b - a
                if gap < DBOTTOM_MIN_GAP or gap > DBOTTOM_MAX_GAP:
                    continue
                l1, l2 = float(low.iloc[a]), float(low.iloc[b])
                if l1 <= 0 or abs(l2 / l1 - 1.0) > DBOTTOM_TOL_PCT / 100.0:
                    continue
                neck = float(high.iloc[a:b + 1].max())
                if neck < max(l1, l2) * (1.0 + DBOTTOM_MIN_DEPTH_PCT / 100.0):
                    continue
                pre = a - 15
                if pre < 0 or float(close.iloc[pre]) < l1 * (1.0 + DBOTTOM_MIN_PRIOR_DROP_PCT / 100.0):
                    continue
                r1, r2 = float(rsi.iloc[max(a - 1, 0):a + 2].min()), float(rsi.iloc[max(b - 1, 0):b + 2].min())
                if not (math.isfinite(r1) and math.isfinite(r2)) or r2 < r1 + DBOTTOM_MIN_RSI_DIV:
                    continue
                if b + 1 < m and float(low.iloc[b + 1:].min()) < l2 * 0.99:
                    continue
                if not (neck * 1.002 < last_close <= neck * 1.10):
                    continue
                if b + 1 < m - 1 and float(close.iloc[b + 1:m - 1].max()) > neck * 1.002:
                    continue
                rvol = _doji_rvol(df)
                if rvol is None or rvol < DBOTTOM_MIN_RVOL:
                    return None                  # اختراق بدون حجم = كسر كاذب محتمل (وقبل الافتتاح ما نحكم)
                bottom = min(l1, l2)
                height = neck - bottom
                stop, target = max(bottom * 0.99, neck - 0.5 * height), neck + height
                return {"invalidation": bottom * 0.99, "rvol": rvol, "low1": l1, "low2": l2, "bars_ago1": m - 1 - a, "bars_ago2": m - 1 - b, "neckline": neck,
                        "close": last_close, "rsi1": r1, "rsi2": r2, "stop": stop, "target": target,
                        "rr": (target - last_close) / max(last_close - stop, 1e-9), "gap_pct": abs(l2 / l1 - 1.0) * 100.0}
        return None
    except Exception as error:
        logger.debug(f"[Patterns] double bottom: {error}")
        return None


def _detect_bullish_engulfing(df):
    """
    ابتلاع شرائي عند دعم تاريخي: شمعة حمراء تليها خضراء يغطي جسمها جسم الحمراء بالكامل، بعد هبوط قصير،
    وقاع الشمعتين عند SMA50 أو SMA200 أو قاع 60 يوم (نفس _doji_support)، وحجم اليوم ≥ ENGULF_MIN_RVOL × المعتاد. → dict أو None.
    """
    try:
        if len(df) < 25:
            return None
        prev, cur = df.iloc[-2], df.iloc[-1]
        po, pc, co, cc = (float(prev["open"]), float(prev["close"]), float(cur["open"]), float(cur["close"]))
        if not (pc < po and cc > co and co <= pc and cc >= po):
            return None
        if float(df["close"].iloc[-2]) > float(df["close"].iloc[-5]) * 0.98:
            return None
        zone_low = min(float(prev["low"]), float(cur["low"]))
        support = _doji_support(df, zone_low)
        if support is None:
            return None
        rvol = _doji_rvol(df)
        if rvol is None or rvol < ENGULF_MIN_RVOL:
            return None
        return {"open": co, "close": cc, "prev_open": po, "prev_close": pc, "support_kind": support[0], "support_level": support[1],
                "rvol": rvol, "stop": zone_low * 0.99, "trigger": float(cur["high"])}
    except Exception as error:
        logger.debug(f"[Patterns] engulfing: {error}")
        return None


def _daily_pattern_messages(symbol, df, today_key):
    """
    رسائل الأنماط اليومية المكتملة لهذا السهم (قاع مزدوج / ابتلاع)، مع منع التكرار (حالة pattern_sent): قاع مزدوج مرة لكل
    خط عنق، وابتلاع مرة باليوم. → قائمة نصوص (فاضية غالبًا).
    """
    found = []
    if DBOTTOM_ENABLED:
        pattern = _detect_double_bottom(df)
        if pattern:
            found.append((f"{symbol}|DBOTTOM|{pattern['neckline']:.4f}", pattern, "dbottom"))
    if ENGULF_ENABLED:
        pattern = _detect_bullish_engulfing(df)
        if pattern:
            found.append((f"{symbol}|ENGULF|{today_key}", pattern, "engulf"))
    messages = []
    for key, pattern, kind in found:
        with state_lock:
            sent = state.setdefault("pattern_sent", {})
            if key in sent:
                continue
            sent[key] = today_key
            if len(sent) > 400:
                for old in sorted(sent, key=lambda k: sent[k])[:150]:
                    sent.pop(old, None)
        news_line = None
        try:
            news_line = get_news_context_line(symbol)
        except Exception:
            pass
        if kind == "dbottom":
            close = pattern["close"]
            msg = (f"🟣 *W-BOTTOM مؤكد: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                   f"📉 قاعان: ${pattern['low1']:.4f} (قبل {pattern['bars_ago1']} يوم) ثم ${pattern['low2']:.4f} (قبل {pattern['bars_ago2']} يوم) — الفرق {pattern['gap_pct']:.1f}%\n"
                   f"📈 اختراق خط العنق ${pattern['neckline']:.4f} — الإغلاق الحالي ${close:.4f}\n"
                   f"🧭 RSI: قاع1 {pattern['rsi1']:.0f} ← قاع2 {pattern['rsi2']:.0f} (دايفرجنس إيجابي)\n"
                   f"🔊 حجم الاختراق {pattern['rvol']:.1f}x المعتاد (المطلوب ≥{DBOTTOM_MIN_RVOL:.1f}x)\n"
                   f"🎯 هدف القياس: ${pattern['target']:.4f} ({(pattern['target'] / close - 1) * 100:+.1f}%) | "
                   f"🛑 وقف: ${pattern['stop']:.4f} ({(pattern['stop'] / close - 1) * 100:+.1f}%) | R:R {pattern['rr']:.1f}\n"
                   f"❌ يُلغى النموذج لو كسر القاع ${pattern['invalidation']:.4f}\n")
        else:
            msg = (f"🟢 *ابتلاع شرائي عند دعم: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                   f"🕯️ شمعة خضراء (فتح ${pattern['open']:.4f} ← إغلاق ${pattern['close']:.4f}) ابتلعت جسم الحمراء قبلها "
                   f"(${pattern['prev_open']:.4f} ← ${pattern['prev_close']:.4f})\n"
                   f"📍 عند دعم: {pattern['support_kind']} (${pattern['support_level']:.4f})\n"
                   f"🔊 حجم اليوم {pattern['rvol']:.1f}x المعتاد (المطلوب ≥{ENGULF_MIN_RVOL:.1f}x)\n"
                   f"🛑 وقف: تحت ${pattern['stop']:.4f} | تأكيد الدخول: فوق ${pattern['trigger']:.4f}\n")
        if news_line:
            msg += f"{news_line}\n"
        msg += "⚠️ نموذج يومي (Swing) مو لحظي. نسب النجاح المتداولة لهذه النماذج غير مثبتة — تأكد من السيولة والسياق قبل أي قرار."
        messages.append(msg)
    return messages


def doji_reversal_scanner():
    """
    دوجي بعد هبوط: شمعة يومية جسمها صغير جدًا بعد DOJI_MIN_DOWN_DAYS أيام هبوط متتالية = "مرشح" (يُتابَع بصمت).
    [FIX-16] بحسب النصيحة (الدوجي لحاله من أضعف نماذج الانعكاس):
      • ما نرسل تنبيه على الدوجي نفسه (DOJI_SEND_CANDIDATES=false افتراضيًا).
      • ما نتابع دوجي بمنتصف المسار السعري: لازم يلامس SMA50/SMA200 أو قاع 60 يوم (DOJI_REQUIRE_SUPPORT).
      • التأكيد: شمعة اليوم التالي خضراء وتغلق فوق قمة الدوجي وبحجم ≥ DOJI_CONFIRM_MIN_RVOL × المتوسط (معدّل بالوقت).
      • المتابعة ما تنحذف من أول فحص تحت القمة (كانت تنحذف فورًا): تُحذف فقط لو كسر قاع الدوجي أو مرّت DOJI_WATCH_MAX_DAYS أيام.
    """
    if not DOJI_ENABLED:
        logger.info("ℹ️ Doji reversal scanner disabled (DOJI_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("doji_reversal")
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(600)
                continue
            today_key = datetime.now().strftime("%Y-%m-%d")
            with state_lock:
                universe = set()
                for x in state.get("hot_watchlist", []):
                    if isinstance(x, dict) and x.get("symbol"):
                        universe.add(str(x["symbol"]).upper().strip())
                universe.update(str(s).upper().strip() for s in state.get("in_play", {}).keys())
                watch = dict(state.get("doji_watch", {}))
            symbols = list(universe)[:150]
            armed_now = confirmed_now = skipped_no_support = patterns_sent = 0
            for symbol in symbols:
                try:
                    df = cached_download(symbol, period=DOJI_HISTORY_PERIOD, interval="1d")
                    if df is None or df.empty or len(df) < DOJI_MIN_DOWN_DAYS + 2:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    if PATTERNS_ENABLED:
                        for pattern_message in _daily_pattern_messages(symbol, df, today_key):       # [FIX-18] قاع مزدوج / ابتلاع
                            send_telegram(pattern_message)
                            patterns_sent += 1
                    last = df.iloc[-1]
                    body = abs(_safe_number(last["close"]) - _safe_number(last["open"]))
                    rng = max(_safe_number(last["high"]) - _safe_number(last["low"]), 1e-6)
                    is_doji = (body / rng * 100.0) <= DOJI_BODY_MAX_PCT
                    prior = df.iloc[-1 - DOJI_MIN_DOWN_DAYS: -1]
                    down_run = bool(len(prior) == DOJI_MIN_DOWN_DAYS and (prior["close"] < prior["open"]).all())

                    rec = watch.get(symbol)
                    if rec and rec.get("armed_date") and rec["armed_date"] != today_key and not rec.get("sent"):
                        confirm_close = _safe_number(last["close"])
                        doji_high = _safe_float(rec.get("doji_high"))
                        doji_low = _safe_float(rec.get("doji_low"))
                        rvol = _doji_rvol(df)
                        green = confirm_close > _safe_number(last["open"])
                        if confirm_close > doji_high and green and rvol is not None and rvol >= DOJI_CONFIRM_MIN_RVOL:
                            msg = (f"✨ *تأكيد نمط دوجي: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                                   f"دوجي بتاريخ {rec['armed_date']} بعد {DOJI_MIN_DOWN_DAYS} أيام هبوط متتالية،\n"
                                   f"واليوم شمعة خضراء أغلقت فوق قمة الدوجي (${doji_high:.4f}) عند *${confirm_close:.4f}*.\n"
                                   f"🔊 حجم اليوم *{rvol:.1f}x* المعتاد (المطلوب ≥{DOJI_CONFIRM_MIN_RVOL:.1f}x)\n")
                            if rec.get("support_kind"):
                                msg += f"📍 الدوجي عند دعم: {rec['support_kind']} (${_safe_float(rec.get('support_level')):.4f})\n"
                            news_line = get_news_context_line(symbol)
                            if news_line:
                                msg += f"\n{news_line}\n"
                            msg += "\n⚠️ نمط انعكاسي واحد ما يعتبر ضمان صعود — تأكد من السيولة والسياق العام قبل أي قرار."
                            send_telegram(msg)
                            confirmed_now += 1
                            with state_lock:
                                state.setdefault("doji_watch", {})[symbol] = {**rec, "sent": True}
                        else:
                            try:
                                age_days = (datetime.strptime(today_key, "%Y-%m-%d") - datetime.strptime(str(rec["armed_date"]), "%Y-%m-%d")).days
                            except Exception:
                                age_days = DOJI_WATCH_MAX_DAYS
                            broke_low = doji_low > 0 and confirm_close < doji_low
                            if broke_low or age_days >= DOJI_WATCH_MAX_DAYS:
                                with state_lock:
                                    state.get("doji_watch", {}).pop(symbol, None)
                        continue

                    if is_doji and down_run and (not rec or rec.get("armed_date") != today_key):
                        doji_high = _safe_number(last["high"])
                        doji_low = _safe_number(last["low"])
                        support = _doji_support(df, doji_low)
                        if DOJI_REQUIRE_SUPPORT and support is None:
                            skipped_no_support += 1
                            continue
                        with state_lock:
                            state.setdefault("doji_watch", {})[symbol] = {
                                "armed_date": today_key, "doji_high": doji_high, "doji_low": doji_low, "sent": False,
                                "support_kind": support[0] if support else "", "support_level": support[1] if support else 0.0,
                            }
                        armed_now += 1
                        if DOJI_SEND_CANDIDATES:
                            msg = (f"🔎 *مرشح دوجي بعد هبوط: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                                   f"هبوط {DOJI_MIN_DOWN_DAYS} أيام متتالية، واليوم شمعة مفرغة (دوجي) عند ${_safe_number(last['close']):.4f}.\n"
                                   f"لسه بدون تأكيد — بنراقب: لو أغلق يوم بعده فوق ${doji_high:.4f} نرسل تأكيد.")
                            send_telegram(msg)
                except Exception as symbol_error:
                    logger.debug(f"[Doji] {symbol}: {symbol_error}")
                time.sleep(0.3)
            with state_lock:
                dw = state.get("doji_watch", {})
                if len(dw) > 300:
                    keep = sorted(dw.keys(), key=lambda k: dw[k].get("armed_date", ""), reverse=True)[:300]
                    state["doji_watch"] = {k: dw[k] for k in keep}
                save_state()
            logger.info(f"[Doji] scanned={len(symbols)} watching={len(dw)} armed_now={armed_now} confirmed_now={confirmed_now} "
                        f"no_support={skipped_no_support} patterns={patterns_sent}")
            time.sleep(DOJI_SCAN_INTERVAL)
        except Exception as error:
            logger.error(f"[Doji] scanner error: {error}")
            time.sleep(120)


def _record_recommendation(rec):
    # Finalize before persisting so history matches the sent plan.
    rec = _finalize_recommendation_plan(dict(rec))
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
    day_change = base.get("day_change")
    day_txt = f" | اليوم: {day_change:+.0f}%" if isinstance(day_change, (int, float)) else ""      # [FIX-4]
    msg += f"💰 السعر: ${price:.4f}{_price_src_label(symbol)} | RVOL: {rvol:.1f}x{day_txt}\n"
    stamp = _session_stamp()
    if stamp:
        msg += stamp + "\n"
    if decision in ("BUY", "WATCH"):
        msg += f"🎯 منطقة الدخول: ${entry_low:.4f} - ${entry_high:.4f}\n"
        msg += f"✅ تأكيد: ${confirm:.4f}\n"
        msg += f"🛑 الوقف: ${sl:.4f}\n"
        msg += f"🎯 الأهداف: ${t1:.4f} / ${t2:.4f}\n"
        msg += f"📐 R:R: {rr:.2f} | الصلاحية: 30 دقيقة\n━━━━━━━━━━━━━━━━\n"
    else:
        # [FIX-1] AVOID/NO_TRADE: لا نطبع أي خطة (دخول/وقف/أهداف) — كانت SELL تنطبع بخطة شراء
        why = "AI حكم هابط والبوت شراء فقط" if base.get("ai_bearish") else "شروط الدخول غير متحققة"
        msg += f"⛔ لا توجد خطة شراء ({why}) — تجنّب الدخول\n━━━━━━━━━━━━━━━━\n"
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
# 🤖 AUTO HUNTER — دُمج جوا hot_watchlist_scanner نفسها (2026-09-23).
# كان ماسح مستقل يعيد نفس تحميل/تسجيل hot_watchlist اللي تسويه Ranking
# بخيط منفصل، فيضاعف طلبات Yahoo. الإثراء (Short/Cap/Ichimoku/SuperTrend)
# الحين يصير على res الجاهز مباشرة، بنفس alert_gate الموحّد.
# ================================================================
AUTO_HUNTER_MIN_SCORE = 80

# ================================================================
# 🚀 MEGA MOVER — تنبيه توعية فقط للأسهم اللي فجّرت % كبير اليوم حتى لو
# فلتر الحيوية بالـRanking حكم عليها dead (وقفت/تراجعت شوي بعد الانفجار).
# بدون نقطة دخول ولا وقف — للمعرفة فقط. مرة وحدة باليوم لكل سهم.
# ================================================================
MEGA_MOVER_ENABLED = os.getenv("MEGA_MOVER_ENABLED", "true").strip().lower() == "true"
MEGA_MOVER_MIN_CHANGE = float(os.getenv("MEGA_MOVER_MIN_CHANGE", "50"))
MEGA_MOVER_MIN_VOLUME = float(os.getenv("MEGA_MOVER_MIN_VOLUME", "300000"))
MEGA_MOVER_INTERVAL = int(os.getenv("MEGA_MOVER_INTERVAL", "90"))

# ================================================================
# ✨ DOJI REVERSAL — شمعة يومية مفرغة (جسم صغير) بعد هبوط متكرر، تحتاج
# تأكيد باليوم اللي بعده (إغلاق فوق قمة الدوجي) قبل ما تتحول لتنبيه فعلي.
# ================================================================
DOJI_ENABLED = os.getenv("DOJI_ENABLED", "true").strip().lower() == "true"
DOJI_MIN_DOWN_DAYS = int(os.getenv("DOJI_MIN_DOWN_DAYS", "2"))
DOJI_BODY_MAX_PCT = float(os.getenv("DOJI_BODY_MAX_PCT", "12"))
DOJI_SCAN_INTERVAL = int(os.getenv("DOJI_SCAN_INTERVAL", "1800"))
DOJI_SEND_CANDIDATES = os.getenv("DOJI_SEND_CANDIDATES", "false").strip().lower() == "true"     # تنبيه على الدوجي نفسه (ضعيف) — مغلق
DOJI_REQUIRE_SUPPORT = os.getenv("DOJI_REQUIRE_SUPPORT", "true").strip().lower() == "true"      # لازم دعم تاريخي (SMA50/200 أو قاع 60 يوم)
DOJI_SUPPORT_TOL_PCT = float(os.getenv("DOJI_SUPPORT_TOL_PCT", "3"))                             # قرب قاع الدوجي من الدعم (%)
DOJI_CONFIRM_MIN_RVOL = float(os.getenv("DOJI_CONFIRM_MIN_RVOL", "3"))                           # حجم شمعة التأكيد مقابل المعتاد
DOJI_WATCH_MAX_DAYS = int(os.getenv("DOJI_WATCH_MAX_DAYS", "3"))                                 # أيام انتظار التأكيد
DOJI_HISTORY_PERIOD = os.getenv("DOJI_HISTORY_PERIOD", "1y")                                     # سنة عشان SMA200
PATTERNS_ENABLED = os.getenv("PATTERNS_ENABLED", "true").strip().lower() == "true"                # [FIX-18] أنماط يومية بنفس شموع ماسح الدوجي
DBOTTOM_ENABLED = os.getenv("DBOTTOM_ENABLED", "true").strip().lower() == "true"                  # القاع المزدوج (W) مؤكد باختراق خط العنق
DBOTTOM_LOOKBACK = int(os.getenv("DBOTTOM_LOOKBACK", "100"))                                      # كم شمعة يومية نفحص
DBOTTOM_PIVOT_K = int(os.getenv("DBOTTOM_PIVOT_K", "3"))                                          # القاع المحلي = الأدنى بنافذة ±k
DBOTTOM_MIN_GAP = int(os.getenv("DBOTTOM_MIN_GAP", "10"))                                         # أقل مسافة بين القاعين (شمعة)
DBOTTOM_MAX_GAP = int(os.getenv("DBOTTOM_MAX_GAP", "45"))
DBOTTOM_TOL_PCT = float(os.getenv("DBOTTOM_TOL_PCT", "3"))                                        # فرق القاعين (%)
DBOTTOM_MIN_DEPTH_PCT = float(os.getenv("DBOTTOM_MIN_DEPTH_PCT", "8"))                            # ارتفاع خط العنق فوق القاعين (%)
DBOTTOM_MIN_PRIOR_DROP_PCT = float(os.getenv("DBOTTOM_MIN_PRIOR_DROP_PCT", "8"))                    # كم كان السهم أعلى قبل القاع الأول (هبوط سابق)
DBOTTOM_MIN_RSI_DIV = float(os.getenv("DBOTTOM_MIN_RSI_DIV", "3"))                                # دايفرجنس RSI (نقاط)
DBOTTOM_MIN_RVOL = float(os.getenv("DBOTTOM_MIN_RVOL", "1.5"))                                    # حجم الاختراق مقابل المعتاد
ENGULF_ENABLED = os.getenv("ENGULF_ENABLED", "true").strip().lower() == "true"                    # ابتلاع شرائي عند دعم
ENGULF_MIN_RVOL = float(os.getenv("ENGULF_MIN_RVOL", "2.5"))

# ================================================================
# 🚀 HIGH BREAKOUT — اختراق قمة N يوم بحجم مؤكد (momentum، عكس فلسفة الدوجي
# الانعكاسية): يمسك زخم صاعد مستمر بدل انتظار ارتداد بعد هبوط.
# ================================================================
HIGH_BREAKOUT_ENABLED       = os.getenv("HIGH_BREAKOUT_ENABLED", "true").strip().lower() == "true"
HIGH_BREAKOUT_LOOKBACK_DAYS = int(os.getenv("HIGH_BREAKOUT_LOOKBACK_DAYS", "20"))
HIGH_BREAKOUT_MIN_RVOL      = float(os.getenv("HIGH_BREAKOUT_MIN_RVOL", "2.0"))
HIGH_BREAKOUT_SCAN_INTERVAL = int(os.getenv("HIGH_BREAKOUT_SCAN_INTERVAL", "900"))


def high_breakout_scanner():
    """
    اختراق قمة آخر HIGH_BREAKOUT_LOOKBACK_DAYS يوم (افتراضي 20) بحجم RVOL
    مؤكد — نمط استمراري (momentum) عكس الدوجي الانعكاسي تمامًا: هنا نركب
    مع القوة الصاعدة من بدايتها بدل انتظار ارتداد بعد ضعف.
    """
    if not HIGH_BREAKOUT_ENABLED:
        logger.info("ℹ️ High breakout scanner disabled (HIGH_BREAKOUT_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("high_breakout")
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(600)
                continue
            today_key = datetime.now().strftime("%Y-%m-%d")
            with state_lock:
                universe = set()
                for x in state.get("hot_watchlist", []):
                    if isinstance(x, dict) and x.get("symbol"):
                        universe.add(str(x["symbol"]).upper().strip())
                universe.update(str(s).upper().strip() for s in state.get("in_play", {}).keys())
                sent_today = dict(state.get("high_breakout_sent", {}))
            symbols = list(universe)[:150]
            sent_count = 0
            for symbol in symbols:
                try:
                    gate_key = f"{symbol}_{today_key}"
                    if gate_key in sent_today:
                        continue
                    df = cached_download(symbol, period="2mo", interval="1d")
                    if df is None or df.empty or len(df) < HIGH_BREAKOUT_LOOKBACK_DAYS + 2:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    df = compute_indicators(df)  # لـbandwidth/ema9/ema21 — لتصنيف جودة الاختراق تحت
                    prior = df.iloc[-1 - HIGH_BREAKOUT_LOOKBACK_DAYS: -1]
                    if len(prior) < HIGH_BREAKOUT_LOOKBACK_DAYS:
                        continue
                    prior_high = float(prior["high"].max())
                    last = df.iloc[-1]
                    last_close = _safe_number(last["close"])
                    if prior_high <= 0 or last_close <= prior_high:
                        continue  # ما اخترق بعد

                    # تأكيد حجم — نفس معيار RVOL المستخدم بكل مكان ثاني بالبوت
                    avg_vol = float(prior["volume"].mean()) if "volume" in prior else 0.0
                    today_vol = _safe_number(last.get("volume", 0))
                    rvol = (today_vol / avg_vol) if avg_vol > 0 else 0.0
                    if rvol < HIGH_BREAKOUT_MIN_RVOL:
                        continue  # اختراق بدون حجم كافٍ = مشكوك فيه، نتجاهله

                    # فكرة "انضغاط التذبذب قبل الاختراق" — مأخوذة من مشروع مفتوح المصدر
                    # (StockTradingBot)، تستخدم bandwidth الموجود أصلاً بـcompute_indicators
                    # (بدون أي حساب/طلب إضافي): لو عرض بولنجر باند قبل الاختراق كان أضيق من
                    # متوسطه المعتاد، هذا اختراق خارج من انضغاط حقيقي (جودة أعلى تاريخيًا من
                    # اختراق عشوائي بدون تمهيد). EMA9>EMA21 يتأكد إننا مو نكسر ترند هابط.
                    was_squeeze = False
                    trend_up = False
                    try:
                        bw_prior = prior["bandwidth"] if "bandwidth" in prior else None
                        if bw_prior is not None and len(bw_prior.dropna()) >= 5:
                            was_squeeze = bool(bw_prior.iloc[-1] < bw_prior.mean())
                        trend_up = bool(_safe_float(last.get("ema9")) > _safe_float(last.get("ema21")))
                    except Exception:
                        pass
                    quality_tag = "🎯 اختراق بعد انضغاط تذبذب حقيقي (جودة أعلى)" if was_squeeze else "📈 اختراق عادي (بدون انضغاط تمهيدي واضح)"
                    if not trend_up:
                        quality_tag += " ⚠️ لكن الترند اليومي لسه مو صاعد رسميًا (EMA9<EMA21)"

                    pct_above = (last_close - prior_high) / prior_high * 100.0
                    msg = (f"🚀 *اختراق قمة {HIGH_BREAKOUT_LOOKBACK_DAYS} يوم: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                           f"💰 السعر: *${last_close:.4f}* (فوق قمة {HIGH_BREAKOUT_LOOKBACK_DAYS} يوم بـ{pct_above:+.1f}%)\n"
                           f"📊 RVOL: *{rvol:.1f}x* (القمة السابقة: ${prior_high:.4f})\n"
                           f"{quality_tag}\n")
                    news_line = get_news_context_line(symbol)
                    if news_line:
                        msg += f"\n{news_line}\n"
                    msg += "\n⚠️ نمط استمراري (momentum) — اختراق بحجم لا يضمن استمرار الصعود، تأكد من السياق العام."
                    send_telegram(msg)
                    with state_lock:
                        state.setdefault("high_breakout_sent", {})[gate_key] = True
                    sent_count += 1
                    if sent_count >= 8:
                        break
                except Exception as symbol_error:
                    logger.debug(f"[HighBreakout] {symbol}: {symbol_error}")
                time.sleep(0.3)
            with state_lock:
                hb = state.get("high_breakout_sent", {})
                if len(hb) > 500:
                    keep = sorted(hb.keys())[-500:]
                    state["high_breakout_sent"] = {k: True for k in keep}
                save_state()
            logger.info(f"[HighBreakout] scanned={len(symbols)} sent={sent_count}")
            time.sleep(HIGH_BREAKOUT_SCAN_INTERVAL)
        except Exception as error:
            logger.error(f"[HighBreakout] scanner error: {error}")
            time.sleep(120)


# ================================================================
# 🕳️ GAP HUNTER — تنبيه فوري لأي سهم فجوته (٪ عن إغلاق أمس) قوية، بدون
# شرط يعدّي فلتر الـRanking/AI — نفس فلسفة Mega Mover بس على فجوات أخف
# (20%+ بدل 50%+). يقرأ change من hot_watchlist الجاهز، بدون تحميل إضافي.
# ================================================================
GAP_HUNTER_ENABLED       = os.getenv("GAP_HUNTER_ENABLED", "true").strip().lower() == "true"
GAP_HUNTER_MIN_CHANGE    = float(os.getenv("GAP_HUNTER_MIN_CHANGE", "20"))      # % أدنى فجوة لإرسال تنبيه
GAP_HUNTER_STRONG_CHANGE = float(os.getenv("GAP_HUNTER_STRONG_CHANGE", "25"))   # % وفوقها = وسم "قوية جدًا" 🔥🔥
GAP_HUNTER_MIN_VOLUME    = float(os.getenv("GAP_HUNTER_MIN_VOLUME", "100000"))  # أدنى سيولة (حجم) لتفادي الأسهم الميتة
GAP_HUNTER_INTERVAL      = int(os.getenv("GAP_HUNTER_INTERVAL", "45"))          # كل كم ثانية يفحص hot_watchlist
GAP_HUNTER_RESEND_DELTA  = float(os.getenv("GAP_HUNTER_RESEND_DELTA", "5"))    # إعادة إرسال لو الفجوة كبرت بهالقد عن آخر تنبيه لنفس السهم اليوم
GAP_HUNTER_MAX_PER_CYCLE = int(os.getenv("GAP_HUNTER_MAX_PER_CYCLE", "8"))      # سقف تنبيهات لكل دورة فحص
GAP_FOLLOWUP_ENABLED     = os.getenv("GAP_FOLLOWUP_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
GAP_FOLLOWUP_CONT_PCT    = float(os.getenv("GAP_FOLLOWUP_CONT_PCT", "5"))       # استمرار من سعر أول تنبيه
GAP_FOLLOWUP_STOP_PCT    = float(os.getenv("GAP_FOLLOWUP_STOP_PCT", "6"))       # ضعف/إلغاء من سعر أول تنبيه
GAP_FOLLOWUP_DUMP_PCT    = float(os.getenv("GAP_FOLLOWUP_DUMP_PCT", "10"))      # هبوط من القمة أثناء المتابعة
GAP_FOLLOWUP_MAX_HOURS   = float(os.getenv("GAP_FOLLOWUP_MAX_HOURS", "8"))
SURGE_ALERT_ENABLED      = os.getenv("SURGE_ALERT_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
SURGE_ALERT_MIN_CHANGE   = float(os.getenv("SURGE_ALERT_MIN_CHANGE", "25"))
SURGE_ALERT_MAX_CHANGE   = float(os.getenv("SURGE_ALERT_MAX_CHANGE", "50"))
SURGE_ALERT_MIN_VOLUME   = float(os.getenv("SURGE_ALERT_MIN_VOLUME", "100000"))
SURGE_ALERT_MAX_PER_DAY  = int(os.getenv("SURGE_ALERT_MAX_PER_DAY", "1"))

def _gap_followup_cycle(candidates, now_ts):
    """متابعة خفيفة من بيانات الرادار: استمرار، إلغاء، وتصريف بدون طلبات HTTP إضافية."""
    if not GAP_FOLLOWUP_ENABLED:
        return
    by_symbol = {str(c.get("symbol", "")).upper().strip(): c for c in candidates if c.get("symbol")}
    with state_lock:
        followups = {str(k).upper(): dict(v) for k, v in state.get("gap_followups", {}).items()
                     if isinstance(v, dict)}
    for symbol, item in list(followups.items()):
        if now_ts - _safe_float(item.get("ts")) > GAP_FOLLOWUP_MAX_HOURS * 3600:
            followups.pop(symbol, None)
            continue
        candidate = by_symbol.get(symbol)
        if not candidate:
            continue
        price, _source, _age = _gap_live_quote(symbol, candidate, now_ts)
        if price <= 0:
            continue
        entry = _safe_float(item.get("entry_price"))
        peak = max(_safe_float(item.get("peak_price"), entry), price)
        item["peak_price"] = peak
        if entry <= 0:
            continue
        move = (price / entry - 1.0) * 100.0
        from_peak = (peak - price) / peak * 100.0 if peak > 0 else 0.0
        if not item.get("continuation_sent") and move >= GAP_FOLLOWUP_CONT_PCT:
            if not _event_gate_allow(symbol, price, "MOMENTUM"):
                continue
            item["continuation_sent"] = True
            send_telegram(
                f"🟢 *GAP متابعة استمرار: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                f"💰 السعر: *${price:.4f}* | من أول رصد: *+{move:.1f}%*\n"
                f"✅ تجاوز استمرار +{GAP_FOLLOWUP_CONT_PCT:.0f}%\n"
                f"🎯 هدف متابعة نظري: *${entry * TARGET_PROFIT_MULTIPLIER:.4f}* (+{TARGET_PROFIT_PCT:.0f}% من أول رصد)\n"
                f"🛑 مستوى إلغاء المتابعة: *${entry * (1 - GAP_FOLLOWUP_STOP_PCT / 100):.4f}* (-{GAP_FOLLOWUP_STOP_PCT:.0f}%)\n"
                f"⚠️ استمرار وليس شراءً آليًا؛ لا تطارد السهم بعد انفجاره."
            )
            _event_gate_mark(symbol, price, "MOMENTUM", "GAP_FOLLOWUP")
        if not item.get("weakness_sent") and price <= entry * (1 - GAP_FOLLOWUP_STOP_PCT / 100):
            if not _event_gate_allow(symbol, price, "EXIT"):
                continue
            item["weakness_sent"] = True
            send_telegram(
                f"⚠️ *GAP ضعف/إلغاء: {symbol}*\n"
                f"السعر ${price:.4f} كسر مستوى المتابعة ${entry * (1 - GAP_FOLLOWUP_STOP_PCT / 100):.4f}\n"
                f"لا يوجد دخول مؤكد — ألغِ المتابعة ولا تطارد الارتداد."
            )
            _event_gate_mark(symbol, price, "EXIT", "GAP_FOLLOWUP")
        if not item.get("dump_sent") and from_peak >= GAP_FOLLOWUP_DUMP_PCT:
            if not _event_gate_allow(symbol, price, "DUMP"):
                continue
            item["dump_sent"] = True
            send_telegram(
                f"🔴 *GAP تصريف محتمل: {symbol}*\n"
                f"هبط *-{from_peak:.1f}%* من القمة ${peak:.4f} إلى ${price:.4f}\n"
                f"الخروج/الحذر أهم من الدخول الآن — لا تعتبرها إشارة شراء."
            )
            _event_gate_mark(symbol, price, "DUMP", "GAP_FOLLOWUP")
    with state_lock:
        state["gap_followups"] = followups
        save_state()


def gap_hunter_scanner():
    """
    🕳️ Gap Hunter: يفحص hot_watchlist الجاهز ويرسل تنبيه فوري لأي سهم فجوته
    (change% عن إغلاق أمس — Yahoo regularMarket/preMarket/postMarketChangePercent
    أصلًا) GAP_HUNTER_MIN_CHANGE أو أكثر، بدون انتظار موافقة الـRanking/AI —
    طبقة توعية إضافية زي Mega Mover بالظبط بس على فجوات أخف (20%+ بدل 50%+)
    وبدون أي تحميل Yahoo إضافي (يقرأ من الحقل الجاهز مباشرة).
    مرة وحدة باليوم لكل سهم، مع إعادة إرسال لو الفجوة كبرت GAP_HUNTER_RESEND_DELTA
    نقطة إضافية عن آخر تنبيه (فجوة تكبر من 22% لـ 40% تستاهل تنبيه جديد).
    """
    if not GAP_HUNTER_ENABLED:
        logger.info("ℹ️ Gap Hunter disabled (GAP_HUNTER_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("gap_hunter")
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(300)
                continue
            today_key = datetime.now().strftime("%Y-%m-%d")
            with state_lock:
                candidates = [dict(x) for x in state.get("hot_watchlist", []) if isinstance(x, dict)]
                sent_today = dict(state.get("gap_hunter_sent", {}))
                surge_today = dict(state.get("surge_alerts", {}))
            _gap_followup_cycle(candidates, time.time())
            sent_count = 0
            surge_count = 0
            for c in candidates:
                symbol = str(c.get("symbol", "")).upper().strip()
                if not symbol:
                    continue
                change = _safe_float(c.get("change"))
                volume = _safe_float(c.get("volume"))
                # مرحلة بين EARLY وPUMP: نلتقط بداية الانفجار قبل أن يصل +50%.
                # لا نفتح صفقة آلية؛ نرسل خطة متابعة مرة واحدة لكل سهم في اليوم.
                surge_key = f"{symbol}_{today_key}"
                if (SURGE_ALERT_ENABLED and surge_count < SURGE_ALERT_MAX_PER_DAY
                        and SURGE_ALERT_MIN_CHANGE <= change < SURGE_ALERT_MAX_CHANGE
                        and volume >= SURGE_ALERT_MIN_VOLUME
                        and surge_key not in surge_today):
                    surge_price, surge_source, surge_age = _gap_live_quote(symbol, c)
                    live_surge_change = _fresh_change(c, surge_price) if surge_price > 0 else change
                    momentum = max(_safe_float(c.get("momentum_5m")), _safe_float(c.get("momentum_1m")))
                    if surge_price > 0 and SURGE_ALERT_MIN_CHANGE <= live_surge_change < SURGE_ALERT_MAX_CHANGE and momentum >= 2.0:
                        surge_entry = surge_price
                        surge_target = surge_entry * TARGET_PROFIT_MULTIPLIER
                        surge_stop = surge_entry * (1 - GAP_FOLLOWUP_STOP_PCT / 100.0)
                        surge_msg = (
                            f"🚀 *SURGE — بداية انفجار قبل PUMP: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                            f"📈 الارتفاع الحالي: *+{live_surge_change:.1f}%* (قبل حد PUMP +{SURGE_ALERT_MAX_CHANGE:.0f}%)\n"
                            f"💰 سعر المتابعة: *${surge_entry:.4f}* ({surge_source}، قبل {surge_age:.0f}ث)\n"
                            f"⚡ زخم آخر 5د: *{momentum:+.1f}%*\n"
                            f"🎯 الهدف المستهدف: *${surge_target:.4f}* (+{TARGET_PROFIT_PCT:.0f}%)\n"
                            f"🛑 وقف المتابعة: *${surge_stop:.4f}* (-{GAP_FOLLOWUP_STOP_PCT:.0f}%)\n"
                            f"⏰ وقت الرصد: *{saudi_time_str()} 🇸🇦*\n"
                            f"📌 إذا تجاوز +{SURGE_ALERT_MAX_CHANGE:.0f}% سيصل PUMP، لكن لا تطارده بعد الانفجار.\n"
                            f"⚠️ هذه *خطة متابعة مبكرة وليست شراءً آليًا أو ضمان ربح*؛ الحركة قد تنعكس بسرعة."
                        )
                        if not _event_gate_allow(symbol, surge_entry, "SURGE"):
                            continue
                        send_telegram(surge_msg)
                        _event_gate_mark(symbol, surge_entry, "SURGE", "SURGE")
                        surge_today[surge_key] = {"ts": time.time(), "price": surge_entry, "change": live_surge_change}
                        surge_count += 1
                if change < GAP_HUNTER_MIN_CHANGE or volume < GAP_HUNTER_MIN_VOLUME:
                    continue
                if _pump_alert_owns_move(change, volume):
                    continue        # [FIX-8] "PUMP مؤكد" يغطي هذي الحركة — رسالة وحدة بدل ثلاث
                gate_key = f"{symbol}_{today_key}"
                prev = sent_today.get(gate_key)
                prev_change = _safe_float(prev.get("change")) if isinstance(prev, dict) else 0.0
                price, price_source, price_age = _gap_live_quote(symbol, c)
                if price <= 0:
                    continue
                change = _fresh_change(c, price)
                if change < GAP_HUNTER_MIN_CHANGE:
                    continue
                resend_threshold = _gap_resend_threshold(prev_change, phase)
                if prev and change < prev_change + resend_threshold:
                    continue
                if not _event_gate_allow(symbol, price, "GAP"):
                    continue
                allowed_move, move_reason, move_regime, _drawdown = _bigmove_gate(
                    symbol, "GAP", change, price, seed_peak=_seed_peak_for(symbol, c))
                if not allowed_move:
                    logger.info(f"[GapHunter] {symbol} suppressed by Move Registry: {move_reason}/{move_regime}")
                    continue
                avg_volume = _safe_float(c.get("avg_volume"))
                rvol_line = f" ({volume / avg_volume:.1f}x المعدل)" if avg_volume > 0 else ""
                is_strong = change >= GAP_HUNTER_STRONG_CHANGE
                tag = "🔥🔥 *فجوة قوية جدًا*" if is_strong else "🔥 *فجوة قوية*"
                msg = (f"🕳️ *GAP HUNTER: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                       f"{tag}\n"
                       f"💰 السعر الحالي: *${price:.4f}* ({price_source}، قبل {price_age:.0f}ث)\n"
                       f"📈 الفجوة عن إغلاق أمس: *+{change:.1f}%*\n"
                       f"📊 الحجم: {volume:,.0f}{rvol_line}\n"
                       f"{_duration_and_speed_line(symbol, c, time.time())}\n")
                if prev:
                    msg += f"🔁 تحديث: الفجوة كبرت من +{prev_change:.1f}% إلى +{change:.1f}%\n"
                news_line = get_news_context_line(symbol)
                if news_line:
                    msg += f"\n{news_line}\n"
                stamp = _session_stamp(volume_note=True)
                if stamp:
                    msg += "\n" + stamp + "\n"
                msg += ("\n⚠️ *تنبيه فجوة فقط — بدون نقطة دخول أو وقف محدد.*\n"
                        "الفجوات القوية (خصوصًا بدون خبر واضح) متقلبة جدًا — تأكد من السيولة والسياق قبل أي قرار.\n"
                        f"📡 المتابعة: استمرار +{GAP_FOLLOWUP_CONT_PCT:.0f}% | إلغاء -{GAP_FOLLOWUP_STOP_PCT:.0f}% | "
                        f"تصريف من القمة -{GAP_FOLLOWUP_DUMP_PCT:.0f}%")
                send_telegram(msg)
                _event_gate_mark(symbol, price, "GAP", "GAP")
                entry = {"change": change, "ts": time.time(), "resend_threshold": resend_threshold}
                with state_lock:
                    state.setdefault("gap_hunter_sent", {})[gate_key] = entry
                    state.setdefault("gap_followups", {})[symbol] = {
                        "entry_price": price, "peak_price": price, "ts": entry["ts"],
                        "continuation_sent": False, "weakness_sent": False, "dump_sent": False,
                    }
                    dw = state["gap_hunter_sent"]
                    if len(dw) > 500:
                        keep = sorted(dw.keys(),
                                      key=lambda k: _safe_float(dw[k].get("ts")) if isinstance(dw[k], dict) else 0.0,
                                      reverse=True)[:500]
                        state["gap_hunter_sent"] = {k: dw[k] for k in keep}
                    save_state()
                sent_today[gate_key] = entry
                sent_count += 1
                if sent_count >= GAP_HUNTER_MAX_PER_CYCLE:
                    break
                time.sleep(1)
            with state_lock:
                state["surge_alerts"] = surge_today
                if len(state["surge_alerts"]) > 1000:
                    state["surge_alerts"] = dict(list(state["surge_alerts"].items())[-1000:])
                save_state()
            logger.info(f"[GapHunter] pool={len(candidates)} sent={sent_count} surge={surge_count}")
            time.sleep(GAP_HUNTER_INTERVAL)
        except Exception as error:
            logger.error(f"[GapHunter] error: {error}")
            time.sleep(60)


ACCUMULATION_ENABLED = os.getenv("ACCUMULATION_ENABLED", "true").strip().lower() == "true"
ACCUMULATION_SCAN_INTERVAL = int(os.getenv("ACCUMULATION_SCAN_INTERVAL", "600"))     # كل 10 دقايق
ACCUMULATION_MAX_SYMBOLS = int(os.getenv("ACCUMULATION_MAX_SYMBOLS", "150"))
ACCUMULATION_MIN_SIGNALS = int(os.getenv("ACCUMULATION_MIN_SIGNALS", "2"))           # كم كاشف لازم يتوافق سوا (من 3)
ACCUMULATION_EXPLOSION_PCT = float(os.getenv("ACCUMULATION_EXPLOSION_PCT", "50"))    # % الانفجار الحقيقي للتأكيد
ACCUMULATION_MAX_WATCH_DAYS = float(os.getenv("ACCUMULATION_MAX_WATCH_DAYS", "5"))   # تنتهي المتابعة بعد كم يوم
ACCUMULATION_MAX_PER_CYCLE = int(os.getenv("ACCUMULATION_MAX_PER_CYCLE", "6"))


def accumulation_tracker_scanner():
    """
    🐋 Accumulation Tracker: يرصد "تجميع" (دخول هادئ بحجم عالٍ قبل الانفجار)
    بدمج 3 كاشفات موجودة ومُختبرة بالكود أصلًا (detect_whale_accumulation +
    detect_accumulation + detect_silent_accumulation — نفس المستخدمة داخل
    محرك التحليل الرئيسي process_symbol، ما فيه أي منطق كشف جديد هنا، فقط
    طبقة تنبيه+تتبع لم تكن موجودة). يتطلب توافق ACCUMULATION_MIN_SIGNALS
    منها سوا (تقاطع لتقليل الإنذارات الكاذبة، مو كاشف وحيد). أول ما يتوافق:
    تنبيه "تجميع مكتشف" (رصد مبكر فقط، بدون أي دخول)، ثم يتابع السهم لغاية
    ACCUMULATION_MAX_WATCH_DAYS أيام — لو فعلاً انفجر ≥ACCUMULATION_EXPLOSION_PCT%
    (افتراضي 50%) من سعر الرصد يرسل تنبيه تأكيد؛ لو ما صار، تنتهي المتابعة
    بصمت بدون إزعاج. يستخدم نفس (period=5d, interval=15m) اللي يستخدمها
    process_symbol أصلًا — أغلب الطلبات Cache HIT (TTL خمس دقايق) بدل ما
    تضيف حمل يذكر على Yahoo.
    """
    if not ACCUMULATION_ENABLED:
        logger.info("ℹ️ Accumulation tracker disabled (ACCUMULATION_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("accumulation")
            if get_market_phase() == "CLOSED":
                time.sleep(300)
                continue
            with state_lock:
                universe = set(state.get("tickers", []))
                for x in state.get("hot_watchlist", []):
                    if isinstance(x, dict) and x.get("symbol"):
                        universe.add(str(x["symbol"]).upper().strip())
                watch = dict(state.get("accumulation_watch", {}))
            symbols = list(universe)[:ACCUMULATION_MAX_SYMBOLS]
            now_ts = time.time()

            still_watching = {}
            for symbol, entry in watch.items():
                if not isinstance(entry, dict):
                    continue
                age_days = (now_ts - _safe_float(entry.get("armed_ts", now_ts))) / 86400.0
                if age_days > ACCUMULATION_MAX_WATCH_DAYS:
                    continue
                base_price = _safe_float(entry.get("armed_price"))
                price_now = _finnhub_current_price(symbol) or 0.0
                if price_now > 0 and base_price > 0:
                    gain = (price_now - base_price) / base_price * 100.0
                    if gain >= ACCUMULATION_EXPLOSION_PCT and not entry.get("confirmed"):
                        hrs = age_days * 24.0
                        msg = (f"✅ *تأكد الانفجار: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                               f"🐋 رصدنا تجميع عليه قبل كذا، وبعد {hrs:.1f} ساعة انفجر فعلاً!\n"
                               f"💰 سعر الرصد: ${base_price:.4f} → الحالي: ${price_now:.4f}\n"
                               f"📈 الصعود: *+{gain:.1f}%* (≥{ACCUMULATION_EXPLOSION_PCT:.0f}% المطلوبة)\n"
                               f"⚠️ سجل تتبع فقط — الدخول الآن بعد الانفجار مخاطرة عالية جدًا، مش توصية دخول.")
                        send_telegram(msg)
                    else:
                        still_watching[symbol] = entry
                else:
                    still_watching[symbol] = entry

            new_arms = 0
            for symbol in symbols:
                if new_arms >= ACCUMULATION_MAX_PER_CYCLE:
                    break
                if symbol in watch and not watch[symbol].get("confirmed"):
                    continue
                try:
                    df = cached_download(symbol, period="5d", interval="15m")
                    if df is None or df.empty or len(df) < 30:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    df = compute_indicators(df)
                    signals_hit, tags = 0, []
                    if detect_whale_accumulation(df):
                        signals_hit += 1; tags.append("Whale 🐋")
                    is_acc, acc_score = detect_accumulation(df)
                    if is_acc:
                        signals_hit += 1; tags.append(f"Accumulation({acc_score})")
                    is_silent, ratio = detect_silent_accumulation(df)
                    if is_silent:
                        signals_hit += 1; tags.append(f"Silent {ratio:.1f}x")
                    if signals_hit < ACCUMULATION_MIN_SIGNALS:
                        continue
                    price = _safe_float(df['close'].iloc[-1])
                    if price <= 0:
                        continue
                    msg = (f"🐋 *تجميع مكتشف: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                           f"🔍 توافق {signals_hit}/3 كاشفات: {', '.join(tags)}\n"
                           f"💰 السعر وقت الرصد: ${price:.4f}\n"
                           f"👀 رصد مبكر فقط — بنتابعه، ولو انفجر فعلاً ≥{ACCUMULATION_EXPLOSION_PCT:.0f}% بنبعتلك تأكيد.\n"
                           f"⚠️ ليست توصية دخول — احتمال التجميع يفشل ويرجع عادي زي أي إشارة مبكرة.")
                    news_line = get_news_context_line(symbol)
                    if news_line:
                        msg += f"\n{news_line}"
                    send_telegram(msg)
                    still_watching[symbol] = {"armed_price": price, "armed_ts": now_ts,
                                               "signals": signals_hit, "confirmed": False}
                    new_arms += 1
                    time.sleep(1)
                except Exception as error:
                    logger.debug(f"[Accumulation] {symbol}: {error}")
                    continue

            with state_lock:
                if len(still_watching) > 400:
                    keep = sorted(still_watching.items(),
                                  key=lambda kv: _safe_float(kv[1].get("armed_ts")), reverse=True)[:400]
                    still_watching = dict(keep)
                state["accumulation_watch"] = still_watching
                save_state()
            logger.info(f"[Accumulation] universe={len(symbols)} watching={len(still_watching)} new={new_arms}")
            time.sleep(ACCUMULATION_SCAN_INTERVAL)
        except Exception as error:
            logger.error(f"[Accumulation] scanner error: {error}")
            time.sleep(120)


PUMP_DUMP_ENABLED = os.getenv("PUMP_DUMP_ENABLED", "true").strip().lower() == "true"
PUMP_DUMP_MIN_PUMP_PCT = float(os.getenv("PUMP_DUMP_MIN_PUMP_PCT", "50"))            # % حد أدنى لاعتباره بمب حقيقي
PUMP_DUMP_MIN_VOLUME = float(os.getenv("PUMP_DUMP_MIN_VOLUME", "300000"))
PUMP_DUMP_DROP_FROM_PEAK_PCT = float(os.getenv("PUMP_DUMP_DROP_FROM_PEAK_PCT", "15"))  # % هبوط من القمة = بداية دامب
PUMP_DUMP_SCAN_INTERVAL = int(os.getenv("PUMP_DUMP_SCAN_INTERVAL", "60"))
PUMP_DUMP_MAX_WATCH_HOURS = float(os.getenv("PUMP_DUMP_MAX_WATCH_HOURS", "48"))
PUMP_DUMP_MAX_PER_CYCLE = int(os.getenv("PUMP_DUMP_MAX_PER_CYCLE", "8"))


# ────────────── [FIX-3] Pump&Dump: حالة لكل سهم (State Machine) بدل الحذف بعد الدامب ──────────────
# السبب الجذري: بعد DUMP WARNING كان السهم يُحذف من pump_dump_watch، فالدورة اللي بعدها (بعد دقيقة) تلقاه
# بـhot_watchlist بتغيّر يومي مخزَّن (يبقى لين HOT_MAX_AGE_SEC=20 دقيقة) ≥50% وغير موجود بالـwatch
# ⇒ "PUMP مؤكد" جديد بنفس الأرقام القديمة (مثال MSGY: "+309.6%" والسعر 3.28 بعد ما كان 8.68).
# الحل — كل سهم له حالة تُحفظ ولا تُحذف عند الإغلاق:
#
#     (لا شيء) ──PUMP──▶ PUMP_ACTIVE ──دامب مؤكد──────▶ DUMPED
#                            └───────انتهاء المتابعة───▶ EXPIRED
#
#   • DUMPED/EXPIRED لا يُفتح لهم PUMP جديد إلا بـ"موجة جديدة فعلًا": سعر ≥ قمتهم السابقة × (1 + هامش)
#     + مرّت فترة تبريد + ما تجاوزنا الحد الأقصى للمرات باليوم.
#   • التغيّر اليومي يُعاد حسابه حيًّا من السعر اللحظي (مو من hot_watchlist القديم).
#   • الحلقات المغلقة تُنسى مع بداية يوم تداول جديد (نيويورك)، والنشطة تبقى لين PUMP_DUMP_MAX_WATCH_HOURS.
PUMP_DUMP_REARM_NEW_HIGH_PCT = float(os.getenv("PUMP_DUMP_REARM_NEW_HIGH_PCT", "2"))    # % فوق القمة السابقة لفتح موجة جديدة
PUMP_DUMP_REARM_COOLDOWN_MIN = float(os.getenv("PUMP_DUMP_REARM_COOLDOWN_MIN", "30"))   # دقائق تبريد بعد إغلاق الحلقة
PUMP_DUMP_MAX_REARMS_PER_DAY = int(os.getenv("PUMP_DUMP_MAX_REARMS_PER_DAY", "1"))      # أقصى موجات جديدة للسهم باليوم
_PD_ACTIVE, _PD_DUMPED, _PD_EXPIRED = "PUMP_ACTIVE", "DUMPED", "EXPIRED"


def _pd_day_key(ts):
    """يوم التداول (نيويورك) لطابع زمني."""
    return datetime.fromtimestamp(float(ts), EASTERN_TZ).date().isoformat()


def _pd_stage(entry):
    """مرحلة السهم؛ تدعم ملفات الحالة القديمة (بدون stage): dumped=True ⇒ DUMPED وإلا PUMP_ACTIVE."""
    if not isinstance(entry, dict):
        return _PD_EXPIRED
    stage = entry.get("stage")
    if stage in (_PD_ACTIVE, _PD_DUMPED, _PD_EXPIRED):
        return stage
    return _PD_DUMPED if entry.get("dumped") else _PD_ACTIVE


def _pd_normalize(entry, now_ts):
    """نسخة كاملة الحقول من الإدخال (تهاجر الإدخالات القديمة تلقائيًا). None لو الإدخال غير صالح."""
    if not isinstance(entry, dict):
        return None
    stage = _pd_stage(entry)
    peak = _safe_float(entry.get("peak_price"))
    armed_ts = _safe_float(entry.get("armed_ts"), now_ts) or now_ts
    normalized = {
        "stage": stage,
        "peak_price": peak,
        "entry_price": _safe_float(entry.get("entry_price"), peak),
        "armed_ts": armed_ts,
        "episodes": int(_safe_float(entry.get("episodes"), 1)) or 1,
        "closed_ts": _safe_float(entry.get("closed_ts")),
        "closed_peak": _safe_float(entry.get("closed_peak")),
        "closed_price": _safe_float(entry.get("closed_price")),
    }
    if stage != _PD_ACTIVE:
        normalized["closed_ts"] = normalized["closed_ts"] or armed_ts
        normalized["closed_peak"] = normalized["closed_peak"] or peak
    normalized["dumped"] = (stage == _PD_DUMPED)     # للتوافق مع أي كود قديم يقرأ المفتاح
    return normalized


def _pd_new_entry(price, now_ts, prior=None):
    """حلقة PUMP جديدة (episodes يزيد لو هذي موجة جديدة بعد حلقة مغلقة)."""
    episodes = (int(_safe_float(prior.get("episodes"), 1)) + 1) if isinstance(prior, dict) else 1
    return {"stage": _PD_ACTIVE, "peak_price": price, "entry_price": price, "armed_ts": now_ts,
            "episodes": episodes, "closed_ts": 0.0, "closed_peak": 0.0, "closed_price": 0.0, "dumped": False}


def _pd_close(entry, stage, now_ts, price=0.0):
    """يغلق الحلقة (DUMPED/EXPIRED) بدون حذف السهم من الحالة."""
    closed = dict(entry)
    closed.update({"stage": stage, "dumped": stage == _PD_DUMPED, "closed_ts": now_ts,
                   "closed_peak": _safe_float(entry.get("peak_price")), "closed_price": price})
    return closed


def _pd_live_change(cand_price, cand_change, live_price):
    """
    التغيّر اليومي الحي %. قيمة change بـhot_watchlist قد تكون قديمة (لين 20 دقيقة) والسعر لحظي، فنشتق إغلاق
    الأساس من نفس زوج (price, change) المخزَّن ونعيد الحساب بالسعر الحي:
        base = cand_price / (1 + change/100)     ⇒     live_change = live_price / base - 1
    لو تعذّر الاشتقاق (بيانات ناقصة) نرجع change المخزَّن كما هو.
    """
    try:
        stored_price, stored_change, live = float(cand_price), float(cand_change), float(live_price)
    except (TypeError, ValueError):
        return _safe_float(cand_change)
    if stored_change != stored_change:
        return 0.0
    if stored_price <= 0 or live <= 0 or stored_change <= -99.0:
        return stored_change
    base = stored_price / (1.0 + stored_change / 100.0)
    if base <= 0:
        return stored_change
    return (live / base - 1.0) * 100.0


def _pd_can_arm(prior, price, live_change, volume, now_ts):
    """
    هل نرسل "PUMP مؤكد" جديد لهذا السهم الآن؟ → (ok, reason).
    price=None ⇒ فحص مسبق (بدون سعر) للحالة/التبريد/الحد الأقصى، لتفادي طلب سعر لحظي لسهم محجوب أصلًا.
      • PUMP_ACTIVE                → لا (مُتابَع أصلًا).
      • DUMPED/EXPIRED (نفس اليوم) → لا، إلا بموجة جديدة فعلًا: سعر ≥ قمة الحلقة السابقة × (1+هامش)
                                       + مرّت فترة التبريد + ما تجاوزنا MAX_REARMS_PER_DAY.
      • بدون تاريخ                 → يلزم التغيّر الحي (مو المخزَّن) ≥ PUMP_DUMP_MIN_PUMP_PCT والحجم ≥ MIN_VOLUME.
    """
    if prior is not None and _pd_stage(prior) == _PD_ACTIVE:
        return False, "already_active"
    if prior is not None:
        if int(_safe_float(prior.get("episodes"), 1)) - 1 >= PUMP_DUMP_MAX_REARMS_PER_DAY:
            return False, "max_rearms"
        closed_ts = _safe_float(prior.get("closed_ts"))
        if closed_ts and (now_ts - closed_ts) < PUMP_DUMP_REARM_COOLDOWN_MIN * 60.0:
            return False, "rearm_cooldown"
    if price is None:
        return True, "precheck_ok"
    if price <= 0:
        return False, "no_price"
    if volume < PUMP_DUMP_MIN_VOLUME:
        return False, "low_volume"
    if live_change < PUMP_DUMP_MIN_PUMP_PCT:
        return False, "live_change_below_min"
    if prior is not None:
        ref_peak = _safe_float(prior.get("closed_peak")) or _safe_float(prior.get("peak_price"))
        if ref_peak > 0 and price < ref_peak * (1.0 + PUMP_DUMP_REARM_NEW_HIGH_PCT / 100.0):
            return False, "below_previous_peak"
        return True, "rearm_new_high"
    return True, "new"


def _pd_confirm_dump(symbol, price_now):
    """تأكيد فني للدامب (2 من 3: تحت VWAP / شمعة حمرا / RSI هابط) — نفس المنطق السابق حرفيًا. → (confirmed, reason)."""
    try:
        df = cached_download(symbol, period="1d", interval="5m")
        if df is not None and not df.empty and len(df) >= 10:
            df.columns = [str(c).lower() for c in df.columns]
            df = compute_indicators(df)
            last = df.iloc[-1]
            below_vwap = _safe_float(last.get('vwap', price_now)) > price_now
            red_candle = _safe_float(last.get('close')) < _safe_float(last.get('open', last.get('close')))
            rsi_falling = (_safe_float(df['rsi'].iloc[-1], 50) < _safe_float(df['rsi'].iloc[-4], 50)) if len(df) >= 4 else False
            hits = sum([below_vwap, red_candle, rsi_falling])
            reason = f"VWAP:{'✓' if below_vwap else '✗'} شمعة حمرا:{'✓' if red_candle else '✗'} RSI هابط:{'✓' if rsi_falling else '✗'}"
            return hits >= 2, reason
    except Exception as error:
        logger.debug(f"[PumpDump] confirm {symbol}: {error}")
    return False, ""


def _pump_dump_cycle(now_ts=None):
    """
    دورة واحدة من Pump&Dump Hunter (مفصولة عن حلقة النوم لتكون قابلة للاختبار). ترجع dict إحصائيات.
    الترتيب: (1) تحميل الحالة وتنظيفها  (2) دخول: بمب جديد أو موجة جديدة مؤكدة  (3) خروج: دامب/انتهاء متابعة  (4) حفظ.
    """
    now_ts = float(now_ts) if now_ts else time.time()
    today = _pd_day_key(now_ts)
    with state_lock:
        candidates = [dict(x) for x in state.get("hot_watchlist", []) if isinstance(x, dict)]
        raw_watch = dict(state.get("pump_dump_watch", {}))

    # (1) تحميل الحالة: الحلقات المغلقة من يوم تداول سابق تُنسى، النشطة تبقى
    watch = {}
    for sym, raw in raw_watch.items():
        entry = _pd_normalize(raw, now_ts)
        if entry is None:
            continue
        if entry["stage"] != _PD_ACTIVE and _pd_day_key(entry["closed_ts"] or entry["armed_ts"]) != today:
            continue
        watch[str(sym).upper().strip()] = entry

    # (2) دخول
    new_pumps, rearms, blocked = 0, 0, {}
    for c in candidates:
        if new_pumps >= PUMP_DUMP_MAX_PER_CYCLE:
            break
        symbol = str(c.get("symbol", "")).upper().strip()
        if not symbol:
            continue
        change = _safe_float(c.get("change"))
        volume = _safe_float(c.get("volume"))
        if change < PUMP_DUMP_MIN_PUMP_PCT or volume < PUMP_DUMP_MIN_VOLUME:
            continue
        prior = watch.get(symbol)
        ok, why = _pd_can_arm(prior, None, 0.0, 0.0, now_ts)          # فحص مسبق بدون طلب سعر
        if not ok:
            if why != "already_active":
                blocked[why] = blocked.get(why, 0) + 1
            continue
        price = _finnhub_current_price(symbol) or _safe_float(c.get("price"))
        if price <= 0:
            continue
        live_change = _fresh_change(c, price)
        ok, why = _pd_can_arm(prior, price, live_change, volume, now_ts)
        if not ok:
            blocked[why] = blocked.get(why, 0) + 1
            logger.info(f"[PumpDump] {symbol} PUMP suppressed ({why}): price={price:.4f} "
                        f"live_change={live_change:+.1f}% stored_change={change:+.1f}%")
            continue
        is_rearm = prior is not None
        title = "PUMP مؤكد (موجة جديدة)" if is_rearm else "PUMP مؤكد"
        msg = (f"💊 *{title}: {symbol}*\n━━━━━━━━━━━━━━━━\n"
               f"📈 صعد *+{live_change:.1f}%* عن إغلاق أمس (≥{PUMP_DUMP_MIN_PUMP_PCT:.0f}% المطلوبة)\n"
               f"💰 السعر الحالي (قمة أولية): ${price:.4f}\n"
               f"📊 الحجم: {volume:,.0f}\n"
               f"{_duration_and_speed_line(symbol, c, now_ts)}\n")
        if is_rearm:
            prev_peak = _safe_float(prior.get("closed_peak")) or _safe_float(prior.get("peak_price"))
            msg += f"🔁 كسر قمته السابقة (${prev_peak:.4f}) بعد تحذير التصريف — موجة جديدة مو استمرار للقديمة.\n"
        msg += (f"⏱️ *متأخر:* الانفجار صار — هذا تنبيه متابعة وخروج مو إشارة دخول (الإشارة المبكرة تجي من EARLY).\n"
                f"👁️ بنتابعه لحظة بلحظة — أول ما يبين انعكاس/توزيع من القمة نبعتلك تحذير خروج.\n"
                f"⚠️ حركة بهالحجم متقلبة جدًا، وبدون خبر واضح غالبًا مضاربة بحتة — لا تدخل بدون خطة خروج واضحة.")
        news_line = get_news_context_line(symbol)
        if news_line:
            msg += f"\n{news_line}"
        if not _event_gate_allow(symbol, price, "PUMP"):
            blocked["event_gate"] = blocked.get("event_gate", 0) + 1
            continue
        send_telegram(msg)
        _event_gate_mark(symbol, price, "PUMP", "PUMP")
        watch[symbol] = _pd_new_entry(price, now_ts, prior)
        new_pumps += 1
        rearms += 1 if is_rearm else 0
        time.sleep(1)

    # (3) خروج: دامب (بدون حذف السهم) أو انتهاء مدة المتابعة
    dumps = 0
    for symbol in list(watch.keys()):
        entry = watch[symbol]
        if entry["stage"] != _PD_ACTIVE:
            continue
        age_hrs = (now_ts - entry["armed_ts"]) / 3600.0
        if age_hrs > PUMP_DUMP_MAX_WATCH_HOURS:
            watch[symbol] = _pd_close(entry, _PD_EXPIRED, now_ts)
            continue
        price_now = _finnhub_current_price(symbol) or 0.0
        if price_now <= 0:
            continue
        peak = max(entry["peak_price"], price_now)
        entry["peak_price"] = peak
        drop_pct = (peak - price_now) / peak * 100.0 if peak > 0 else 0.0
        if drop_pct >= PUMP_DUMP_DROP_FROM_PEAK_PCT and dumps < PUMP_DUMP_MAX_PER_CYCLE:
            confirmed, reason = _pd_confirm_dump(symbol, price_now)
            if confirmed:
                entry_price = entry["entry_price"]
                total_gain = (peak - entry_price) / entry_price * 100.0 if entry_price > 0 else 0.0
                msg = (f"🔴 *DUMP WARNING: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                       f"⚠️ هبط *-{drop_pct:.1f}%* من قمته (${peak:.4f} → ${price_now:.4f})\n"
                       f"🧪 تأكيد فني: {reason}\n"
                       f"📊 كان صعد بإجمالي +{total_gain:.1f}% من أول رصد قبل {age_hrs:.1f} ساعة\n"
                       f"🏃 احتمال بداية توزيع/تصريف — لو لسه ماسك، هذا وقت التفكير بالخروج مو الدخول.\n"
                       f"🔒 انتهت المتابعة لهذا السهم — ما نرسل PUMP جديد إلا لو كسر قمته السابقة (${peak:.4f}) بشكل مؤكد.")
                if not _event_gate_allow(symbol, price_now, "DUMP"):
                    continue
                send_telegram(msg)
                _event_gate_mark(symbol, price_now, "DUMP", "DUMP")
                watch[symbol] = _pd_close(entry, _PD_DUMPED, now_ts, price_now)
                dumps += 1

    # (4) حفظ (الحد الأقصى 300: النشطة أولًا ثم أحدث المغلقة)
    with state_lock:
        if len(watch) > 300:
            active_items = sorted(((s, e) for s, e in watch.items() if e["stage"] == _PD_ACTIVE),
                                  key=lambda kv: kv[1]["armed_ts"], reverse=True)[:300]
            closed_items = sorted(((s, e) for s, e in watch.items() if e["stage"] != _PD_ACTIVE),
                                  key=lambda kv: kv[1]["closed_ts"] or kv[1]["armed_ts"], reverse=True)
            watch = dict(active_items + closed_items[:max(0, 300 - len(active_items))])
        state["pump_dump_watch"] = watch
        save_state()
    active_count = sum(1 for e in watch.values() if e["stage"] == _PD_ACTIVE)
    blocked_txt = ",".join(f"{k}:{v}" for k, v in sorted(blocked.items())) or "0"
    logger.info(f"[PumpDump] pool={len(candidates)} watching={active_count} new={new_pumps} dumps={dumps} "
                f"closed={len(watch) - active_count} rearms={rearms} blocked={blocked_txt}")
    return {"pool": len(candidates), "watching": active_count, "new": new_pumps, "dumps": dumps,
            "closed": len(watch) - active_count, "rearms": rearms, "blocked": dict(blocked)}


def pump_dump_scanner():
    """
    💊🔻 Pump & Dump Hunter — طبقتين منفصلتين بنفس عتبة الـ50%:
    (1) دخول: سهم بـhot_watchlist صعوده الحي ≥PUMP_DUMP_MIN_PUMP_PCT% (50% افتراضي) عن إغلاق أمس → "PUMP مؤكد"
        ويبدأ يُتابَع لحظيًا (تكملة لـMega Mover اللي ينبه بنفس المنطقة بس ما يتابع بعدها).
    (2) خروج: نحدّث أعلى قمة لكل سهم متابَع، ولو هبط PUMP_DUMP_DROP_FROM_PEAK_PCT% (15%) من قمته + تأكيد فني
        (2 من 3: VWAP / شمعة حمرا / RSI هابط) → "DUMP WARNING" ويُغلق السهم (DUMPED) بدون حذفه من الحالة.
    [FIX-3] بعد الإغلاق لا يُرسل PUMP جديد لنفس السهم إلا بموجة جديدة مؤكدة (قمة أعلى + تبريد + حد يومي)
    — تفاصيل القواعد في _pd_can_arm. المتابعة تعتمد على _finnhub_current_price (خفيف، بدون OHLCV)،
    وما يحمّل بيانات كاملة إلا للمرشحين اللي فعلًا هبطوا من قمتهم.
    """
    if not PUMP_DUMP_ENABLED:
        logger.info("ℹ️ Pump & Dump hunter disabled (PUMP_DUMP_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("pump_dump")
            if get_market_phase() == "CLOSED":
                time.sleep(300)
                continue
            _pump_dump_cycle()
            time.sleep(PUMP_DUMP_SCAN_INTERVAL)
        except Exception as error:
            logger.error(f"[PumpDump] scanner error: {error}")
            time.sleep(90)


# ================= [FIX-12] EARLY — إشارة قبل الانفجار مو عند القمة =================
# التشخيص (من الكود ولوقك): كل ماسحات التنبيه متأخرة بالتصميم — Gap Hunter عند ≥20% عن الإغلاق، Mega Mover وPUMP عند ≥50%.
# وماسحات البداية ما توصلك: QuickJump ما يرسل تيليجرام (يمرّر لـRanking بس؛ بلوقك رصد KNDI +10.7% خلال 3 دقايق ولا وصلتك
# رسالة)، وExplosive Setup يحتاج اندفاعة ≥30% ثم قاعدة ثم اختراق (موجة ثانية أصلًا) وما أرسل شي بنافذة اللوق.
# الحل: ماسح EARLY يقرأ hot_watchlist + بث ياهو المباشر (بدون أي طلب HTTP جديد) كل EARLY_SCAN_INTERVAL ثانية
# ويرسل لما تجتمع علامات "بداية حركة" وهي لسه صغيرة:
#   زخم سعر (≥3% خلال 3د أو ≥4.5% خلال 5د) + تسارع حجم (≥3x من المعدل أو ≥20K سهم بآخر 3د) + صاعد فعلًا (مو ذيل/تراجع)
#   + لسه بالبداية (اليوم بين +1% و+35%، أما ≥50% فهذا مجال PUMP وهو "متأخر").
# ويتتبّع أداء كل إشارة بعدها (أعلى سعر وصله السهم) عشان تتحقق بنفسك: هل وصلت قبل الانفجار؟ — الأمر /early.
EARLY_ENABLED = os.getenv("EARLY_ENABLED", "true").strip().lower() == "true"
EARLY_SCAN_INTERVAL = int(os.getenv("EARLY_SCAN_INTERVAL", "15"))
EARLY_MIN_DAY_CHANGE = float(os.getenv("EARLY_MIN_DAY_CHANGE", "1"))          # اليوم لازم موجب (مو ارتداد من هبوط)
EARLY_MAX_DAY_CHANGE = float(os.getenv("EARLY_MAX_DAY_CHANGE", "25"))         # فوق هذا = انفجر (مجال PUMP)
EARLY_MIN_MOVE_3M = float(os.getenv("EARLY_MIN_MOVE_3M", "3"))                # % صعود خلال ~3 دقايق
EARLY_MIN_MOVE_5M = float(os.getenv("EARLY_MIN_MOVE_5M", "4.5"))              # أو % خلال ~5 دقايق
EARLY_MIN_VOL_RATIO = float(os.getenv("EARLY_MIN_VOL_RATIO", "3"))            # تسارع الحجم مقابل المعدل (الجلسة العادية)
EARLY_MIN_SHARES_3M = float(os.getenv("EARLY_MIN_SHARES_3M", "20000"))        # أو أسهم مُتداولة بآخر ~3 دقايق (كل الجلسات)
EARLY_MIN_DAY_VOLUME = float(os.getenv("EARLY_MIN_DAY_VOLUME", "100000"))
EARLY_PRICE_MIN = float(os.getenv("EARLY_PRICE_MIN", "0.3"))
EARLY_PRICE_MAX = float(os.getenv("EARLY_PRICE_MAX", "15"))
EARLY_FADE_MAX_PCT = float(os.getenv("EARLY_FADE_MAX_PCT", "1.5"))            # أقصى نزول عن أعلى سعر بآخر 5 دقايق
EARLY_MAX_PER_CYCLE = int(os.getenv("EARLY_MAX_PER_CYCLE", "3"))
EARLY_MAX_PER_DAY = int(os.getenv("EARLY_MAX_PER_DAY", "12"))
EARLY_MAX_PER_SYMBOL_DAY = int(os.getenv("EARLY_MAX_PER_SYMBOL_DAY", "2"))
EARLY_SYMBOL_GAP_MIN = float(os.getenv("EARLY_SYMBOL_GAP_MIN", "60"))         # دقائق بين إشارتين لنفس السهم
EARLY_MIN_HIST_SPAN_SEC = 90                                                   # أقل مدة عيّنات عشان نثق بالزخم
EARLY_TRACK_HOURS = 6.0
_early_hist = {}
_early_log = {"ts": 0.0}


# ================= [FIX-13..15] الحزمة الرابعة: تغطية EARLY · VWAP · مكوّنات الإعداد القوي (A+) =================
#  [FIX-13] hot_watchlist مقفولة على 120 (HOT_MAX_ITEMS) وكانت ممتلئة دايمًا بلوقك (pool=120) بينما الدورة تطلع 223–258 مرشح
#           (وهذا رقم "Universe" بـ/status): ~110–140 مرشح يُقصّون كل دورة قبل ما يشوفهم أي ماسح، بما فيهم الأسهم اللي
#           لسه بدأت تتسارع. الحين EARLY يقرأ كل مرشحي آخر دورة (بدون أي طلب شبكة جديد ودون تغيير بقية الماسحات).
#  [FIX-14] مكوّنات "الإعداد القوي" (A+) من نصيحة الـAI الثاني اللي قابلة للقياس ببياناتنا: حجم ≥10x · فلوت <15M · خبر · فوق VWAP،
#           + شورت عالي (سكوييز) + قرب قمة 52 أسبوع. تُعرض كقائمة تحقق بإشارة EARLY، وما تحجب شي (تجميع شروط مو احتمال نجاح).
#  [FIX-15] بوابة VWAP بالجلسة العادية: السهم تحت VWAP = ما ينرسل EARLY (VWAP من شموع 1د بعدد محدود من الطلبات لكل دورة).
EARLY_POOL_MAX_AGE_SEC = 180
_early_pool = {"ts": 0.0, "rows": []}
_early_pool_lock = threading.Lock()
EARLY_VWAP_GATE = os.getenv("EARLY_VWAP_GATE", "true").strip().lower() == "true"
EARLY_VWAP_FETCH_PER_CYCLE = int(os.getenv("EARLY_VWAP_FETCH_PER_CYCLE", "3"))
EARLY_VWAP_CACHE_SEC = 60
_early_vwap_cache = {}
EARLY_APLUS_VOL_RATIO = float(os.getenv("EARLY_APLUS_VOL_RATIO", "10"))
EARLY_APLUS_FLOAT_MAX = float(os.getenv("EARLY_APLUS_FLOAT_MAX", "15000000"))
EARLY_SQUEEZE_SHORT_PCT = float(os.getenv("EARLY_SQUEEZE_SHORT_PCT", "20"))
PROFILE_CACHE_TTL = 86400
PROFILE_FAIL_TTL = 900
PROFILE_MAX_FETCH_PER_HOUR = int(os.getenv("PROFILE_MAX_FETCH_PER_HOUR", "40"))
_profile_cache = {}
_profile_fetch_log = deque(maxlen=400)
_profile_lock = threading.Lock()


def _publish_early_pool(cands, now_ts):
    """[FIX-13] يحفظ كل مرشحي آخر دورة Discovery (قبل قص القائمة الساخنة على 120) ليقرأها EARLY."""
    rows = []
    for c in cands or []:
        symbol = str(c.get("symbol", "")).upper().strip()
        if symbol:
            row = dict(c)
            row["symbol"], row["last_seen"] = symbol, now_ts
            rows.append(row)
    with _early_pool_lock:
        _early_pool["ts"], _early_pool["rows"] = float(now_ts), rows


def _early_universe(hot, now_ts):
    """[FIX-13] القائمة الساخنة + مرشحو آخر دورة غير الموجودين فيها → (القائمة الموحّدة، عدد الإضافي)."""
    with _early_pool_lock:
        fresh = now_ts - _safe_float(_early_pool["ts"]) <= EARLY_POOL_MAX_AGE_SEC
        rows = [dict(r) for r in _early_pool["rows"]] if fresh else []
    known = {str(r.get("symbol", "")).upper().strip() for r in hot}
    extra = [r for r in rows if r["symbol"] not in known]
    return list(hot) + extra, len(extra)


def _session_vwap(symbol):
    """[FIX-15] VWAP جلسة اليوم من شموع 1د (النموذجي × الحجم). None لو تعذّر."""
    try:
        df = cached_download(symbol, period="1d", interval="1m")
        if df is None or df.empty or len(df) < 5:
            return None
        cols = {str(c).lower(): c for c in df.columns}
        if not all(k in cols for k in ("high", "low", "close", "volume")):
            return None
        high, low, close, volume = (pd.to_numeric(df[cols[k]], errors="coerce") for k in ("high", "low", "close", "volume"))
        total = float(volume.sum())
        if not math.isfinite(total) or total <= 0:
            return None
        vwap = float((((high + low + close) / 3.0) * volume).sum() / total)
        return vwap if math.isfinite(vwap) and vwap > 0 else None
    except Exception as error:
        logger.debug(f"[Early] vwap {symbol}: {error}")
        return None


def _early_vwap_check(symbol, now_ts, budget):
    """VWAP مع كاش 60ث (حتى الفاشل) وميزانية طلبات لكل دورة، فالسهم اللي تحت VWAP ما يضرب ياهو كل 15 ثانية."""
    hit = _early_vwap_cache.get(symbol)
    if hit and now_ts - hit[0] < EARLY_VWAP_CACHE_SEC:
        return hit[1]
    if budget["left"] <= 0:
        return None
    budget["left"] -= 1
    vwap = _session_vwap(symbol)
    _early_vwap_cache[symbol] = (now_ts, vwap)
    if len(_early_vwap_cache) > 300:
        for key in sorted(_early_vwap_cache, key=lambda k: _early_vwap_cache[k][0])[:100]:
            _early_vwap_cache.pop(key, None)
    return vwap


def _stock_profile(symbol, now_ts=None):
    """
    [FIX-14] فلوت / شورت / قمة 52 أسبوع / القيمة السوقية من yfinance .info — كاش 24 ساعة (والفشل 15 دقيقة)،
    وسقف PROFILE_MAX_FETCH_PER_HOUR طلب جديد بالساعة، ويتوقف لو قاطع ياهو شغّال. {} لو غير متوفر.
    """
    symbol = str(symbol or "").upper().strip()
    now = float(now_ts) if now_ts else time.time()
    if not symbol:
        return {}
    with _profile_lock:
        hit = _profile_cache.get(symbol)
    if hit and now - hit[0] < (PROFILE_CACHE_TTL if hit[1] else PROFILE_FAIL_TTL):
        return dict(hit[1])
    if _yahoo_cooldown_remaining(now) > 0:
        return {}
    with _profile_lock:
        if sum(1 for t in _profile_fetch_log if now - t < 3600) >= PROFILE_MAX_FETCH_PER_HOUR:
            return {}
        _profile_fetch_log.append(now)
    data = {}
    try:
        info = yf.Ticker(symbol).info or {}
        float_shares = _safe_float(info.get("floatShares")) or _safe_float(info.get("sharesOutstanding"))
        if float_shares > 0:
            data["float_shares"] = float_shares
        short = info.get("shortPercentOfFloat")
        if short is not None and _safe_float(short) > 0:
            data["short_pct"] = _safe_float(short) * 100.0
        for key, source in (("market_cap", "marketCap"), ("high_52w", "fiftyTwoWeekHigh")):
            value = _safe_float(info.get(source))
            if value > 0:
                data[key] = value
    except Exception as error:
        logger.debug(f"[Early] profile {symbol}: {error}")
    with _profile_lock:
        _profile_cache[symbol] = (now, data)
        if len(_profile_cache) > 400:
            for key in sorted(_profile_cache, key=lambda k: _profile_cache[k][0])[:150]:
                _profile_cache.pop(key, None)
    return dict(data)


def _early_aplus(profile, vol_ratio, has_news, vwap, price):
    """
    [FIX-14] قائمة تحقق "الإعداد القوي": حجم ≥10x · فلوت <15M · خبر · فوق VWAP (الأربعة من نصيحة الـAI الثاني)،
    + ملاحظات: شورت ≥20% (سكوييز)، وقرب قمة 52 أسبوع. الاكتمال 4/4 = شارة A+؛ وهذا تجميع شروط مو احتمال نجاح مُقاس.
    """
    profile = profile or {}
    float_shares = _safe_float(profile.get("float_shares"))
    ok_vol = vol_ratio >= EARLY_APLUS_VOL_RATIO
    ok_float = 0 < float_shares < EARLY_APLUS_FLOAT_MAX
    ok_news = bool(has_news)
    ok_vwap = vwap is not None and price >= vwap
    count = sum([ok_vol, ok_float, ok_news, ok_vwap])

    def mark(flag, unknown=False):
        return "✅" if flag else ("❔" if unknown else "➖")

    parts = [f"حجم {vol_ratio:.0f}x {mark(ok_vol)}",
             (f"فلوت {float_shares / 1e6:.1f}M {mark(ok_float)}" if float_shares > 0 else "فلوت ❔"),
             f"خبر {mark(ok_news)}",
             f"فوق VWAP {mark(ok_vwap, unknown=vwap is None)}"]
    lines = [f"🧩 مكوّنات الإعداد القوي (A+): {count}/4 — " + " | ".join(parts)]
    short_pct = _safe_float(profile.get("short_pct"))
    if short_pct >= EARLY_SQUEEZE_SHORT_PCT:
        lines.append(f"🔥 شورت {short_pct:.0f}% من الفلوت — احتمال سكوييز لو تسارع الشراء")
    high_52w = _safe_float(profile.get("high_52w"))
    if high_52w > 0 and price >= high_52w * 0.97:
        lines.append(f"🚀 قرب/فوق قمة 52 أسبوع (${high_52w:.2f}) — ما فيه بائعين عالقين فوق")
    if count == 4:
        lines.append("⭐ *A+ SETUP* — اجتمعت الأربعة (تجميع شروط مو ضمان نجاح)")
    return {"count": count, "is_aplus": count == 4, "lines": lines}


def _early_sample(symbol, rec, now_ts):
    """
    أحدث (وقت، سعر، حجم اليوم، مصدر): بث ياهو المباشر لو حديث، وإلا آخر قراءة شاشات (لو عمرها ≤ 120ث). None لو ما فيه.
    """
    with _live_prices_lock:
        live = dict(_live_prices.get(symbol) or {})
    live_price = _safe_float(live.get("price"))
    if live_price > 0 and now_ts - _safe_float(live.get("ts")) <= YAHOO_STREAM_STALE_SEC:
        volume = _safe_float(live.get("volume")) or _safe_float(rec.get("volume"))
        return _safe_float(live.get("ts")), live_price, volume, "stream"
    seen = _safe_float(rec.get("last_seen", rec.get("timestamp")))
    price = _safe_float(rec.get("price"))
    if seen > 0 and price > 0 and now_ts - seen <= 120:
        return seen, price, _safe_float(rec.get("volume")), "screen"
    return None


def _early_push(symbol, sample):
    """يضيف العيّنة (لو جديدة زمنيًا) ويقص أي شي أقدم من 10 دقايق. يرجع قائمة العيّنات."""
    ts, price, volume = sample[0], sample[1], sample[2]
    dq = _early_hist.get(symbol)
    if dq is None:
        dq = deque(maxlen=120)
        _early_hist[symbol] = dq
    if not dq or ts > dq[-1][0]:
        dq.append((ts, price, volume))
    while dq and dq[-1][0] - dq[0][0] > 600:
        dq.popleft()
    return list(dq)


def _early_metrics(hist, avg_volume, vol_valid):
    """
    مقاييس البداية من عيّنات السعر/الحجم (hist = [(ts, price, volume), ...] مرتبة زمنيًا). None لو أقل من 3 عيّنات.
    m3/m1/m5 = % تغيّر السعر؛ vol_ratio = تسارع الحجم مقابل المعدل (فقط بالجلسة العادية)؛ shares_3m = أسهم آخر ~3 دقايق؛
    fade_pct = النزول عن أعلى سعر بآخر 5 دقايق؛ rising = نسبة الخطوات غير الهابطة بآخر 5 عيّنات.
    """
    if len(hist) < 3:
        return None
    ts_n, p_n, v_n = hist[-1]
    r3, r1 = _ref_sample(hist, 180), _ref_sample(hist, 60)
    m3 = ((p_n / r3[1] - 1.0) * 100.0) if (r3 and r3[1] > 0) else 0.0
    m1 = ((p_n / r1[1] - 1.0) * 100.0) if (r1 and r1[1] > 0) else 0.0
    m5, _m1_unused, vol_ratio = _momentum_from_history(hist, avg_volume, vol_valid)
    shares_3m = (v_n - r3[2]) if (r3 and r3[2] > 0 and v_n >= r3[2]) else 0.0
    window = [s for s in hist if ts_n - s[0] <= 300] or hist[-1:]
    hi5, lo5 = max(s[1] for s in window), min(s[1] for s in window)
    fade_pct = ((hi5 - p_n) / hi5 * 100.0) if hi5 > 0 else 0.0
    last5 = [s[1] for s in hist[-5:]]
    steps = max(len(last5) - 1, 1)
    rising = sum(1 for a, b in zip(last5, last5[1:]) if b >= a) / steps
    # تأكيد الاستمرار: ومضة سعر بنبضة وحدة (طبعة شاذة) ما تعدّي — لازم عيّنتين من آخر 3 فوق نصف الحد المطلوب
    confirm = 0
    if r3 and r3[1] > 0:
        floor_price = r3[1] * (1.0 + 0.5 * EARLY_MIN_MOVE_3M / 100.0)
        confirm = sum(1 for s in hist[-3:] if s[1] >= floor_price)
    return {"m1": m1, "m3": m3, "m5": m5, "vol_ratio": vol_ratio, "shares_3m": shares_3m, "fade_pct": fade_pct,
            "rising": rising, "confirm": confirm, "hi5": hi5, "lo5": lo5, "span": ts_n - hist[0][0]}


def _early_evaluate(rec, metrics, day_change, phase, price, day_volume):
    """
    بوابات EARLY الصلبة → (ok, reason, details). reason = أول سبب رفض (للوق والاختبارات).
    الفكرة: الزخم والحجم يبدون (فرصة بدأت)، ولسه اليوم صغير (ما انفجر)، والسعر صاعد فعلًا (مو فاد).
    """
    if phase not in ("REGULAR", "PRE", "AFTER"):
        return False, "market_closed", {}
    if not (EARLY_PRICE_MIN <= price <= EARLY_PRICE_MAX):
        return False, "price_range", {}
    if day_volume < EARLY_MIN_DAY_VOLUME:
        return False, "low_volume", {}
    if metrics is None or metrics["span"] < EARLY_MIN_HIST_SPAN_SEC:
        return False, "not_enough_history", {}
    if day_change >= EARLY_MAX_DAY_CHANGE:
        return False, "extended", {}
    if day_change < EARLY_MIN_DAY_CHANGE:
        return False, "day_change_too_low", {}
    m3 = metrics["m3"]
    m5 = max(metrics["m5"], _safe_float(rec.get("momentum_5m")))
    if m3 < EARLY_MIN_MOVE_3M and m5 < EARLY_MIN_MOVE_5M:
        return False, "weak_momentum", {}
    recorded_ratio = _safe_float(rec.get("vol_rate_ratio")) if phase == "REGULAR" else 0.0
    vol_ratio = max(metrics["vol_ratio"], recorded_ratio)
    # بعض شاشات Yahoo لا ترجع averageDailyVolume، فيصبح الحساب النسبي صفرًا رغم
    # وجود حجم حقيقي (shares_3m). لا نعرض 0.0x حينها: نستخدم مقياسًا محافظًا
    # مقابل الحد المطلق المطلوب، ونوضح مصدره في الرسالة. هذا ليس اختلاقًا لـRVOL؛
    # هو "نشاط مقابل الحد الأدنى" ويظل محكومًا ببوابة EARLY_MIN_SHARES_3M.
    volume_ratio_source = "متوسط السوق"
    if vol_ratio <= 0.0 and metrics["shares_3m"] > 0:
        vol_ratio = metrics["shares_3m"] / max(EARLY_MIN_SHARES_3M, 1.0)
        volume_ratio_source = "الحد الأدنى المطلق (لعدم توفر متوسط Yahoo)"
    relative_ok = vol_ratio >= EARLY_MIN_VOL_RATIO
    absolute_ok = metrics["shares_3m"] >= EARLY_MIN_SHARES_3M
    if phase == "REGULAR" and _safe_float(rec.get("avg_volume")) > 0:
        volume_ok = relative_ok            # معدل الحجم معروف ⇒ المهم النسبي (20K سهم بآخر 3د عادية لسهم يتداول ملايين)
    else:
        volume_ok = relative_ok or absolute_ok      # قبل/بعد الجلسة أو معدل مجهول ⇒ المطلق يكفي
    if not volume_ok:
        return False, "no_volume_surge", {}
    if metrics["fade_pct"] > EARLY_FADE_MAX_PCT:
        return False, "fading", {}
    if metrics["rising"] < 0.6 or metrics["m1"] < 0:
        return False, "not_rising", {}
    if metrics.get("confirm", 0) < 2:
        return False, "not_confirmed", {}
    score = min(vol_ratio, 20.0) * 2.0 + max(m3, 0.0) * 3.0 + max(m5, 0.0) * 1.5 - day_change * 0.4
    return True, "ok", {"m3": m3, "m5": m5, "vol_ratio": vol_ratio, "volume_ratio_source": volume_ratio_source,
                        "shares_3m": metrics["shares_3m"], "score": score,
                        "stop_ref": metrics["lo5"]}


def _early_message(symbol, price, day_change, details, day_volume, phase, radar_line="", news_line=None, extra_lines=None):
    """نص إشارة EARLY (ماركداون آمن). وقت التنبيه بتوقيت السعودية يُلحق تلقائيًا بنهاية الرسالة عند الإرسال."""
    stop_ref = _safe_float(details.get("stop_ref"))
    stop_pct = ((price - stop_ref) / price * 100.0) if (stop_ref > 0 and price > 0) else 0.0
    msg = (f"🟢 *EARLY — بداية حركة: {symbol}*\n━━━━━━━━━━━━━━━━\n"
           f"💰 السعر: ${price:.4f} | اليوم: {day_change:+.1f}%\n"
           f"⚡ آخر 3 دقايق: {details['m3']:+.1f}% | آخر 5 دقايق: {details['m5']:+.1f}%\n"
           f"📊 تسارع/نشاط الحجم: {details['vol_ratio']:.1f}x ({details.get('volume_ratio_source', 'المعدل')}) | "
           f"تداول آخر 3د: {details['shares_3m']:,.0f} سهم | حجم اليوم: {day_volume:,.0f}\n")
    if radar_line:
        msg += f"{radar_line}\n"
    if news_line:
        msg += f"{news_line}\n"
    for line in extra_lines or []:
        msg += f"{line}\n"
    if phase in ("PRE", "AFTER"):
        session = "ما قبل الافتتاح" if phase == "PRE" else "بعد الإغلاق"
        msg += f"🌙 جلسة {session}: سيولة رقيقة والحركة تنعكس بسرعة.\n"
    msg += (f"🟢 لسه بالبداية: صاعد {day_change:.0f}% فقط اليوم — الانفجار (لو صار) يجي بعد هالنقطة مو قبلها.\n")
    if stop_ref > 0 and stop_pct > 0:
        msg += f"🛑 تُلغى الإشارة لو كسر ${stop_ref:.4f} ({stop_pct:.1f}% تحت السعر) — أدنى سعر بآخر 5 دقايق.\n"
    stamp = _session_stamp(volume_note=True)
    if stamp:
        msg += stamp + "\n"
    src = _price_src_label(symbol)
    if src:
        msg += f"💰 مصدر السعر{src}\n"
    msg += ("🔁 لو استمر وانفجر بيوصلك PUMP ثم DUMP WARNING للخروج.\n"
            "⚠️ إشارة مبكرة = مخاطرة أعلى وغير مضمونة (Paper Trading) — لا تدخل بدون وقف وحجم صغير.")
    return msg


def _early_track_update(alerts, now_ts):
    """يحدّث لكل إشارة سابقة (آخر EARLY_TRACK_HOURS ساعات): أعلى سعر وصله السهم بعدها، وآخر سعر — بدون أي طلب شبكة."""
    for entry in alerts:
        if now_ts - _safe_float(entry.get("ts")) > EARLY_TRACK_HOURS * 3600:
            continue
        dq = _early_hist.get(str(entry.get("symbol", "")).upper())
        if not dq:
            continue
        ts, price = dq[-1][0], dq[-1][1]
        if price <= 0 or ts < _safe_float(entry.get("ts")):
            continue
        entry["last_price"], entry["last_ts"] = price, ts
        if price > _safe_float(entry.get("peak_price")):
            entry["peak_price"], entry["peak_ts"] = price, ts
    return alerts


def _early_report_text(alerts, now_ts=None, limit=10):
    """نص /early: كل إشارة وأداؤها بعدها (أعلى صعود وصله، وأين السعر الحين) — الدليل هل وصلتك قبل الانفجار أو عند القمة."""
    now = float(now_ts) if now_ts else time.time()
    today = _pd_day_key(now)
    todays = [a for a in alerts if a.get("day") == today]
    recent = sorted(alerts, key=lambda a: _safe_float(a.get("ts")), reverse=True)[:limit]
    if not recent:
        return ("📭 ما وصلت أي إشارة EARLY بعد.\nالماسح يراقب hot_watchlist وبث ياهو كل "
                f"{EARLY_SCAN_INTERVAL}ث ويرسل لما يبدأ الزخم والحجم والسهم لسه صغير (اليوم +{EARLY_MIN_DAY_CHANGE:.0f}% إلى +{EARLY_MAX_DAY_CHANGE:.0f}%).")
    lines = ["🟢 *EARLY — آخر الإشارات وأداؤها بعدها*", "━━━━━━━━━━━━━━━━"]
    peaks = []
    for a in recent:
        entry_price = _safe_float(a.get("price"))
        peak = max(_safe_float(a.get("peak_price")), entry_price)
        last = _safe_float(a.get("last_price")) or entry_price
        if entry_price > 0:
            peaks.append((peak / entry_price - 1.0) * 100.0)
        mins = max(0.0, (_safe_float(a.get("peak_ts")) - _safe_float(a.get("ts"))) / 60.0) if peak > entry_price else 0.0
        lines.append(f"• *{a.get('symbol')}* {_ksa_hm(a.get('ts'))} — دخول ${entry_price:.4f} (اليوم {_safe_float(a.get('day_change')):+.1f}%)")
        if entry_price > 0:
            tail = f" بعد {mins:.0f}د" if peak > entry_price else ""
            lines.append(f"   الحين ${last:.4f} ({(last / entry_price - 1) * 100:+.1f}%) | أعلى سعر بعدها ${peak:.4f} ({(peak / entry_price - 1) * 100:+.1f}%){tail}")
    if peaks:
        best = sum(1 for p in peaks if p >= 10.0)
        lines.append("━━━━━━━━━━━━━━━━")
        aplus_n = sum(1 for a in recent if _safe_float(a.get("aplus")) >= 4)
        if aplus_n:
            lines.append(f"⭐ منها {aplus_n} بشارة A+ (اجتمعت مكوّنات الإعداد القوي الأربعة)")
        lines.append(f"📈 اليوم: {len(todays)} إشارة | {best} من آخر {len(peaks)} وصلت +10% أو أكثر بعد الإشارة | متوسط أعلى صعود بعدها {sum(peaks) / len(peaks):+.1f}%")
    lines.append("ℹ️ التتبع يستمر طالما السهم بالقائمة الساخنة والبوت شغّال (تقريبي).")
    return "\n".join(lines)


def _early_cycle(now_ts=None):
    """
    دورة واحدة من ماسح EARLY (مفصولة عن حلقة النوم لتكون قابلة للاختبار). ترجع dict إحصائيات.
    (1) عيّنات لكل سهم ساخن  (2) تقييم بوابات EARLY  (3) ترتيب + سقوف + إرسال  (4) تتبّع أداء الإشارات السابقة.
    """
    now_ts = float(now_ts) if now_ts else time.time()
    stats = {"hot": 0, "sampled": 0, "sent": 0, "rejected": {}}
    phase = get_market_phase()
    if phase == "CLOSED":
        return stats
    today = _pd_day_key(now_ts)
    with state_lock:
        hot = [dict(x) for x in state.get("hot_watchlist", []) if isinstance(x, dict)]
        alerts = [dict(a) for a in state.get("early_alerts", []) if isinstance(a, dict)]
        active_pumps = {str(s).upper() for s, e in state.get("pump_dump_watch", {}).items() if _pd_stage(e) == _PD_ACTIVE}
    hot, pool_extra = _early_universe(hot, now_ts)          # [FIX-13] كل مرشحي آخر دورة، مو بس أول 120
    stats["hot"], stats["pool_extra"] = len(hot), pool_extra
    vol_valid = (phase == "REGULAR")
    todays = [a for a in alerts if a.get("day") == today]
    candidates, seen_symbols = [], set()

    def reject(reason):
        stats["rejected"][reason] = stats["rejected"].get(reason, 0) + 1

    for rec in hot:
        symbol = str(rec.get("symbol", "")).upper().strip()
        if not symbol:
            continue
        seen_symbols.add(symbol)
        sample = _early_sample(symbol, rec, now_ts)
        if sample is None:
            reject("no_fresh_price")
            continue
        hist = _early_push(symbol, sample)
        stats["sampled"] += 1
        if symbol in active_pumps:
            reject("pump_active")
            continue
        mine = [a for a in todays if a.get("symbol") == symbol]
        if len(mine) >= EARLY_MAX_PER_SYMBOL_DAY:
            reject("symbol_daily_cap")
            continue
        if mine and now_ts - max(_safe_float(a.get("ts")) for a in mine) < EARLY_SYMBOL_GAP_MIN * 60.0:
            reject("symbol_gap")
            continue
        price, day_volume = sample[1], max(sample[2], _safe_float(rec.get("volume")))
        day_change = _pd_live_change(rec.get("price"), rec.get("change"), price)
        metrics = _early_metrics(hist, _safe_float(rec.get("avg_volume")), vol_valid)
        ok, reason, details = _early_evaluate(rec, metrics, day_change, phase, price, day_volume)
        if not ok:
            reject(reason)
            continue
        candidates.append((details["score"], symbol, rec, price, day_change, day_volume, details))

    # ذاكرة العيّنات لا تكبر: نحذف الرموز اللي طلعت من القائمة الساخنة من أكثر من 15 دقيقة
    for symbol in [s for s, dq in _early_hist.items() if s not in seen_symbols and dq and now_ts - dq[-1][0] > 900]:
        _early_hist.pop(symbol, None)

    candidates.sort(key=lambda item: item[0], reverse=True)
    vwap_budget = {"left": EARLY_VWAP_FETCH_PER_CYCLE}
    room_today = max(0, EARLY_MAX_PER_DAY - len(todays))
    for score, symbol, rec, price, day_change, day_volume, details in candidates:
        if stats["sent"] >= EARLY_MAX_PER_CYCLE:
            reject("cycle_cap")
            break
        if stats["sent"] >= room_today:
            reject("daily_cap")
            break
        if not _alert_gate_allow(symbol, price, "EARLY"):
            reject("alert_gate")
            continue
        vwap = None
        if phase == "REGULAR":                                       # [FIX-15] VWAP بالجلسة العادية فقط (الرقيقة تشوّهه)
            vwap = _early_vwap_check(symbol, now_ts, vwap_budget)
            if EARLY_VWAP_GATE and vwap is not None and price < vwap:
                reject("below_vwap")
                continue
        try:
            news_line = get_news_context_line(symbol)
        except Exception:
            news_line = None
        try:
            radar_line = _duration_and_speed_line(symbol, rec, now_ts)
        except Exception:
            radar_line = ""
        aplus = _early_aplus(_stock_profile(symbol, now_ts), details["vol_ratio"], bool(news_line), vwap, price)    # [FIX-14]
        send_telegram(_early_message(symbol, price, day_change, details, day_volume, phase, radar_line, news_line, aplus["lines"]))
        _alert_gate_mark(symbol, price, "EARLY")
        alerts.append({"symbol": symbol, "ts": now_ts, "day": today, "price": price, "day_change": round(day_change, 2),
                       "m3": round(details["m3"], 2), "m5": round(details["m5"], 2), "vol_ratio": round(details["vol_ratio"], 2),
                       "aplus": aplus["count"], "peak_price": price, "peak_ts": now_ts, "last_price": price, "last_ts": now_ts,
                       "phase": phase})
        stats["sent"] += 1
        time.sleep(1)

    _early_track_update(alerts, now_ts)
    alerts = alerts[-80:]
    with state_lock:
        state["early_alerts"] = alerts
        if stats["sent"]:
            save_state()
    if stats["sent"] or now_ts - _early_log["ts"] >= 300:
        _early_log["ts"] = now_ts
        rejected = ",".join(f"{k}:{v}" for k, v in sorted(stats["rejected"].items())) or "0"
        logger.info(f"[Early] hot={stats['hot']} (pool+{stats.get('pool_extra', 0)}) sampled={stats['sampled']} sent={stats['sent']} today={len(todays) + stats['sent']} rejected={rejected}")
    return stats


def early_mover_scanner():
    """🟢 EARLY — يبحث عن بداية الحركة قبل الانفجار (يعتمد على hot_watchlist وبث ياهو؛ بدون طلبات شبكة جديدة)."""
    if not EARLY_ENABLED:
        logger.info("ℹ️ EARLY mover disabled (EARLY_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("early")
            if get_market_phase() == "CLOSED":
                time.sleep(60)
                continue
            _early_cycle()
        except Exception as error:
            logger.error(f"[Early] scanner error: {error}")
        time.sleep(EARLY_SCAN_INTERVAL)


SWING_ENABLED = os.getenv("SWING_ENABLED", "true").strip().lower() == "true"
SWING_SCAN_INTERVAL = int(os.getenv("SWING_SCAN_INTERVAL", "21600"))          # كل 6 ساعات
SWING_MAX_SYMBOLS = int(os.getenv("SWING_MAX_SYMBOLS", "150"))
SWING_MIN_PRICE = float(os.getenv("SWING_MIN_PRICE", "1.0"))
SWING_MAX_PRICE = float(os.getenv("SWING_MAX_PRICE", "30.0"))
SWING_MIN_DOLLAR_VOLUME = float(os.getenv("SWING_MIN_DOLLAR_VOLUME", "2000000"))  # سيولة يومية (سعر×حجم)
SWING_MAX_PICKS_PER_CYCLE = int(os.getenv("SWING_MAX_PICKS_PER_CYCLE", "5"))
SWING_HOLD_DAYS = float(os.getenv("SWING_HOLD_DAYS", "7"))
SWING_TARGET_ATR_MULT = float(os.getenv("SWING_TARGET_ATR_MULT", "7.0"))
SWING_STOP_ATR_MULT = float(os.getenv("SWING_STOP_ATR_MULT", "2.0"))          # R:R ≈ 3.5 بالمضاعفات الافتراضية


def _kelly_position_pct(reward_ratio, win_prob=0.50, modifier=0.25):
    """
    معيار Kelly لحساب حجم المركز الأمثل نظريًا كنسبة من رأس المال — فكرة
    مأخوذة من مشروع stock-bot-master مفتوح المصدر (نفس الحساب: fraction =
    (عائد×احتمال_ربح - احتمال_خسارة) / عائد). احتمال الربح 50% افتراضي محافظ
    (ما عندنا نسبة نجاح حقيقية موثّقة لإعدادات Swing بعد)، وmodifier=0.25
    (ربع-Kelly) تقليدي معروف لتفادي التذبذب العنيف لو الاحتمال الحقيقي أقل من
    المفترض — Kelly الكامل عدواني جدًا على بيانات تقديرية.
    يرجع نسبة % من رأس المال (0 لو الحافة سالبة، يعني ما تدخل الصفقة أصلاً).
    """
    if reward_ratio <= 0:
        return 0.0
    win_prob = max(0.01, min(0.99, win_prob))
    lose_prob = 1 - win_prob
    fraction = ((reward_ratio * win_prob) - lose_prob) / reward_ratio
    return max(0.0, fraction * modifier * 100)


def weekly_swing_scanner():
    """
    📅 Weekly Swing Scanner: نمط مختلف تمامًا عن باقي البوت (اللي كله سريع
    وعلى شموع 15m). يفحص شموع يومية على مهل (كل عدة ساعات، مو كل دقيقة)
    ويطلع أفضل كم مرشح لمركز يُمسك لغاية أسبوع — يعيد استخدام
    calculate_support_resistance/calculate_atr/compute_indicators الموجودة
    أصلًا (بدون أي منطق كشف جديد)، بس على إطار زمني وفلترة مختلفين: ترند
    صاعد على اليومي (EMA9>EMA21) + السهم إما قريب من دعم (فرصة ارتداد) أو
    طالع اختراق مؤكد بحجم، وبسيولة يومية أعلى (مناسب لمسكة أيام، مو بنس
    رخيص جدًا يصعب الخروج منه). ما يرسل كل مرشح يعدي الفلتر — يرتّب ويختار
    أفضل SWING_MAX_PICKS_PER_CYCLE بس، لأن الفكرة "قائمة مختارة للأسبوع"
    مو تنبيهات متكررة. ما يتقيّد بحالة السوق (مفتوح/مقفل) لأنه يحلل شمعة
    يومية مكتملة، ومهلته الطويلة (6 ساعات) وحجم الرموز المحدود (≤150) يخليه
    خفيف جدًا على Yahoo رغم إنه يفحص بيانات يومية لكل رمز.
    """
    if not SWING_ENABLED:
        logger.info("ℹ️ Weekly Swing scanner disabled (SWING_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("swing")
            with state_lock:
                universe = list(state.get("tickers", []))[:SWING_MAX_SYMBOLS]
                watch = dict(state.get("swing_watch", {}))
            now_ts = time.time()

            watch = {s: e for s, e in watch.items() if isinstance(e, dict) and
                     (now_ts - _safe_float(e.get("picked_ts", now_ts))) / 86400.0 <= SWING_HOLD_DAYS}

            candidates = []
            for symbol in universe:
                if symbol in watch:
                    continue
                try:
                    df = cached_download(symbol, period="6mo", interval="1d")
                    if df is None or df.empty or len(df) < 60:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    df = compute_indicators(df)
                    price = _safe_float(df['close'].iloc[-1])
                    if price < SWING_MIN_PRICE or price > SWING_MAX_PRICE:
                        continue
                    avg_vol = _safe_float(df['volume'].tail(20).mean())
                    if price * avg_vol < SWING_MIN_DOLLAR_VOLUME:
                        continue
                    ema9 = _safe_float(df['ema9'].iloc[-1])
                    ema21 = _safe_float(df['ema21'].iloc[-1])
                    if ema9 <= ema21:
                        continue
                    sr = calculate_support_resistance(df)
                    support_dist = _safe_float(sr.get('support_dist'), 999)
                    breakout = bool(sr.get('breakout'))
                    near_support = support_dist <= 5.0
                    if not (near_support or breakout):
                        continue
                    trend_strength = (ema9 - ema21) / ema21 * 100 if ema21 > 0 else 0
                    rank_score = (5.0 - min(support_dist, 5.0)) * 10 + trend_strength * 5 + (15 if breakout else 0)
                    candidates.append({
                        "symbol": symbol, "price": price, "support": _sr_level(sr.get('support')),
                        "breakout": breakout, "rank_score": rank_score,
                        "atr": max(_safe_float(calculate_atr(df)), price * 0.03),
                    })
                except Exception as error:
                    logger.debug(f"[Swing] {symbol}: {error}")
                finally:
                    time.sleep(0.25)

            candidates.sort(key=lambda c: c["rank_score"], reverse=True)
            picked = 0
            for c in candidates:
                if picked >= SWING_MAX_PICKS_PER_CYCLE:
                    break
                symbol, price, atr = c["symbol"], c["price"], c["atr"]
                stop = price - atr * SWING_STOP_ATR_MULT
                target = price + atr * SWING_TARGET_ATR_MULT
                rr = (target - price) / max(price - stop, 1e-9)
                kelly_pct = _kelly_position_pct(rr)
                setup_desc = "اختراق مؤكد بحجم" if c["breakout"] else f"ارتداد من دعم (${c['support']:.4f})"
                msg = (f"📅 *SWING PICK (للأسبوع): {symbol}*\n━━━━━━━━━━━━━━━━\n"
                       f"🎯 Setup: {setup_desc}\n"
                       f"💰 السعر: ${price:.4f}\n"
                       f"🛑 وقف مقترح: ${stop:.4f}\n"
                       f"🎯 هدف الأسبوع: ${target:.4f}\n"
                       f"📐 R:R: {rr:.2f} | أفق زمني: حتى {SWING_HOLD_DAYS:.0f} أيام (مو دقايق/ساعات زي باقي التنبيهات)\n"
                       f"💰 حجم مقترح (Kelly تقديري، ربع-Kelly، افتراض 50% نجاح): ~{kelly_pct:.1f}% من رأس المال\n"
                       f"📊 ترند يومي صاعد (EMA9>EMA21) — مركز يُمسك أيام، يحتاج صبر ومتابعة يومية مو لحظية.\n"
                       f"⚠️ توصية تحليلية Paper Trading — راجعه يوميًا، هذا مو تنبيه فوري متكرر.")
                news_line = get_news_context_line(symbol)
                if news_line:
                    msg += f"\n{news_line}"
                send_telegram(msg)
                watch[symbol] = {"picked_ts": now_ts, "entry_price": price, "target": target, "stop": stop}
                picked += 1
                time.sleep(1)

            with state_lock:
                state["swing_watch"] = watch
                save_state()
            logger.info(f"[Swing] universe={len(universe)} candidates={len(candidates)} picked={picked}")
            time.sleep(SWING_SCAN_INTERVAL)
        except Exception as error:
            logger.error(f"[Swing] scanner error: {error}")
            time.sleep(600)


ANOMALY_ENABLED = os.getenv("ANOMALY_ENABLED", "true").strip().lower() == "true"
ANOMALY_SCAN_INTERVAL = int(os.getenv("ANOMALY_SCAN_INTERVAL", "600"))
ANOMALY_MIN_SYMBOLS = int(os.getenv("ANOMALY_MIN_SYMBOLS", "20"))     # لازم عينة كافية عشان المقارنة الإحصائية تصير منطقية
ANOMALY_MAX_ALERTS = int(os.getenv("ANOMALY_MAX_ALERTS", "3"))
ANOMALY_CONTAMINATION = float(os.getenv("ANOMALY_CONTAMINATION", "0.05"))  # نتوقع ~5% من العينة "شاذة" إحصائيًا


def anomaly_scanner():
    """
    🔬 كاشف شذوذ إحصائي (Isolation Forest) — فكرة مأخوذة من مشروع مفتوح المصدر
    اسمه surpriver. الفرق الجوهري عن كل ماسحات البوت الثانية: تلك كلها قواعد
    ثابتة أنت/أنا حددناها (RVOL≥X، تغيّر≥Y%...)، وهذا بدل القواعد الثابتة يقارن
    كل سهم بباقي الأسهم بنفس اللحظة عبر عدة خصائص سوا (عوائد 5/15 دقيقة، نسبة
    الحجم، التذبذب) ويكتشف رياضيًا مين "غريب إحصائيًا" عن البقية — ممكن يمسك
    نمط ما يطابق أي قاعدة صريحة عندنا. يحتاج مكتبة scikit-learn المحلية بس
    (مجانية، بدون API أو حساب، pip install عادي).
    تجريبي بصراحة: هذا أسلوب مختلف تمامًا عن باقي البوت، ونسبة الإنذارات
    الكاذبة الحقيقية على بياناتك غير معروفة لين تجربه فترة.
    """
    if not ANOMALY_ENABLED:
        logger.info("ℹ️ Anomaly scanner disabled (ANOMALY_ENABLED=false)")
        return
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError:
        logger.warning("[Anomaly] scikit-learn غير مثبت — الماسح معطّل. أضفه لـ requirements.txt: scikit-learn")
        return
    while True:
        try:
            scanner_heartbeat("anomaly")
            if get_market_phase() == "CLOSED":
                time.sleep(600)
                continue
            today_key = datetime.now().strftime("%Y-%m-%d")
            with state_lock:
                universe = set(state.get("tickers", []))
                for x in state.get("hot_watchlist", []):
                    if isinstance(x, dict) and x.get("symbol"):
                        universe.add(str(x["symbol"]).upper().strip())
                sent_today = dict(state.get("anomaly_sent", {}))
            symbols = list(universe)[:150]

            rows, valid = [], []
            for symbol in symbols:
                if f"{symbol}_{today_key}" in sent_today:
                    continue
                try:
                    df = cached_download(symbol, period="5d", interval="15m")
                    if df is None or df.empty or len(df) < 20:
                        continue
                    df.columns = [str(c).lower() for c in df.columns]
                    closes = df['close'].values
                    vols = df['volume'].values
                    ret_5 = (closes[-1] / closes[-5] - 1) * 100 if len(closes) >= 5 and closes[-5] > 0 else 0.0
                    ret_15 = (closes[-1] / closes[-15] - 1) * 100 if len(closes) >= 15 and closes[-15] > 0 else 0.0
                    recent_vol = float(np.mean(vols[-5:]))
                    base_vol = float(np.mean(vols[:-5])) if len(vols) > 5 else recent_vol
                    vol_ratio = recent_vol / max(base_vol, 1.0)
                    volatility = float(np.std(closes[-20:]) / max(np.mean(closes[-20:]), 1e-9) * 100)
                    rows.append([ret_5, ret_15, vol_ratio, volatility])
                    valid.append((symbol, float(closes[-1]), ret_15, vol_ratio))
                except Exception as error:
                    logger.debug(f"[Anomaly] {symbol}: {error}")
                time.sleep(0.2)

            sent_count = 0
            if len(rows) >= ANOMALY_MIN_SYMBOLS:
                X = np.array(rows)
                detector = IsolationForest(n_estimators=100, contamination=ANOMALY_CONTAMINATION, random_state=0)
                labels = detector.fit_predict(X)
                scores = detector.score_samples(X)
                ranked = sorted(zip(valid, labels, scores), key=lambda r: r[2])
                for (symbol, price, ret_15, vol_ratio), label, _score in ranked:
                    if sent_count >= ANOMALY_MAX_ALERTS:
                        break
                    if label != -1:
                        continue  # -1 فقط = شاذ حسب Isolation Forest
                    if vol_ratio < 1.5 or ret_15 < 0:
                        continue  # فلتر اتجاه: نبي شذوذ "صاعد بحجم"، مو أي شذوذ (حتى هابط)
                    msg = (f"🔬 *شذوذ إحصائي مكتشف: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                           f"💰 السعر: ${price:.4f}\n"
                           f"📊 نمط غير عادي إحصائيًا مقارنة بـ{len(rows)} سهم آخر بنفس اللحظة "
                           f"(حجم×{vol_ratio:.1f}، تغيّر 15د {ret_15:+.1f}%)\n"
                           f"🧪 طريقة مختلفة عن باقي التنبيهات (Isolation Forest) — رصد مبكر تجريبي، مو إشارة دخول.\n"
                           f"⚠️ اكتشاف تجريبي جديد — نسبة الإنذارات الكاذبة الحقيقية غير معروفة بعد.")
                    news_line = get_news_context_line(symbol)
                    if news_line:
                        msg += f"\n{news_line}"
                    send_telegram(msg)
                    with state_lock:
                        state.setdefault("anomaly_sent", {})[f"{symbol}_{today_key}"] = True
                        save_state()
                    sent_count += 1
                    time.sleep(1)
            logger.info(f"[Anomaly] pool={len(rows)} sent={sent_count}")
            time.sleep(ANOMALY_SCAN_INTERVAL)
        except Exception as error:
            logger.error(f"[Anomaly] scanner error: {error}")
            time.sleep(180)


INSIDER_BUY_ENABLED = os.getenv("INSIDER_BUY_ENABLED", "true").strip().lower() == "true"
INSIDER_BUY_SCAN_INTERVAL = int(os.getenv("INSIDER_BUY_SCAN_INTERVAL", "3600"))  # ساعة تكفي — فلترات SEC ما تتحدث لحظيًا
INSIDER_BUY_MIN_VALUE = float(os.getenv("INSIDER_BUY_MIN_VALUE", "25000"))
INSIDER_BUY_MAX_PRICE = float(os.getenv("INSIDER_BUY_MAX_PRICE", "5"))
INSIDER_BUY_MAX_PER_CYCLE = int(os.getenv("INSIDER_BUY_MAX_PER_CYCLE", "5"))


def insider_buy_scanner():
    """
    👔 شراء الداخليين (Insider Buying) — من OpenInsider.com، موقع عام مجاني
    بدون حساب يجمّع نماذج Form 4 الرسمية من SEC EDGAR (شراء مدراء/كبار الملاك
    لسهم شركتهم بأموالهم الخاصة — إشارة ثقة معروفة، ونادرة نسبيًا بأسهم البنس
    تحديدًا). فكرة من مشروع مفتوح المصدر. مختلف كليًا عن كل ماسحات البوت
    الثانية (بيانات ملكية رسمية، مو سعر/حجم). يقرأ أعمدة الجدول بالاسم (مو
    ترقيم ثابت) عشان يبقى صامد لو ترتيب أعمدة الموقع تغيّر بالمستقبل.
    ⚠️ ملاحظة صراحة: بنية جدول الموقع ما قدرت أتحقق منها بفتح صفحة حية وقت
    كتابة هذا الكود — راقب أول تنبيه يوصلك تتأكد البيانات معقولة (سعر/قيمة
    منطقية)، وقول لي لو طلعت غريبة عشان أصلحها بسرعة.
    """
    if not INSIDER_BUY_ENABLED:
        logger.info("ℹ️ Insider buy scanner disabled (INSIDER_BUY_ENABLED=false)")
        return
    url = (f"http://openinsider.com/screener?s=&o=&pl=&ph={INSIDER_BUY_MAX_PRICE:.0f}&ll=&lh=&fd=7&fdr=&td=0&tdr="
           f"&fdlyl=&fdlyh=&daysago=&xp=1&vl={int(INSIDER_BUY_MIN_VALUE / 1000)}&vh=&ocl=&och=&sic1=-1&sicl=100"
           f"&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=100&page=1")
    while True:
        try:
            scanner_heartbeat("insider_buy")
            today_key = datetime.now().strftime("%Y-%m-%d")
            with state_lock:
                sent_today = dict(state.get("insider_buy_sent", {}))
            from bs4 import BeautifulSoup
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table', class_='tinytable') or soup.find('table')
            sent_count = 0
            if table:
                header_cells = [th.get_text(strip=True).lower() for th in table.find_all('th')]

                def col(*names):
                    for name in names:
                        for i, h in enumerate(header_cells):
                            if name in h:
                                return i
                    return -1

                idx = {"symbol": col("ticker"), "insider": col("insider name"), "title": col("title"),
                       "type": col("trade type"), "price": col("price"), "qty": col("qty", "shares"),
                       "value": col("value")}
                rows = table.find_all('tr')[1:]
                for row in rows:
                    if sent_count >= INSIDER_BUY_MAX_PER_CYCLE:
                        break
                    cells = [c.get_text(strip=True) for c in row.find_all('td')]
                    if len(cells) < 5 or min(idx.values()) < 0 or max(idx.values()) >= len(cells):
                        continue
                    try:
                        symbol = cells[idx["symbol"]].upper().strip()
                        insider_name = cells[idx["insider"]]
                        title = cells[idx["title"]]
                        trade_type = cells[idx["type"]]
                        price = _safe_float(cells[idx["price"]].replace('$', '').replace(',', ''))
                        qty = _safe_float(cells[idx["qty"]].replace(',', '').replace('+', ''))
                        value = _safe_float(cells[idx["value"]].replace('$', '').replace(',', '').replace('+', ''))
                    except (IndexError, ValueError):
                        continue
                    if not symbol or 'P' not in trade_type[:2].upper() or value < INSIDER_BUY_MIN_VALUE:
                        continue  # نبي شراء فعلي (P - Purchase) فوق الحد الأدنى بس
                    gate_key = f"{symbol}_{insider_name}_{today_key}"
                    if gate_key in sent_today:
                        continue
                    msg = (f"👔 *شراء داخلي مؤكد: {symbol}*\n━━━━━━━━━━━━━━━━\n"
                           f"👤 {insider_name} ({title})\n"
                           f"💰 اشترى بسعر ${price:.2f} × {qty:,.0f} سهم ≈ ${value:,.0f}\n"
                           f"📋 المصدر: نموذج Form 4 رسمي (SEC EDGAR عبر OpenInsider)\n"
                           f"💡 شراء بأموال شخصية = إشارة ثقة، مو ضمان حركة سعر قريبة أو توصية.")
                    news_line = get_news_context_line(symbol)
                    if news_line:
                        msg += f"\n{news_line}"
                    send_telegram(msg)
                    with state_lock:
                        state.setdefault("insider_buy_sent", {})[gate_key] = True
                        save_state()
                    sent_today[gate_key] = True
                    sent_count += 1
                    time.sleep(1)
            logger.info(f"[InsiderBuy] sent={sent_count}")
        except Exception as error:
            logger.error(f"[InsiderBuy] scanner error: {error}")
        time.sleep(INSIDER_BUY_SCAN_INTERVAL)


CLOSED_REVIEW_ENABLED = os.getenv("CLOSED_REVIEW_ENABLED", "true").strip().lower() == "true"
CLOSED_REVIEW_LOOKBACK_DAYS = int(os.getenv("CLOSED_REVIEW_LOOKBACK_DAYS", "7"))
CLOSED_REVIEW_CHECK_INTERVAL = int(os.getenv("CLOSED_REVIEW_CHECK_INTERVAL", "1800"))


def closed_hours_review_scanner():
    """
    📊 يستغل وقت إغلاق السوق (عطلة نهاية الأسبوع، أو ~8 ساعات يوميًا بعد
    إغلاق الأفتر لين قبل البري ماركت) بدل ما يكون فاضي بالكامل: يراجع كل
    توصيات Ranking (recommendation_history) من آخر CLOSED_REVIEW_LOOKBACK_DAYS
    أيام، يجيب السعر الحالي الفعلي لكل وحدة، ويحسب وين وصلت حقًا (لمست
    الهدف؟ ضربت الوقف؟ لسه مفتوحة؟) — تقرير أداء بأرقام فعلية محسوبة، مو
    كلام نظري. يرسل مرة وحدة بس لكل يوم إغلاق (مو كل نصف ساعة).
    """
    if not CLOSED_REVIEW_ENABLED:
        logger.info("ℹ️ Closed-hours review disabled (CLOSED_REVIEW_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("closed_review")
            if get_market_phase() != "CLOSED":
                time.sleep(CLOSED_REVIEW_CHECK_INTERVAL)
                continue
            today_key = datetime.now().strftime("%Y-%m-%d")
            with state_lock:
                last_review = state.get("last_closed_review_date")
                history = list(state.get("recommendation_history", []))
            if last_review == today_key:
                time.sleep(CLOSED_REVIEW_CHECK_INTERVAL)
                continue  # صار تقرير اليوم أصلاً، ما نكرر

            cutoff = time.time() - CLOSED_REVIEW_LOOKBACK_DAYS * 86400
            recent = [r for r in history if _safe_float(r.get("time", 0)) >= cutoff and r.get("symbol")]
            if not recent:
                with state_lock:
                    state["last_closed_review_date"] = today_key
                    save_state()
                time.sleep(CLOSED_REVIEW_CHECK_INTERVAL)
                continue

            wins = losses = 0
            total_pct, lines = 0.0, []
            for rec in recent[-20:]:
                symbol = rec.get("symbol")
                entry_price = _safe_float(rec.get("price"))
                target = _safe_float(rec.get("target_1"))
                stop = _safe_float(rec.get("stop_loss"))
                if entry_price <= 0:
                    continue
                current = _finnhub_current_price(symbol)
                if not current or current <= 0:
                    continue
                pct = (current - entry_price) / entry_price * 100
                total_pct += pct
                if target > 0 and current >= target:
                    wins += 1; tag = "✅ هدف"
                elif stop > 0 and current <= stop:
                    losses += 1; tag = "🛑 وقف"
                else:
                    tag = "➖ مفتوح"
                lines.append(f"{symbol}: {pct:+.1f}% {tag}")
                time.sleep(0.3)

            decided = wins + losses
            win_rate = (wins / decided * 100) if decided > 0 else 0.0
            avg_pct = (total_pct / len(lines)) if lines else 0.0
            msg = (f"📊 *مراجعة أداء آخر {CLOSED_REVIEW_LOOKBACK_DAYS} أيام* (وقت إغلاق السوق)\n━━━━━━━━━━━━━━━━\n"
                   f"📈 {len(lines)} توصية رُوجعت | نسبة الحسم: {win_rate:.0f}% ({wins} هدف / {losses} وقف)\n"
                   f"💰 متوسط التغيّر الحالي: {avg_pct:+.1f}%\n\n" + "\n".join(lines[:15]))
            if len(lines) > 15:
                msg += f"\n… و{len(lines) - 15} أخرى"
            msg += "\n\n⚠️ أرقام فعلية محسوبة الآن من الأسعار الحقيقية — مو تقدير."
            send_telegram(msg)
            with state_lock:
                state["last_closed_review_date"] = today_key
                save_state()
            logger.info(f"[ClosedReview] reviewed={len(lines)} win_rate={win_rate:.0f}%")
        except Exception as error:
            logger.error(f"[ClosedReview] error: {error}")
        time.sleep(CLOSED_REVIEW_CHECK_INTERVAL)


def _bigmove_tier(change):
    return sum(1 for t in _BIGMOVE_TIERS if change >= t)

def _bigmove_eval(rec, engine, change, price, now_ts, today, reserve=True, seed_peak=0.0):
    """
    قرار نقي (بدون أي I/O) — هل نسمح بتنبيه «حركة كبيرة» لهذا السهم الآن؟
    rec = سجل السهم اليوم (أو None). يرجع (مسموح، السبب، rec_محدّث، نسبة_الهبوط_من_القمة).
    القواعد: (1) قمة اليوم تُتبَّع، والنظام يتصاعد ACTIVE→DISTRIBUTION→DUMP ولا ينزل
    إلا بقمة جديدة أعلى من القديمة بـBIGMOVE_NEWHIGH_MARGIN (حدث جديد). (2) خارج ACTIVE:
    ما نرسل «دخول/اندفاع» جديد. (3) داخل ACTIVE: رسالة جديدة فقط لو عبر شريحة أعلى
    (20/50/100/200%) أو نما ≥BIGMOVE_UPDATE_DELTA نقطة (مع كولداون، إلا لو النمو قوي).
    """
    fresh = {"date": today, "peak": 0.0, "regime": "ACTIVE"}
    rec = dict(rec) if isinstance(rec, dict) and rec.get("date") == today else fresh
    peak = _safe_float(rec.get("peak"))
    regime = rec.get("regime", "ACTIVE")
    if seed_peak and seed_peak > peak:
        peak = seed_peak
    if price > 0:
        if peak > 0 and price >= peak * (1 + BIGMOVE_NEWHIGH_MARGIN) and regime != "ACTIVE":
            regime = "ACTIVE"
            for k in ("last_ts", "last_change", "last_engine", "last_price", "last_tier"):
                rec.pop(k, None)
            peak = price
        elif price > peak:
            peak = price
    drawdown = (peak - price) / peak * 100.0 if peak > 0 and price > 0 else 0.0
    derived = "DUMP" if drawdown >= BIGMOVE_DUMP_PCT else ("DISTRIBUTION" if drawdown >= BIGMOVE_DISTRIBUTION_PCT else "ACTIVE")
    if _REGIME_RANK[derived] > _REGIME_RANK.get(regime, 0):
        regime = derived
    rec["peak"], rec["regime"], rec["seen_ts"] = peak, regime, now_ts
    if regime != "ACTIVE":
        return False, f"{regime}_ACTIVE", rec, drawdown
    last_ts = _safe_float(rec.get("last_ts"))
    if last_ts > 0:
        last_change = _safe_float(rec.get("last_change"))
        grown = change - last_change
        tier_up = _bigmove_tier(change) > int(rec.get("last_tier", 0))
        if not tier_up and grown < BIGMOVE_UPDATE_DELTA:
            return False, "DUPLICATE", rec, drawdown
        if not tier_up and (now_ts - last_ts) < BIGMOVE_UPDATE_COOLDOWN_SEC and grown < BIGMOVE_STRONG_DELTA:
            return False, "COOLDOWN", rec, drawdown
    if reserve:
        rec.update({"last_engine": engine, "last_change": change, "last_ts": now_ts,
                    "last_price": price, "last_tier": _bigmove_tier(change)})
    return True, "OK", rec, drawdown

def _bigmove_gate(symbol, engine, change, price, now_ts=None, seed_peak=0.0, reserve=True):
    """غلاف بحالة (state) حول _bigmove_eval — ذرّي بقفل واحد، فمحركان ما يقدرون يرسلون نفس الحركة سوا.
    يرجع (مسموح، السبب، النظام، الهبوط%)."""
    now_ts = now_ts or time.time()
    today = now_est().strftime("%Y-%m-%d")
    with state_lock:
        reg = state.setdefault("bigmove", {})
        allowed, reason, new_rec, drawdown = _bigmove_eval(reg.get(symbol), engine, change, price, now_ts, today,
                                                           reserve, seed_peak)
        reg[symbol] = new_rec
        if len(reg) > 400:
            keep = sorted(reg.items(), key=lambda kv: _safe_float(kv[1].get("seen_ts")), reverse=True)[:300]
            state["bigmove"] = dict(keep)
    return allowed, reason, new_rec.get("regime", "ACTIVE"), drawdown

def _seed_peak_for(symbol, candidate=None):
    """أعلى سعر شفناه فعليًا (سجل الأسعار + بُعد المرشح عن قمة اليوم) — عشان ما نعتبر سهم منهار شافه البوت متأخرًا كأنه بلا قمة."""
    peak = 0.0
    try:
        with _price_hist_lock:
            hist = list(_price_hist.get(symbol, []))
        if hist:
            peak = max(_safe_float(h[1]) for h in hist)
    except Exception:
        pass
    if isinstance(candidate, dict):
        p0 = _safe_float(candidate.get("price"))
        off = _safe_float(candidate.get("off_high_pct"))
        if p0 > 0 and 0 < off < 95:
            peak = max(peak, p0 / (1 - off / 100.0))
    return peak

def _fresh_change(candidate, fresh_price):
    """
    نسبة التغيّر محسوبة من السعر الحي الحالي بدل نسبة قديمة عالقة بمرشح hot_watchlist.
    السعر والنسبة بالمرشح جايين من نفس عرض ياهو (متسقين)، فنشتق منهم الإغلاق المرجعي
    ونحسب النسبة الحقيقية الآن — كانت رسالة PUMP تعرض +309.6% بسعر انهار فعليًا لـ+59%.
    """
    p0 = _safe_float(candidate.get("price"))
    ch0 = _safe_float(candidate.get("change"))
    if p0 > 0 and fresh_price and fresh_price > 0 and ch0 > -99:
        ref_close = p0 / (1 + ch0 / 100.0)
        if ref_close > 0:
            return (fresh_price / ref_close - 1) * 100.0
    return ch0


def _session_stamp(volume_note=False):
    """سطر واضح بوقت الرسالة وجلستها (نيويورك + السعودية) — عشان تعرف بالضبط أي جلسة صدر منها التنبيه.
    volume_note=True (تنبيهات Gap/Mega اللي تعرض الحجم): بالجلسات الممتدة الحجم المعروض هو حجم الجلسة العادية
    (regularMarketVolume من ياهو) مو حجم الجلسة الممتدة — RVOL بالرانكنق/التوصيات محسوب أصلاً على نفس الجلسة."""
    try:
        et = now_est()
        sa = et.astimezone(pytz.timezone("Asia/Riyadh"))
        phase = get_market_phase()
        line = f"🕐 {et.strftime('%H:%M')} ET | {sa.strftime('%H:%M')} السعودية — {_PHASE_LABEL_AR.get(phase, phase)}"
        if volume_note and phase in ("PRE", "AFTER"):
            line += "\nℹ️ جلسة ممتدة: الحجم المعروض = حجم الجلسة العادية (مو الجلسة الممتدة) — السيولة الفعلية الحين أضعف"
        return line
    except Exception:
        return ""

def _price_src_label(symbol):
    """مصدر آخر سعر جبناه لهذا السهم (بث مباشر / Finnhub / ياهو متأخر) وعمره — شفافية عن سبب اختلاف الأسعار بين المحركات."""
    src = _last_price_source.get(symbol)
    if not src:
        return ""
    label, ts = src
    age = int(time.time() - ts)
    return f" ({label}، قبل {age}ث)" if age >= 5 else f" ({label})"


def _finalize_recommendation_plan(base):
    """
    تحقق صارم قبل أي تسجيل/إرسال (مو مجرد تعليمات للـAI):
    • البوت للشراء فقط (إشارات، والمستخدم ينفّذ يدويًا): قرار AI بيع/تجنّب ما ينتج خطة صاعدة أبدًا →
      final=AVOID + plan_mode=AVOID_LONG (كانت SELL + technical≥60 تطلع WATCH بوقف تحت السعر وأهداف فوقه).
    • منطقة الدخول تُطبَّع (low≤high) — كانت تنعكس لما السعر تحت VWAP.
    • ترتيب الخطة الصاعدة: stop < entry_low ≤ entry_high < t1 < t2 وconfirm ≥ entry_high، وإلا
      تُسجَّل تحذيرات، وBUY ينزل لـWATCH (ما نعتمد صفقة بخطة متناقضة).
    """
    base["plan_warnings"] = []
    ai_dec = str(base.get("ai_decision", "")).upper()
    if ai_dec in BEARISH_AI_DECISIONS:
        base["plan_mode"] = "AVOID_LONG"
        base["final_decision"] = "AVOID"
        return base
    base["plan_mode"] = "LONG"
    warnings = base["plan_warnings"]
    lo = _safe_float(base.get("entry_zone_low"))
    hi = _safe_float(base.get("entry_zone_high"))
    structural = False
    if lo > hi:
        lo, hi = hi, lo
        structural = True   # منطقة معكوسة = السعر تحت VWAP: خطة ما تنفع كـBUY
        warnings.append("منطقة الدخول كانت معكوسة وتم تصحيحها — السعر تحت VWAP والخطة تفترض استعادته")
    base["entry_zone_low"], base["entry_zone_high"] = lo, hi
    stop = _safe_float(base.get("stop_loss"))
    t1, t2 = _safe_float(base.get("target_1")), _safe_float(base.get("target_2"))
    conf = _safe_float(base.get("confirmation_price"))
    if stop and lo and not stop < lo:
        warnings.append("الوقف مو تحت منطقة الدخول"); structural = True
    if t1 and t2 and hi and not (hi < t1 < t2):
        warnings.append("الأهداف مو أعلى من منطقة الدخول بترتيب صحيح"); structural = True
    if conf and hi and conf < hi:
        warnings.append("سعر التأكيد أقل من أعلى منطقة الدخول")
    if structural and base.get("final_decision") == "BUY":
        base["final_decision"] = "WATCH"
    return base

def _journal_record(symbol, engine, price, extra=None):
    """يسجل كل تنبيه أُرسل فعلًا (محرك، سعر، جلسة، وقت) ليُقاس لاحقًا بمراحل 15/30/60/180 دقيقة."""
    try:
        price = _safe_float(price)
        if not JOURNAL_ENABLED or not symbol or price <= 0:
            return
        entry = {"ts": time.time(), "symbol": str(symbol).upper(), "engine": engine, "price": price,
                 "session": get_market_phase(), "hi": price, "lo": price, "out": {}, "done": False}
        for k, v in (extra or {}).items():
            if k in ("change", "score", "rvol") and v is not None:
                entry[k] = round(_safe_float(v), 2)
            elif k == "gen" and v:
                entry["gen"] = str(v)   # [SLE] الجيل الذي أصدر الإشارة — أساس حساب لياقة كل جينوم
        with state_lock:
            j = state.setdefault("signal_journal", [])
            j.append(entry)
            if len(j) > JOURNAL_MAX:
                del j[: len(j) - JOURNAL_MAX]
    except Exception as error:
        logger.debug(f"[Journal] record error: {error}")

def _dqn_features(entry):
    """خصائص ثابتة وقابلة للتسلسل من الإشارة؛ لا تستخدم أي معلومة مستقبلية."""
    return np.asarray([
        np.clip(_safe_float(entry.get("change")) / 100.0, -1.0, 3.0),
        np.clip(_safe_float(entry.get("score")) / 100.0, 0.0, 1.0),
        np.clip(_safe_float(entry.get("rvol")) / 10.0, 0.0, 3.0),
        1.0 if str(entry.get("session")) == "REGULAR" else 0.5,
        1.0 if str(entry.get("engine")) in ("ANOMALY", "SWING") else 0.0,
        0.0,  # خانة احتياطية؛ لا نستخدم أعلى السعر اللاحق حتى لا يحدث تسريب للمستقبل
    ], dtype=np.float32)

def _dqn_init_model():
    rng = np.random.default_rng(17)
    return {"w1": rng.normal(0, 0.08, (6, 12)).tolist(), "b1": [0.0] * 12,
            "w2": rng.normal(0, 0.08, (12, 3)).tolist(), "b2": [0.0] * 3,
            "target_w1": rng.normal(0, 0.08, (6, 12)).tolist(), "target_b1": [0.0] * 12,
            "target_w2": rng.normal(0, 0.08, (12, 3)).tolist(), "target_b2": [0.0] * 3,
            "steps": 0, "samples": 0, "ready": False, "epsilon": DQN_EPSILON}

def _dqn_forward(model, x, target=False):
    p = "target_" if target else ""
    w1 = np.asarray(model[p+"w1"], dtype=np.float32); b1 = np.asarray(model[p+"b1"], dtype=np.float32)
    w2 = np.asarray(model[p+"w2"], dtype=np.float32); b2 = np.asarray(model[p+"b2"], dtype=np.float32)
    h = np.maximum(0.0, np.asarray(x, dtype=np.float32) @ w1 + b1)
    return h @ w2 + b2

def _dqn_train_from_journal():
    """تدريب صغير دوري من نتائج حقيقية؛ الوضع Shadow لا يغيّر التنبيهات تلقائيًا."""
    if not DQN_ENABLED:
        return {"enabled": False}
    try:
        with state_lock:
            journal = [dict(e) for e in (state.get("signal_journal") or [])]
        samples = []
        for e in journal:
            o = (e.get("out") or {}).get(SLE_HORIZON)
            if not o or o.get("missed"):
                continue
            reward = _safe_float(o.get("net_pct"), _safe_float(o.get("pct")))
            action = 0 if reward >= SLE_EVAL_TARGET_PCT else (2 if reward <= -SLE_EVAL_STOP_PCT else 1)
            samples.append((_dqn_features(e), action, float(np.clip(reward / 25.0, -1.0, 1.0))))
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            agent = sl.setdefault("dqn_agent", _dqn_init_model())
            if not agent.get("w1"):
                agent.update(_dqn_init_model())
            replay = agent.setdefault("replay", [])
            seen = set(agent.setdefault("replay_keys", []))
            for e in journal:
                o = (e.get("out") or {}).get(SLE_HORIZON)
                if not o or o.get("missed"):
                    continue
                key = f"{e.get('ts')}|{e.get('symbol')}|{e.get('engine')}|{SLE_HORIZON}"
                if key in seen:
                    continue
                reward = _safe_float(o.get("net_pct"), _safe_float(o.get("pct")))
                action = 0 if reward >= SLE_EVAL_TARGET_PCT else (2 if reward <= -SLE_EVAL_STOP_PCT else 1)
                replay.append({"x": _dqn_features(e).tolist(), "a": action,
                               "r": float(np.clip(reward / 25.0, -1.0, 1.0)), "key": key})
                seen.add(key)
            if len(seen) > DQN_REPLAY_MAX * 2:
                seen = set(x.get("key") for x in replay[-DQN_REPLAY_MAX:] if x.get("key"))
            agent["replay_keys"] = list(seen)[-DQN_REPLAY_MAX * 2:]
            del replay[:-DQN_REPLAY_MAX]
        if len(replay) < DQN_MIN_SAMPLES:
            with _SLE_LOCK:
                agent["samples"] = len(replay); agent["ready"] = False
            return {"enabled": True, "status": "insufficient_data", "samples": len(replay)}
        batch_size = min(64, len(replay))
        batch = random.sample(replay, batch_size) if len(replay) > batch_size else list(replay)
        for item in batch:
            x = np.asarray(item["x"], dtype=np.float32); a = int(item["a"]); r = float(item["r"])
            q = _dqn_forward(agent, x)
            target_q = _dqn_forward(agent, x, target=True)
            # One-step journal samples have no next state; gamma is retained for
            # compatibility but the target remains the observed reward.
            target = r + 0.0 * DQN_GAMMA * float(np.max(target_q))
            err = target - float(q[a])
            h = np.maximum(0.0, x @ np.asarray(agent["w1"]) + np.asarray(agent["b1"]))
            grad2 = np.zeros(3); grad2[a] = err
            w2_before = np.asarray(agent["w2"], dtype=np.float32)
            agent["w2"] = (w2_before + DQN_LEARNING_RATE * np.outer(h, grad2)).tolist()
            agent["b2"] = (np.asarray(agent["b2"]) + DQN_LEARNING_RATE * grad2).tolist()
            grad_h = (w2_before[:, a] * err) * (h > 0)
            agent["w1"] = (np.asarray(agent["w1"]) + DQN_LEARNING_RATE * np.outer(x, grad_h)).tolist()
            agent["b1"] = (np.asarray(agent["b1"]) + DQN_LEARNING_RATE * grad_h).tolist()
        # Soft target update prevents abrupt target drift.
        tau = 0.05
        for key in ("w1", "b1", "w2", "b2"):
            target_key = "target_" + key
            agent[target_key] = (
                (1.0 - tau) * np.asarray(agent[target_key])
                + tau * np.asarray(agent[key])
            ).tolist()
        agent["steps"] = int(agent.get("steps", 0)) + len(batch)
        agent["samples"] = len(replay); agent["ready"] = True
        agent["last_train_ts"] = time.time()
        save_state()
        return {"enabled": True, "status": "trained", "samples": len(replay), "steps": agent["steps"]}
    except Exception as error:
        logger.debug(f"[DQN] train error: {error}")
        return {"enabled": True, "status": "error", "error": str(error)}


_PPOJournalEnv = None


def _get_ppo_env_class():
    """يبني بيئة PPO عند الحاجة فقط (بعد الاستيراد الكسول)."""
    global _PPOJournalEnv
    if _PPOJournalEnv is None and gym is not None and spaces is not None:
        class _Env(gym.Env):
            """بيئة حلقة واحدة: كل خطوة قرار Paper على عينة تاريخية حقيقية."""
            metadata = {"render_modes": []}
            def __init__(self, rows):
                super().__init__(); self.rows = rows; self.idx = 0
                self.observation_space = spaces.Box(low=-5.0, high=5.0, shape=(6,), dtype=np.float32)
                self.action_space = spaces.Discrete(3)  # HOLD, BUY, SELL
            def reset(self, seed=None, options=None):
                super().reset(seed=seed); self.idx = int(self.np_random.integers(0, len(self.rows)))
                return np.asarray(self.rows[self.idx][0], dtype=np.float32), {}
            def step(self, action):
                _, reward = self.rows[self.idx]
                action = int(action)
                shaped = reward if action == 1 else (-reward if action == 2 else -abs(reward) * 0.10)
                return np.asarray(self.rows[self.idx][0], dtype=np.float32), float(shaped), True, False, {}
        _PPOJournalEnv = _Env
    return _PPOJournalEnv


def _ppo_train_from_journal():
    """تدريب PPO اختياري مع حفظ نموذج عام ونماذج الأسهم؛ لا يلمس التنبيهات الحالية."""
    if not PPO_ENABLED:
        return {"enabled": False, "status": "disabled"}
    try:
        with _SLE_LOCK:
            previous = dict((state.get("self_learning") or {}).get("ppo_agent") or {})
        last_train = _safe_float(previous.get("last_train_ts"), 0.0)
        if last_train and time.time() - last_train < PPO_RETRAIN_INTERVAL_SEC:
            return {"enabled": True, "status": "cooldown", "samples": int(previous.get("samples", 0)),
                    "models": int(previous.get("models", 0)),
                    "next_train_in_sec": int(PPO_RETRAIN_INTERVAL_SEC - (time.time() - last_train))}
        with state_lock:
            journal = [dict(e) for e in (state.get("signal_journal") or [])]
        grouped = {}
        for e in journal:
            o = (e.get("out") or {}).get(SLE_HORIZON)
            if not o or o.get("missed"):
                continue
            reward = float(np.clip(_safe_float(o.get("net_pct"), _safe_float(o.get("pct"))) / 25.0, -1.0, 1.0))
            grouped.setdefault(str(e.get("symbol") or "GENERAL").upper(), []).append((_dqn_features(e), reward))
        all_rows = [x for rows in grouped.values() for x in rows]
        if len(all_rows) < PPO_MIN_SAMPLES:
            return {"enabled": True, "status": "insufficient_data", "samples": len(all_rows)}
        # [MemFix] لا نحمّل torch إلا لو في ذاكرة تكفي، وإلا نتخطى هالدورة (بدون ما نكسر شي ولا نعلّم cooldown).
        if not _RL_STATE["ok"]:
            try:
                _rss_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024) if psutil else 0.0
            except Exception:
                _rss_mb = 0.0
            if _rss_mb and _rss_mb + PPO_TORCH_RESERVE_MB > MEMORY_LIMIT_MB:
                logger.info(f"[PPO] skipped: RSS={_rss_mb:.0f}MB + torch~{PPO_TORCH_RESERVE_MB}MB > limit {MEMORY_LIMIT_MB}MB")
                return {"enabled": True, "status": "skipped_low_memory", "samples": len(all_rows)}
            if not _rl_lazy_import():
                return {"enabled": True, "status": "dependencies_missing", "install": "stable-baselines3 gymnasium"}
        env_cls = _get_ppo_env_class()
        if env_cls is None:
            return {"enabled": True, "status": "dependencies_missing", "install": "stable-baselines3 gymnasium"}
        os.makedirs(PPO_MODEL_DIR, exist_ok=True)
        trained = 0
        targets = [("GENERAL", all_rows)] + [(s, r) for s, r in grouped.items() if len(r) >= max(20, PPO_MIN_SAMPLES // 2)]
        for symbol, rows in targets[:13]:
            path = os.path.join(PPO_MODEL_DIR, "agent_" + re.sub(r"[^A-Z0-9_-]", "_", symbol) + ".zip")
            env = env_cls(rows)
            model = None
            if os.path.exists(path):
                try: model = PPO.load(path, env=env)
                except Exception: model = None
            if model is None:
                model = PPO("MlpPolicy", env, verbose=0, seed=17, n_steps=32, batch_size=32, learning_rate=3e-4)
            model.learn(total_timesteps=max(64, PPO_TRAIN_STEPS), reset_num_timesteps=False)
            model.save(path); trained += 1
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            sl["ppo_agent"] = {"status": "trained", "samples": len(all_rows), "models": trained,
                                "last_train_ts": time.time(), "model_dir": PPO_MODEL_DIR,
                                "shadow_only": PPO_SHADOW_ONLY}
        return {"enabled": True, "status": "trained", "samples": len(all_rows), "models": trained}
    except Exception as error:
        logger.debug(f"[PPO] train error: {error}")
        return {"enabled": True, "status": "error", "error": str(error)}


def _rolling_ml_train_from_journal():
    """Rolling Walk-forward: تدريب على الماضي واختبار على الجزء الزمني اللاحق فقط."""
    if not ROLLING_ML_ENABLED:
        return {"enabled": False, "status": "disabled"}
    try:
        with state_lock:
            journal = sorted((dict(e) for e in (state.get("signal_journal") or [])),
                             key=lambda e: _safe_float(e.get("ts")))
        rows = []
        for e in journal[-ROLLING_ML_WINDOW:]:
            o = (e.get("out") or {}).get(SLE_HORIZON)
            if not o or o.get("missed"):
                continue
            net = _safe_float(o.get("net_pct"), _safe_float(o.get("pct")))
            label = 0 if _safe_float(o.get("hi")) >= SLE_EVAL_TARGET_PCT else (2 if net <= -SLE_EVAL_STOP_PCT else 1)
            rows.append((_dqn_features(e), label))
        if len(rows) < ROLLING_ML_MIN_SAMPLES:
            return {"enabled": True, "status": "insufficient_data", "samples": len(rows)}
        cut = max(30, int(len(rows) * 0.70)); train, test = rows[:cut], rows[cut:]
        X = np.vstack([x for x, _ in train]); y = np.asarray([label for _, label in train])
        W = np.zeros((6, 3), dtype=np.float32); b = np.zeros(3, dtype=np.float32)
        for _ in range(ROLLING_ML_EPOCHS):
            logits = X @ W + b; logits -= logits.max(axis=1, keepdims=True)
            probs = np.exp(logits); probs /= np.maximum(probs.sum(axis=1, keepdims=True), 1e-9)
            one = np.zeros_like(probs); one[np.arange(len(y)), y] = 1.0
            grad = (probs - one) / len(y)
            W -= 0.08 * (X.T @ grad); b -= 0.08 * grad.sum(axis=0)
        Xt = np.vstack([x for x, _ in test]); yt = np.asarray([label for _, label in test])
        pred = np.argmax(Xt @ W + b, axis=1)
        accuracy = float((pred == yt).mean()) if len(yt) else 0.0
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            sl["rolling_model"] = {"w": W.tolist(), "b": b.tolist(), "samples": len(rows),
                                    "train": len(train), "test": len(test), "accuracy": round(accuracy, 3),
                                    "trained_ts": time.time(), "shadow_only": ROLLING_ML_SHADOW_ONLY}
        save_state()
        return {"enabled": True, "status": "trained", "samples": len(rows), "train": len(train),
                "test": len(test), "accuracy": round(accuracy, 3)}
    except Exception as error:
        logger.debug(f"[RollingML] train error: {error}")
        return {"enabled": True, "status": "error", "error": str(error)}


def _finrl_review_from_journal():
    """بيئة FinRL خفيفة خاصة بالـPenny: مكافأة صافية، تكلفة تداول، تقلب، وسحب أقصى."""
    if not FINRL_ENABLED:
        return {"enabled": False, "status": "disabled"}
    try:
        with state_lock:
            journal = sorted((dict(e) for e in (state.get("signal_journal") or [])),
                             key=lambda e: _safe_float(e.get("ts")))
        returns = []
        for e in journal:
            o = (e.get("out") or {}).get(SLE_HORIZON)
            if not o or o.get("missed"):
                continue
            raw = _safe_float(o.get("net_pct"), _safe_float(o.get("pct")))
            # تكلفة دخول/خروج تقريبية + عقوبة المخاطرة، مع عدم اختلاق بيانات Level 2.
            net = raw - FINRL_TRADE_COST_PCT
            risk = max(0.0, -_safe_float(o.get("lo")) / max(SLE_EVAL_STOP_PCT, 1.0))
            returns.append(net - FINRL_RISK_PENALTY * risk)
        if len(returns) < 10:
            result = {"enabled": True, "status": "insufficient_data", "samples": len(returns)}
        else:
            curve = np.cumsum(np.asarray(returns, dtype=np.float32))
            peak = np.maximum.accumulate(curve)
            drawdown = float(np.max(peak - curve)) if len(curve) else 0.0
            avg = float(np.mean(returns)); vol = float(np.std(returns))
            sharpe = avg / max(vol, 1e-6) * np.sqrt(len(returns))
            result = {"enabled": True, "status": "ok", "samples": len(returns),
                      "reward": round(float(np.sum(returns)), 3), "avg_reward": round(avg, 3),
                      "volatility": round(vol, 3), "sharpe_proxy": round(float(sharpe), 3),
                      "max_drawdown": round(drawdown, 3),
                      "circuit_breaker": bool(drawdown >= FINRL_MAX_DRAWDOWN_PCT),
                      "shadow_only": FINRL_SHADOW_ONLY, "updated_ts": time.time()}
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            sl["finrl_review"] = result
        return result
    except Exception as error:
        logger.debug(f"[FinRL] review error: {error}")
        return {"enabled": True, "status": "error", "error": str(error)}

def _journal_apply_sample(entry, price_now, now_ts):
    """نقي: يحدّث أعلى/أدنى سعر بعد التنبيه ويسجّل نتيجة كل مرحلة زمنية انقضت. يرجع نسخة محدّثة."""
    e = dict(entry)
    e["out"] = dict(e.get("out", {}))
    p0 = _safe_float(e.get("price"))
    if p0 <= 0 or not price_now or price_now <= 0:
        return e
    e["hi"] = max(_safe_float(e.get("hi"), p0) or p0, price_now)
    e["lo"] = min(_safe_float(e.get("lo"), p0) or p0, price_now)
    age_min = (now_ts - _safe_float(e.get("ts"))) / 60.0
    for h in JOURNAL_HORIZONS_MIN:
        key = str(h)
        if age_min >= h and key not in e["out"]:
            if age_min > h + max(12.0, h * 0.5):
                # فجوة أخذ عيّنات طويلة (البوت كان متوقف/مشغول): سعر الآن ما يمثّل نتيجة هالمرحلة، نعلّمها فائتة بدل ما نكذب
                e["out"][key] = {"missed": True}
            else:
                raw_pct = (price_now / p0 - 1) * 100
                net_pct = raw_pct - (ML4T_FEES_BPS / 100.0) - ML4T_SLIPPAGE_PCT - ML4T_SPREAD_PCT
                e["out"][key] = {"pct": round(raw_pct, 2), "net_pct": round(net_pct, 2),
                                 "hi": round((e["hi"] / p0 - 1) * 100, 2),
                                 "lo": round((e["lo"] / p0 - 1) * 100, 2)}
    e["done"] = all(str(h) in e["out"] for h in JOURNAL_HORIZONS_MIN)
    return e

def _journal_summary_text(journal, horizon="60", min_samples=1):
    """ملخص أداء كل محرك: عدد الإشارات، نسبة الإيجابي عند المرحلة، متوسط العائد، متوسط أعلى ربح وأسوأ هبوط بعد التنبيه."""
    by_engine = {}
    for e in journal:
        o = (e.get("out") or {}).get(horizon)
        if o and not o.get("missed"):
            by_engine.setdefault(e.get("engine", "?"), []).append(o)
    if not by_engine:
        return None
    lines = []
    for engine, outs in sorted(by_engine.items(), key=lambda kv: -len(kv[1])):
        n = len(outs)
        if n < min_samples:
            continue
        avg = sum(o["pct"] for o in outs) / n
        win = sum(1 for o in outs if o["pct"] > 0) / n * 100
        avg_hi = sum(o["hi"] for o in outs) / n
        avg_lo = sum(o["lo"] for o in outs) / n
        label = str(engine).replace("_", " ")   # الشرطة السفلية تكسر Markdown تيليجرام (ESE_ARMED مثلًا)
        lines.append(f"• *{label}* ({n}): إيجابي {win:.0f}% | متوسط {avg:+.1f}% | أعلى ربح {avg_hi:+.1f}% | أسوأ هبوط {avg_lo:+.1f}%")
    return "\n".join(lines) if lines else None

def signal_journal_worker():
    """
    📒 يقيس نتيجة كل إشارة أرسلها البوت فعليًا بعد 15/30/60/180 دقيقة (السعر + أعلى ربح وأسوأ هبوط وصلها السهم
    بعد التنبيه، بعيّنات كل ~5 دقايق). هذا اللي يجاوب بأرقام حقيقية: أي محرك يستاهل تثق فيه وأي واحد ضجيج؟ — بدل الانطباع.
    يستخدم أسرع سعر متاح (بث مباشر → Finnhub → ياهو)، وما يزوّر نتيجة مرحلة لو صار فجوة بأخذ العيّنات (يعلّمها فائتة).
    """
    if not JOURNAL_ENABLED:
        logger.info("ℹ️ Signal journal disabled (JOURNAL_ENABLED=false)")
        return
    while True:
        try:
            scanner_heartbeat("journal")
            now_ts = time.time()
            with state_lock:
                open_entries = [dict(e) for e in state.get("signal_journal", []) if not e.get("done")]
            open_entries.sort(key=lambda e: _safe_float(e.get("ts")))
            updates, checked = {}, 0
            for e in open_entries:
                if checked >= JOURNAL_MAX_CHECKS_PER_CYCLE:
                    break
                if now_ts - _safe_float(e.get("ts")) < 120:
                    continue   # طازجة جدًا — أول عيّنة بعد دقيقتين
                price_now = _finnhub_current_price(e.get("symbol", ""))
                if not price_now or price_now <= 0:
                    continue
                updates[(e.get("ts"), e.get("symbol"), e.get("engine"))] = _journal_apply_sample(e, price_now, now_ts)
                checked += 1
                time.sleep(0.4)
            if updates:
                with state_lock:
                    j = state.get("signal_journal", [])
                    for i, e in enumerate(j):
                        key = (e.get("ts"), e.get("symbol"), e.get("engine"))
                        if key in updates:
                            j[i] = updates[key]
                    save_state()
            logger.info(f"[Journal] open={len(open_entries)} sampled={checked}")
        except Exception as error:
            logger.error(f"[Journal] worker error: {error}")
        time.sleep(JOURNAL_CHECK_INTERVAL)

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


# =====================================================================
# 🧠 SLE — SELF-LEARNING & AUTONOMOUS EVOLUTION ENGINE   ("الروبوت")
# ---------------------------------------------------------------------
# هذا القسم يحوّل البوت من "سكربت يرسل تنبيهات" إلى كائن يتعلّم من نتائج
# قراراته نفسه ويغيّر سلوكه بناءً على الأرقام. لا يلمس منطق أي ماسح؛ يعمل
# كطبقة مستقلة فوقه:
#
#   1) الإدراك (Perception): كل تنبيه يخرج فعليًا يُلتقط تلقائيًا (سعر + سهم
#      + محرك) ويُسجَّل في سجل الإشارات الموجود، فيُقاس بعد 15/30/60/180 دقيقة.
#   2) الذاكرة (Memory): جينوم (Genome) = مجموعة الإعدادات القابلة للتعديل،
#      + سجل أجيال + دروس مكتوبة + إحصاءات لكل محرك.
#   3) التعلّم (Learning): لكل محرك ثقة محسوبة باحتمال بايزي (Beta posterior)
#      من نتائجه الحقيقية، وكل جيل له لياقة (fitness) من متوسط عوائده الحقيقية.
#   4) التطوّر (Evolution): اختيار/تهجين/طفرة على الجينوم. الأفضل يبقى،
#      والأضعف يُستبدل، والطفرة تُسجَّل بشرح عربي (وش غيّر وليش).
#   5) الجهاز المناعي (Immune): محرك أداؤه سيّئ فعليًا يُخفَّض وزنه
#      (throttle جزئي + هامش استكشاف) ويرجع تلقائيًا لو تحسّن.
#   6) الحكم الذاتي (Autonomy): مشرف يعيد تشغيل أي خيط مات، يشخّص نفسه،
#      يضع أهدافًا ويقيس تقدّمه، ويرسل مراجعة ذاتية يومية.
#
# كل شيء قابل للإطفاء: SELF_LEARNING_ENABLED=false → يرجع البوت لسلوكه الأصلي.
# وكل شيء محفوظ في state["self_learning"] فيعيش بعد أي إعادة تشغيل.
# =====================================================================

SLE_ENABLED = os.getenv("SELF_LEARNING_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
SLE_SHADOW_ONLY = os.getenv("SLE_SHADOW_ONLY", "true").strip().lower() in ("1", "true", "yes", "on")
SLE_THROTTLE_ENABLED = os.getenv("SELF_LEARNING_THROTTLE", "true").strip().lower() in ("1", "true", "yes", "on")
SLE_HORIZON = str(os.getenv("SLE_HORIZON", "60"))          # المرحلة الزمنية المعتمدة للحكم
SLE_HORIZON_FALLBACK = ("30", "15", "180")
SLE_EVOLVE_INTERVAL_SEC = int(os.getenv("SLE_EVOLVE_INTERVAL_SEC", str(6 * 3600)))
SLE_MIN_NEW_SAMPLES = int(os.getenv("SLE_MIN_NEW_SAMPLES", "6"))       # أقل عدد نتائج جديدة قبل تطوّر
SLE_MIN_GEN_SAMPLES = int(os.getenv("SLE_MIN_GEN_SAMPLES", "8"))       # أقل عدد نتائج لجينوم ليُحكم عليه
SLE_MIN_ENGINE_SAMPLES = int(os.getenv("SLE_MIN_ENGINE_SAMPLES", "10"))  # أقل عدد نتائج لبناء ثقة محرك
SLE_POPULATION_SIZE = int(os.getenv("SLE_POPULATION_SIZE", "6"))
SLE_THROTTLE_STRENGTH = float(os.getenv("SLE_THROTTLE_STRENGTH", "1.0"))   # كتم أقوى للمحركات المثبت ضعفها
SLE_EXPLORATION_FLOOR = float(os.getenv("SLE_EXPLORATION_FLOOR", "0.05"))  # نترك 5% للاستكشاف لا أكثر
SLE_EVAL_TARGET_PCT = float(os.getenv("SLE_EVAL_TARGET_PCT", "25"))        # الهدف الذي يكافئه التعلم
SLE_EVAL_STOP_PCT = float(os.getenv("SLE_EVAL_STOP_PCT", "6"))             # وقف مرجعي للتقييم
SLE_GEN_EPOCH_SEC = int(os.getenv("SLE_GEN_EPOCH_SEC", "5400"))   # مدة تجربة الجيل الدنيا (90 دقيقة)
SLE_GEN_EPOCH_MAX_SEC = int(os.getenv("SLE_GEN_EPOCH_MAX_SEC", "14400"))  # حد أقصى (4 ساعات) لو الإشارات قليلة
SLE_CHAMPION_EPOCH_SHARE = float(os.getenv("SLE_CHAMPION_EPOCH_SHARE", "0.5"))  # نصيب الجيل الأفضل من التجارب
SLE_DEDUPE_SEC = int(os.getenv("SLE_DEDUPE_SEC", "180"))
SLE_LESSONS_MAX = int(os.getenv("SLE_LESSONS_MAX", "250"))
SLE_SUPERVISOR_INTERVAL = int(os.getenv("SLE_SUPERVISOR_INTERVAL", "60"))
SLE_MAX_THREAD_RESTARTS_PER_HOUR = int(os.getenv("SLE_MAX_THREAD_RESTARTS_PER_HOUR", "8"))
ML4T_ENABLED = os.getenv("ML4T_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
ML4T_FEES_BPS = float(os.getenv("ML4T_FEES_BPS", "0"))
ML4T_SLIPPAGE_PCT = float(os.getenv("ML4T_SLIPPAGE_PCT", "0.20"))
ML4T_SPREAD_PCT = float(os.getenv("ML4T_SPREAD_PCT", "0.20"))
ML4T_MIN_TRAIN_SAMPLES = int(os.getenv("ML4T_MIN_TRAIN_SAMPLES", "30"))
ML4T_MIN_TEST_SAMPLES = int(os.getenv("ML4T_MIN_TEST_SAMPLES", "10"))
ML4T_DRIFT_THRESHOLD_PCT = float(os.getenv("ML4T_DRIFT_THRESHOLD_PCT", "2.0"))
ML4T_CIRCUIT_BREAKER_LOSS_PCT = float(os.getenv("ML4T_CIRCUIT_BREAKER_LOSS_PCT", "-3.0"))
DQN_ENABLED = os.getenv("DQN_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
DQN_SHADOW_ONLY = os.getenv("DQN_SHADOW_ONLY", "true").strip().lower() in ("1", "true", "yes", "on")
DQN_MIN_SAMPLES = int(os.getenv("DQN_MIN_SAMPLES", "50"))
DQN_REPLAY_MAX = int(os.getenv("DQN_REPLAY_MAX", "2000"))
DQN_GAMMA = float(os.getenv("DQN_GAMMA", "0.90"))
DQN_LEARNING_RATE = float(os.getenv("DQN_LEARNING_RATE", "0.01"))
DQN_EPSILON = float(os.getenv("DQN_EPSILON", "0.10"))
PPO_ENABLED = os.getenv("PPO_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
PPO_SHADOW_ONLY = os.getenv("PPO_SHADOW_ONLY", "true").strip().lower() in ("1", "true", "yes", "on")
PPO_MIN_SAMPLES = int(os.getenv("PPO_MIN_SAMPLES", "50"))
PPO_TRAIN_STEPS = int(os.getenv("PPO_TRAIN_STEPS", "256"))
PPO_RETRAIN_INTERVAL_SEC = int(os.getenv("PPO_RETRAIN_INTERVAL_SEC", "21600"))
PPO_MODEL_DIR = os.getenv("PPO_MODEL_DIR", os.path.join(_VOLUME_ROOT, "ppo_agents"))
PPO_TORCH_RESERVE_MB = int(os.getenv("PPO_TORCH_RESERVE_MB", "350"))   # [MemFix] تقدير تكلفة torch قبل تحميلها
ROLLING_ML_ENABLED = os.getenv("ROLLING_ML_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
ROLLING_ML_SHADOW_ONLY = os.getenv("ROLLING_ML_SHADOW_ONLY", "true").strip().lower() in ("1", "true", "yes", "on")
ROLLING_ML_WINDOW = int(os.getenv("ROLLING_ML_WINDOW", "500"))
ROLLING_ML_EPOCHS = int(os.getenv("ROLLING_ML_EPOCHS", "40"))
ROLLING_ML_MIN_SAMPLES = int(os.getenv("ROLLING_ML_MIN_SAMPLES", "60"))
FINRL_ENABLED = os.getenv("FINRL_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
FINRL_SHADOW_ONLY = os.getenv("FINRL_SHADOW_ONLY", "true").strip().lower() in ("1", "true", "yes", "on")
FINRL_MAX_DRAWDOWN_PCT = float(os.getenv("FINRL_MAX_DRAWDOWN_PCT", "12"))
FINRL_TRADE_COST_PCT = float(os.getenv("FINRL_TRADE_COST_PCT", "0.40"))
FINRL_RISK_PENALTY = float(os.getenv("FINRL_RISK_PENALTY", "0.50"))
SLE_DAILY_REVIEW_ENABLED = os.getenv("SLE_DAILY_REVIEW", "true").strip().lower() in ("1", "true", "yes", "on")
SLE_TELEGRAM_HOOK = os.getenv("SLE_TELEGRAM_HOOK", "true").strip().lower() in ("1", "true", "yes", "on")

_SLE_LOCK = threading.RLock()

# ---------------------------------------------------------------------
# 1) الجينوم: المعاملات اللي يقدر الروبوت يعدّلها بنفسه
#    (الاسم، النوع، الأدنى، الأعلى، حجم الخطوة/الطفرة، التصنيف، وصف عربي)
#    القيم الافتراضية تُقرأ لحظة التشغيل من متغيرات البيئة، فتصير هي "الأصل"
#    اللي يُقاس عليه الانحراف — ما نتجاوزها إلا بنسبة محدودة.
# ---------------------------------------------------------------------
SLE_PARAM_SPECS = [
    ("DISCOVERY_MIN_CHANGE",   "float", 1.0,   12.0,  0.6,  "الاكتشاف",   "أدنى تغيّر يومي لدخول قائمة المتابعة"),
    ("HOT_MAX_AGE_SEC",        "int",   300,   3600,  120,  "الاكتشاف",   "عمر السهم قبل اعتباره ميتًا (ثانية)"),
    ("RANK_MIN_AI_SCORE",      "int",   45,    92,    3,    "الترتيب",     "أدنى سكور للترشيح في الرانكنق"),
    ("AUTO_HUNTER_MIN_SCORE",  "int",   55,    95,    3,    "الترتيب",     "أدنى سكور لتنبيه AUTO HUNTER"),
    ("ALERT_SYMBOL_COOLDOWN",  "int",   600,   7200,  240,  "الكتم",       "كولداون التنبيه الموحّد لنفس السهم (ثانية)"),
    ("ALERT_CONTINUATION_PCT", "float", 3.0,   20.0,  1.0,  "الكتم",       "نسبة الصعود اللي تسمح بتنبيه ثانٍ"),
    ("SIGNAL_COOLDOWN",        "int",   900,   10800, 300,  "الكتم",       "كولداون الإشارة العام (ثانية)"),
    ("MAX_DAILY_SIGNALS",      "int",   40,    300,   10,   "الكتم",       "أقصى عدد تنبيهات في اليوم"),
    ("MIDDAY_MIN_SCORE",       "int",   50,    92,    3,    "الجودة",      "أدنى سكور وسط الجلسة"),
    ("RECOMMENDATION_MIN_CONFIDENCE", "int", 60, 95,   2,    "التوصيات",    "أدنى ثقة AI لقبول التوصية"),
    ("RECOMMENDATION_MIN_TECH_SCORE", "int", 55, 95,   2,    "التوصيات",    "أدنى سكور فني للتوصية"),
    ("RECOMMENDATION_MIN_RVOL", "float", 1.0,  6.0,   0.2,  "التوصيات",    "أدنى RVOL للتوصية"),
    ("RECOMMENDATION_MIN_RR",  "float", 1.0,   4.0,   0.15, "التوصيات",    "أدنى نسبة مخاطرة/عائد"),
    ("PENNY_MIN_GAIN",         "float", 5.0,   40.0,  1.5,  "البيني",      "أدنى صعود يومي لسهم بيني"),
    ("PENNY_MIN_RVOL",         "float", 1.5,   10.0,  0.3,  "البيني",      "أدنى RVOL لسهم بيني"),
    ("MIN_RVOL_PRE",           "float", 1.0,   6.0,   0.25, "السيولة",     "أدنى RVOL قبل الافتتاح"),
    ("MIN_RVOL_REGULAR",       "float", 0.8,   5.0,   0.25, "السيولة",     "أدنى RVOL في الجلسة"),
    ("MIN_RVOL_AFTER",         "float", 1.0,   7.0,   0.25, "السيولة",     "أدنى RVOL بعد الإغلاق"),
    ("SETUP_ARM_MIN",          "int",   45,    88,    3,    "ESE",         "أدنى سكور لتسليح إعداد ESE"),
    ("SETUP_MAX_ALERTS_PER_DAY", "int", 3,    40,    2,    "ESE",         "أقصى تنبيهات ESE يوميًا"),
    ("EARLY_MIN_MOVE_3M",      "float", 1.0,   8.0,   0.4,  "EARLY",       "أدنى حركة 3 دقائق لإشارة EARLY"),
    ("EARLY_MIN_VOL_RATIO",    "float", 1.5,   8.0,   0.4,  "EARLY",       "أدنى تسارع حجم لإشارة EARLY"),
    ("EARLY_MAX_PER_DAY",      "int",   3,     30,    2,    "EARLY",       "أقصى إشارات EARLY يوميًا"),
    ("HIGH_BREAKOUT_MIN_RVOL", "float", 1.0,   6.0,   0.3,  "الاختراق",    "أدنى RVOL لاختراق القمة"),
    ("GAP_HUNTER_MIN_CHANGE",  "float", 8.0,   45.0,  2.0,  "الفجوات",     "أدنى فجوة لتنبيه Gap Hunter"),
    ("PUMP_DUMP_MIN_PUMP_PCT", "float", 25.0,  100.0, 4.0,  "البمب",       "أدنى صعود لاعتباره بمب"),
    ("PUMP_DUMP_DROP_FROM_PEAK_PCT", "float", 6.0, 30.0, 1.5, "البمب",     "نسبة الهبوط من القمة = دامب"),
    ("BASE_TP_PCT",            "float", 1.02,  1.20,  0.01, "الصفقات",     "هدف الربح الأساسي (معامل)"),
    ("SL_PCT",                 "float", 0.85,  0.995, 0.01, "الصفقات",     "وقف الخسارة (معامل)"),
]

SLE_PHASE_RVOL_MAP = {"MIN_RVOL_PRE": "PRE", "MIN_RVOL_REGULAR": "REGULAR", "MIN_RVOL_AFTER": "AFTER"}
SLE_SPEC_BY_NAME = {s[0]: s for s in SLE_PARAM_SPECS}


def _sle_baseline_values():
    """القيم الأصلية (من متغيرات البيئة) — تُستخدم كمرجع للأمان ولعرض الانحراف."""
    out = {}
    for name, kind, lo, hi, step, cat, label in SLE_PARAM_SPECS:
        if name in SLE_PHASE_RVOL_MAP:
            phase = SLE_PHASE_RVOL_MAP[name]
            try:
                out[name] = float((globals().get("MIN_RVOL_BY_PHASE") or {}).get(phase, lo))
            except Exception:
                out[name] = float(lo)
        else:
            try:
                out[name] = float(globals().get(name, lo))
            except Exception:
                out[name] = float(lo)
    return out


_SLE_BASELINE = _sle_baseline_values()


def _sle_clamp_param(name, value):
    spec = SLE_SPEC_BY_NAME.get(name)
    if not spec:
        return value
    _, kind, lo, hi, step, cat, label = spec
    lo, hi = float(lo), float(hi)
    try:
        v = float(value)
    except Exception:
        v = float(lo)
    if v != v:  # NaN
        v = float(lo)
    v = max(lo, min(hi, v))
    # سقف انحراف عن الأصل: لا يتجاوز نصف المدى في أي اتجاه (يمنع الانفلات)
    base = float(_SLE_BASELINE.get(name, lo))
    half = (hi - lo) * 0.5
    v = max(base - half, min(base + half, v))
    if kind == "int":
        return int(round(v))
    return round(v, 4)


def _sle_apply_param(name, value):
    """يطبّق معاملًا على المتغير العام الفعلي اللي تقرأه الماسحات."""
    try:
        if name in SLE_PHASE_RVOL_MAP:
            phase = SLE_PHASE_RVOL_MAP[name]
            current = dict(globals().get("MIN_RVOL_BY_PHASE") or {})
            current[phase] = float(value)
            globals()["MIN_RVOL_BY_PHASE"] = current
            return True
        globals()[name] = value
        return True
    except Exception as error:
        logger.debug(f"[SLE] apply {name} failed: {error}")
        return False


def _sle_apply_genome(values):
    # Shadow-only: يقيس ويتطور داخليًا، ولا يغيّر عتبات الإنتاج الحية تلقائيًا.
    if SLE_SHADOW_ONLY:
        return 0
    applied = 0
    for name, val in (values or {}).items():
        if name in SLE_SPEC_BY_NAME:
            safe_value = _sle_clamp_param(name, val)
            if _sle_apply_param(name, safe_value):
                applied += 1
    return applied


def _sle_random_genome_values():
    vals = {}
    for name, kind, lo, hi, step, cat, label in SLE_PARAM_SPECS:
        base = float(_SLE_BASELINE.get(name, lo))
        spread = (float(hi) - float(lo)) * 0.10
        v = base + random.uniform(-spread, spread)
        vals[name] = _sle_clamp_param(name, v)
    return vals


def _sle_mutate_values(values, strength=1.0, only=None):
    """طفرة: نعدّل مجموعة فرعية عشوائية من المعاملات (تجربة مضبوطة، مو فوضى)."""
    names = list(values.keys())
    if not names:
        return {}
    if only is None:
        k = max(1, int(round(len(names) * random.uniform(0.15, 0.40))))
        only = random.sample(names, min(k, len(names)))
    out = dict(values)
    for name in only:
        spec = SLE_SPEC_BY_NAME.get(name)
        if not spec:
            continue
        _, kind, lo, hi, step, cat, label = spec
        sigma = float(step) * float(strength) * random.uniform(0.6, 1.8)
        if kind == "float":
            sigma = max(sigma, (float(hi) - float(lo)) * 0.015 * float(strength))
        move = random.gauss(0, sigma)
        out[name] = _sle_clamp_param(name, float(out[name]) + move)
    return out


def _sle_sensitivity_map():
    """{اسم المعامل: قوة ارتباطه بالنتيجة} — الروبوت يستنتجه من سجل أجياله."""
    try:
        return {name: r for name, r, _ in _sle_param_sensitivity()}
    except Exception:
        return {}


def _sle_guided_mutation(base_values, strength=1.0):
    """
    طفرة موجّهة (تعلّم كيف يتعلّم): بعد ما يتراكم سجل أجيال كافٍ، الروبوت يعرف
    أي معامل ارتبط فعليًا بالنتيجة، فيركّز تجاربه عليه ويميل لاتجاه الارتباط —
    بدل ما يوزّع تجاربه على 29 مقبض بالتساوي فيتعلّم ببطء شديد.
    """
    names = list(base_values.keys())
    sens = _sle_sensitivity_map()
    if not sens or not names:
        return _sle_mutate_values(base_values, strength=strength)
    k = max(2, int(round(len(names) * random.uniform(0.10, 0.25))))
    pool, weights = list(names), [1.0 + 6.0 * abs(sens.get(n, 0.0)) for n in names]
    picks = []
    for _ in range(min(k, len(pool))):
        idx = random.choices(range(len(pool)), weights=weights, k=1)[0]
        picks.append(pool.pop(idx))
        weights.pop(idx)
    out = dict(base_values)
    for name in picks:
        spec = SLE_SPEC_BY_NAME.get(name)
        if not spec:
            continue
        _, kind, lo, hi, step, cat, label = spec
        r = float(sens.get(name, 0.0))
        sigma = float(step) * float(strength) * random.uniform(0.7, 1.8)
        if kind == "float":
            sigma = max(sigma, (float(hi) - float(lo)) * 0.02 * float(strength))
        move = random.gauss(0, sigma)
        if r and random.random() < 0.75:
            move = abs(move) * (1.0 if r > 0 else -1.0)
        out[name] = _sle_clamp_param(name, float(out[name]) + move)
    return out


def _sle_crossover(a, b):
    out = {}
    for name in a:
        if name in b:
            out[name] = a[name] if random.random() < 0.5 else b[name]
    return out


def _sle_param_delta_text(name, new_value, ref_value):
    spec = SLE_SPEC_BY_NAME.get(name)
    label = spec[6] if spec else name
    try:
        d = float(new_value) - float(ref_value)
    except Exception:
        d = 0.0
    if abs(d) < 1e-9:
        return None
    sign = "▲" if d > 0 else "▼"
    if spec and spec[1] == "int":
        return f"{label}: {int(ref_value)} → {int(new_value)} {sign}"
    return f"{label}: {float(ref_value):.3g} → {float(new_value):.3g} {sign}"


# ---------------------------------------------------------------------
# 2) الإدراك: تصنيف كل رسالة تخرج إلى (محرك، سهم، سعر) وتسجيلها للقياس
# ---------------------------------------------------------------------
SLE_ENGINE_SIGNATURES = (
    ("ESE",       ("ESE —", "ESE -")),
    ("REC",       ("FINAL RECOMMENDATION",)),
    ("RANK",      ("AUTO HUNTER",)),
    ("EARLY",     ("EARLY —", "EARLY -")),
    ("MEGA",      ("MEGA MOVER",)),
    ("GAP",       ("GAP HUNTER",)),
    ("ACC",       ("تجميع مكتشف", "تأكد الانفجار")),
    ("PUMPDUMP",  ("DUMP WARNING", "💊", "بمب")),
    ("SWING",     ("SWING PICK",)),
    ("ANOMALY",   ("شذوذ إحصائي",)),
    ("INSIDER",   ("شراء داخلي",)),
    ("BREAKOUT",  ("اختراق قمة",)),
    ("DOJI",      ("دوجي",)),
    ("NEWS",      ("Finnhub Catalyst", "Catalyst")),
    ("HALT",      ("عودة التداول", "إيقاف التداول", "Trade Halt")),
    ("PREMARKET", ("Pre-Market", "قبل الافتتاح")),
    ("GAINERS",   ("أكبر الرابحين", "TOP GAINERS")),
)

# رسائل إدارة/خروج لا تُكتم أبدًا حتى لو المحرك ضعيف (خطر أعلى من فائدة الكتم)
SLE_CRITICAL_BYPASS = (
    "كسر الوقف", "تحقق", "عودة التداول", "DUMP WARNING", "تنبيه سعري",
    "⚠️ تحذير", "تصفية", "خروج",
)

# ردود الأوامر والتقارير لا تُكتم أبدًا — بعضها يحتوي أسماء المحركات (مثل /early)
# فبدون هذا الاستثناء كان الكتم الذكي ممكن يبلع ردًا طلبته أنت بنفسك.
SLE_REPORT_MARKERS = (
    "آخر الإشارات وأداؤها", "المرشحين", "المرصودة", "إحصائيات", "الأوامر المتاحة",
    "عقل الروبوت", "الجينوم", "دروس الروبوت", "أهداف الروبوت", "تشخيص الروبوت",
    "ثقة الروبوت", "مراجعة الروبوت", "سجل الإشارات", "قائمة المراقبة", "الأداء",
)


def _sle_is_fresh_alert(text):
    """
    هل هذه رسالة تنبيه طازجة (تستحق التقييم/الكتم) أم رد أمر أو تقرير؟
    القاعدة صارمة عن قصد: أي شك ⇒ نعتبرها ردًا ولا نلمسها.
    """
    if not text or len(text) > 900:
        return False
    if "━━━━━━━━━━━━━━━━" not in text:
        return False
    if text.count("💰") != 1:
        return False
    if text.count("• ") > 4:
        return False
    for marker in SLE_REPORT_MARKERS:
        if marker in text:
            return False
    return True

SLE_SKIP_MARKERS = (
    "الأوامر المتاحة", "📊 *Status*", "سجل الإشارات", "📭", "إحصائيات",
    "مراجعة أداء", "لا توجد", "تم ", "✅ PENNY HUNTER",
)

_SLE_TICKER_STOPWORDS = {
    "AI", "ETF", "CEO", "USD", "USA", "IPO", "RSI", "EMA", "VWAP", "ATR", "SL", "TP", "OK",
    "THE", "AND", "FOR", "YOU", "NOT", "ALL", "NEW", "ESE", "REC", "RANK", "EARLY", "MEGA",
    "GAP", "ACC", "SLE", "PNL", "ET", "KSA", "RVOL", "RR", "SEC", "HALT", "TOP", "BUY",
    "SELL", "WATCH", "AVOID", "DUMP", "PUMP", "SWING", "NEWS",
}


def _sle_classify_engine(message):
    """يستنتج المحرك من بصمة الرسالة نفسها — بدون أي تعديل على الماسحات."""
    try:
        text = str(message or "")
        for engine, signatures in SLE_ENGINE_SIGNATURES:
            for sig in signatures:
                if sig in text:
                    return engine
    except Exception:
        pass
    return None


def _sle_extract_symbol(message):
    text = str(message or "")
    patterns = (
        r"\*([A-Z]{1,5})\*",                      # **AAPL** أو *AAPL*
        r":\s*\*?([A-Z]{1,5})\*?(?:\s|$|\n)",     # HEADER: AAPL
        r"\*[^*\n]{0,40}?:\s*([A-Z]{1,5})\*",     # *MEGA MOVER: AAPL*
        r"\b([A-Z]{2,5})\b",                      # أي رمز كبير بالحروف
    )
    for pat in patterns:
        try:
            for match in re.findall(pat, text):
                if match and match not in _SLE_TICKER_STOPWORDS:
                    return match
        except Exception:
            continue
    return None


def _sle_extract_price(message):
    text = str(message or "")
    near_price = re.search(r"(?:السعر|سعر|Price)[^\n$]{0,25}\$?\s*([0-9]+(?:\.[0-9]+)?)", text)
    if near_price:
        try:
            v = float(near_price.group(1))
            if v > 0:
                return v
        except Exception:
            pass
    for raw in re.findall(r"\$\s*([0-9]+(?:\.[0-9]+)?)", text):
        try:
            v = float(raw)
            if v > 0:
                return v
        except Exception:
            continue
    return 0.0


def _sle_price_from_cache(symbol):
    """سعر احتياطي محلي (بدون أي طلب شبكة) من price_cache لو الرسالة ما فيها سعر."""
    try:
        with state_lock:
            rows = list((state.get("price_cache") or {}).get(str(symbol).upper()) or [])
        for row in reversed(rows):
            if isinstance(row, (list, tuple)) and len(row) >= 5:
                v = _safe_float(row[4])
                if v > 0:
                    return v
    except Exception:
        pass
    return 0.0


def _sle_gen_id():
    with _SLE_LOCK:
        sl = state.get("self_learning") or {}
        # مهم: الإشارة تُنسب للجينوم الذي كان *حيًّا فعليًا* لحظة صدورها
        # (مو للأفضل تاريخيًا) — هذا أساس العدالة في تقييم الطفرات.
        return str(sl.get("active_genome_id") or sl.get("champion_id") or "g0")


def sle_journal_signal(symbol, engine, price, extra=None):
    """
    نقطة التسجيل الموحّدة: تمنع التكرار، تُثري البيانات، ثم تستدعي سجل الإشارات
    الأصلي (فيتولّى قياس النتيجة بعد 15/30/60/180 دقيقة كما كان).
    """
    if not SLE_ENABLED or not symbol or not engine:
        return False
    try:
        symbol = str(symbol).upper().strip()
        price = _safe_float(price)
        if price <= 0:
            price = _sle_price_from_cache(symbol)
        if price <= 0:
            return False
        key = f"{symbol}|{engine}"
        now_ts = time.time()
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            seen = sl.setdefault("recent_obs", {})
            if now_ts - _safe_float(seen.get(key)) < SLE_DEDUPE_SEC:
                return False
            seen[key] = now_ts
            if len(seen) > 800:
                cutoff = now_ts - 86400
                for k in [k for k, v in seen.items() if _safe_float(v) < cutoff]:
                    seen.pop(k, None)
            sl["signals_seen_total"] = int(sl.get("signals_seen_total", 0)) + 1
        payload = {"gen": _sle_gen_id()}
        for k, v in (extra or {}).items():
            payload[k] = v
        _journal_record(symbol, engine, price, payload)
        return True
    except Exception as error:
        logger.debug(f"[SLE] journal_signal error: {error}")
        return False


def _sle_engine_trust(engine):
    """ثقة [0.2..1.0] بالمحرك من نتائجه الحقيقية (Beta posterior). المجهول = 1.0 (بريء حتى تثبت إدانته)."""
    if not SLE_ENABLED or not engine:
        return 1.0
    try:
        with _SLE_LOCK:
            trust_map = (state.get("self_learning") or {}).get("engine_trust") or {}
        rec = trust_map.get(engine)
        if not rec:
            return 1.0
        if int(rec.get("n", 0)) < SLE_MIN_ENGINE_SAMPLES:
            return 1.0
        return max(0.2, min(1.0, _safe_float(rec.get("trust"), 1.0)))
    except Exception:
        return 1.0


def _sle_should_send(engine, message):
    """
    بوابة الكتم الذكية: محرك ثقته منخفضة تُكتم نسبة من تنبيهاته (مو كلها —
    نحتفظ بهامش استكشاف عشان نقدر نكتشف لو تحسّن). رسائل الخروج/الإدارة تمر دائمًا.
    """
    if not SLE_ENABLED or SLE_SHADOW_ONLY or not SLE_THROTTLE_ENABLED or not engine:
        return True, 0.0
    try:
        text = str(message or "")
        for marker in SLE_CRITICAL_BYPASS:
            if marker in text:
                return True, 0.0
        if not _sle_is_fresh_alert(text):
            return True, 1.0          # رد أمر/تقرير — لا يُكتم أبدًا
        trust = _sle_engine_trust(engine)
        if trust >= 0.999:
            return True, trust
        drop_prob = (1.0 - trust) * SLE_THROTTLE_STRENGTH
        drop_prob = min(drop_prob, 1.0 - SLE_EXPLORATION_FLOOR)
        if drop_prob <= 0:
            return True, trust
        if random.random() < drop_prob:
            with _SLE_LOCK:
                sl = state.setdefault("self_learning", {})
                stats = sl.setdefault("throttle_stats", {})
                s = stats.setdefault(engine, {"suppressed": 0, "allowed": 0})
                s["suppressed"] = int(s.get("suppressed", 0)) + 1
                sl["suppressed_total"] = int(sl.get("suppressed_total", 0)) + 1
            return False, trust
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            stats = sl.setdefault("throttle_stats", {})
            s = stats.setdefault(engine, {"suppressed": 0, "allowed": 0})
            s["allowed"] = int(s.get("allowed", 0)) + 1
        return True, trust
    except Exception:
        return True, 1.0


def _sle_observe_outgoing(message, photo=None):
    """تُستدعى من غلاف send_telegram: تحوّل الرسالة الصادرة إلى عيّنة تعلّم."""
    try:
        if not SLE_ENABLED or not SLE_TELEGRAM_HOOK or photo is not None:
            return
        text = str(message or "")
        if not text or len(text) < 12:
            return
        for marker in SLE_SKIP_MARKERS:
            if marker in text:
                return
        engine = _sle_classify_engine(text)
        if not engine:
            return
        symbol = _sle_extract_symbol(text)
        if not symbol:
            return
        price = _sle_extract_price(text)
        sle_journal_signal(symbol, engine, price)
    except Exception as error:
        logger.debug(f"[SLE] observe error: {error}")


def _sle_install_hooks():
    """
    تركيب المراقبة على نقطتين مشتركتين فقط — بدون لمس أي ماسح:
      • _alert_gate_mark: تعرف المحرك والسعر بدقة (SETUP/REC/RANK/EARLY).
      • send_telegram  : تلتقط كل بقية التنبيهات من نص الرسالة + تنفّذ الكتم الذكي.
    """
    global _SLE_HOOKS_INSTALLED
    if _SLE_HOOKS_INSTALLED:
        return
    _SLE_HOOKS_INSTALLED = True

    original_gate_mark = globals().get("_alert_gate_mark")
    if callable(original_gate_mark):
        def _sle_gate_mark(symbol, price=0.0, source=""):
            try:
                if SLE_ENABLED and source:
                    sle_journal_signal(symbol, str(source).upper(), price)
            except Exception:
                pass
            return original_gate_mark(symbol, price, source)
        globals()["_alert_gate_mark"] = _sle_gate_mark

    original_send = globals().get("send_telegram")
    if callable(original_send):
        def _sle_send_telegram(message, photo=None):
            try:
                engine = _sle_classify_engine(message) if (SLE_ENABLED and photo is None) else None
                if (engine and ACTIVE_ENGINES_ONLY and _sle_is_fresh_alert(message)
                        and engine not in ACTIVE_ENGINES_ONLY):
                    logger.info(f"[SLE] حصرية المحركات: كتم {engine} — المسموح: {','.join(sorted(ACTIVE_ENGINES_ONLY))}")
                    return None
                if engine:
                    allowed, trust = _sle_should_send(engine, message)
                    if not allowed:
                        logger.info(f"[SLE] كتم تنبيه {engine} (الثقة {trust:.2f}) — الاستكشاف مستمر")
                        return None
            except Exception:
                pass
            result = original_send(message, photo)
            try:
                _sle_observe_outgoing(message, photo)
            except Exception:
                pass
            return result
        globals()["send_telegram"] = _sle_send_telegram
    logger.info("🧠 [SLE] تم تركيب المراقبة الذاتية على مسار التنبيهات")


# ---------------------------------------------------------------------
# 3) التعلّم: من سجل الإشارات الحقيقي → إحصاء لكل محرك + لياقة لكل جيل
# ---------------------------------------------------------------------
_SLE_HOOKS_INSTALLED = False


def _sle_entry_outcome(entry):
    """نتيجة الإشارة عند المرحلة المعتمدة (60د) مع بدائل لو ما اكتملت."""
    outs = entry.get("out") or {}
    for hz in (SLE_HORIZON,) + SLE_HORIZON_FALLBACK:
        o = outs.get(hz)
        if o and not o.get("missed"):
            try:
                return float(o.get("pct", 0.0)), float(o.get("hi", 0.0)), float(o.get("lo", 0.0)), hz
            except Exception:
                continue
    return None


def _sle_scan_journal():
    """يقرأ سجل الإشارات مرة واحدة ويبني: نتائج لكل محرك + نتائج لكل جيل."""
    with state_lock:
        journal = [dict(e) for e in (state.get("signal_journal") or [])]
    by_engine, by_gen = {}, {}
    for e in journal:
        res = _sle_entry_outcome(e)
        if not res:
            continue
        pct, hi, lo, hz = res
        engine = str(e.get("engine") or "?")
        gen = str(e.get("gen") or "g0")
        by_engine.setdefault(engine, []).append((pct, hi, lo))
        by_gen.setdefault(gen, []).append((pct, hi, lo))
    return by_engine, by_gen, len(journal)


def _sle_stats(samples):
    n = len(samples)
    if n == 0:
        return None
    pcts = [s[0] for s in samples]
    avg = sum(pcts) / n
    wins = sum(1 for p in pcts if p > 0)
    var = sum((p - avg) ** 2 for p in pcts) / n
    std = var ** 0.5
    avg_hi = sum(s[1] for s in samples) / n
    avg_lo = sum(s[2] for s in samples) / n
    downside = sum(p for p in pcts if p < 0) / n
    target_hits = sum(1 for s in samples if _safe_float(s[1]) >= SLE_EVAL_TARGET_PCT)
    stop_touches = sum(1 for s in samples if _safe_float(s[2]) <= -SLE_EVAL_STOP_PCT)
    target_rate = target_hits / n
    stop_rate = stop_touches / n
    # لا نعتبر ارتفاعًا عابرًا +0.5% مساويًا لصفقة حققت خطة +25%.
    # هذا المقياس يوازن الإيجابية العامة مع الوصول للهدف وخصم لمس الوقف.
    quality = (0.45 * (wins / n)) + (0.55 * target_rate) - (0.35 * stop_rate)
    return {
        "n": n, "avg": avg, "std": std, "win_rate": wins / n,
        "avg_hi": avg_hi, "avg_lo": avg_lo, "downside": downside,
        "target_hits": target_hits, "target_rate": target_rate, "stop_rate": stop_rate,
        "quality": quality,
        "expectancy": avg - 0.5 * std - (stop_rate * SLE_EVAL_STOP_PCT),
    }


def _sle_refresh_engine_trust(by_engine):
    """يبني ثقة كل محرك (Beta posterior على نسبة الإيجابي) ويحفظها في الحالة."""
    trust_map, notable = {}, []
    with _SLE_LOCK:
        sl = state.setdefault("self_learning", {})
        previous = dict(sl.get("engine_trust") or {})
    for engine, samples in by_engine.items():
        st = _sle_stats(samples)
        if not st:
            continue
        n = st["n"]
        a0, b0 = 2.0, 2.0                      # prior متحفظ (يرجّح الأداء المحايد)
        # الثقة لا تعتمد على كون السعر موجبًا فقط؛ الوصول للهدف له وزن أكبر.
        effective_win = 0.45 * st["win_rate"] + 0.55 * st["target_rate"]
        p_win = (effective_win * n + a0) / (n + a0 + b0)
        trust = 0.35 + 0.65 * ((p_win - 0.35) / 0.35)
        trust = max(0.2, min(1.0, trust))
        if n < SLE_MIN_ENGINE_SAMPLES:
            trust = 1.0                        # عيّنات قليلة = لا نحكم
        trust_map[engine] = {
            "n": n, "win_rate": round(st["win_rate"], 3), "avg": round(st["avg"], 3),
            "expectancy": round(st["expectancy"], 3), "downside": round(st["downside"], 3),
            "target_rate": round(st["target_rate"], 3), "stop_rate": round(st["stop_rate"], 3),
            "trust": round(trust, 3), "p_win": round(p_win, 3),
        }
        old = previous.get(engine) or {}
        old_trust = _safe_float(old.get("trust"), 1.0)
        if old_trust >= 0.999 and trust < 0.85 and n >= SLE_MIN_ENGINE_SAMPLES:
            notable.append(("throttle", engine, trust, st))
        elif old_trust < 0.85 and trust >= 0.95 and n >= SLE_MIN_ENGINE_SAMPLES:
            notable.append(("revive", engine, trust, st))
    with _SLE_LOCK:
        sl = state.setdefault("self_learning", {})
        sl["engine_trust"] = trust_map
        sl["engine_trust_ts"] = time.time()
    for kind, engine, trust, st in notable:
        if kind == "throttle":
            _sle_add_lesson("throttle",
                            f"خفّضت وزن {engine}: {st['n']} إشارة، إيجابي {st['win_rate']*100:.0f}%، "
                            f"متوسط {st['avg']:+.2f}% — صار ثقته {trust:.2f} (تنبيهاته تُكتم جزئيًا لحد ما يتحسن).")
        else:
            _sle_add_lesson("revive",
                            f"رجّعت وزن {engine}: تحسّن فعلي — {st['n']} إشارة، إيجابي {st['win_rate']*100:.0f}%، "
                            f"متوسط {st['avg']:+.2f}%.")
    return trust_map


def _sle_genome_fitness(by_gen, by_engine):
    """
    لياقة الجينوم = متوسط نتائجه الحقيقية − عقوبة تقلّب، مع تقليص بايزي
    نحو متوسط المحركات لو العيّنة صغيرة (عشان ما نختار جيل بالحظ).
    """
    all_samples = [s for lst in by_engine.values() for s in lst]
    global_stat = _sle_stats(all_samples)
    prior = global_stat["expectancy"] if global_stat else 0.0
    out = {}
    for gen, samples in by_gen.items():
        st = _sle_stats(samples)
        if not st:
            continue
        n = st["n"]
        # تقليص بايزي أخف (n/(n+1.5)): التقليص القوي كان يظلم الطفرات الواعدة
        # لأن متوسط السوق أقل من لياقة البطل، فيُسحب تقديرها للأسفل فلا تُرقّى أبدًا.
        w = n / (n + 1.5)
        # لياقة الجيل تفضّل جودة خطة التداول (هدف/وقف) لا مجرد حركة موجبة صغيرة.
        fitness = w * (st["expectancy"] + 10.0 * st["quality"]) + (1 - w) * prior
        out[gen] = {"n": n, "fitness": round(fitness, 4), "avg": round(st["avg"], 3),
                    "win_rate": round(st["win_rate"], 3), "expectancy": round(st["expectancy"], 3)}
    return out


def _sle_param_sensitivity():
    """
    حساسية المعاملات: أي إعداد فعليًا يفرق مع النتيجة؟ (ارتباط بيرسون بين قيمة
    المعامل عبر الأجيال ولياقة كل جيل). هذا "ما تعلّمه الروبوت عن نفسه".
    """
    with _SLE_LOCK:
        sl = state.get("self_learning") or {}
        history = list(sl.get("gen_history") or [])
    rows = [h for h in history if h.get("fitness") is not None and h.get("values")]
    if len(rows) < 3:
        return []
    fitness = [float(h["fitness"]) for h in rows]
    mf = sum(fitness) / len(fitness)
    vf = sum((f - mf) ** 2 for f in fitness)
    if vf <= 1e-12:
        return []
    results = []
    for name in SLE_SPEC_BY_NAME:
        xs, ys = [], []
        for h in rows:
            if name in h["values"]:
                xs.append(float(h["values"][name]))
                ys.append(float(h["fitness"]))
        if len(xs) < 3:
            continue
        mx = sum(xs) / len(xs)
        vx = sum((x - mx) ** 2 for x in xs)
        if vx <= 1e-12:
            continue
        my = sum(ys) / len(ys)
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        r = cov / ((vx ** 0.5) * ((sum((y - my) ** 2 for y in ys)) ** 0.5) + 1e-12)
        if abs(r) >= 0.35:
            results.append((name, r, len(xs)))
    results.sort(key=lambda t: -abs(t[1]))
    return results


def _sle_new_genome(gen_values, parent_id=None, note=""):
    # معرّف فريد فعليًا: العدّاد المتسلسل يمنع تصادم المعرّفات داخل نفس الثانية
    # (التصادم كان يدمج عدة أجيال في جينوم واحد فتفشل الترقية تمامًا).
    with _SLE_LOCK:
        sl = state.setdefault("self_learning", {})
        gen_no = int(sl.get("generation", 0))
        sl["genome_seq"] = int(sl.get("genome_seq", 0)) + 1
        seq = int(sl["genome_seq"])
    return {
        "id": f"g{gen_no + 1}.{seq}",
        "values": dict(gen_values),
        "parent": parent_id,
        "note": note,
        "created_ts": time.time(),
        "signals": 0,
        "fitness": None,
    }


def _sle_ensure_population():
    with _SLE_LOCK:
        sl = state.setdefault("self_learning", {})
        pop = sl.get("population") or []
        if not pop:
            champion_values = dict(_SLE_BASELINE)
            champion = {
                "id": "g0_base", "values": champion_values, "parent": None,
                "note": "الجيل الأساسي — إعدادات البيئة كما هي",
                "created_ts": time.time(), "signals": 0, "fitness": None,
            }
            pop = [champion]
            for _ in range(max(0, SLE_POPULATION_SIZE - 1)):
                values = _sle_mutate_values(champion_values, strength=1.0)
                pop.append({
                    "id": f"g0_x{len(pop)}", "values": values, "parent": "g0_base",
                    "note": "استكشاف أولي حول الإعدادات الأصلية",
                    "created_ts": time.time(), "signals": 0, "fitness": None,
                })
            sl["population"] = pop
            sl["champion_id"] = "g0_base"
            sl["active_genome_id"] = "g0_base"
            sl["active_since_ts"] = time.time()
            sl.setdefault("epochs", [])
            sl.setdefault("rotation_stats", {})
            sl.setdefault("generation", 0)
            sl.setdefault("gen_history", [])
            sl.setdefault("evolution_log", [])
            sl.setdefault("lessons", [])
            sl.setdefault("goals", {})
            sl.setdefault("throttle_stats", {})
            sl.setdefault("recent_obs", {})
            sl.setdefault("signals_seen_total", 0)
            sl.setdefault("last_evolution_ts", time.time())
            sl.setdefault("matured_at_last_evolution", 0)
            sl.setdefault("health", {})
            sl.setdefault("thread_registry", {})
        return sl


def _sle_add_lesson(kind, text, notify=False):
    try:
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            lessons = sl.setdefault("lessons", [])
            lessons.append({"ts": time.time(), "kind": kind, "text": text})
            if len(lessons) > SLE_LESSONS_MAX:
                del lessons[: len(lessons) - SLE_LESSONS_MAX]
        logger.info(f"[SLE:{kind}] {text}")
        if notify:
            try:
                send_telegram(f"🧠 *تعلّم جديد ({kind})*\n{text}")
            except Exception:
                pass
        save_state()
    except Exception as error:
        logger.debug(f"[SLE] add_lesson error: {error}")


def _sle_champion():
    with _SLE_LOCK:
        sl = state.get("self_learning") or {}
        cid = sl.get("champion_id")
        for g in sl.get("population") or []:
            if g.get("id") == cid:
                return dict(g)
        pop = sl.get("population") or []
        return dict(pop[0]) if pop else None


def sle_evolve(force=False, reason="مجدول"):
    """
    خطوة تطوّر واحدة: قياس الأجيال → انتخاب → تهجين → طفرة → ترقية الأفضل.
    تُسجَّل كل خطوة بشرح عربي واضح (وش تغيّر وليش).
    """
    if not SLE_ENABLED:
        return {"ok": False, "reason": "التعلّم الذاتي مطفّى"}
    try:
        wf = _ml4t_walk_forward_review() if ML4T_ENABLED else {}
        if wf.get("circuit_breaker") and not force:
            _sle_add_lesson("model_guard", "Circuit Breaker: أوقفت ترقية الجينوم مؤقتًا بسبب تدهور اختبار Walk-forward خارج العينة.")
            return {"ok": False, "reason": "Circuit Breaker: تدهور خارج العينة", "walk_forward": wf}
        sl = _sle_ensure_population()
        by_engine, by_gen, journal_len = _sle_scan_journal()
        _sle_refresh_engine_trust(by_engine)          # ثقة المحركات من النتائج الحيّة فقط (الأوفلاين محاكاة تقريبية)
        offline_total = _offline_merge_into_gen(by_gen, list(sl.get("population") or []))   # 🌙 رصيد التدريب والسوق مقفل
        fitness_map = _sle_genome_fitness(by_gen, by_engine)
        matured = sum(len(v) for v in by_engine.values())

        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            pop = list(sl.get("population") or [])
            last_matured = int(sl.get("matured_at_last_evolution", 0))
            last_ts = _safe_float(sl.get("last_evolution_ts"), 0.0)
            offline_seen = int(sl.get("offline_events_at_last_evolution", 0))

        # الأحداث الأوفلاين الجديدة تحسب كعيّنات محسومة (كل OFFLINE_CREDIT_PER_EVENTS حدث = عيّنة) لما السوق مقفل
        new_samples = (matured - last_matured) + max(0, offline_total - offline_seen) // max(1, OFFLINE_CREDIT_PER_EVENTS)
        if not force:
            if new_samples < SLE_MIN_NEW_SAMPLES:
                return {"ok": False, "reason": f"عيّنات جديدة غير كافية ({new_samples}/{SLE_MIN_NEW_SAMPLES})"}
            min_gap = OFFLINE_EVOLVE_MIN_GAP_SEC if _offline.get("active") else SLE_EVOLVE_INTERVAL_SEC * 0.5
            if time.time() - last_ts < min_gap:
                return {"ok": False, "reason": "تطوّر قريب جدًا — ننتظر تراكم نتائج"}

        # 1) تحديث لياقة كل جينوم من نتائجه الحقيقية
        for g in pop:
            info = fitness_map.get(g.get("id"))
            if info:
                g["fitness"] = info["fitness"]
                g["stats"] = info
                g["signals"] = info["n"]
        scored = [g for g in pop if g.get("fitness") is not None and int(g.get("signals", 0)) >= SLE_MIN_GEN_SAMPLES]
        scored.sort(key=lambda g: -float(g.get("fitness", -99)))
        prev_champion = _sle_champion() or (scored[0] if scored else pop[0])
        champion = prev_champion
        champion_fitness = champion.get("fitness")

        # 🛡️ حارس الأوفلاين: بطل تحققه أسوأ من الإعدادات الأصلية يُرجَع للأساس (بدل ما يبقى مطبّق حيّاً لأنه "أقل سوءاً" من أقرانه)
        reverted_to_base = False
        if (OFFLINE_ENABLED and champion.get("id") != "g0_base" and offline_total >= OFFLINE_GUARD_MIN_EVENTS
                and not _offline_genome_ok(champion.get("id"))):
            old_id = champion.get("id")
            info = _offline_eval_cache.get("val") or {}
            base_g = next((dict(g) for g in pop if g.get("id") == "g0_base"), None) or _sle_new_genome(
                dict(_SLE_BASELINE), parent_id=old_id, note="رجوع للأساس: البطل السابق أسوأ بالتحقق")
            base_g["values"] = dict(_SLE_BASELINE)
            base_g["fitness"] = None
            _sle_add_lesson("model_guard",
                            f"حارس الأوفلاين: أرجعت الإعدادات للأساس لأن البطل *{old_id}* أسوأ منه على بيانات التحقق "
                            f"(صافي {(info.get(old_id) or {}).get('hold_net', 0):+.2f}% مقابل "
                            f"{(info.get('__base__') or {}).get('hold_net', 0):+.2f}% للأساس للصفقة).")
            scored = [g for g in scored if g.get("id") != old_id]
            champion = prev_champion = base_g
            champion_fitness = None
            reverted_to_base = True

        # 2) لو جينوم آخر أثبت أفضلية واضحة → ترقية (هذا "التطوّر الطبيعي")
        #    نقارن العائد المتوقع الخام مع هامش صغير، وليس اللياقة المقليصة فقط،
        #    عشان الطفرات الواعدة ما تُظلم بسبب أن متوسط السوق أقل من البطل.
        promoted = None
        if scored:
            best = scored[0]
            if best.get("id") != champion.get("id"):
                best_exp = _safe_float((best.get("stats") or {}).get("expectancy"), -99)
                champ_exp = _safe_float((champion.get("stats") or {}).get("expectancy"), -99)
                beats_by_fitness = (champion_fitness is None
                                    or float(best["fitness"]) > float(champion_fitness) + 0.02)
                # هامش الترقية يكبر لما تكون العيّنات أقل (ضجيج أعلى): نطلب تفوقًا
                # يتجاوز الخطأ المعياري بدل تفوق 0.25% اللي ممكن يكون حظ.
                n_best = int(best.get("signals", 0) or 0)
                n_champ = int(champion.get("signals", 0) or 0)
                margin = 0.25 + 1.2 / max(4.0, float(min(n_best or 4, n_champ or 4))) ** 0.5
                beats_by_raw = (champion.get("stats") is not None
                                and best_exp > champ_exp + margin)
                if (beats_by_fitness or beats_by_raw) and not _offline_promotion_ok(best.get("id"), champion.get("id")):
                    logger.info(f"[Offline] guard: promotion of {best.get('id')} blocked (validation not better than baseline/champion)")
                    beats_by_fitness = beats_by_raw = False
                if beats_by_fitness or beats_by_raw:
                    old_fitness = champion_fitness
                    old_id = champion.get("id")
                    promoted = best
                    champion = best
                    champion_fitness = best["fitness"]
                    old_txt = f"{float(old_fitness):+.2f}" if old_fitness is not None else "غير محسوبة"
                    _sle_add_lesson("evolve",
                                    f"ترقية جيل: *{best['id']}* تفوّق على الجيل السابق *{old_id}* "
                                    f"(عائد متوقع {best_exp:+.2f}% مقابل {champ_exp:+.2f}%، "
                                    f"لياقة {float(best['fitness']):+.2f} مقابل {old_txt}) "
                                    f"على {best['signals']} إشارة حقيقية.")

        # 3) بناء الجيل التالي: نخبة + تهجين أفضل اثنين + طفرات + مستكشف جديد
        base_values = dict(champion.get("values") or _SLE_BASELINE)
        next_pop = [{
            "id": champion.get("id") or "g_champ",
            "values": base_values,
            "parent": champion.get("parent"),
            "note": "الجيل الحالي (نخبة محفوظة)",
            "created_ts": champion.get("created_ts", time.time()),
            "signals": champion.get("signals", 0),
            "fitness": champion_fitness,
        }]
        # نخبة موسّعة: نحتفظ دائمًا بالبطل السابق + أفضل جينوم مُختبَر آخر.
        # (البطل السابق يُحفظ حتى لو خسر الترقية — عشان ضجيج القياس ما يضيّع
        #  أفضل ما وصلنا له، وهذي أهم ضمانة ضد التراجع.)
        elites = ([prev_champion] if prev_champion.get("id") != next_pop[0]["id"] else []) + list(scored[:3])
        for g in elites:
            if len(next_pop) >= 2:
                break
            if g.get("id") != next_pop[0]["id"] and all(g.get("id") != x["id"] for x in next_pop):
                next_pop.append({
                    "id": g["id"], "values": dict(g.get("values") or base_values),
                    "parent": g.get("parent"), "note": "نخبة محفوظة بنتائجها الحقيقية",
                    "created_ts": g.get("created_ts", time.time()),
                    "signals": g.get("signals", 0), "fitness": g.get("fitness"),
                })
        elite_pool = scored[:3] if scored else [champion]
        # تبريد تدريجي: الأجيال الأولى تستكشف بخطوات كبيرة، وكل ما تعلّمنا نقلّل
        # حجم الطفرة → تحسين دقيق حول أفضل ما وصلنا له بدل قفزات عشوائية دائمة.
        gen_strength = max(0.45, 1.0 - int(sl.get("generation", 0)) * 0.03)
        while len(next_pop) < max(2, SLE_POPULATION_SIZE):
            roll = random.random()
            if roll < 0.30 and len(elite_pool) >= 2:
                a = random.choice(elite_pool)
                b = random.choice([x for x in elite_pool if x is not a] or elite_pool)
                child_values = _sle_crossover(a.get("values") or base_values, b.get("values") or base_values)
                note = f"تهجين بين {a['id']} و {b['id']}"
            elif roll < 0.60:
                # طفرة موجّهة: تركّز على المعاملات اللي أثبتت ارتباطًا بالنتيجة
                # (أول ما يتراكم سجل أجيال) — تتعلم أسرع بكثير من الطفرات العشوائية
                child_values = _sle_guided_mutation(base_values, strength=1.25 * gen_strength)
                note = "طفرة موجّهة (على المعاملات الأكثر تأثيرًا)"
            elif roll < 0.90:
                child_values = _sle_mutate_values(base_values, strength=gen_strength)
                note = "طفرة على الجيل الحالي (تجربة معاملات محدودة)"
            else:
                child_values = _sle_random_genome_values()
                note = "مستكشف جديد بعيد — يمنع الوقوع في حل محلي"
            if _offline.get("active"):
                child_values = _offline_freeze_unmeasured(child_values, base_values)
            child = _sle_new_genome(child_values, parent_id=champion.get("id"), note=note)
            next_pop.append(child)

        # 4) تسجيل الجيل القديم في السجل التاريخي (لتحليل الحساسية لاحقًا)
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            history = sl.setdefault("gen_history", [])
            # نسجّل كل جينوم *مُختبَر* (مو البطل فقط) — تحليل الحساسية يحتاج
            # تباينًا في القيم والنتائج، وإلا صارت كل الصفوف متطابقة بلا فائدة.
            known = {h.get("id"): i for i, h in enumerate(history)}
            for g in (scored or [champion]):
                row = {
                    "id": g.get("id"), "ts": time.time(),
                    "fitness": g.get("fitness"), "signals": g.get("signals", 0),
                    "values": dict(g.get("values") or {}), "note": g.get("note", ""),
                    "stats": g.get("stats") or {},
                }
                if g.get("id") in known:
                    history[known[g["id"]]] = row
                else:
                    history.append(row)
            if len(history) > 400:
                del history[: len(history) - 400]
            sl["population"] = next_pop
            sl["champion_id"] = next_pop[0]["id"]
            sl["generation"] = int(sl.get("generation", 0)) + 1
            sl["last_evolution_ts"] = time.time()
            sl["matured_at_last_evolution"] = matured
            sl["offline_events_at_last_evolution"] = offline_total

        _sle_apply_genome(base_values)
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            sl["active_genome_id"] = next_pop[0]["id"]
            sl["active_since_ts"] = time.time()

        # 5) شرح عربي: وش تغيّر مقارنة بالأصل
        changes = []
        for name, val in base_values.items():
            txt = _sle_param_delta_text(name, val, _SLE_BASELINE.get(name, val))
            if txt:
                changes.append(txt)
        gen_no = int((state.get("self_learning") or {}).get("generation", 0))
        summary = (f"الجيل *{gen_no}* — {reason} | عيّنات محسومة: {matured} | "
                   f"لياقة الجيل: " + (f"{float(champion_fitness):+.2f}" if champion_fitness is not None else "غير كافية بعد"))
        if changes:
            summary += "\n" + "\n".join("• " + c for c in changes[:8])
        else:
            summary += "\n• لا تغيير عن الإعدادات الأصلية (الجينوم الحالي هو الأفضل حتى الآن)"
        _sle_add_lesson("evolve", summary)
        try:
            with _SLE_LOCK:
                sl = state.setdefault("self_learning", {})
                log = sl.setdefault("evolution_log", [])
                log.append({"ts": time.time(), "generation": gen_no, "reason": reason,
                            "matured": matured, "champion": next_pop[0]["id"],
                            "fitness": champion_fitness, "changes": changes[:12],
                            "promoted": bool(promoted)})
                if len(log) > 200:
                    del log[: len(log) - 200]
        except Exception:
            pass
        logger.info(f"[SLE] evolve ok: gen={gen_no} matured={matured} changes={len(changes)} promoted={bool(promoted)}")
        return {"ok": True, "generation": gen_no, "matured": matured, "changes": changes, "summary": summary}
    except Exception as error:
        logger.error(f"[SLE] evolve error: {error}")
        return {"ok": False, "reason": f"خطأ: {error}"}


def _ml4t_walk_forward_review():
    """تقييم Walk-forward خفيف من سجل الإشارات: تدريب زمني ثم اختبار لاحق، بلا تسريب مستقبل."""
    if not ML4T_ENABLED:
        return {"enabled": False}
    try:
        dqn_status = _dqn_train_from_journal() if DQN_ENABLED else {"enabled": False}
        ppo_status = _ppo_train_from_journal() if PPO_ENABLED else {"enabled": False}
        rolling_status = _rolling_ml_train_from_journal() if ROLLING_ML_ENABLED else {"enabled": False}
        finrl_status = _finrl_review_from_journal() if FINRL_ENABLED else {"enabled": False}
        with state_lock:
            journal = sorted((dict(e) for e in (state.get("signal_journal") or [])),
                             key=lambda e: _safe_float(e.get("ts")))
        rows = []
        for e in journal:
            o = (e.get("out") or {}).get(SLE_HORIZON)
            if not o or o.get("missed"):
                continue
            rows.append({"ts": _safe_float(e.get("ts")),
                         "net": _safe_float(o.get("net_pct"), _safe_float(o.get("pct"))),
                         "hi": _safe_float(o.get("hi")), "lo": _safe_float(o.get("lo")),
                         "engine": str(e.get("engine") or "?")})
        if len(rows) < ML4T_MIN_TRAIN_SAMPLES + ML4T_MIN_TEST_SAMPLES:
            result = {"enabled": True, "status": "insufficient_data", "n": len(rows),
                      "train": 0, "test": 0, "drift": None, "circuit_breaker": False}
        else:
            cut = max(ML4T_MIN_TRAIN_SAMPLES, int(len(rows) * 0.70))
            train, test = rows[:cut], rows[cut:]
            train_avg = sum(x["net"] for x in train) / len(train)
            test_avg = sum(x["net"] for x in test) / len(test)
            train_target = sum(x["hi"] >= SLE_EVAL_TARGET_PCT for x in train) / len(train)
            test_target = sum(x["hi"] >= SLE_EVAL_TARGET_PCT for x in test) / len(test)
            test_stop = sum(x["lo"] <= -SLE_EVAL_STOP_PCT for x in test) / len(test)
            drift = train_avg - test_avg
            breaker = (test_avg <= ML4T_CIRCUIT_BREAKER_LOSS_PCT
                       or drift >= ML4T_DRIFT_THRESHOLD_PCT
                       or test_stop >= 0.70)
            result = {"enabled": True, "status": "ok", "n": len(rows), "train": len(train), "test": len(test),
                      "train_avg_net": round(train_avg, 3), "test_avg_net": round(test_avg, 3),
                      "train_target_rate": round(train_target, 3), "test_target_rate": round(test_target, 3),
                      "test_stop_rate": round(test_stop, 3), "drift": round(drift, 3),
                      "circuit_breaker": bool(breaker), "updated_ts": time.time()}
        result["dqn"] = dqn_status
        result["ppo"] = ppo_status
        result["rolling_ml"] = rolling_status
        result["finrl"] = finrl_status
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            sl["ml4t_walk_forward"] = result
            sl["model_guard"] = {"promotion_blocked": bool(result.get("circuit_breaker")),
                                  "reason": "تدهور خارج العينة" if result.get("circuit_breaker") else "سليم",
                                  "ts": time.time()}
        return result
    except Exception as error:
        logger.debug(f"[ML4T] walk-forward error: {error}")
        return {"enabled": True, "status": "error", "error": str(error)}

def _sle_evolution_loop():
    """حلقة التطوّر: تنتظر تراكم نتائج حقيقية ثم تطوّر الجينوم تلقائيًا."""
    while True:
        try:
            scanner_heartbeat("sle_evolution")
            if SLE_ENABLED:
                _ml4t_walk_forward_review()
                sle_evolve(force=False, reason="تطوّر دوري تلقائي")
        except Exception as error:
            logger.error(f"[SLE] evolution loop error: {error}")
        time.sleep(max(300, SLE_EVOLVE_INTERVAL_SEC // 4))


def _sle_active_matured(active_id, started_ts):
    """كم نتيجة *محسومة* حصدها الجينوم الحيّ منذ بداية حقيقته؟ (أساس الانتقال للحقبة التالية)."""
    try:
        with state_lock:
            journal = [dict(e) for e in (state.get("signal_journal") or [])]
        count = 0
        for e in journal:
            if str(e.get("gen") or "") != str(active_id):
                continue
            if _safe_float(e.get("ts")) < started_ts:
                continue
            if _sle_entry_outcome(e):
                count += 1
        return count
    except Exception:
        return 0


def _sle_pick_next_genome():
    """يختار الجينوم التالي للتجربة: للأفضل نصيب ثابت، والبقية تتنافس على الباقي."""
    with _SLE_LOCK:
        sl = state.setdefault("self_learning", {})
        pop = list(sl.get("population") or [])
        champion_id = sl.get("champion_id")
    if not pop:
        return None
    if random.random() < SLE_CHAMPION_EPOCH_SHARE:
        for g in pop:
            if g.get("id") == champion_id:
                return g
    others = [g for g in pop if g.get("id") != champion_id]
    if not others:
        return pop[0]
    # ترجيح "تفاؤل في مواجهة المجهول" (UCB): الجينوم الجيد له وزن أعلى، لكن
    # الجينوم الذي لم يُختبر بعد له أولوية كبيرة — بدونه كانت الطفرات الجديدة
    # تُهمَل ولا تُقاس أبدًا، فيتوقف التطوّر عمليًا بعد أول جيل.
    weights = []
    for g in others:
        f = g.get("fitness")
        s = int(g.get("signals", 0) or 0)
        known = 1.0 + max(0.0, float(f) if f is not None else 0.0)
        unknown = 12.0 / max(1.0, float(s))
        weights.append(known + unknown)
    total = sum(weights)
    r = random.uniform(0, total)
    acc = 0.0
    for g, w in zip(others, weights):
        acc += w
        if r <= acc:
            return g
    return others[-1]


def _sle_close_epoch(active_id, started_ts, reason):
    """يقفل حقبة تجربة جينوم ويسجّل نتيجتها الفعلية (أو يعلّمها غير كافية)."""
    try:
        with state_lock:
            journal = [dict(e) for e in (state.get("signal_journal") or [])]
        samples = []
        for e in journal:
            if str(e.get("gen") or "") != str(active_id):
                continue
            if _safe_float(e.get("ts")) < started_ts:
                continue
            res = _sle_entry_outcome(e)
            if res:
                samples.append((res[0], res[1], res[2]))
        st = _sle_stats(samples)
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            epochs = sl.setdefault("epochs", [])
            epochs.append({
                "genome": active_id, "start": started_ts, "end": time.time(), "reason": reason,
                "signals": len(samples),
                "avg": round(st["avg"], 3) if st else None,
                "win_rate": round(st["win_rate"], 3) if st else None,
                "sufficient": bool(st and st["n"] >= SLE_MIN_GEN_SAMPLES),
            })
            if len(epochs) > 300:
                del epochs[: len(epochs) - 300]
        return st
    except Exception as error:
        logger.debug(f"[SLE] close_epoch error: {error}")
        return None


def _sle_rotation_loop():
    """
    تناوب الأجيال حيًّا: كل حقبة (90 دقيقة افتراضيًا) يُطبَّق جينوم مختلف على البوت
    الفعلي، فتُقاس طفراته على إشارات حقيقية في السوق. هذا اللي يجعل التطوّر
    "تعلّمًا من الواقع" بدل تعديل عشوائي للإعدادات.
    """
    while True:
        try:
            scanner_heartbeat("sle_rotation")
            if SLE_ENABLED:
                phase = get_market_phase()
                with _SLE_LOCK:
                    sl = state.setdefault("self_learning", {})
                    active_id = sl.get("active_genome_id") or sl.get("champion_id")
                    started_ts = _safe_float(sl.get("active_since_ts"), time.time())
                elapsed = time.time() - started_ts
                matured_now = _sle_active_matured(active_id, started_ts)
                # ننتقل للجينوم التالي لما تصير عندنا عيّنات كافية للحكم عليه،
                # أو لما نستنفد الحد الأقصى للوقت (أيام هادئة = ما نعلق على جينوم للأبد).
                ready = ((elapsed >= SLE_GEN_EPOCH_SEC and matured_now >= SLE_MIN_GEN_SAMPLES)
                         or elapsed >= SLE_GEN_EPOCH_MAX_SEC)
                # في الإغلاق/عطلة نهاية الأسبوع لا توجد إشارات حية جديدة، لكن
                # يمكن إنهاء حقبة لديها عينات محسومة مسبقًا وتطبيق الجينوم التالي
                # بأمان. لا ننشئ إشارات أو أسعارًا وهمية.
                if ready and (phase in ("PRE", "REGULAR", "AFTER")
                              or (phase == "CLOSED" and matured_now >= SLE_MIN_GEN_SAMPLES)):
                    _sle_close_epoch(active_id, started_ts, "انتهاء الحقبة")
                    nxt = _sle_pick_next_genome()
                    if nxt:
                        _sle_apply_genome(nxt.get("values") or {})
                        with _SLE_LOCK:
                            sl = state.setdefault("self_learning", {})
                            sl["active_genome_id"] = nxt["id"]
                            sl["active_since_ts"] = time.time()
                            stats = sl.setdefault("rotation_stats", {})
                            rec = stats.setdefault(nxt["id"], {"epochs": 0})
                            rec["epochs"] = int(rec.get("epochs", 0)) + 1
                        logger.info(f"[SLE] حقبة جديدة: الجينوم {nxt['id']} — {nxt.get('note')}")
        except Exception as error:
            logger.error(f"[SLE] rotation loop error: {error}")
        time.sleep(120)


# ---------------------------------------------------------------------
# 4) الحكم الذاتي: مشرف الخيوط + التشخيص الذاتي + الأهداف + المراجعة اليومية
# ---------------------------------------------------------------------
SLE_THREADS = {}


def sle_spawn(name, target, daemon=True, supervise=True):
    """
    يشغّل خيطًا ويسجّله عند المشرف — أي خيط يموت يُعاد تشغيله تلقائيًا.
    supervise=False لمهام تُنفّذ مرة واحدة (تحميل قائمة الأسهم مثلًا) — إحياؤها
    المتكرر ما له معنى.
    """
    entry = SLE_THREADS.setdefault(name, {"target": target, "restarts": [], "thread": None})
    entry["target"] = target
    entry["supervise"] = bool(supervise)
    entry["started_ts"] = time.time()
    thread = threading.Thread(target=target, daemon=daemon, name=name)
    entry["thread"] = thread
    thread.start()
    return thread


def _sle_restart_allowed(entry):
    now_ts = time.time()
    entry["restarts"] = [t for t in entry.get("restarts", []) if now_ts - t < 3600]
    return len(entry["restarts"]) < min(SLE_MAX_THREAD_RESTARTS_PER_HOUR, 3)


def _sle_supervise_threads():
    """الجهاز العصبي: يراقب كل خيط، ويعيد إحياء الميت، ويسجّل السبب كدرس."""
    revived = []
    for name, entry in list(SLE_THREADS.items()):
        try:
            thread = entry.get("thread")
            if thread is not None and thread.is_alive():
                continue
            if name == "SLE_Supervisor":
                continue
            if entry.get("supervise") is False:
                continue
            # خيط عاش ثوانٍ فقط ومات؟ غالبًا ينهي عمله بنفسه (مثل ماسح يفتقد مكتبة
            # اختيارية). بدل ما نحييه كل دقيقة للأبد (ضجيج وموارد مهدورة)، نتراجع
            # أُسّيًا: 60ث ← 120 ← 240 ← … بحد أقصى 30 دقيقة، مع سقف 3 محاولات/ساعة.
            # ولا نتخلى عنه نهائيًا — عشان يرجع لو كان السبب مؤقتًا (بيانات لسه ما
            # تحمّلت، أو انقطاع شبكة).
            started = _safe_float(entry.get("started_ts"), 0.0)
            # نافذة "الموت السريع" لازم تكون أطول من دورة المشرف نفسها، وإلا كل
            # خيط يموت فورًا يُشاهد بعد 60 ثانية (أطول من النافذة) فيُصفَّر العدّاد
            # ولا يشتغل التراجع الأُسّي أبدًا.
            quick_window = max(120.0, SLE_SUPERVISOR_INTERVAL * 2.0)
            if started and (time.time() - started) < quick_window:
                entry["quick_deaths"] = int(entry.get("quick_deaths", 0)) + 1
            else:
                entry["quick_deaths"] = 0
            qd = int(entry.get("quick_deaths", 0))
            if qd >= 2:
                backoff = min(1800, 60 * (2 ** (qd - 1)))
                if time.time() < _safe_float(entry.get("next_retry_ts"), 0.0):
                    continue
                entry["next_retry_ts"] = time.time() + backoff
                if qd == 3:
                    _sle_add_lesson("selfcheck",
                                    f"*{name}* يخرج فورًا من نفسه — بطّأت إحياءه تدريجيًا "
                                    f"(قد يكون ينتظر بيانات أو مكتبة اختيارية غير مثبتة).")
            if not _sle_restart_allowed(entry):
                continue
            target = entry.get("target")
            if not callable(target):
                continue
            new_thread = threading.Thread(target=target, daemon=True, name=name)
            entry["thread"] = new_thread
            entry["restarts"].append(time.time())
            entry["started_ts"] = time.time()
            new_thread.start()
            revived.append((name, len(entry["restarts"])))
        except Exception as error:
            logger.error(f"[SLE] supervise {name} error: {error}")
    for name, count in revived:
        _sle_add_lesson("repair", f"أعدت تشغيل الماسح *{name}* — كان متوقفًا. (مرة {count} خلال آخر ساعة)")
    return revived


def _sle_health_snapshot():
    """تشخيص ذاتي: خيوط، حداثة بيانات، حماية ياهو، ذاكرة، وحجم الحالة."""
    now_ts = time.time()
    alive = sum(1 for e in SLE_THREADS.values() if (e.get("thread") is not None and e["thread"].is_alive()))
    hb = dict(_heartbeats) if isinstance(globals().get("_heartbeats"), dict) else {}
    stale = []
    phase = "CLOSED"
    try:
        phase = get_market_phase()
    except Exception:
        pass
    for name, ts in hb.items():
        age = now_ts - _safe_float(ts)
        limit = 900 if phase in ("REGULAR", "PRE", "AFTER") else 3600
        if age > limit:
            stale.append({"scanner": name, "age_sec": int(age)})
    yahoo = {"level": 0, "trips": 0}
    try:
        st = globals().get("_yahoo_429_state") or {}
        yahoo = {"level": int(st.get("level", 0)), "trips": int(st.get("trips", 0))}
    except Exception:
        pass
    mem_pct = None
    try:
        if psutil is not None:
            mem_pct = float(psutil.virtual_memory().percent)
    except Exception:
        pass
    state_mb = None
    try:
        if os.path.exists(STATE_FILE):
            state_mb = round(os.path.getsize(STATE_FILE) / 1048576.0, 2)
    except Exception:
        pass
    score = 100
    if SLE_THREADS:
        score -= int((len(SLE_THREADS) - alive) / max(1, len(SLE_THREADS)) * 40)
    score -= min(30, len(stale) * 5)
    if yahoo.get("level", 0) >= 2:
        score -= 15
    if mem_pct is not None and mem_pct > 88:
        score -= 20
    if state_mb is not None and state_mb > 40:
        score -= 10
    snap = {
        "ts": now_ts, "phase": phase, "threads_total": len(SLE_THREADS), "threads_alive": alive,
        "stale_scanners": stale[:12], "yahoo_429_level": yahoo.get("level", 0),
        "memory_pct": mem_pct, "state_file_mb": state_mb, "health_score": max(0, score),
    }
    with _SLE_LOCK:
        state.setdefault("self_learning", {})["health"] = snap
    return snap


def _sle_default_goals():
    """أهداف الروبوت: يقيس نفسه عليها ويوجّه تطوّره نحوها."""
    return {
        "win_rate_60": {"target": 0.55, "label": "نسبة الإشارات الإيجابية (60د)", "unit": "pct"},
        "expectancy_60": {"target": 0.6, "label": "العائد المتوقع المعدّل بالمخاطرة (60د)", "unit": "pct"},
        "noise_ratio": {"target": 0.30, "label": "نسبة المحركات ضعيفة الأداء", "unit": "pct", "lower_is_better": True},
        "sample_coverage": {"target": 8.0, "label": "إشارات محسومة يوميًا", "unit": "count"},
    }


def _sle_update_goals(by_engine):
    """يقيس الأهداف من الأرقام الحقيقية ويحدّد الإجراء التالي لكل هدف بعيد."""
    all_samples = [s for lst in by_engine.values() for s in lst]
    st = _sle_stats(all_samples)
    with _SLE_LOCK:
        sl = state.setdefault("self_learning", {})
        goals = sl.get("goals") or {}
        if not goals:
            goals = _sle_default_goals()
    total_n = st["n"] if st else 0
    noise = 0.0
    if by_engine:
        weak = sum(1 for lst in by_engine.values() if (len(lst) >= SLE_MIN_ENGINE_SAMPLES
                   and (_sle_stats(lst) or {}).get("expectancy", 0) < 0))
        judged = sum(1 for lst in by_engine.values() if len(lst) >= SLE_MIN_ENGINE_SAMPLES)
        noise = (weak / judged) if judged else 0.0
    current = {
        "win_rate_60": round(st["win_rate"], 3) if st else 0.0,
        "expectancy_60": round(st["expectancy"], 3) if st else 0.0,
        "noise_ratio": round(noise, 3),
        "sample_coverage": round(total_n / 5.0, 2),   # تقدير: آخر ~5 أيام
    }
    for key, g in goals.items():
        cur = current.get(key, 0.0)
        tgt = float(g.get("target", 0))
        if g.get("lower_is_better"):
            progress = 1.0 if cur <= tgt else max(0.0, 1.0 - (cur - tgt) / max(tgt, 0.1))
        else:
            progress = min(1.0, cur / tgt) if tgt else 0.0
        g["current"] = cur
        g["progress"] = round(progress, 3)
        if progress >= 0.999:
            g["action"] = "الهدف متحقق — الحفاظ عليه (الطفرات تصير أصغر)"
        elif key == "win_rate_60":
            g["action"] = "أرفع معايير الدخول (سكور/RVOL) وأكتم المحركات ضعيفة الأداء"
        elif key == "expectancy_60":
            g["action"] = "أقلّل التنبيهات الممتدة وأشدّد فلاتر الزخم"
        elif key == "noise_ratio":
            g["action"] = "أعطي فرصًا أكثر للمحركات الواعدة وأقل للضجيج"
        else:
            g["action"] = "أوسّع الاكتشاف قليلًا عشان أسرّع جمع العيّنات"
    with _SLE_LOCK:
        state.setdefault("self_learning", {})["goals"] = goals
    return goals, current


def _sle_goals_loop():
    while True:
        try:
            scanner_heartbeat("sle_goals")
            if SLE_ENABLED:
                by_engine, _, _ = _sle_scan_journal()
                _sle_update_goals(by_engine)
        except Exception as error:
            logger.error(f"[SLE] goals loop error: {error}")
        time.sleep(600)


def _sle_supervisor_loop():
    """المشرف: إحياء الخيوط الميتة + تشخيص ذاتي دوري + درس عند تدهور الحالة."""
    global _SLE_SUPERVISOR_READY
    last_health_note = 0.0
    while True:
        try:
            scanner_heartbeat("sle_supervisor")
            if SLE_ENABLED:
                _sle_supervise_threads()
                snap = _sle_health_snapshot()
                sle_persist_thread_registry()
                _SLE_SUPERVISOR_READY = True
                now_ts = time.time()
                if snap.get("health_score", 100) < 60 and now_ts - last_health_note > 3600:
                    last_health_note = now_ts
                    issues = []
                    if snap["threads_alive"] < snap["threads_total"]:
                        issues.append(f"{snap['threads_total'] - snap['threads_alive']} خيط متوقف")
                    if snap["stale_scanners"]:
                        issues.append("ماسحات متأخرة: " + ", ".join(s["scanner"] for s in snap["stale_scanners"][:5]))
                    if snap["yahoo_429_level"] >= 2:
                        issues.append(f"حماية ياهو مفعّلة (مستوى {snap['yahoo_429_level']})")
                    if snap.get("memory_pct") and snap["memory_pct"] > 88:
                        issues.append(f"ضغط ذاكرة {snap['memory_pct']:.0f}%")
                    _sle_add_lesson("selfcheck",
                                    f"صحة النظام {snap['health_score']}/100 — " + " | ".join(issues or ["لا تفاصيل"]))
        except Exception as error:
            logger.error(f"[SLE] supervisor loop error: {error}")
        time.sleep(SLE_SUPERVISOR_INTERVAL)


def _sle_daily_review_loop():
    """مراجعة ذاتية يومية: الروبوت يشرح وش تعلّم، وش غيّر، ووش خطته."""
    while True:
        try:
            scanner_heartbeat("sle_review")
            if SLE_ENABLED and SLE_DAILY_REVIEW_ENABLED:
                now = now_est()
                today = now.strftime("%Y-%m-%d")
                with _SLE_LOCK:
                    sl = state.setdefault("self_learning", {})
                    last = sl.get("last_daily_review")
                if (last != today and now.hour == DAILY_REPORT_HOUR and now.minute >= DAILY_REPORT_MINUTE
                        and get_market_phase() in ("AFTER", "CLOSED")):
                    with _SLE_LOCK:
                        state.setdefault("self_learning", {})["last_daily_review"] = today
                    save_state()
                    send_telegram(sle_brain_report(daily=True))
        except Exception as error:
            logger.error(f"[SLE] daily review error: {error}")
        time.sleep(300)


# ---------------------------------------------------------------------
# 5) تقارير عربية للروبوت (تُستخدم من الأوامر والمراجعة اليومية)
# ---------------------------------------------------------------------
def _sle_trust_label(trust):
    if trust >= 0.95:
        return "موثوق"
    if trust >= 0.75:
        return "مقبول"
    if trust >= 0.5:
        return "ضعيف — يُكتم جزئيًا"
    return "سيّئ — يُكتم"


def sle_brain_report(daily=False):
    """تقرير "عقل" الروبوت: الجيل، اللياقة، المحركات، الأهداف، الدروس."""
    try:
        by_engine, by_gen, journal_len = _sle_scan_journal()
        _sle_refresh_engine_trust(by_engine)
        goals, current = _sle_update_goals(by_engine)
        with _SLE_LOCK:
            sl = state.get("self_learning") or {}
            gen = int(sl.get("generation", 0))
            champion = _sle_champion() or {}
            lessons = list(sl.get("lessons") or [])[-5:]
            trust_map = dict(sl.get("engine_trust") or {})
            throttle = dict(sl.get("throttle_stats") or {})
            health = dict(sl.get("health") or {})
            seen = int(sl.get("signals_seen_total", 0))
            suppressed = int(sl.get("suppressed_total", 0))
        all_samples = [s for lst in by_engine.values() for s in lst]
        st = _sle_stats(all_samples)
        title = "🧠 *مراجعة الروبوت اليومية*" if daily else "🧠 *عقل الروبوت*"
        lines = [title, "━━━━━━━━━━━━━━━━"]
        if ACTIVE_ENGINES_ONLY:
            lines.append(f"🎛️ وضع المحركات الحصري: *{', '.join(sorted(ACTIVE_ENGINES_ONLY))} فقط*")
        active_id = sl.get("active_genome_id") or champion.get("id")
        active_since = _safe_float(sl.get("active_since_ts"), 0.0)
        epoch_age = int((time.time() - active_since) / 60) if active_since else 0
        lines.append(f"🔬 الجيل الحالي: *{gen}* | الجينوم المُختبَر الآن: `{active_id}` (منذ {epoch_age} دقيقة)")
        if active_id != champion.get("id"):
            lines.append(f"🏆 الأفضل تاريخيًا: `{champion.get('id', '—')}` — الجينوم الحالي تجربة تُقاس على السوق الآن")
        champ_fit = champion.get("fitness")
        lines.append("📈 لياقة الجيل: " + (f"*{float(champ_fit):+.2f}*" if champ_fit is not None else "لسه ما اكتملت عيّنات كافية"))
        lines.append(f"👁️ إشارات مرصودة: *{seen}* | إشارات مقاسة: *{len(all_samples)}* | سجل: *{journal_len}*")
        if st:
            lines.append(f"🎯 إجمالي الأداء: إيجابي *{st['win_rate']*100:.0f}%* | متوسط *{st['avg']:+.2f}%* | "
                         f"عائد معدّل بالمخاطرة *{st['expectancy']:+.2f}%* | "
                         f"وصل الهدف +{SLE_EVAL_TARGET_PCT:.0f}%: *{st['target_rate']*100:.0f}%* | لمس الوقف: *{st['stop_rate']*100:.0f}%*")
        if health:
            lines.append(f"❤️ صحة النظام: *{health.get('health_score', '—')}/100* "
                         f"(خيوط {health.get('threads_alive', '?')}/{health.get('threads_total', '?')})")
        wf = dict(sl.get("ml4t_walk_forward") or {})
        if wf.get("status") == "ok":
            guard = "موقوف مؤقتًا" if wf.get("circuit_breaker") else "سليم"
            lines.append(f"🧪 Walk-forward: {wf.get('train', 0)} تدريب / {wf.get('test', 0)} اختبار | "
                         f"صافي الاختبار {wf.get('test_avg_net', 0):+.2f}% | "
                         f"انحراف {wf.get('drift', 0):+.2f}% | الحماية: *{guard}*")
        elif wf.get("status") == "insufficient_data":
            lines.append(f"🧪 Walk-forward: عينات غير كافية ({wf.get('n', 0)}) — لا اعتماد تلقائي بعد")
        dqn = wf.get("dqn") or {}
        if DQN_ENABLED:
            dqn_mode = "Shadow فقط" if DQN_SHADOW_ONLY else "فلتر Paper"
            lines.append(f"🤖 DQN: {dqn.get('status', 'غير مدرّب')} | عينات {dqn.get('samples', 0)} | الوضع: {dqn_mode}")
        ppo = wf.get("ppo") or {}
        if PPO_ENABLED:
            ppo_mode = "Shadow فقط" if PPO_SHADOW_ONLY else "فلتر Paper"
            lines.append(f"🧠 PPO: {ppo.get('status', 'غير مدرّب')} | عينات {ppo.get('samples', 0)} | نماذج {ppo.get('models', 0)} | الوضع: {ppo_mode}")
        rolling = wf.get("rolling_ml") or {}
        if ROLLING_ML_ENABLED:
            rolling_mode = "Shadow فقط" if ROLLING_ML_SHADOW_ONLY else "فلتر Paper"
            lines.append(f"🔁 Rolling ML: {rolling.get('status', 'غير مدرّب')} | عينات {rolling.get('samples', 0)} | "
                         f"اختبار {rolling.get('test', 0)} | دقة خارج العينة {rolling.get('accuracy', 0)*100:.0f}% | الوضع: {rolling_mode}")
        if daily:
            lines.append("\n🏆 *أداء المحركات (مقاسة فعليًا):*")
            rows = sorted(trust_map.items(), key=lambda kv: -_safe_float(kv[1].get("expectancy"), -99))
            if rows:
                for engine, rec in rows[:10]:
                    lines.append(f"• *{engine}*: {rec['n']} إشارة | إيجابي {rec['win_rate']*100:.0f}% | "
                                 f"متوسط {rec['avg']:+.2f}% | هدف +{SLE_EVAL_TARGET_PCT:.0f}%: {rec.get('target_rate', 0)*100:.0f}% | "
                                 f"وقف: {rec.get('stop_rate', 0)*100:.0f}% | ثقة {rec['trust']:.2f} ({_sle_trust_label(rec['trust'])})")
            else:
                lines.append("• لسه ما تراكمت نتائج كافية — أول قياس بعد 15 دقيقة من كل تنبيه.")
            lines.append("\n🎯 *الأهداف والتقدّم:*")
            for key, g in goals.items():
                mark = "✅" if g.get("progress", 0) >= 0.999 else "🔸"
                lines.append(f"{mark} {g.get('label')}: الآن *{g.get('current')}* / الهدف *{g.get('target')}* "
                             f"({g.get('progress', 0)*100:.0f}%)")
                if g.get("progress", 0) < 0.999:
                    lines.append(f"   ↳ خطتي: {g.get('action')}")
            if suppressed:
                lines.append(f"\n🚫 كتم ذكي: *{suppressed}* تنبيه (محركات ضعيفة الأداء) — الاستكشاف مستمر حتى ترجع.")
            if throttle:
                active = [f"{k}({v.get('suppressed', 0)})" for k, v in throttle.items() if v.get("suppressed")]
                if active:
                    lines.append("🚫 أكثر المحركات المكتومة: " + ", ".join(active[:6]))
            sens = _sle_param_sensitivity()
            if sens:
                lines.append("\n🔍 *أهم الإعدادات تأثيرًا في النتيجة:*")
                for name, r, n in sens[:4]:
                    spec = SLE_SPEC_BY_NAME.get(name)
                    direction = "زيادته تحسّن الأداء" if r > 0 else "تقليله يحسّن الأداء"
                    lines.append(f"• {spec[6] if spec else name} ({direction}, ارتباط {r:+.2f} على {n} جيل)")
        if lessons:
            lines.append("\n📚 *آخر ما تعلّمته:*")
            for lesson in lessons:
                lines.append(f"• {lesson.get('text')}")
        lines.append("\nℹ️ كل الأرقام من نتائج حقيقية لإشارات أرسلها البوت فعليًا — مو باكتست.")
        return "\n".join(lines)
    except Exception as error:
        logger.error(f"[SLE] brain report error: {error}")
        return f"⚠️ تعذّر بناء تقرير الروبوت: {error}"


def sle_genome_report():
    """يعرض الجينوم الحالي مقابل الإعدادات الأصلية."""
    try:
        champion = _sle_champion() or {}
        values = champion.get("values") or _SLE_BASELINE
        with _SLE_LOCK:
            sl = state.get("self_learning") or {}
            gen = int(sl.get("generation", 0))
        lines = [f"🧬 *الجينوم — الجيل {gen}* (`{champion.get('id', '—')}`)", "━━━━━━━━━━━━━━━━"]
        changed, same = [], []
        for name, kind, lo, hi, step, cat, label in SLE_PARAM_SPECS:
            cur = values.get(name, _SLE_BASELINE.get(name))
            base = _SLE_BASELINE.get(name)
            txt = _sle_param_delta_text(name, cur, base)
            (changed if txt else same).append(txt or f"{label}: {cur}")
        if changed:
            lines.append("*معاملات عدّلها الروبوت بنفسه:*")
            lines.extend("• " + c for c in changed)
        lines.append(f"\n*معاملات على الإعداد الأصلي ({len(same)}):*")
        lines.extend("• " + s for s in same[:10])
        if len(same) > 10:
            lines.append(f"… و{len(same) - 10} معامل آخر")
        lines.append("\nℹ️ الأصل = متغيرات البيئة وقت التشغيل. الانحراف مسقوف بنصف مدى كل معامل.")
        return "\n".join(lines)
    except Exception as error:
        return f"⚠️ تعذّر عرض الجينوم: {error}"


def sle_lessons_report(limit=12):
    with _SLE_LOCK:
        lessons = list((state.get("self_learning") or {}).get("lessons") or [])
    if not lessons:
        return "📚 لسه ما فيه دروس — الروبوت يحتاج تنبيهات مكتملة القياس (15/30/60/180 دقيقة)."
    lines = ["📚 *دروس الروبوت (الأحدث أولًا)*", "━━━━━━━━━━━━━━━━"]
    for lesson in reversed(lessons[-limit:]):
        ts = datetime.fromtimestamp(_safe_float(lesson.get("ts")), KSA_TZ).strftime("%m-%d %H:%M")
        lines.append(f"• [{ts}] ({lesson.get('kind')}) {lesson.get('text')}")
    return "\n".join(lines)


def sle_selfcheck_report():
    snap = _sle_health_snapshot()
    lines = ["🩺 *تشخيص الروبوت لنفسه*", "━━━━━━━━━━━━━━━━"]
    lines.append(f"❤️ صحة عامة: *{snap.get('health_score')}/100* | الجلسة: {_PHASE_LABEL_AR.get(snap.get('phase'), snap.get('phase'))}")
    lines.append(f"🧵 الخيوط: *{snap.get('threads_alive')}/{snap.get('threads_total')}* شغالة")
    if snap.get("stale_scanners"):
        lines.append("⏰ ماسحات متأخرة:")
        for item in snap["stale_scanners"][:8]:
            lines.append(f"• {item['scanner']}: آخر نبضة قبل {item['age_sec']//60} دقيقة")
    else:
        lines.append("⏰ كل الماسحات تنبض في وقتها")
    lines.append(f"🛡️ حماية ياهو: مستوى {snap.get('yahoo_429_level')}")
    if snap.get("memory_pct") is not None:
        lines.append(f"💾 الذاكرة: {snap['memory_pct']:.0f}%")
    if snap.get("state_file_mb") is not None:
        lines.append(f"🗂️ ملف الحالة: {snap['state_file_mb']} MB")
    with _SLE_LOCK:
        reg = dict((state.get("self_learning") or {}).get("thread_registry") or {})
    revived = {k: v for k, v in reg.items() if v.get("restarts")}
    if revived:
        lines.append("🔧 إعادات تشغيل تلقائية:")
        for name, info in list(revived.items())[:6]:
            lines.append(f"• {name}: {info.get('restarts')} مرة")
    return "\n".join(lines)


def sle_goals_report():
    by_engine, _, _ = _sle_scan_journal()
    goals, current = _sle_update_goals(by_engine)
    lines = ["🎯 *أهداف الروبوت وخطته*", "━━━━━━━━━━━━━━━━"]
    for key, g in goals.items():
        mark = "✅" if g.get("progress", 0) >= 0.999 else "🔸"
        lines.append(f"{mark} *{g.get('label')}*: الآن {g.get('current')} / الهدف {g.get('target')} "
                     f"({g.get('progress', 0)*100:.0f}%)")
        lines.append(f"   ↳ {g.get('action')}")
    lines.append("\nℹ️ الروبوت يوجّه تطوّره نحو هذه الأهداف تلقائيًا مع كل جيل.")
    return "\n".join(lines)


def sle_trust_report():
    by_engine, _, _ = _sle_scan_journal()
    trust_map = _sle_refresh_engine_trust(by_engine)
    if not trust_map:
        return "🤖 لسه ما فيه نتائج مقاسة كافية — أول قياس بعد 15 دقيقة من أي تنبيه."
    lines = ["🤖 *ثقة الروبوت بكل محرك (من نتائج حقيقية)*", "━━━━━━━━━━━━━━━━"]
    for engine, rec in sorted(trust_map.items(), key=lambda kv: -_safe_float(kv[1].get("expectancy"), -99)):
        lines.append(f"• *{engine}* — ثقة *{rec['trust']:.2f}* ({_sle_trust_label(rec['trust'])})\n"
                     f"   {rec['n']} إشارة | إيجابي {rec['win_rate']*100:.0f}% | متوسط {rec['avg']:+.2f}% | "
                     f"أسوأ مساهمة {rec['downside']:+.2f}%")
    lines.append(f"\n⚙️ الكتم الذكي: {'مفعّل' if SLE_THROTTLE_ENABLED else 'مطفّى'} | "
                 f"أدنى عيّنات للحكم: {SLE_MIN_ENGINE_SAMPLES} | هامش الاستكشاف: {SLE_EXPLORATION_FLOOR*100:.0f}%")
    return "\n".join(lines)


# ---------------------------------------------------------------------
# 6) التهيئة والتشغيل
# ---------------------------------------------------------------------
def sle_init():
    """يحمّل ذاكرة الروبوت، يطبّق الجينوم الحالي، ويركّب المراقبة."""
    if not SLE_ENABLED:
        logger.info("🧠 [SLE] التعلّم الذاتي مطفّى (SELF_LEARNING_ENABLED=false) — البوت يشتغل بسلوكه الأصلي")
        return False
    try:
        _sle_ensure_population()
        champion = _sle_champion()
        values = (champion or {}).get("values") or {}
        applied = _sle_apply_genome(values)
        _sle_install_hooks()
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            sl["enabled"] = True
            sl["started_ts"] = sl.get("started_ts") or time.time()
            sl["baseline"] = {k: v for k, v in _SLE_BASELINE.items()}
            if not sl.get("active_genome_id"):
                sl["active_genome_id"] = (champion or {}).get("id") or sl.get("champion_id")
            sl.setdefault("active_since_ts", time.time())
            sl.setdefault("epochs", [])
            sl.setdefault("rotation_stats", {})
        logger.info(f"🧠 [SLE] الروبوت جاهز — جيل {int((state.get('self_learning') or {}).get('generation', 0))} | "
                    f"طُبّق {applied} معامل | محركات مرصودة سابقًا: "
                    f"{len((state.get('self_learning') or {}).get('engine_trust') or {})}")
        save_state()
        return True
    except Exception as error:
        logger.error(f"[SLE] init error: {error}")
        return False


def sle_start_loops():
    """يشغّل حلقات الروبوت تحت إشراف المشرف نفسه (فيعيد تشغيلها لو ماتت)."""
    if not SLE_ENABLED:
        return
    try:
        sle_spawn("SLE_Evolution", _sle_evolution_loop)
        sle_spawn("SLE_Rotation", _sle_rotation_loop)
        sle_spawn("SLE_Goals", _sle_goals_loop)
        sle_spawn("SLE_Review", _sle_daily_review_loop)
        sle_spawn("SLE_Supervisor", _sle_supervisor_loop)
        logger.info("🧠 [SLE] حلقات التعلّم والتطوّر والحكم الذاتي اشتغلت")
    except Exception as error:
        logger.error(f"[SLE] start_loops error: {error}")


def sle_persist_thread_registry():
    """يحفظ سجل الخيوط (عدد الإعادات) عشان يبقى بعد إعادة التشغيل."""
    try:
        with _SLE_LOCK:
            sl = state.setdefault("self_learning", {})
            reg = sl.setdefault("thread_registry", {})
            for name, entry in SLE_THREADS.items():
                rec = reg.setdefault(name, {})
                rec["restarts"] = len(entry.get("restarts", []))
                rec["alive"] = bool(entry.get("thread") is not None and entry["thread"].is_alive())
    except Exception:
        pass


def sle_runtime_toggle(enabled):
    """تشغيل/إطفاء التعلّم والتطوّر وقت التشغيل (بدون إعادة تشغيل البوت)."""
    global SLE_ENABLED, SLE_THROTTLE_ENABLED
    SLE_ENABLED = bool(enabled)
    with _SLE_LOCK:
        sl = state.setdefault("self_learning", {})
        sl["enabled"] = bool(enabled)
        sl["toggled_ts"] = time.time()
    save_state()
    return SLE_ENABLED


# ================================================================
# 🌙 OFFLINE TRAINER — تدريب الروبوت والسوق مقفل (عطلة نهاية الأسبوع، الليل، العطل الرسمية)
# ----------------------------------------------------------------
# الفكرة: ياهو المجاني يعطي شموع 5 دقايق لآخر ~55 يوم لأي سهم، فنعيد تشغيل الأيام الماضية على آلاف الأسهم:
#   1) نسحب شموع 5د لسهم واحد في كل مرة (كل الجلسات PRE/REGULAR/AFTER) ونستخرج "أحداث" (لحظات حركة) بدون أي نظر للمستقبل.
#   2) لكل حدث نحفظ أرقامه (تغيّر اليوم، RVOL، تسارع الحجم، حركة الشمعة، اختراق 20 يوم) + نتيجته الفعلية بعد 60 دقيقة
#      (أعلى/أدنى سعر ومتى، وسعر الإغلاق) — ملف مستقل (offline_training.json.gz) ما يلمس ملف الحالة.
#   3) أي جينوم (مجموعة إعدادات) يُقيَّم فوراً على كل الأحداث: يطبّق عتباته (فجوة/RVOL/زخم/كولداون/هدف/وقف) ويطلع نتيجة صفقاته.
#   4) هذي النتائج تُضاف للياقة الجينوم في sle_evolve، فيتطوّر حتى لو ما فيه إشارات حيّة (بنفس حدود الأمان القديمة).
# حمايات: 70% قديم للتدريب و30% أحدث "حجز" للتحقق — جينوم يربح بالتدريب ويخسر بالحجز يُلغى عنه رصيد الأوفلاين (فرط تعلّم).
# ما يشتغل إلا والسوق CLOSED، ويوقف قبل افتتاح PRE بـ 25 دقيقة، وأول ما يتحرك السوق يقف بين سهم وسهم ويحفظ تقدمه.
# ⚠️ هذا "محاكاة تقريبية": شموع 5د مو تيكات، فالمحاكاة تقدّر المحركات الحيّة (GAP/EARLY/BREAKOUT) ولا تطابقها حرفياً.
# ================================================================
import gzip as _gzip

OFFLINE_ENABLED = os.getenv("OFFLINE_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
OFFLINE_MAX_SYMBOLS = int(os.getenv("OFFLINE_MAX_SYMBOLS", "10000"))            # سقف الأسهم بالدورة الواحدة
OFFLINE_HISTORY_PERIOD = os.getenv("OFFLINE_HISTORY_PERIOD", "55d")             # ياهو يسمح بـ5د لحد 60 يوم
OFFLINE_MAX_EVENTS = int(os.getenv("OFFLINE_MAX_EVENTS", "80000"))              # الأقدم يُحذف لو تجاوزنا
OFFLINE_MIN_BARS = 120                                                          # أقل عدد شموع لنقبل السهم
OFFLINE_FORWARD_BARS = 12                                                       # 12 شمعة 5د = 60 دقيقة (نفس مرحلة الحكم SLE_HORIZON)
OFFLINE_MIN_FORWARD_BARS = 6
OFFLINE_SAVE_EVERY = int(os.getenv("OFFLINE_SAVE_EVERY", "40"))                 # احفظ كل كم سهم
OFFLINE_EVOLVE_EVERY = int(os.getenv("OFFLINE_EVOLVE_EVERY", "250"))            # خطوة تطوّر كل كم سهم
OFFLINE_EVOLVE_MIN_GAP_SEC = int(os.getenv("OFFLINE_EVOLVE_MIN_GAP_SEC", "1800"))
OFFLINE_CREDIT_PER_EVENTS = int(os.getenv("OFFLINE_CREDIT_PER_EVENTS", "40"))   # كل كم حدث جديد = "عيّنة محسومة" لبوابة التطوّر
OFFLINE_FITNESS_CAP = int(os.getenv("OFFLINE_FITNESS_CAP", "400"))              # سقف عيّنات الأوفلاين لكل جينوم (كي ما تغرق الحيّة)
OFFLINE_HOLDOUT_SHARE = 0.30
OFFLINE_STOP_BEFORE_PRE_MIN = int(os.getenv("OFFLINE_STOP_BEFORE_PRE_MIN", "25"))
OFFLINE_FILE = os.path.join(_VOLUME_ROOT, "offline_training.json.gz")          # تقدّم التدريب (صغير)
OFFLINE_EVENTS_NPZ = os.path.join(_VOLUME_ROOT, "offline_events.npz")          # الأحداث نفسها (مصفوفة مضغوطة)
OFFLINE_ENGINES = ("GAP", "EARLY", "BREAKOUT")
# المعاملات اللي المحاكاة تقدر تقيسها فعلاً. أي معامل ثانٍ (سكور/كولداون عام/سقوف يومية…) ما له أثر على نتيجة الأوفلاين،
# فتركه يتحرك أثناء التدريب = مشي عشوائي على إعدادات الروبوت الحيّ بدون أي دليل. لذلك نجمّده وقت التدريب الأوفلاين.
OFFLINE_REPLAYABLE = frozenset({"DISCOVERY_MIN_CHANGE", "GAP_HUNTER_MIN_CHANGE", "MIN_RVOL_PRE", "MIN_RVOL_REGULAR", "MIN_RVOL_AFTER",
                                "HIGH_BREAKOUT_MIN_RVOL", "EARLY_MIN_MOVE_3M", "EARLY_MIN_VOL_RATIO", "ALERT_SYMBOL_COOLDOWN",
                                "ALERT_CONTINUATION_PCT", "BASE_TP_PCT", "SL_PCT"})
OFFLINE_HOLD_TOLERANCE_PCT = float(os.getenv("OFFLINE_HOLD_TOLERANCE_PCT", "0.05"))   # أقصى تراجع عن الأساس بالتحقق (صافي % للصفقة)
OFFLINE_MIN_HOLD_TRADES = int(os.getenv("OFFLINE_MIN_HOLD_TRADES", "60"))             # أقل صفقات تحقق للحكم على جينوم
OFFLINE_GUARD_MIN_EVENTS = int(os.getenv("OFFLINE_GUARD_MIN_EVENTS", "3000"))         # تحت هذا العدد ما نحكم بالأوفلاين
_OFFLINE_SESS = {0: "PRE", 1: "REGULAR", 2: "AFTER"}
_offline_lock = threading.RLock()

class _EventStore:
    """
    أحداث الأوفلاين بشكل مضغوط: مصفوفة numpy (n × 14) بدل 80 ألف قائمة بايثون (~45MB → ~9MB).
    نفس الواجهة اللي يستخدمها باقي الكود: len / for / [i] / [a:b] (عرض بدون نسخ) / extend.
    الأعمدة: ts, رقم السهم (index بقائمة names), price, day_chg, rvol, vol_ratio, move5, brk, session, fwd_hi, fwd_lo, fwd_close, idx_hi, idx_lo
    """
    NCOL = 14

    def __init__(self, buf=None, n=0, names=None, idx=None):
        self._buf = buf if buf is not None else np.zeros((1024, self.NCOL), dtype=np.float64)
        self.n = int(n)
        self.names = names if names is not None else []
        self._idx = idx if idx is not None else {}

    @property
    def arr(self):
        return self._buf[:self.n]

    def __len__(self):
        return self.n

    def _tuples(self, a):
        names, out = self.names, []
        for r in a.tolist():
            r[0] = int(r[0]); r[1] = names[int(r[1])]; r[7] = int(r[7]); r[8] = int(r[8]); r[12] = int(r[12]); r[13] = int(r[13])
            out.append(tuple(r))
        return out

    def __iter__(self):
        a = self.arr
        for s in range(0, self.n, 4096):
            yield from self._tuples(a[s:s + 4096])

    def __getitem__(self, k):
        if isinstance(k, slice):
            a = self.arr[k]
            return _EventStore(a, len(a), self.names, self._idx)
        k = int(k)
        if k < 0:
            k += self.n
        if not 0 <= k < self.n:
            raise IndexError(k)
        return self._tuples(self.arr[k:k + 1])[0]

    def extend(self, events):
        if not events:
            return
        m = len(events)
        add = np.empty((m, self.NCOL), dtype=np.float64)
        for i, e in enumerate(events):
            sid = self._idx.get(e[1])
            if sid is None:
                sid = len(self.names); self.names.append(e[1]); self._idx[e[1]] = sid
            row = list(e); row[1] = sid
            add[i] = row
        need = self.n + m
        if need > len(self._buf):
            nb = np.zeros((max(need, int(len(self._buf) * 1.5) + 1024), self.NCOL), dtype=np.float64)
            nb[:self.n] = self._buf[:self.n]
            self._buf = nb
        self._buf[self.n:need] = add
        self.n = need

    def sort_by_ts(self):
        a = self.arr
        self._buf[:self.n] = a[np.argsort(a[:, 0], kind="stable")]

    def trim_front(self, k):
        k = int(k)
        if k <= 0:
            return
        k = min(k, self.n)
        keep = self._buf[k:self.n].copy()
        self._buf[:len(keep)] = keep
        self.n = len(keep)

    def sorted_copy(self):
        a = self.arr
        b = a[np.argsort(a[:, 0], kind="stable")]
        return _EventStore(b, len(b), self.names, self._idx)

    def purge_symbols(self, pred):
        """يحذف أحداث الرموز اللي ما تنجح بالشرط pred (مثل warrants). يرجع عدد المحذوف."""
        if not self.n or not self.names:
            return 0
        ok = np.array([bool(pred(nm)) for nm in self.names])
        a = self.arr
        keep = ok[a[:, 1].astype(np.int64)]
        kept = a[keep]
        removed = self.n - len(kept)
        if removed:
            self._buf[:len(kept)] = kept
            self.n = len(kept)
        return removed

    @classmethod
    def from_npz(cls, path):
        z = np.load(path, allow_pickle=False)
        arr = np.asarray(z["arr"], dtype=np.float64)
        names = [str(x) for x in z["names"]]
        buf = np.zeros((max(1024, len(arr) + 1024), cls.NCOL), dtype=np.float64)
        buf[:len(arr)] = arr
        return cls(buf, len(arr), names, {n: i for i, n in enumerate(names)})


def _trim_memory():
    """يرجّع للنظام الذاكرة الفاضية (gc + malloc_trim). آمن: أي فشل يُتجاهل."""
    try:
        gc.collect()
        import ctypes
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def _mem_summary():
    """أحجام أكبر المستهلكات — يطبعها مراقب الذاكرة كل فترة عشان نعرف وين تروح الذاكرة."""
    parts = []
    try:
        ev = _offline.get("events")
        parts.append(f"أحداث_أوفلاين={getattr(ev, '_buf', np.zeros(0)).nbytes / 1048576:.0f}MB({len(ev):,})")
    except Exception:
        pass
    try:
        with _pl_lock:
            X, M = _pl.get("X"), _pl.get("meta")
            pend = sum(c[0].nbytes + c[1].nbytes for c in _pl["chunks"])
            nb = (X.nbytes if X is not None else 0) + (M.nbytes if M is not None else 0) + pend
        parts.append(f"أنماط={nb / 1048576:.0f}MB")
    except Exception:
        pass
    parts.append(f"خيوط={threading.active_count()}")
    return " ".join(parts)


_offline = {"events": _EventStore(), "sym_last": {}, "ev_last": {}, "cursor": 0, "pass_no": 1, "seed": 7, "symbols_done": 0,
            "bad": [], "started_ts": 0.0, "last_save": 0.0, "session_symbols": 0, "session_events": 0,
            "total_symbols_fetched": 0, "empty_fetches": 0, "universe_size": 0, "last_evolve_ts": 0.0, "active": False}
_offline_eval_cache = {"key": None, "val": None}


_OFFLINE_MIGRATE_CODE = r"""
import sys, gzip, json, os
import numpy as np
src, npz, meta = sys.argv[1], sys.argv[2], sys.argv[3]
with gzip.open(src, "rt", encoding="utf-8") as f:
    d = json.load(f)
ev = d.pop("events", None) or []
names, idx = [], {}
arr = np.zeros((len(ev), 14), dtype=np.float64)
for i, e in enumerate(ev):
    s = e[1]
    k = idx.get(s)
    if k is None:
        k = len(names); names.append(s); idx[s] = k
    row = list(e); row[1] = k
    arr[i] = row
with open(npz + ".tmp", "wb") as f:
    np.savez_compressed(f, arr=arr, names=np.array(names, dtype="U16"))
os.replace(npz + ".tmp", npz)
with gzip.open(meta + ".tmp", "wb") as f:
    f.write(json.dumps(d, separators=(",", ":")).encode("utf-8"))
os.replace(meta + ".tmp", meta)
print(len(ev))
"""


def _offline_migrate_legacy():
    """ملف الأحداث القديم (JSON فيه عشرات الآلاف من القوائم) → npz. يتم بعملية منفصلة كي ما يتفتت heap البوت الرئيسي."""
    import subprocess, shutil
    try:
        res = subprocess.run([sys.executable, "-c", _OFFLINE_MIGRATE_CODE, OFFLINE_FILE, OFFLINE_EVENTS_NPZ, OFFLINE_FILE + ".migrated"],
                             capture_output=True, text=True, timeout=300)
        if res.returncode != 0:
            logger.warning(f"[Offline] migration failed: {res.stderr[-300:]}")
            return False
        shutil.copy2(OFFLINE_FILE, OFFLINE_FILE + ".legacy")       # نسخة أمان من الملف القديم
        os.replace(OFFLINE_FILE + ".migrated", OFFLINE_FILE)
        logger.info(f"[Offline] ملف الأحداث القديم تحوّل لصيغة مضغوطة ({res.stdout.strip()} حدث)")
        return True
    except Exception as error:
        logger.warning(f"[Offline] migration error: {error}")
        return False


def _offline_load():
    try:
        if os.path.exists(OFFLINE_FILE):
            if not os.path.exists(OFFLINE_EVENTS_NPZ):
                _offline_migrate_legacy()
            with _gzip.open(OFFLINE_FILE, "rt", encoding="utf-8") as f:
                data = json.load(f)
            legacy_events = data.pop("events", None)                # لو بقي بالملف (فشل الترحيل) نحمّله بالطريقة القديمة
            with _offline_lock:
                for k in ("sym_last", "ev_last", "cursor", "pass_no", "seed", "symbols_done", "bad", "total_symbols_fetched", "empty_fetches"):
                    if k in data:
                        _offline[k] = data[k]
                if os.path.exists(OFFLINE_EVENTS_NPZ):
                    _offline["events"] = _EventStore.from_npz(OFFLINE_EVENTS_NPZ)
                elif legacy_events:
                    _offline["events"] = _EventStore()
                    _offline["events"].extend(legacy_events)
            del data, legacy_events
            with _offline_lock:
                removed = _offline["events"].purge_symbols(_is_plain_common_symbol)
                if removed:
                    logger.info(f"[Offline] purged {removed} events (warrants/rights/units)")
            _trim_memory()
            logger.info(f"[Offline] loaded: events={len(_offline['events'])} cursor={_offline['cursor']} pass={_offline['pass_no']}")
    except Exception as error:
        logger.warning(f"[Offline] load failed (starting fresh): {error}")


def _offline_save(force=False):
    try:
        with _offline_lock:
            snap = {k: _offline[k] for k in ("sym_last", "ev_last", "cursor", "pass_no", "seed", "symbols_done", "bad",
                                              "total_symbols_fetched", "empty_fetches")}
            snap["saved_ts"] = time.time()
            payload = json.dumps(snap, separators=(",", ":")).encode("utf-8")
            ev_arr = _offline["events"].arr.copy()
            ev_names = np.array(list(_offline["events"].names), dtype="U16")
        os.makedirs(os.path.dirname(OFFLINE_FILE) or ".", exist_ok=True)
        tmpn = OFFLINE_EVENTS_NPZ + ".tmp"
        with open(tmpn, "wb") as f:
            np.savez_compressed(f, arr=ev_arr, names=ev_names)
        os.replace(tmpn, OFFLINE_EVENTS_NPZ)
        tmp = OFFLINE_FILE + ".tmp"
        with _gzip.open(tmp, "wb", compresslevel=5) as f:
            f.write(payload)
        os.replace(tmp, OFFLINE_FILE)
        _offline["last_save"] = time.time()
        return True
    except Exception as error:
        logger.warning(f"[Offline] save failed: {error}")
        return False


def _offline_window_open(now=None):
    """السوق CLOSED فعلاً، وما بقي أقل من OFFLINE_STOP_BEFORE_PRE_MIN دقيقة على افتتاح PRE (4:00 ص نيويورك) بيوم تداول."""
    try:
        if get_market_phase() != "CLOSED":
            return False
        now = now or now_est()
        minutes = now.hour * 60 + now.minute
        if now.weekday() < 5 and not _is_market_holiday(now):
            if 4 * 60 - OFFLINE_STOP_BEFORE_PRE_MIN <= minutes < 4 * 60:
                return False
        return True
    except Exception:
        return False


def _offline_extract_events(symbol, df):
    """
    شموع 5د (index بتوقيت نيويورك) → قائمة أحداث. بدون أي نظر للمستقبل في الميزات (RVOL/تسارع/اختراق كلها من شموع سابقة).
    الحدث = [ts, symbol, price, day_chg%, rvol, vol_ratio, move5%, brk20, session, fwd_hi%, fwd_lo%, fwd_close%, idx_hi, idx_lo].
    """
    try:
        if df is None or df.empty or len(df) < OFFLINE_MIN_BARS:
            return []
        d = df[["open", "high", "low", "close", "volume"]].astype(float).dropna()
        d = d[(d["close"] > 0) & (d["high"] > 0) & (d["low"] > 0)]
        n = len(d)
        if n < OFFLINE_MIN_BARS:
            return []
        idx = d.index
        minutes = (idx.hour * 60 + idx.minute).to_numpy()
        sess = np.full(n, 3, dtype=int)
        sess[(minutes >= 240) & (minutes < 570)] = 0
        sess[(minutes >= 570) & (minutes < 960)] = 1
        sess[(minutes >= 960) & (minutes < 1200)] = 2
        dates = np.array(idx.date)
        close = d["close"].to_numpy(); high = d["high"].to_numpy(); low = d["low"].to_numpy(); vol = d["volume"].to_numpy()
        # ثواني UTC بغض النظر عن وحدة الزمن بنسخة pandas (ns/us/s) — القسمة المباشرة على 10**9 تنكسر بنسخ pandas الحديثة
        ts_arr = ((idx - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).to_numpy().astype("int64")

        uniq = sorted(set(dates))
        reg_close, reg_high = {}, {}
        for i in range(n):
            if sess[i] == 1:
                reg_close[dates[i]] = close[i]
                reg_high[dates[i]] = max(reg_high.get(dates[i], 0.0), high[i])
        reg_days = sorted(reg_close)
        # الأيام اللي ما فيها جلسة عادية (عطل) ما لها مرجع؛ نربط أي يوم بآخر يوم عادي سابق له
        ref_close, ref_brk = {}, {}
        for day in uniq:
            prior = [x for x in reg_days if x < day]
            if prior:
                ref_close[day] = reg_close[prior[-1]]
                if len(prior) >= 5:
                    ref_brk[day] = max(reg_high[x] for x in prior[-20:])

        s = pd.Series(vol, index=range(n))
        sess_s = pd.Series(sess, index=range(n))
        base20 = s.groupby(sess_s).transform(lambda x: x.shift(1).rolling(20, min_periods=8).mean()).to_numpy()
        base6 = s.shift(1).rolling(6, min_periods=3).mean().to_numpy()
        last_idx_of_date = {}
        for i in range(n):
            last_idx_of_date[dates[i]] = i

        events, last_slot = [], {}
        min_since = _offline["sym_last"].get(symbol, 0)
        price_lo, price_hi = DISCOVERY_PRICE_MIN, DISCOVERY_PRICE_MAX
        for i in range(1, n):
            if sess[i] == 3 or ts_arr[i] <= min_since:
                continue
            day = dates[i]
            pc = ref_close.get(day)
            if not pc or pc <= 0:
                continue
            price = close[i]
            if not (price_lo <= price <= price_hi) or vol[i] < 1000:
                continue
            dayc = (price / pc - 1.0) * 100.0
            if dayc < 1.0:
                continue
            rvol = (vol[i] / base20[i]) if (base20[i] and base20[i] > 0) else 0.0
            vr = (vol[i] / base6[i]) if (base6[i] and base6[i] > 0) else 0.0
            mv5 = ((price / close[i - 1] - 1.0) * 100.0) if dates[i - 1] == day else 0.0
            lvl = ref_brk.get(day)
            brk = 1 if (lvl and price > lvl) else 0
            gap_c = dayc >= 8.0 and rvol >= 0.8
            early_c = mv5 >= 1.5 and vr >= 1.5
            brk_c = bool(brk) and rvol >= 1.0
            if not (gap_c or early_c or brk_c):
                continue
            slot_len = 600 if (early_c or brk_c) else 1800
            slot = (day, int(ts_arr[i] // slot_len))
            if slot in last_slot:
                continue
            end = min(i + OFFLINE_FORWARD_BARS, last_idx_of_date[day])
            if end - i < OFFLINE_MIN_FORWARD_BARS:
                continue
            last_slot[slot] = True
            fwd_h, fwd_l = high[i + 1:end + 1], low[i + 1:end + 1]
            ih, il = int(np.argmax(fwd_h)) + 1, int(np.argmin(fwd_l)) + 1
            events.append([int(ts_arr[i]), symbol, round(float(price), 4), round(float(dayc), 2), round(float(rvol), 2),
                           round(float(vr), 2), round(float(mv5), 2), brk, int(sess[i]),
                           round(float(fwd_h.max() / price - 1.0) * 100.0, 2), round(float(fwd_l.min() / price - 1.0) * 100.0, 2),
                           round(float(close[end] / price - 1.0) * 100.0, 2), ih, il])
        return events
    except Exception as error:
        logger.debug(f"[Offline] extract {symbol}: {error}")
        return []


def _offline_genome_signals(values, events):
    """يطبّق عتبات الجينوم على الأحداث (مرتّبة زمنياً) → [(engine, ts, pct, hi, lo)] بعد كولداون السهم وخطة الهدف/الوقف."""
    v = values or {}
    def g(name, default):
        try:
            return float(v.get(name, default))
        except Exception:
            return float(default)
    disc = g("DISCOVERY_MIN_CHANGE", DISCOVERY_MIN_CHANGE)
    gap_min = g("GAP_HUNTER_MIN_CHANGE", GAP_HUNTER_MIN_CHANGE)
    brk_rvol = g("HIGH_BREAKOUT_MIN_RVOL", 1.5)
    e_move = g("EARLY_MIN_MOVE_3M", EARLY_MIN_MOVE_3M) * (EARLY_MIN_MOVE_5M / max(EARLY_MIN_MOVE_3M, 0.1))   # 3د→5د بنفس نسبة ثوابت EARLY (1.5)
    e_vr = g("EARLY_MIN_VOL_RATIO", EARLY_MIN_VOL_RATIO)
    cool = g("ALERT_SYMBOL_COOLDOWN", ALERT_SYMBOL_COOLDOWN)
    cont = g("ALERT_CONTINUATION_PCT", ALERT_CONTINUATION_PCT)
    rvol_gate = {0: g("MIN_RVOL_PRE", MIN_RVOL_BY_PHASE["PRE"]), 1: g("MIN_RVOL_REGULAR", MIN_RVOL_BY_PHASE["REGULAR"]),
                 2: g("MIN_RVOL_AFTER", MIN_RVOL_BY_PHASE["AFTER"])}
    tp = max(0.5, (g("BASE_TP_PCT", BASE_TP_PCT) - 1.0) * 100.0)
    sl = max(0.5, (1.0 - g("SL_PCT", SL_PCT)) * 100.0)
    last = {}
    out = []
    for ev in events:
        ts, sym, price, dayc, rvol, vr, mv5, brk, sess, fh, fl, fc, ih, il = ev
        engine = None
        if dayc >= disc:
            if dayc >= gap_min and rvol >= rvol_gate.get(sess, 1.5):
                engine = "GAP"
            elif brk and rvol >= brk_rvol:
                engine = "BREAKOUT"
        if engine is None and EARLY_MIN_DAY_CHANGE <= dayc < EARLY_MAX_DAY_CHANGE and mv5 >= e_move and vr >= e_vr:
            engine = "EARLY"
        if engine is None:
            continue
        prev = last.get(sym)
        if prev and ts - prev[0] < cool and price < prev[1] * (1.0 + cont / 100.0):
            continue
        last[sym] = (ts, price)
        hit_tp, hit_sl = fh >= tp, fl <= -sl
        if hit_tp and hit_sl:
            pct = tp if ih < il else -sl          # التعادل/السبق للوقف = محافظ
        elif hit_tp:
            pct = tp
        elif hit_sl:
            pct = -sl
        else:
            pct = fc
        out.append((engine, ts, pct, fh, fl))
    return out


def _offline_split(events):
    """(train, holdout) حسب الزمن: الأقدم تدريب، الأحدث 30% حجز. events لازم مرتّبة بـts."""
    if not events:
        return [], []
    cut = events[int(len(events) * (1.0 - OFFLINE_HOLDOUT_SHARE))][0] if len(events) >= 20 else events[-1][0] + 1
    if isinstance(events, _EventStore):
        k = int(np.searchsorted(events.arr[:, 0], cut, side="left"))      # مرتّب بالزمن → التدريب/الحجز شريحتان بدون نسخ
        return events[:k], events[k:]
    return [e for e in events if e[0] < cut], [e for e in events if e[0] >= cut]


def _offline_sorted_events():
    with _offline_lock:
        return _offline["events"].sorted_copy()      # نسخة مرتّبة مضغوطة (~9MB) بدل قائمة ضخمة


def _offline_evaluate(values, split=None):
    """يقيّم جينوم على الأحداث: يرجع dict فيه عيّنات التدريب/الحجز (pct,hi,lo) وإحصاءات كل محرك."""
    split = split or _offline_split(_offline_sorted_events())
    train, hold = split
    res = {"train": [], "hold": [], "engines": {}}
    for part, evs in (("train", train), ("hold", hold)):
        sigs = _offline_genome_signals(values, evs)
        res[part] = [(s[2], s[3], s[4]) for s in sigs]
        if part == "train":
            for s in sigs:
                res["engines"].setdefault(s[0], []).append((s[2], s[3], s[4]))
    return res


def _offline_overfit(train_samples, hold_samples):
    """ربح بالتدريب وخسارة بالحجز (مع عيّنة كافية بالحجز) = علامة فرط تعلّم."""
    if len(train_samples) < 20 or len(hold_samples) < 15:
        return False
    tr = sum(s[0] for s in train_samples) / len(train_samples)
    ho = sum(s[0] for s in hold_samples) / len(hold_samples)
    return tr > 0 and ho < 0


def _offline_credit_events():
    with _offline_lock:
        return len(_offline["events"])


def _offline_genome_verdict(values, split):
    """يقيّم جينوم على التدريب والتحقق *بعد التكلفة* (OFFLINE_TRADE_COST_PCT) — لأن الربح الحقيقي هو الصافي، والروبوت كان يكافئ
    كثرة الصفقات ونسبة الربح (win-rate) بدل المتوسط الصافي، فيرقّي إعدادات أكثر تداولاً وأسوأ ربحاً."""
    r = _offline_evaluate(values, split)
    cost = OFFLINE_TRADE_COST_PCT
    tr = [(p - cost, hi, lo) for (p, hi, lo) in r["train"]]
    ho = [(p - cost, hi, lo) for (p, hi, lo) in r["hold"]]
    info = {"n_train": len(tr), "n_hold": len(ho),
            "train_net": (sum(x[0] for x in tr) / len(tr)) if tr else 0.0,
            "hold_net": (sum(x[0] for x in ho) / len(ho)) if ho else 0.0, "overfit": _offline_overfit(tr, ho)}
    if info["overfit"]:
        info["samples"] = []
        return info
    if len(tr) > OFFLINE_FITNESS_CAP:
        step = len(tr) / float(OFFLINE_FITNESS_CAP)
        tr = [tr[int(i * step)] for i in range(OFFLINE_FITNESS_CAP)]
    info["samples"] = tr
    return info


def _offline_genome_ok(gid, strict=False):
    """🛡️ حارس: هل الجينوم مسموح يبقى/يترقّى؟ لازم تحققه (الأحدث 30%) ما يكون أسوأ من إعدادات البيئة الأصلية بأكثر من هامش صغير.
    strict=True (للترقية): لازم صفقات تحقق كافية وإلا مرفوض. بدون بيانات أوفلاين كافية = ما نحكم (True)."""
    try:
        if _offline_credit_events() < OFFLINE_GUARD_MIN_EVENTS:
            return True
        info = _offline_eval_cache.get("val") or {}
        g, b = info.get(gid), info.get("__base__")
        if not g or not b:
            return not strict
        if g["overfit"]:
            return False
        if g["n_hold"] < OFFLINE_MIN_HOLD_TRADES or b["n_hold"] < OFFLINE_MIN_HOLD_TRADES:
            return not strict
        return g["hold_net"] >= b["hold_net"] - OFFLINE_HOLD_TOLERANCE_PCT
    except Exception:
        return True


def _offline_promotion_ok(best_id, champ_id):
    """الترقية تحتاج: الجينوم الجديد يجتاز الحارس بصرامة، وتحققه ما يقل عن تحقق البطل الحالي."""
    if not _offline_genome_ok(best_id, strict=True):
        return False
    info = _offline_eval_cache.get("val") or {}
    b, c = info.get(best_id), info.get(champ_id)
    if b and c and c["n_hold"] >= OFFLINE_MIN_HOLD_TRADES and b["hold_net"] < c["hold_net"]:
        return False
    return True


def _offline_freeze_unmeasured(child_values, base_values):
    """وقت التدريب الأوفلاين: المعاملات اللي المحاكاة ما تقيسها ترجع لقيمة البطل (ما نغيّرها بلا دليل)."""
    return {k: (v if k in OFFLINE_REPLAYABLE else base_values.get(k, v)) for k, v in child_values.items()}


def _offline_merge_into_gen(by_gen, population):
    """[يُستدعى من sle_evolve] يضيف عيّنات الأوفلاين (منقّاة وبسقف) لكل جينوم بالجيل. يرجع عدد الأحداث الكلي."""
    total = _offline_credit_events()
    if not OFFLINE_ENABLED or total < 50:
        return total
    try:
        key = (total, tuple(sorted((g.get("id"), tuple(sorted((g.get("values") or {}).items()))) for g in population)))
        if _offline_eval_cache["key"] == key:
            merged = _offline_eval_cache["val"]
        else:
            split = _offline_split(_offline_sorted_events())
            merged = {}
            for gen in population:
                gid, vals = gen.get("id"), gen.get("values") or {}
                if not gid:
                    continue
                merged[gid] = _offline_genome_verdict(vals, split)
            merged["__base__"] = _offline_genome_verdict(dict(_SLE_BASELINE), split)
            _offline_eval_cache["key"], _offline_eval_cache["val"] = key, merged
        for gid, info in merged.items():
            if gid != "__base__" and info["samples"]:
                by_gen.setdefault(gid, []).extend(info["samples"])
        skipped = [gid for gid, info in merged.items() if info.get("overfit")]
        if skipped:
            logger.info(f"[Offline] overfit guard: offline credit withheld from {skipped}")
    except Exception as error:
        logger.warning(f"[Offline] merge error: {error}")
    return total


def _offline_universe():
    with state_lock:
        have = list(state.get("full_tickers") or [])
    if not have:
        try:
            _load_full_ticker_list()
        except Exception as error:
            logger.warning(f"[Offline] ticker list load failed: {error}")
        with state_lock:
            have = list(state.get("full_tickers") or [])
    bad = set(_offline["bad"])
    syms = [s for s in dict.fromkeys(have) if s and s not in bad and _is_plain_common_symbol(s)]
    rnd = random.Random(int(_offline["seed"]) * 1000 + int(_offline["pass_no"]))
    rnd.shuffle(syms)
    return syms[:OFFLINE_MAX_SYMBOLS]


def _offline_status_text():
    with _offline_lock:
        o = dict(_offline); n_ev = len(o["events"])
    evs = _offline_sorted_events()
    est_hours = (max(0, o.get("universe_size", 0) - o["cursor"]) * (YAHOO_MIN_REQUEST_GAP + 0.35)) / 3600.0
    lines = ["🌙 *إحصائيات التدريب الأوفلاين*", "━━━━━━━━━━━━━━━━",
             f"الحالة: {'🟢 يتدرّب الحين' if o.get('active') else ('⏸️ ينتظر إغلاق السوق' if OFFLINE_ENABLED else '⛔ مطفّى')}",
             f"الدورة رقم {o['pass_no']} — تقدّم: {o['cursor']:,} / {o.get('universe_size', 0):,} سهم (إجمالي سُحب: {o['total_symbols_fetched']:,})",
             f"أحداث محفوظة: {n_ev:,} (سقف {OFFLINE_MAX_EVENTS:,})"]
    if o.get("universe_size"):
        lines.append(f"الباقي لإنهاء الدورة: ~{est_hours:.1f} ساعة تدريب")
    if evs:
        d0 = datetime.fromtimestamp(evs[0][0], EASTERN_TZ).strftime("%Y-%m-%d")
        d1 = datetime.fromtimestamp(evs[-1][0], EASTERN_TZ).strftime("%Y-%m-%d")
        lines.append(f"فترة البيانات: {d0} → {d1}")
        split = _offline_split(evs)
        sl = _sle_ensure_population()
        pop = {g.get("id"): g for g in (sl.get("population") or [])}
        champ = pop.get(sl.get("champion_id"))
        rows = [("الأساس (إعدادات البيئة)", dict(_SLE_BASELINE))]
        if champ:
            rows.append((f"البطل الحالي {champ.get('id')}", champ.get("values") or {}))
        for title, vals in rows:
            r = _offline_evaluate(vals, split)
            def fmt(smp):
                if not smp:
                    return "لا صفقات"
                avg = sum(x[0] for x in smp) / len(smp); wr = sum(1 for x in smp if x[0] > 0) / len(smp) * 100
                return f"{len(smp)} صفقة | متوسط {avg:+.2f}% (صافي {avg - OFFLINE_TRADE_COST_PCT:+.2f}%) | ربحية {wr:.0f}%"
            lines.append(f"\n*{title}*\n• تدريب: {fmt(r['train'])}\n• تحقق (أحدث 30%): {fmt(r['hold'])}")
        v_base = _offline_genome_verdict(dict(_SLE_BASELINE), split)
        if champ and champ.get("id") != "g0_base":
            v_champ = _offline_genome_verdict(champ.get("values") or {}, split)
            enough = v_champ["n_hold"] >= OFFLINE_MIN_HOLD_TRADES and v_base["n_hold"] >= OFFLINE_MIN_HOLD_TRADES
            if enough and v_champ["hold_net"] < v_base["hold_net"] - OFFLINE_HOLD_TOLERANCE_PCT:
                lines.append("\n🛡️ الحكم: البطل أسوأ من الأساس بالتحقق → راح يُرجَع للأساس بخطوة التطوّر الجاية.")
            elif enough:
                lines.append("\n🛡️ الحكم: البطل ما هو أسوأ من الأساس بالتحقق.")
            else:
                lines.append("\n🛡️ الحكم: صفقات التحقق قليلة للحكم على البطل.")
        if v_base["n_hold"] >= OFFLINE_MIN_HOLD_TRADES and v_base["hold_net"] < 0:
            lines.append("⚠️ حتى الأساس خاسر صافياً بهالمحاكاة: التطوّر يقلّل الخسارة بس، ما يصنع ربحاً.")
        r0 = _offline_evaluate(dict(_SLE_BASELINE), split)
        eng = []
        for name in OFFLINE_ENGINES:
            smp = r0["engines"].get(name) or []
            if smp:
                eng.append(f"{name}: {len(smp)} | {sum(x[0] for x in smp) / len(smp):+.2f}%")
        if eng:
            lines.append("\nالمحركات (على الأساس): " + " — ".join(eng))
    else:
        lines.append("لسه ما فيه أحداث — أول ما يقفل السوق يبدأ السحب تلقائياً.")
    lines.append("\n⚠️ محاكاة تقريبية على شموع 5د؛ الأرقام للمقارنة بين الإعدادات مو وعد بربح.")
    return "\n".join(lines)


# ================================================================
# 📒 سجل الصفقات الوهمية الأوفلاين (/offlinetrades) — قراءة فقط، ما يغيّر التعلّم ولا الإعدادات
# يبني من الأحداث المحفوظة أصلاً: شراء/هدف/وقف/بيع/وقت/ربح لكل صفقة + أفضل الأسهم + ملف CSV كامل.
# ================================================================
OFFLINE_TRADE_SIZE = float(os.getenv("OFFLINE_TRADE_SIZE", "1000"))          # دولار لكل صفقة وهمية
OFFLINE_TRADE_COST_PCT = float(os.getenv("OFFLINE_TRADE_COST_PCT", "0.4"))   # تكلفة دخول+خروج تقديرية (سبريد/انزلاق)
OFFLINE_BAR_SEC = 300


def _offline_trade_ledger(values, events, tp=None, sl=None):
    """
    نفس بوابات _offline_genome_signals (نسخة منها عمداً كي لا نلمس الأصل) لكن ترجع الصفقة كاملة.
    tp/sl بالنسبة المئوية؛ إن لم تُحدَّد تُؤخذ من الجينوم مثل المحاكاة الأصلية.
    """
    v = values or {}
    def g(name, default):
        try:
            return float(v.get(name, default))
        except Exception:
            return float(default)
    disc = g("DISCOVERY_MIN_CHANGE", DISCOVERY_MIN_CHANGE)
    gap_min = g("GAP_HUNTER_MIN_CHANGE", GAP_HUNTER_MIN_CHANGE)
    brk_rvol = g("HIGH_BREAKOUT_MIN_RVOL", 1.5)
    e_move = g("EARLY_MIN_MOVE_3M", EARLY_MIN_MOVE_3M) * (EARLY_MIN_MOVE_5M / max(EARLY_MIN_MOVE_3M, 0.1))
    e_vr = g("EARLY_MIN_VOL_RATIO", EARLY_MIN_VOL_RATIO)
    cool = g("ALERT_SYMBOL_COOLDOWN", ALERT_SYMBOL_COOLDOWN)
    cont = g("ALERT_CONTINUATION_PCT", ALERT_CONTINUATION_PCT)
    rvol_gate = {0: g("MIN_RVOL_PRE", MIN_RVOL_BY_PHASE["PRE"]), 1: g("MIN_RVOL_REGULAR", MIN_RVOL_BY_PHASE["REGULAR"]),
                 2: g("MIN_RVOL_AFTER", MIN_RVOL_BY_PHASE["AFTER"])}
    if tp is None:
        tp = max(0.5, (g("BASE_TP_PCT", BASE_TP_PCT) - 1.0) * 100.0)
    if sl is None:
        sl = max(0.5, (1.0 - g("SL_PCT", SL_PCT)) * 100.0)
    last, trades = {}, []
    for ev in events:
        ts, sym, price, dayc, rvol, vr, mv5, brk, sess, fh, fl, fc, ih, il = ev
        engine = None
        if dayc >= disc:
            if dayc >= gap_min and rvol >= rvol_gate.get(sess, 1.5):
                engine = "GAP"
            elif brk and rvol >= brk_rvol:
                engine = "BREAKOUT"
        if engine is None and EARLY_MIN_DAY_CHANGE <= dayc < EARLY_MAX_DAY_CHANGE and mv5 >= e_move and vr >= e_vr:
            engine = "EARLY"
        if engine is None:
            continue
        prev = last.get(sym)
        if prev and ts - prev[0] < cool and price < prev[1] * (1.0 + cont / 100.0):
            continue
        last[sym] = (ts, price)
        hit_tp, hit_sl = fh >= tp, fl <= -sl
        if hit_tp and hit_sl:
            reason = "TP" if ih < il else "SL"
        elif hit_tp:
            reason = "TP"
        elif hit_sl:
            reason = "SL"
        else:
            reason = "TIME"
        pct = tp if reason == "TP" else (-sl if reason == "SL" else fc)
        entry_ts = ts + OFFLINE_BAR_SEC                      # الدخول عند إغلاق شمعة الإشارة
        bars = ih if reason == "TP" else (il if reason == "SL" else OFFLINE_FORWARD_BARS)
        trades.append({
            "symbol": sym, "engine": engine, "entry_ts": entry_ts, "entry": price,
            "target": price * (1.0 + tp / 100.0), "stop": price * (1.0 - sl / 100.0),
            "reason": reason, "exit": price * (1.0 + pct / 100.0),
            "exit_ts": entry_ts + OFFLINE_BAR_SEC * bars, "hold_min": bars * 5,
            "pct": pct, "net_pct": pct - OFFLINE_TRADE_COST_PCT,
            "usd": OFFLINE_TRADE_SIZE * (pct - OFFLINE_TRADE_COST_PCT) / 100.0,
            "max_gain": fh, "max_dd": fl, "day_chg": dayc, "rvol": rvol,
        })
    return trades


def _offline_trades_report(tp=None, sl=None):
    """يرجع (نص_تيليجرام, نص_CSV أو None)."""
    evs = _offline_sorted_events()
    if not evs:
        return "📒 لسه ما فيه أحداث محفوظة — انتظر يبدأ التدريب الأوفلاين.", None
    sle = _sle_ensure_population()
    pop = {g.get("id"): g for g in (sle.get("population") or [])}
    champ = pop.get(sle.get("champion_id"))
    values = dict(champ.get("values") or {}) if champ else dict(_SLE_BASELINE)
    gname = champ.get("id") if champ else "الأساس"
    trades = _offline_trade_ledger(values, evs, tp, sl)
    if not trades:
        return "📒 ما طلعت صفقات وهمية بهالإعدادات حتى الآن.", None
    used_tp = tp if tp is not None else max(0.5, (float(values.get("BASE_TP_PCT", BASE_TP_PCT)) - 1.0) * 100.0)
    used_sl = sl if sl is not None else max(0.5, (1.0 - float(values.get("SL_PCT", SL_PCT))) * 100.0)
    n = len(trades)
    n_tp = sum(1 for t in trades if t["reason"] == "TP")
    n_sl = sum(1 for t in trades if t["reason"] == "SL")
    n_time = n - n_tp - n_sl
    wins = sum(1 for t in trades if t["net_pct"] > 0)
    gross = sum(t["pct"] for t in trades) / n
    net = sum(t["net_pct"] for t in trades) / n
    total_usd = sum(t["usd"] for t in trades)
    reach25 = sum(1 for t in trades if t["max_gain"] >= 25.0)
    ksa = lambda ts: datetime.fromtimestamp(ts, KSA_TZ).strftime("%m-%d %H:%M")
    d0, d1 = ksa(trades[0]["entry_ts"])[:5], ksa(trades[-1]["entry_ts"])[:5]

    by_sym = {}
    for t in trades:
        r = by_sym.setdefault(t["symbol"], [0, 0.0, 0.0])
        r[0] += 1; r[1] += t["net_pct"]; r[2] += t["usd"]
    ranked = sorted(by_sym.items(), key=lambda kv: kv[1][2], reverse=True)
    best_trades = sorted(trades, key=lambda t: t["pct"], reverse=True)[:5]

    def line(t):
        mark = {"TP": "✅ هدف", "SL": "🛑 وقف", "TIME": "⏱️ وقت"}[t["reason"]]
        return (f"{t['symbol']} {t['engine']} | شراء ${_fmt_px(t['entry'])} ({ksa(t['entry_ts'])}) | هدف ${_fmt_px(t['target'])} | "
                f"وقف ${_fmt_px(t['stop'])} | بيع ${_fmt_px(t['exit'])} {mark} بعد ~{t['hold_min']}د | {t['pct']:+.1f}%")

    L = ["📒 *الصفقات الوهمية (أوفلاين)*", "━━━━━━━━━━━━━━━━",
         f"الإعداد: {gname} | هدف +{used_tp:g}% | وقف -{used_sl:g}% | مدة قصوى ~60د",
         f"حجم الصفقة: ${OFFLINE_TRADE_SIZE:,.0f} | تكلفة تقديرية {OFFLINE_TRADE_COST_PCT:g}% لكل صفقة",
         f"الفترة: {d0} → {d1} | عدد الصفقات: {n:,}",
         "",
         f"✅ ضربت الهدف: {n_tp} ({n_tp / n * 100:.0f}%) | 🛑 ضربت الوقف: {n_sl} ({n_sl / n * 100:.0f}%) | ⏱️ انتهى الوقت: {n_time}",
         f"رابحة: {wins} ({wins / n * 100:.0f}%)",
         f"متوسط الصفقة: {gross:+.2f}% قبل التكلفة | {net:+.2f}% بعدها",
         f"إجمالي وهمي: *{total_usd:+,.0f}$* على ${OFFLINE_TRADE_SIZE * n:,.0f} مُستثمرة افتراضياً",
         f"🎯 وصلت +25% خلال ساعة: {reach25} من {n} ({reach25 / n * 100:.1f}%)",
         "", "*أفضل 5 أسهم (صافي):*"]
    for sym, (c, pct_sum, usd) in ranked[:5]:
        L.append(f"• {sym}: {c} صفقة | {usd:+,.0f}$")
    if len(ranked) > 8:      # لا نكرر نفس الأسهم لو العدد قليل
        L.append("\n*أسوأ 3 أسهم:*")
        for sym, (c, pct_sum, usd) in ranked[-3:][::-1]:
            L.append(f"• {sym}: {c} صفقة | {usd:+,.0f}$")
    L.append("\n*أفضل 5 صفقات:*")
    for t in best_trades:
        L.append(line(t))
    L.append("\n*آخر 5 صفقات:*")
    for t in trades[-5:]:
        L.append(line(t))
    L.append("\n📎 الملف المرفق فيه كل الصفقات (يفتح بإكسل).")
    L.append("⚠️ محاكاة على شموع 5د: وقت البيع تقريبي ±5د، وبدون سبريد حقيقي. مو وعد بربح.")
    text = "\n".join(L)

    import csv as _csv
    buf = io.StringIO()
    w = _csv.writer(buf)
    w.writerow(["symbol", "engine", "entry_time_ksa", "entry_price", "target_price", "stop_price", "exit_reason",
                "exit_price", "exit_time_ksa_approx", "hold_min_approx", "profit_pct_gross", "profit_pct_net",
                "profit_usd_net", "max_gain_in_hour_pct", "max_drawdown_in_hour_pct", "day_change_at_entry_pct", "rvol"])
    for t in trades:
        w.writerow([t["symbol"], t["engine"], ksa(t["entry_ts"]), round(t["entry"], 4), round(t["target"], 4),
                    round(t["stop"], 4), t["reason"], round(t["exit"], 4), ksa(t["exit_ts"]), t["hold_min"],
                    round(t["pct"], 2), round(t["net_pct"], 2), round(t["usd"], 2), t["max_gain"], t["max_dd"],
                    round(t["day_chg"], 2), t["rvol"]])
    return text, buf.getvalue()


def _offline_trades_send(tp=None, sl=None):
    try:
        text, csv_text = _offline_trades_report(tp, sl)
        send_telegram(text)
        if csv_text and bot and CHAT_ID:
            doc = io.BytesIO(("\ufeff" + csv_text).encode("utf-8"))
            doc.name = "offline_paper_trades.csv"
            bot.send_document(CHAT_ID, doc)
    except Exception as error:
        logger.warning(f"[Offline] trades report error: {error}")
        send_telegram(f"⚠️ ما قدرت أطلّع سجل الصفقات: {error}")


def offline_trainer_loop():
    """خيط التدريب: ينام والسوق مفتوح، ولما يقفل يسحب أسهم قديمة سهم سهم ويستخرج أحداث ويدفع التطوّر. محمي بالمشرف."""
    if not OFFLINE_ENABLED:
        logger.info("ℹ️ Offline trainer disabled (OFFLINE_ENABLED=false)")
        return
    _offline_load()
    _pl_load(_pl)   # 🧬 يستعيد عينات ونماذج متعلّم أنماط 5د من الـVolume
    _pl_load(_dl)   # 📅 ويستعيد متعلّم الشارت اليومي
    _trim_memory()
    session_started = False
    while True:
        try:
            scanner_heartbeat("offline_trainer")
            if not OFFLINE_ENABLED or not _offline_window_open():
                if session_started:
                    _offline["active"] = False
                    _offline_save()
                    if _offline["session_symbols"] >= 50:
                        send_telegram("🌙 *انتهت جلسة التدريب (فتح السوق قريب)*\n"
                                      f"سحبت {_offline['session_symbols']:,} سهم وأضفت {_offline['session_events']:,} حدث.\n"
                                      "التفاصيل: /offline  (إحصائيات)")
                    _pl_session_end()
                    session_started = False
                time.sleep(120)
                continue
            if not session_started:
                session_started = True
                _offline["active"] = True
                _offline["session_symbols"] = _offline["session_events"] = 0
                _offline["started_ts"] = time.time()
                logger.info("🌙 [Offline] السوق مقفل — بدأ التدريب")
            universe = _offline_universe()
            _offline["universe_size"] = len(universe)
            if _offline["cursor"] >= len(universe):
                _offline["pass_no"] += 1
                _offline["cursor"] = 0
                _offline_save()
                logger.info(f"🌙 [Offline] انتهت الدورة؛ بدء دورة {_offline['pass_no']} (تدخل أيام جديدة وأحداث جديدة فقط)")
                continue
            symbol = universe[_offline["cursor"]]
            _offline["cursor"] += 1
            if _yahoo_cooldown_remaining() > 0:
                time.sleep(min(60, _yahoo_cooldown_remaining()))
                _offline["cursor"] -= 1
                continue
            df = fetch_from_yahoo(symbol, OFFLINE_HISTORY_PERIOD, "5m", prepost=True)
            _offline["total_symbols_fetched"] += 1
            _offline["session_symbols"] += 1
            if df is None or df.empty:
                _offline["empty_fetches"] += 1
                if _is_bad_ticker(symbol):
                    _offline["bad"].append(symbol)
                    _offline["bad"] = _offline["bad"][-3000:]
                continue
            try:
                _pl_ingest(symbol, df)      # 🧬 متعلّم الأنماط: نفس الشموع بدون سحب إضافي
                _pl_after_symbol()
            except Exception as error:
                logger.debug(f"[Patterns] ingest {symbol}: {error}")
            try:
                _dl_step(symbol)            # 📅 الشارت اليومي (سحب واحد لكل سهم كل ~5 أيام)
            except Exception as error:
                logger.debug(f"[DailyPatterns] {symbol}: {error}")
            new_events = _offline_extract_events(symbol, df)
            last_bar_ts = int((df.index[-1] - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1))
            with _offline_lock:
                # لا نكرر أحداث الدورة السابقة: نقبل فقط الأحدث من آخر حدث محفوظ للسهم (+10 دقايق = نفس نافذة التخفيف)
                prev_ev = _offline["ev_last"].get(symbol, 0)
                new_events = [e for e in new_events if prev_ev == 0 or e[0] >= prev_ev + 600]
                if new_events:
                    _offline["events"].extend(new_events)
                    _offline["ev_last"][symbol] = max(e[0] for e in new_events)
                    if len(_offline["events"]) > OFFLINE_MAX_EVENTS * 1.02:
                        _offline["events"].sort_by_ts()
                        _offline["events"].trim_front(len(_offline["events"]) - OFFLINE_MAX_EVENTS)
                # آخر ساعة تُعاد بالدورة الجاية: أحداثها ما كان لها 60 دقيقة مستقبل كاملة
                _offline["sym_last"][symbol] = max(_offline["sym_last"].get(symbol, 0), last_bar_ts - 3600)
            _offline["session_events"] += len(new_events)
            del df
            if _offline["session_symbols"] % OFFLINE_SAVE_EVERY == 0:
                _offline_save()
                _trim_memory()
                logger.info(f"[Offline] progress {_offline['cursor']}/{len(universe)} events={len(_offline['events'])} (+{_offline['session_events']} هالجلسة)")
            if (_offline["session_symbols"] % OFFLINE_EVOLVE_EVERY == 0
                    and time.time() - _offline["last_evolve_ts"] >= OFFLINE_EVOLVE_MIN_GAP_SEC):
                _offline["last_evolve_ts"] = time.time()
                result = sle_evolve(force=False, reason="تدريب أوفلاين (السوق مقفل)") or {}
                if result.get("ok"):
                    logger.info(f"🌙 [Offline] تطوّر: جيل {result.get('generation')}")
        except Exception as error:
            logger.error(f"[Offline] loop error: {error}")
            time.sleep(30)



# ================================================================
# 🧬 PATTERN LEARNER — يتعلّم شكل الشموع "قبل" الانفجار والسوق مقفل (سبت/أحد/ليل)
# ----------------------------------------------------------------
# الفرق عن الأوفلاين القديم: القديم يسجّل لحظة اشتغال الماسحات فقط ويضبط أرقام العتبات.
# هذا يمسح كل 15 دقيقة من تاريخ كل سهم (مو بس لما تشتغل الماسحات) ويحفظ "شكل" آخر ساعات الشموع:
#   انضغاط التذبذب، جفاف/تسارع الحجم، قيعان أعلى، قرب القمة، VWAP، قفزات الأيام السابقة…  (31 خاصية، كلها من الماضي فقط)
# ويربطها بالنتيجة اللاحقة: هل ارتفع السهم +25% خلال ساعة؟ +25% / +50% / +100% خلال اليوم التالي؟
# يتعلّم بطريقتين:
#   1) شبكة عصبية صغيرة (numpy فقط، بدون torch) تتطوّر بأجيال: كل جلسة تجرّب أبناء بإعدادات مختلفة وتبقي الأفضل.
#   2) تنقيب قواعد مقروءة: "إذا كان X و Y و Z → احتمال الانفجار كذا أضعاف المعتاد".
# حمايات: التقسيم زمني (60% تدريب / 20% اختيار / 20% أحدث حجز للتقرير فقط)، والقواعد تُقاس على الحجز.
# ما يلمس التنبيهات ولا الإعدادات ولا حالة الروبوت. يأخذ نفس الشموع التي يسحبها الأوفلاين (بدون طلبات ياهو إضافية).
# ================================================================
import zlib as _zlib

PL_ENABLED = os.getenv("PL_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
PL_MAX_SAMPLES = int(os.getenv("PL_MAX_SAMPLES", "80000"))          # سقف العينات المحفوظة (الذاكرة)
PL_NEG_KEEP = float(os.getenv("PL_NEG_KEEP", "0.004"))               # نسبة اللحظات العادية المحفوظة
PL_HARD_KEEP = float(os.getenv("PL_HARD_KEEP", "0.04"))              # نسبة اللحظات "المشابهة للانفجار" التي ما انفجرت
PL_MAX_PRIOR_MOVE = float(os.getenv("PL_MAX_PRIOR_MOVE", "12"))      # نتجاهل اللحظات اللي سخنت فعلاً (ارتفاع آخر ساعة % فوق هذا)
PL_MIN_VOL12 = float(os.getenv("PL_MIN_VOL12", "5000"))              # أقل حجم أسهم في آخر ساعة (يستبعد الأسهم الميتة)
PL_MIN_BARS = 150
PL_FWD_1H = 12                                                       # 12 شمعة 5د = ساعة
PL_SAVE_EVERY = int(os.getenv("PL_SAVE_EVERY", "200"))               # احفظ كل كم سهم
PL_TRAIN_EVERY = int(os.getenv("PL_TRAIN_EVERY", "250"))             # جلسة تدريب/تطوّر كل كم سهم
PL_TRAIN_MIN_GAP_SEC = int(os.getenv("PL_TRAIN_MIN_GAP_SEC", "1500"))
PL_MIN_TRAIN_POS = int(os.getenv("PL_MIN_TRAIN_POS", "30"))          # أقل عدد حالات انفجار بالتدريب لنتدرّب
PL_CHILDREN = int(os.getenv("PL_CHILDREN", "3"))                     # أبناء الجيل لكل هدف
PL_EPOCHS = int(os.getenv("PL_EPOCHS", "25"))
PL_MEM_HEADROOM_MB = int(os.getenv("PL_MEM_HEADROOM_MB", "100"))     # هامش ذاكرة لازم يتوفر قبل التدريب
PL_FILE_NPZ = os.path.join(_VOLUME_ROOT, "pattern_samples.npz")
PL_FILE_JSON = os.path.join(_VOLUME_ROOT, "pattern_models.json")

PL_FEATS = ["r1", "r3", "r6", "r12", "r36", "dayc", "gap", "squeeze", "bbw_ratio", "vr6", "vtrend", "rvol", "dvol12",
            "dist_brk", "dist_dayhigh", "vwap_dev", "pos78", "hl12", "hh12", "green12", "body12", "uwick6", "clpos3",
            "quiet12", "volspike3", "logp", "pre", "after", "prev_range", "prev10_spike", "tod"]
PL_FEAT_AR = {
    "r1": "حركة آخر شمعة%", "r3": "حركة 15د%", "r6": "حركة 30د%", "r12": "حركة ساعة%", "r36": "حركة 3 ساعات%",
    "dayc": "تغير اليوم%", "gap": "فجوة الافتتاح%", "squeeze": "انضغاط التذبذب", "bbw_ratio": "ضيق البولنجر",
    "vr6": "تسارع الحجم", "vtrend": "اتجاه الحجم", "rvol": "RVOL", "dvol12": "سيولة الساعة (لوغ)",
    "dist_brk": "البعد عن قمة 20 يوم%", "dist_dayhigh": "البعد عن قمة اليوم%", "vwap_dev": "البعد عن VWAP%",
    "pos78": "الموقع بنطاق 6 ساعات", "hl12": "قيعان أعلى", "hh12": "قمم أعلى", "green12": "شموع خضراء",
    "body12": "جسم الشمعة", "uwick6": "ذيل علوي", "clpos3": "الإغلاق قرب القمة", "quiet12": "شموع هادئة",
    "volspike3": "طفرة حجم", "logp": "السعر (لوغ)", "pre": "قبل السوق", "after": "بعد السوق",
    "prev_range": "مدى أمس%", "prev10_spike": "أكبر قفزة آخر 10 أيام%", "tod": "وقت اليوم",
}
PL_SLOG = {"r1", "r3", "r6", "r12", "r36", "dayc", "gap", "dist_brk", "dist_dayhigh", "vwap_dev", "vr6", "vtrend", "rvol",
           "volspike3", "prev_range", "prev10_spike"}
_PL_SLOG_IDX = np.array([i for i, n in enumerate(PL_FEATS) if n in PL_SLOG], dtype=int)
_PL_F = len(PL_FEATS)
# (id, عمود النتيجة في meta, العتبة %, وصف)
PL_TARGETS = [("h1_25", 3, 25.0, "+25% خلال ساعة"), ("d1_25", 4, 25.0, "+25% خلال يوم"),
              ("d1_50", 4, 50.0, "+50% خلال يوم"), ("d1_80", 4, 80.0, "+80% خلال يوم"),
              ("d1_100", 4, 100.0, "+100% خلال يوم"), ("d1_170", 4, 170.0, "+170% خلال يوم")]
# سلّم الارتفاعات اللي نقيس عليها: كل مستوى له نموذج مستقل لو أمثلته كافية (PL_MIN_TRAIN_POS)، وإلا نقيسه بسلّم نموذج +25%/+50%
# (لأن تدريب نموذج على 3 أمثلة +1200% يحفظها حفظاً ولا يتعلّم منها).
PL_LADDER = (25, 50, 80, 100, 170, 230, 500, 1200)
PL_LADDER_MIN_POS = int(os.getenv("PL_LADDER_MIN_POS", "10"))      # أقل انفجارات بالحجز كي نحكم على مستوى بالسلّم
M_TS, M_W, M_SYM, M_G1, M_GD, M_POS = 0, 1, 2, 3, 4, 5

_pl_train_lock = threading.Lock()                        # جلسة تدريب وحدة بنفس الوقت (5د أو يومي) لحماية الذاكرة


def _pl_new_state(name, title, footer, feats, slog_names, targets, feat_ar, npz, jsn, max_samples, train_every, ladder_tids=("d1_25", "d1_50"), horizon="يوم"):
    return {"name": name, "title": title, "footer": footer, "feats": list(feats), "feat_ar": feat_ar, "targets": targets,
            "slog": np.array([i for i, n in enumerate(feats) if n in slog_names], dtype=int),
            "npz": npz, "json": jsn, "max_samples": max_samples, "train_every": train_every, "lock": threading.RLock(),
            "X": None, "meta": None, "chunks": [], "sym_last": {}, "sym_names": [], "sym_idx": {}, "models": {}, "rules": {},
            "history": [], "symbols_ingested": 0, "since_train": 0, "last_train_ts": 0.0, "training": False, "sessions": 0,
            "session_samples": 0, "last_save": 0.0, "last_msg": "", "sym_fetch": {}, "last_feat": {},
            "ladder": {}, "ladder_tids": tuple(ladder_tids), "horizon": horizon}


_pl = _pl_new_state("5m", "🧬 *تعلّم أنماط الانفجار (شموع 5د — قبل الحركة)*",
                    "⚠️ يتعلّم من آخر ~55 يوم فقط (حد ياهو لشموع 5د)، فأمثلة +500% و+1200% نادرة جداً. الأرقام تقدير تاريخي مو وعد.",
                    PL_FEATS, PL_SLOG, PL_TARGETS, PL_FEAT_AR, PL_FILE_NPZ, PL_FILE_JSON, PL_MAX_SAMPLES, PL_TRAIN_EVERY)
_pl_lock = _pl["lock"]


def _pl_symbol_ok(symbol):
    try:
        return bool(_is_plain_common_symbol(symbol))
    except Exception:
        return True


def _pl_shift(a, k):
    out = np.full(a.shape, np.nan, dtype=float)
    if k < len(a):
        out[k:] = a[:-k]
    return out


def _pl_prepare(df):
    """شموع 5د → قاموس مصفوفات + مصفوفة الخصائص X (n × 31). بدون أي معلومة من المستقبل."""
    if df is None or df.empty or len(df) < PL_MIN_BARS:
        return None
    d = df[["open", "high", "low", "close", "volume"]].astype(float).dropna()
    d = d[(d["close"] > 0) & (d["high"] > 0) & (d["low"] > 0) & (d["high"] >= d["low"])]
    n = len(d)
    if n < PL_MIN_BARS:
        return None
    idx = d.index
    minutes = (idx.hour * 60 + idx.minute).to_numpy()
    sess = np.full(n, 3, dtype=int)
    sess[(minutes >= 240) & (minutes < 570)] = 0
    sess[(minutes >= 570) & (minutes < 960)] = 1
    sess[(minutes >= 960) & (minutes < 1200)] = 2
    dates = np.array(idx.date)
    o = d["open"].to_numpy(); h = d["high"].to_numpy(); l = d["low"].to_numpy(); c = d["close"].to_numpy(); v = d["volume"].to_numpy()
    ts = ((idx - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).to_numpy().astype("int64")

    chg = np.ones(n, dtype=bool)
    chg[1:] = dates[1:] != dates[:-1]
    day_id = np.cumsum(chg) - 1
    nd = int(day_id[-1]) + 1
    last_idx_day = np.r_[np.flatnonzero(chg)[1:] - 1, n - 1]

    # --- مرجع كل يوم: إغلاق آخر جلسة عادية، قمة 20 يوم، مدى أمس، أكبر قفزة بآخر 10 أيام ---
    regm = sess == 1
    reg_o = np.full(nd, np.nan); reg_c = np.full(nd, np.nan); reg_h = np.full(nd, np.nan); reg_l = np.full(nd, np.nan)
    if regm.any():
        g = pd.DataFrame({"d": day_id[regm], "o": o[regm], "h": h[regm], "l": l[regm], "c": c[regm]}).groupby("d")
        for col, arr, fn in (("o", reg_o, "first"), ("c", reg_c, "last"), ("h", reg_h, "max"), ("l", reg_l, "min")):
            s = getattr(g[col], fn)()
            arr[s.index.to_numpy()] = s.to_numpy()
    reg_days = [k for k in range(nd) if np.isfinite(reg_c[k])]
    refc_d = np.full(nd, np.nan); refbrk_d = np.full(nd, np.nan)
    prevrng_d = np.zeros(nd); prev10_d = np.zeros(nd); spike_d = np.full(nd, np.nan); gap_d = np.zeros(nd)
    for k in range(nd):
        prior = [x for x in reg_days if x < k]
        if prior:
            refc_d[k] = reg_c[prior[-1]]
            if len(prior) >= 5:
                refbrk_d[k] = max(reg_h[x] for x in prior[-20:])
            lo_p = reg_l[prior[-1]]
            prevrng_d[k] = min(1000.0, (reg_h[prior[-1]] / lo_p - 1.0) * 100.0) if lo_p > 0 else 0.0
            sp = [spike_d[x] for x in prior[-10:] if np.isfinite(spike_d[x])]
            prev10_d[k] = max(sp) if sp else 0.0
        if np.isfinite(reg_c[k]) and np.isfinite(refc_d[k]) and refc_d[k] > 0:
            spike_d[k] = min(2000.0, (reg_h[k] / refc_d[k] - 1.0) * 100.0)
            if np.isfinite(reg_o[k]):
                gap_d[k] = (reg_o[k] / refc_d[k] - 1.0) * 100.0
    refc = refc_d[day_id]; refbrk = refbrk_d[day_id]

    S = pd.Series
    r = {k: (c / _pl_shift(c, k) - 1.0) * 100.0 for k in (1, 3, 6, 12, 36)}
    dayc = (c / refc - 1.0) * 100.0
    rp = (h - l) / c
    atr12 = S(rp).rolling(12, min_periods=6).mean().to_numpy()
    atr78 = S(rp).rolling(78, min_periods=20).mean().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        squeeze = np.where(atr78 > 0, atr12 / atr78, np.nan)
        m20 = S(c).rolling(20, min_periods=10).mean().to_numpy()
        sd20 = S(c).rolling(20, min_periods=10).std().to_numpy()
        bbw = sd20 / m20
        bbw_base = S(bbw).rolling(100, min_periods=30).mean().to_numpy()
        bbw_ratio = np.where(bbw_base > 0, bbw / bbw_base, np.nan)
        vs = S(v)
        base6 = vs.shift(1).rolling(6, min_periods=3).mean().to_numpy()
        vr6 = np.where(base6 > 0, v / base6, np.nan)
        v6 = vs.rolling(6, min_periods=3).mean().to_numpy()
        base30 = vs.shift(6).rolling(30, min_periods=10).mean().to_numpy()
        vtrend = np.where(base30 > 0, v6 / base30, np.nan)
        rvol = np.full(n, np.nan)
        for sv in (0, 1, 2):
            ii = np.flatnonzero(sess == sv)
            if len(ii) > 10:
                b = S(v[ii]).shift(1).rolling(20, min_periods=8).mean().to_numpy()
                rvol[ii] = np.where(b > 0, v[ii] / b, np.nan)
        vol12 = vs.rolling(12, min_periods=1).sum().to_numpy()
        dvol12 = np.log10(1.0 + S(c * v).rolling(12, min_periods=1).sum().to_numpy())
        dist_brk = (c / refbrk - 1.0) * 100.0
        dayhi = S(h).groupby(day_id).cummax().to_numpy()
        dist_dayhigh = (c / dayhi - 1.0) * 100.0
        tp = (h + l + c) / 3.0
        cv = S(tp * v).groupby(day_id).cumsum().to_numpy()
        cvol = S(v).groupby(day_id).cumsum().to_numpy()
        vwap = np.where(cvol > 0, cv / cvol, np.nan)
        vwap_dev = (c / vwap - 1.0) * 100.0
        lo78 = S(l).rolling(78, min_periods=20).min().to_numpy()
        hi78 = S(h).rolling(78, min_periods=20).max().to_numpy()
        pos78 = np.where(hi78 > lo78, (c - lo78) / (hi78 - lo78), np.nan)
        hl12 = S((l > _pl_shift(l, 1)).astype(float)).rolling(12, min_periods=6).mean().to_numpy()
        hh12 = S((h > _pl_shift(h, 1)).astype(float)).rolling(12, min_periods=6).mean().to_numpy()
        green12 = S((c > o).astype(float)).rolling(12, min_periods=6).mean().to_numpy()
        rng_ = h - l
        body = np.where(rng_ > 0, np.abs(c - o) / rng_, 0.0)
        body12 = S(body).rolling(12, min_periods=6).mean().to_numpy()
        uw = np.where(rng_ > 0, (h - np.maximum(o, c)) / rng_, 0.0)
        uwick6 = S(uw).rolling(6, min_periods=3).mean().to_numpy()
        cl = np.where(rng_ > 0, (c - l) / rng_, 0.5)
        clpos3 = S(cl).rolling(3, min_periods=1).mean().to_numpy()
        quiet12 = S((rp < 0.5 * atr78).astype(float)).rolling(12, min_periods=6).mean().to_numpy()
        med36 = vs.rolling(36, min_periods=12).median().to_numpy()
        volspike3 = vs.rolling(3, min_periods=1).max().to_numpy() / (med36 + 1.0)
    logp = np.log(c)
    tod = np.clip((minutes - 240) / 960.0, 0.0, 1.0)
    cols = {"r1": r[1], "r3": r[3], "r6": r[6], "r12": r[12], "r36": r[36], "dayc": dayc, "gap": np.where(sess == 0, 0.0, gap_d[day_id]),   # قبل الافتتاح ما نعرف سعر الافتتاح بعد
            
            "squeeze": squeeze, "bbw_ratio": bbw_ratio, "vr6": vr6, "vtrend": vtrend, "rvol": rvol, "dvol12": dvol12,
            "dist_brk": dist_brk, "dist_dayhigh": dist_dayhigh, "vwap_dev": vwap_dev, "pos78": pos78, "hl12": hl12,
            "hh12": hh12, "green12": green12, "body12": body12, "uwick6": uwick6, "clpos3": clpos3, "quiet12": quiet12,
            "volspike3": volspike3, "logp": logp, "pre": (sess == 0).astype(float), "after": (sess == 2).astype(float),
            "prev_range": prevrng_d[day_id], "prev10_spike": prev10_d[day_id], "tod": tod}
    X = np.column_stack([cols[name] for name in PL_FEATS]).astype(np.float32)
    X[~np.isfinite(X)] = np.nan
    return {"n": n, "c": c, "ts": ts, "sess": sess, "day_id": day_id, "nd": nd, "last_idx_day": last_idx_day,
            "refc": refc, "refbrk": refbrk, "vol12": vol12, "X": X, "r12": r[12], "r6": r[6], "dayc": dayc, "vr6": vr6}


def _pl_extract(symbol, df, since_ts, rng):
    """يرجع (meta, X, last_ts) لعينات: أول شمعة بكل 15 دقيقة، مع نتيجتها اللاحقة. الإيجابي = انفجر ≥25% (ساعة أو يوم)."""
    P = _pl_prepare(df)
    if P is None or P["nd"] < 3:
        return None
    n, c, ts, sess, day_id, nd = P["n"], P["c"], P["ts"], P["sess"], P["day_id"], P["nd"]
    X = P["X"]
    slot = ts // 900
    first = np.ones(n, dtype=bool)
    first[1:] = slot[1:] != slot[:-1]
    price_lo, price_hi = DISCOVERY_PRICE_MIN, DISCOVERY_PRICE_MAX
    with np.errstate(invalid="ignore"):
        ok = (first & (sess != 3) & (ts > since_ts) & (day_id < nd - 1) & np.isfinite(P["refc"]) & np.isfinite(P["refbrk"])
              & (c >= price_lo) & (c <= price_hi) & (P["vol12"] >= PL_MIN_VOL12) & (np.arange(n) >= 40)
              & (P["r12"] < PL_MAX_PRIOR_MOVE) & (np.isnan(X).sum(axis=1) <= 4))
    last_ts = int(ts[P["last_idx_day"][nd - 2]])
    cand = np.flatnonzero(ok)
    if len(cand) == 0:
        return np.zeros((0, 6)), np.zeros((0, _PL_F), dtype=np.float32), last_ts
    dl = P["last_idx_day"]
    m = len(cand)
    g1 = np.full(m, np.nan); gd = np.full(m, np.nan)
    for q in range(m):
        i = int(cand[q]); k = int(day_id[i])
        end1 = min(i + PL_FWD_1H, int(dl[k]))
        if end1 - i >= 6:
            g1[q] = (c[i + 1:end1 + 1].max() / c[i] - 1.0) * 100.0
        endd = int(dl[k + 1])
        if endd > i:
            gd[q] = (c[i + 1:endd + 1].max() / c[i] - 1.0) * 100.0
    with np.errstate(invalid="ignore"):
        glitch = (g1 > 3000.0) | (gd > 3000.0)          # شمعة بيانات خاطئة من ياهو
        pos = (g1 >= 25.0) | (gd >= 25.0)
        Xc = X[cand]
        vr6 = np.nan_to_num(Xc[:, PL_FEATS.index("vr6")], nan=0.0)
        r6 = np.nan_to_num(Xc[:, PL_FEATS.index("r6")], nan=0.0)
        dayc = np.nan_to_num(Xc[:, PL_FEATS.index("dayc")], nan=0.0)
        hard = (vr6 >= 2.0) | (np.abs(r6) >= 4.0) | (dayc >= 8.0)
    u = rng.random(m)
    keep_pos = pos & ~glitch
    keep_hard = (~pos) & hard & (u < PL_HARD_KEEP) & ~glitch
    keep_neg = (~pos) & (~hard) & (u < PL_NEG_KEEP) & ~glitch
    keep = keep_pos | keep_hard | keep_neg
    if not keep.any():
        return np.zeros((0, 6)), np.zeros((0, _PL_F), dtype=np.float32), last_ts
    w = np.where(keep_pos, 1.0, np.where(keep_hard, 1.0 / PL_HARD_KEEP, 1.0 / PL_NEG_KEEP))
    sel = np.flatnonzero(keep)
    meta = np.zeros((len(sel), 6), dtype=np.float64)
    meta[:, M_TS] = ts[cand[sel]]
    meta[:, M_W] = w[sel]
    meta[:, M_G1] = g1[sel]
    meta[:, M_GD] = gd[sel]
    meta[:, M_POS] = pos[sel].astype(float)
    return meta, Xc[sel].astype(np.float32), last_ts


def _pl_sym_id(symbol, S=None):
    S = S or _pl
    i = S["sym_idx"].get(symbol)
    if i is None:
        i = len(S["sym_names"])
        S["sym_names"].append(symbol)
        S["sym_idx"][symbol] = i
    return i


def _pl_ingest(symbol, df):
    """يُستدعى من خيط الأوفلاين بنفس الشموع المسحوبة (بدون سحب إضافي). آمن: أي خطأ يُتجاهل."""
    if not PL_ENABLED or not _pl_symbol_ok(symbol):
        return 0
    since = int(_pl["sym_last"].get(symbol, 0))
    seed = (_zlib.crc32(symbol.encode()) ^ (int(_offline.get("pass_no", 1)) * 7919)) & 0xFFFFFFFF
    out = _pl_extract(symbol, df, since, np.random.default_rng(seed))
    if out is None:
        return 0
    meta, X, last_ts = out
    with _pl_lock:
        _pl["sym_last"][symbol] = max(since, last_ts)
        if len(meta):
            meta[:, M_SYM] = _pl_sym_id(symbol)
            _pl["chunks"].append((meta, X))
            _pl["session_samples"] += len(meta)
    return len(meta)


def _pl_downsample(meta, X, cap, rng):
    pos = meta[:, M_POS] > 0.5
    ip, ineg = np.flatnonzero(pos), np.flatnonzero(~pos)
    keep_p = min(len(ip), int(cap * 0.6))
    keep_n = min(len(ineg), cap - keep_p)
    sel_p = np.sort(rng.choice(ip, keep_p, replace=False)) if keep_p < len(ip) else ip
    sel_n = np.sort(rng.choice(ineg, keep_n, replace=False)) if keep_n < len(ineg) else ineg
    meta = meta.copy()
    if keep_p and keep_p < len(ip):
        meta[sel_p, M_W] *= len(ip) / float(keep_p)
    if keep_n and keep_n < len(ineg):
        meta[sel_n, M_W] *= len(ineg) / float(keep_n)
    sel = np.sort(np.concatenate([sel_p, sel_n]))
    return meta[sel], X[sel]


def _pl_consolidate(force=False, S=None):
    S = S or _pl
    with S["lock"]:
        if not S["chunks"] and not force:
            return
        if len(S["chunks"]) < 60 and not force:
            return
        pm = ([S["meta"]] if S["meta"] is not None else []) + [ch[0] for ch in S["chunks"]]
        px = ([S["X"]] if S["X"] is not None else []) + [ch[1] for ch in S["chunks"]]
        S["chunks"] = []
        if not pm:
            return
        meta = np.concatenate(pm); X = np.concatenate(px)
        if len(meta) > S["max_samples"]:
            meta, X = _pl_downsample(meta, X, S["max_samples"], np.random.default_rng(int(time.time()) % 100000))
        S["meta"], S["X"] = meta, X


def _pl_jsonable(o):
    if isinstance(o, dict):
        return {str(k): _pl_jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_pl_jsonable(v) for v in o]
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    return o


def _pl_save(force=False, S=None):
    S = S or _pl
    try:
        _pl_consolidate(force=True, S=S)
        with S["lock"]:
            meta, X = S["meta"], S["X"]
            blob = _pl_jsonable({"models": S["models"], "rules": S["rules"], "history": S["history"][-60:],
                                 "sym_last": S["sym_last"], "sym_names": S["sym_names"], "sym_fetch": S["sym_fetch"],
                                 "last_feat": S["last_feat"], "ladder": S.get("ladder") or {}, "symbols_ingested": S["symbols_ingested"],
                                 "sessions": S["sessions"], "last_train_ts": S["last_train_ts"], "saved_ts": time.time()})
        os.makedirs(os.path.dirname(S["json"]) or ".", exist_ok=True)
        if meta is not None and X is not None and len(meta):
            tmp = S["npz"] + ".tmp.npz"
            np.savez_compressed(tmp, meta=meta, X=X)
            os.replace(tmp, S["npz"])
        tmpj = S["json"] + ".tmp"
        with open(tmpj, "w", encoding="utf-8") as f:
            json.dump(blob, f, separators=(",", ":"))
        os.replace(tmpj, S["json"])
        S["last_save"] = time.time()
        return True
    except Exception as error:
        logger.warning(f"[Patterns/{S['name']}] save failed: {error}")
        return False


def _pl_load(S=None):
    S = S or _pl
    try:
        if os.path.exists(S["json"]):
            with open(S["json"], "r", encoding="utf-8") as f:
                data = json.load(f)
            with S["lock"]:
                S["models"] = data.get("models") or {}
                for mdl in S["models"].values():
                    mdl["params"] = {k: np.asarray(v, dtype=np.float32) for k, v in (mdl.get("params") or {}).items()}
                S["rules"] = data.get("rules") or {}
                S["history"] = data.get("history") or []
                S["sym_last"] = {k: int(v) for k, v in (data.get("sym_last") or {}).items()}
                S["sym_fetch"] = {k: float(v) for k, v in (data.get("sym_fetch") or {}).items()}
                S["last_feat"] = data.get("last_feat") or {}
                S["sym_names"] = list(data.get("sym_names") or [])
                S["sym_idx"] = {x: i for i, x in enumerate(S["sym_names"])}
                S["symbols_ingested"] = int(data.get("symbols_ingested", 0))
                S["sessions"] = int(data.get("sessions", 0))
                S["last_train_ts"] = float(data.get("last_train_ts", 0.0))
                S["ladder"] = data.get("ladder") or {}
        if os.path.exists(S["npz"]):
            z = np.load(S["npz"])
            with S["lock"]:
                S["meta"], S["X"] = z["meta"], z["X"]
        n = 0 if S["meta"] is None else len(S["meta"])
        logger.info(f"[Patterns/{S['name']}] loaded: samples={n} symbols={len(S['sym_names'])} models={len(S['models'])}")
    except Exception as error:
        logger.warning(f"[Patterns/{S['name']}] load failed (starting fresh): {error}")


# ---------------- النموذج: شبكة صغيرة بـ numpy ----------------
def _pl_sigmoid(z):
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30.0, 30.0)))


def _pl_prep(Xraw, mu, sd, slog=None):
    slog = _PL_SLOG_IDX if slog is None else np.asarray(slog, dtype=int)
    Z = np.array(Xraw, dtype=np.float32, copy=True)
    if len(slog):
        sub = Z[:, slog]
        Z[:, slog] = np.sign(sub) * np.log1p(np.abs(sub))
    Z = (Z - np.asarray(mu, dtype=np.float32)) / np.asarray(sd, dtype=np.float32)
    np.clip(Z, -6.0, 6.0, out=Z)
    return np.nan_to_num(Z, nan=0.0, posinf=0.0, neginf=0.0)


def _pl_fit_scaler(Xraw, w, slog=None):
    slog = _PL_SLOG_IDX if slog is None else np.asarray(slog, dtype=int)
    Z = np.array(Xraw, dtype=np.float32, copy=True)
    F = Z.shape[1]
    if len(slog):
        sub = Z[:, slog]
        Z[:, slog] = np.sign(sub) * np.log1p(np.abs(sub))
    mu = np.zeros(F, dtype=np.float32); sd = np.ones(F, dtype=np.float32)
    ww = np.asarray(w, dtype=np.float64)
    for j in range(F):
        col = Z[:, j].astype(np.float64)
        ok = np.isfinite(col)
        if ok.sum() < 10:
            continue
        wj = ww[ok]; cj = col[ok]
        m = float((wj * cj).sum() / wj.sum())
        s = float(np.sqrt((wj * (cj - m) ** 2).sum() / wj.sum()))
        mu[j] = m; sd[j] = s if s > 1e-6 else 1.0
    return mu, sd


def _pl_forward(p, Z):
    if "w" in p:
        return _pl_sigmoid(Z @ p["w"] + p["b"][0]), None
    A = np.tanh(Z @ p["W1"] + p["b1"])
    return _pl_sigmoid(A @ p["W2"] + p["b2"][0]), A


def _pl_init_params(F, h, rng):
    if h == 0:
        return {"w": np.zeros(F, dtype=np.float32), "b": np.zeros(1, dtype=np.float32)}
    return {"W1": (rng.normal(0, 1.0 / np.sqrt(F), (F, h))).astype(np.float32), "b1": np.zeros(h, dtype=np.float32),
            "W2": (rng.normal(0, 1.0 / np.sqrt(h), h)).astype(np.float32), "b2": np.zeros(1, dtype=np.float32)}


def _pl_wap(score, y, w):
    """متوسط الدقة الموزون (AP) — الأوزان تعيد العينة لحجمها الحقيقي بالسوق."""
    y = np.asarray(y, dtype=np.float64); w = np.asarray(w, dtype=np.float64)
    if y.sum() <= 0 or len(y) == 0:
        return 0.0
    o = np.argsort(-score, kind="stable")
    yy, ww = y[o], w[o]
    tp = np.cumsum(ww * yy); aw = np.cumsum(ww)
    prec = tp / np.maximum(aw, 1e-12)
    wp = ww * yy
    return float((prec * wp).sum() / max(wp.sum(), 1e-12))


def _pl_fit(Ztr, ytr, wfit, Zva, yva, wva, g, seed, stop_fn):
    rng = np.random.default_rng(seed)
    F = Ztr.shape[1]
    p = _pl_init_params(F, int(g["h"]), rng)
    mask = np.ones(F, dtype=np.float32)
    for j in g.get("drop", []):
        if 0 <= int(j) < F:
            mask[int(j)] = 0.0
    mo = {k: np.zeros_like(v) for k, v in p.items()}; ve = {k: np.zeros_like(v) for k, v in p.items()}
    lr = float(g["lr"]); l2 = float(g["l2"])
    N = len(ytr); bs = int(max(256, min(2048, N // 40))); t = 0          # ≥40 خطوة بالحقبة حتى لو العيّنات قليلة
    best, best_ap, bad = None, -1.0, 0
    Zva_m = Zva * mask
    for _ in range(PL_EPOCHS):
        if stop_fn():
            return None, -1.0
        perm = rng.permutation(N)
        for s in range(0, N, bs):
            ib = perm[s:s + bs]
            Xb = Ztr[ib] * mask; yb = ytr[ib]; wb = wfit[ib]
            pr, A = _pl_forward(p, Xb)
            dz = ((pr - yb) * wb / max(float(wb.sum()), 1e-9)).astype(np.float32)
            if A is None:
                gr = {"w": Xb.T @ dz + l2 * p["w"], "b": np.array([dz.sum()], dtype=np.float32)}
            else:
                dA = np.outer(dz, p["W2"]) * (1.0 - A * A)
                gr = {"W2": A.T @ dz + l2 * p["W2"], "b2": np.array([dz.sum()], dtype=np.float32),
                      "W1": Xb.T @ dA + l2 * p["W1"], "b1": dA.sum(axis=0)}
            t += 1
            for k in p:
                gk = gr[k].astype(np.float32)
                mo[k] = 0.9 * mo[k] + 0.1 * gk
                ve[k] = 0.999 * ve[k] + 0.001 * gk * gk
                p[k] = p[k] - lr * (mo[k] / (1 - 0.9 ** t)) / (np.sqrt(ve[k] / (1 - 0.999 ** t)) + 1e-8)
        pv, _a = _pl_forward(p, Zva_m)
        ap = _pl_wap(pv, yva, wva)
        if ap > best_ap + 1e-6:
            best_ap, best, bad = ap, {k: v.copy() for k, v in p.items()}, 0
        else:
            bad += 1
            if bad >= 6:
                break
    return best, best_ap


def _pl_predict(model, Xraw):
    Z = _pl_prep(Xraw, model["mu"], model["sd"], model.get("slog"))
    for j in model["g"].get("drop", []):
        Z[:, int(j)] = 0.0
    return _pl_forward(model["params"], Z)[0]


# ---------------- الجينوم (إعدادات النموذج) والتطوّر ----------------
_PL_OPTS = {"h": (0, 8, 16, 32), "l2": (1e-5, 1e-4, 1e-3, 1e-2), "lr": (0.003, 0.01, 0.02), "wpow": (0.0, 0.25, 0.5)}


def _pl_default_genome():
    return {"h": 16, "l2": 1e-4, "lr": 0.01, "wpow": 0.0, "drop": []}


def _pl_mutate(g, rng, F=None):
    F = F or _PL_F
    c = dict(g); c["drop"] = list(g.get("drop", []))
    for _ in range(int(rng.integers(1, 3))):
        what = str(rng.choice(["h", "l2", "lr", "wpow", "drop"]))
        if what == "drop":
            j = int(rng.integers(0, F))
            if j in c["drop"]:
                c["drop"].remove(j)
            elif len(c["drop"]) < 8:
                c["drop"].append(j)
        else:
            opts = _PL_OPTS[what]
            val = opts[int(rng.integers(0, len(opts)))]
            c[what] = int(val) if what == "h" else float(val)
    return c


def _pl_levels(score, y, w, fracs=(0.005, 0.01, 0.02, 0.05)):
    """أعلى X% من لحظات السوق (موزونة): الدقة، المضاعف مقابل المعدل العام، وكم انفجار انلقط."""
    w = np.asarray(w, dtype=np.float64); y = np.asarray(y, dtype=np.float64)
    tot, pos_w = float(w.sum()), float((w * y).sum())
    base = pos_w / tot if tot > 0 else 0.0
    o = np.argsort(-score, kind="stable")
    cw = np.cumsum(w[o]); cy = np.cumsum((w * y)[o]); cn = np.cumsum(y[o])
    out = []
    for fr in fracs:
        k = int(min(len(o) - 1, np.searchsorted(cw, fr * tot)))
        prec = float(cy[k] / max(cw[k], 1e-12))
        out.append({"top": fr, "thr": float(score[o[k]]), "prec": prec, "lift": (prec / base) if base > 0 else 0.0,
                    "hits": int(cn[k]), "recall": float(cy[k] / pos_w) if pos_w > 0 else 0.0})
    return out, base, int(y.sum())


def _pl_hold_metrics(model, Xh, yh, wh):
    sc = _pl_predict(model, Xh)
    lv, base, npos = _pl_levels(sc, yh, wh)
    return {"base": base, "ap": _pl_wap(sc, yh, wh), "npos": npos, "n": int(len(yh)), "levels": lv}


# ---------------- تنقيب القواعد المقروءة ----------------
def _pl_wquantiles(col, w, qs):
    ok = np.isfinite(col)
    if ok.sum() < 20:
        return []
    c, ww = col[ok], w[ok]
    o = np.argsort(c)
    cw = np.cumsum(ww[o]); cw /= cw[-1]
    return sorted(set(float(c[o][min(len(o) - 1, int(np.searchsorted(cw, q)))]) for q in qs))


def _pl_cond_mask(col, op, thr):
    with np.errstate(invalid="ignore"):
        return (col >= thr) if op == ">=" else (col <= thr)


def _pl_mine_rules(Xf, yf, wf, Xh, yh, wh, stop_fn, min_pos=8, beam=6, depth=3, top=4, feats=None):
    feats = feats or PL_FEATS
    F = Xf.shape[1]
    """بحث شعاعي عن شروط (حتى 3) ترفع احتمال الانفجار؛ التقييم على بيانات الحجز."""
    base = float((wf * yf).sum() / max(wf.sum(), 1e-12))
    if base <= 0:
        return []
    prior = 3.0 / base                                   # كأن عندنا 3 انفجارات بالمعدل العام (يكبح القواعد الصغيرة)
    conds = []
    for j in range(F):
        for thr in _pl_wquantiles(Xf[:, j], wf, (0.1, 0.25, 0.4, 0.6, 0.75, 0.9, 0.97)):
            conds.append((j, ">=", thr)); conds.append((j, "<=", thr))
    def score(idx):
        if len(idx) == 0:
            return -1.0, 0
        tw = float(wf[idx].sum()); pw = float((wf[idx] * yf[idx]).sum()); npos = int(yf[idx].sum())
        return (pw + 3.0) / (tw + prior), npos
    frontier = [((), np.arange(len(yf)))]
    found = {}
    for _lvl in range(depth):
        if stop_fn():
            return []
        nxt = []
        for rule, idx in frontier:
            used = {r[0] for r in rule}
            Xi = Xf[idx]
            for cnd in conds:
                if cnd[0] in used:
                    continue
                sub = idx[_pl_cond_mask(Xi[:, cnd[0]], cnd[1], cnd[2])]
                sc, npos = score(sub)
                if npos < min_pos:
                    continue
                nxt.append((sc, rule + (cnd,), sub, npos))
        nxt.sort(key=lambda t: -t[0])
        frontier = [(r, i) for _s, r, i, _n in nxt[:beam]]
        for sc, r, _i, npos in nxt[:beam * 2]:
            key = tuple(sorted((x[0], x[1]) for x in r))
            if key not in found or found[key][0] < sc:
                found[key] = (sc, r, npos)
        if not frontier:
            break
    ranked = sorted(found.values(), key=lambda t: -t[0])
    out, seen_feats = [], []
    for sc, r, npos in ranked:
        fs = {x[0] for x in r}
        if any(len(fs & s) >= max(1, len(fs) - 0) and len(fs) <= len(s) for s in seen_feats):
            continue
        seen_feats.append(fs)
        mask_h = np.ones(len(yh), dtype=bool)
        for (j, op, thr) in r:
            mask_h &= _pl_cond_mask(Xh[:, j], op, thr)
        tw = float(wh[mask_h].sum()); pw = float((wh[mask_h] * yh[mask_h]).sum())
        base_h = float((wh * yh).sum() / max(wh.sum(), 1e-12))
        prec_h = pw / tw if tw > 0 else 0.0
        out.append({"conds": [(feats[j], op, float(thr)) for (j, op, thr) in r],
                    "fit_prec": float(sc), "fit_pos": int(npos), "fit_lift": float(sc / base),
                    "hold_prec": prec_h, "hold_lift": (prec_h / base_h) if base_h > 0 else 0.0,
                    "hold_hits": int(yh[mask_h].sum()), "hold_n": int(mask_h.sum()), "hold_pos_total": int(yh.sum()),
                    "base": base})
        if len(out) >= top:
            break
    return out


# ---------------- جلسة التدريب والتطوّر ----------------
def _pl_headroom_mb(S=None):
    """الهامش المطلوب للتدريب يتناسب مع حجم العينات الفعلي (مو رقم ثابت 100MB): ~4 نسخ من المصفوفة + هامش للقواعد."""
    S = S or _pl
    try:
        with S["lock"]:
            X = S.get("X")
            nb = X.nbytes if X is not None else 0
        est = 4.0 * nb / 1048576 + 25.0
        return int(min(PL_MEM_HEADROOM_MB, max(40.0, est)))
    except Exception:
        return PL_MEM_HEADROOM_MB


def _pl_mem_ok(S=None):
    try:
        if psutil is None:
            return True
        proc = psutil.Process(os.getpid())
        need = _pl_headroom_mb(S)
        rss = proc.memory_info().rss / (1024 * 1024)
        if rss + need >= MEMORY_LIMIT_MB:
            _trim_memory()                                     # جرّب ترجّع ذاكرة فاضية قبل ما ترفض التدريب
            rss = proc.memory_info().rss / (1024 * 1024)
        if rss + need >= MEMORY_LIMIT_MB:
            logger.warning(f"[Patterns] skip training: RSS={rss:.0f}MB + headroom {need} >= limit {MEMORY_LIMIT_MB}MB (ارفع MEMORY_LIMIT_MB بـRailway أو قلّل PL_MAX_SAMPLES)")
            return False
    except Exception:
        pass
    return True


def _pl_stop():
    return (not OFFLINE_ENABLED) or (not PL_ENABLED) or (not _offline_window_open())


def _pl_compute_ladder(X, M, i2, S=None):
    """سلّم الارتفاع على بيانات الحجز (أحدث 20% ما شافها التدريب): من أعلى 1%/2% لحظات بنموذج الحد الأدنى،
    كم وصلت فعلاً لكل مستوى (+50 … +1200%)؟ دقة، مضاعف مقابل المعدل العام، وكم انفجار انلقط من كل الموجودين."""
    S = S or _pl
    Xh, Mh = X[i2:], M[i2:]
    if len(Xh) < 200:
        return
    gd = Mh[:, M_GD]
    valid = np.isfinite(gd)
    if valid.sum() < 200:
        return
    out = {}
    for tid in S["ladder_tids"]:
        model = S["models"].get(tid)
        if not model:
            continue
        sc = _pl_predict(model, Xh[valid])
        w = Mh[valid, M_W]; g = gd[valid]
        rows = []
        for thr in PL_LADDER:
            y = (g >= thr).astype(np.float64)
            lv, base, npos = _pl_levels(sc, y, w, fracs=(0.01, 0.02))
            rows.append({"thr": thr, "npos": int(npos), "base": float(base), "l1": lv[0], "l2": lv[1]})
        out[tid] = rows
    with S["lock"]:
        S["ladder"] = out


def _pl_train_all(force=False, S=None):
    S = S or _pl
    if not _pl_train_lock.acquire(blocking=False):
        S["since_train"] = S["train_every"]                    # نعيد المحاولة بعد السهم القادم
        return
    t0 = time.time(); tag = S["name"]
    try:
        S["training"] = True
        stop_fn = (lambda: False) if force else _pl_stop
        if not force and _pl_stop():
            return
        if not _pl_mem_ok(S):
            return
        _pl_consolidate(force=True, S=S)
        with S["lock"]:
            X, M = S["X"], S["meta"]
        if X is None or M is None or len(X) < 3000:
            logger.info(f"[Patterns/{tag}] تدريب مؤجّل: العينات قليلة")
            return
        order = np.argsort(M[:, M_TS], kind="stable")
        X = X[order]; M = M[order]
        with S["lock"]:
            S["X"], S["meta"] = X, M                          # نخزّن المرتّب (بدون نسخة مكررة)
        n = len(X); i1, i2 = int(n * 0.6), int(n * 0.8)
        mu, sd = _pl_fit_scaler(X[:i1], M[:i1, M_W], S["slog"])
        Z = _pl_prep(X, mu, sd, S["slog"])
        promoted_any = 0
        for tid, col, thr, label in S["targets"]:
            if stop_fn():
                logger.info(f"[Patterns/{tag}] أوقفت التدريب: السوق قرّب يفتح")
                break
            valid = np.isfinite(M[:, col])
            y_all = (np.nan_to_num(M[:, col], nan=-1.0) >= thr).astype(np.float32)
            idx_tr = np.flatnonzero(valid[:i1]); idx_va = i1 + np.flatnonzero(valid[i1:i2]); idx_ho = i2 + np.flatnonzero(valid[i2:])
            tr_pos, va_pos = int(y_all[idx_tr].sum()), int(y_all[idx_va].sum())
            if tr_pos < PL_MIN_TRAIN_POS or va_pos < 5:
                logger.info(f"[Patterns/{tag}] {tid}: حالات انفجار قليلة (تدريب {tr_pos}/اختيار {va_pos}) — ننتظر مزيد من الأسهم")
                continue
            Ztr, ytr = Z[idx_tr], y_all[idx_tr]; wtr = M[idx_tr, M_W]
            Zva, yva, wva = Z[idx_va], y_all[idx_va], M[idx_va, M_W]
            rng = np.random.default_rng(int(time.time()) % 1000003 + _zlib.crc32((tag + tid).encode()) % 9973)
            old = S["models"].get(tid)
            old_ap = None
            if old:
                try:
                    old_ap = _pl_wap(_pl_predict(old, X[idx_va]), yva, wva)
                except Exception:
                    old = None
            base_g = dict(old["g"]) if old else _pl_default_genome()
            cands = [base_g] + [_pl_mutate(base_g, rng, X.shape[1]) for _ in range(PL_CHILDREN)]
            best = None
            for g in cands:
                wfit = (wtr ** float(g["wpow"])).astype(np.float32)
                params, ap = _pl_fit(Ztr, ytr, wfit, Zva, yva, wva, g, int(rng.integers(1, 10 ** 6)), stop_fn)
                if params is None:
                    continue
                if best is None or ap > best[0]:
                    best = (ap, params, g)
            if best is None:
                continue
            ap, params, g = best
            promote = (old is None) or (old_ap is None) or (ap > old_ap * 1.03)
            gen = (int(old.get("gen", 0)) + (1 if promote else 0)) if old else 1
            if promote:
                model = {"g": g, "params": params, "mu": mu, "sd": sd, "slog": S["slog"].tolist(), "val_ap": float(ap), "gen": gen,
                         "n_train": int(len(idx_tr)), "pos_train": tr_pos, "trained_ts": time.time()}
                promoted_any += 1
            else:
                model = old; model["val_ap"] = float(old_ap)
            hold = _pl_hold_metrics(model, X[idx_ho], y_all[idx_ho], M[idx_ho, M_W]) if len(idx_ho) > 20 else None
            model["hold"] = hold
            rules = []
            if not stop_fn():
                idx_fit = np.concatenate([idx_tr, idx_va])
                rules = _pl_mine_rules(X[idx_fit], y_all[idx_fit], M[idx_fit, M_W], X[idx_ho], y_all[idx_ho], M[idx_ho, M_W],
                                       stop_fn, feats=S["feats"])
            with S["lock"]:
                S["models"][tid] = model
                if rules:
                    S["rules"][tid] = rules
                S["history"].append({"ts": time.time(), "tid": tid, "gen": gen, "promoted": bool(promote), "val_ap": float(ap),
                                     "old_val_ap": (float(old_ap) if old_ap is not None else None),
                                     "hold_lift1": (hold["levels"][1]["lift"] if hold else None)})
            l1 = hold["levels"][1] if hold else None
            logger.info(f"🧬 [Patterns/{tag}] {tid}: جيل {gen} {'⬆ ترقية' if promote else '= بقي'} | val AP {ap:.4f}"
                        + (f" | حجز: أعلى1% دقة {l1['prec'] * 100:.1f}% (×{l1['lift']:.1f}) التقط {l1['hits']}/{hold['npos']}" if l1 else ""))
        try:
            _pl_compute_ladder(X, M, i2, S)
        except Exception as error:
            logger.warning(f"[Patterns/{tag}] ladder error: {error}")
        with S["lock"]:
            S["last_train_ts"] = time.time(); S["sessions"] += 1
        _pl_save(S=S)
        del Z
        _trim_memory()
        logger.info(f"🧬 [Patterns/{tag}] جلسة تدريب انتهت بـ {time.time() - t0:.0f}ث | ترقيات: {promoted_any}")
    except Exception as error:
        logger.error(f"[Patterns/{S['name']}] train error: {error}")
    finally:
        S["training"] = False
        _pl_train_lock.release()


def _pl_after_symbol():
    """يُستدعى بعد كل سهم بخيط الأوفلاين: توحيد/حفظ/إطلاق جلسة تدريب بخيط مستقل (ما يعطّل السحب)."""
    if not PL_ENABLED:
        return
    with _pl_lock:
        _pl["symbols_ingested"] += 1; _pl["since_train"] += 1
        cnt = _pl["symbols_ingested"]
    if cnt % 25 == 0:
        _pl_consolidate()
    if cnt % PL_SAVE_EVERY == 0:
        _pl_save()
    if (_pl["since_train"] >= PL_TRAIN_EVERY and not _pl["training"]
            and time.time() - _pl["last_train_ts"] >= PL_TRAIN_MIN_GAP_SEC):
        _pl["since_train"] = 0
        threading.Thread(target=_pl_train_all, daemon=True, name="PatternTrainer").start()


# ---------------- التقارير والأوامر ----------------
def _pl_rule_text(rule, feat_ar=None):
    feat_ar = feat_ar or PL_FEAT_AR
    parts = []
    for (name, op, thr) in rule["conds"]:
        parts.append(f"{feat_ar.get(name, name)} {'≥' if op == '>=' else '≤'} {thr:.3g}")
    return " و ".join(parts)


def _pl_ladder_text(meta, short=False, S=None):
    """سلّم الارتفاع: كم مثال عندنا لكل مستوى، وكم من الأمثلة اللي بالحجز التقطها النموذج."""
    S = S or _pl
    lines = []
    if meta is not None and len(meta):
        gd = meta[:, M_GD]
        counts = []
        for thr in PL_LADDER[1:]:
            sel = np.isfinite(gd) & (gd >= thr)
            counts.append(f"+{thr}%: {int(sel.sum())} مثال/{len(np.unique(meta[sel, M_SYM]))} سهم")
        lines.append(f"\n📈 *الأمثلة الموجودة بالبيانات (أعلى ارتفاع خلال {S['horizon']}):* " + " | ".join(counts))
    with S["lock"]:
        ladder = dict(S.get("ladder") or {})
    tid = next((t for t in S["ladder_tids"] if ladder.get(t)), None)
    if tid:
        lab = next((l for t, _c, _th, l in S["targets"] if t == tid), tid)
        lines.append(f"\n🪜 *سلّم الارتفاع* — أعلى 1% لحظات بنموذج {lab} (على بيانات ما شافها):")
        for r in ladder[tid]:
            if short and r["thr"] not in (50, 100, 500, 1200):
                continue
            l1 = r["l1"]
            if r["npos"] < PL_LADDER_MIN_POS:
                lines.append(f"  +{r['thr']}%: ⚠️ {r['npos']} مثال فقط بالحجز — ما يكفي للحكم (التقط {l1['hits']})")
            else:
                lines.append(f"  +{r['thr']}%: دقة {l1['prec'] * 100:.2f}% مقابل {r['base'] * 100:.2f}% عام (×{l1['lift']:.1f}) | التقط {l1['hits']} من {r['npos']}")
    return lines


def _pl_report_text(short=False, S=None):
    S = S or _pl
    with S["lock"]:
        meta = S["meta"]; models = dict(S["models"]); rules = dict(S["rules"])
        nsym = len(S["sym_names"]); sess = S["sessions"]; pending = sum(len(c[0]) for c in S["chunks"])
    n = (0 if meta is None else len(meta)) + pending
    npos = int((meta[:, M_POS] > 0.5).sum()) if meta is not None else 0
    lines = [S["title"], "━━━━━━━━━━━━━━━━",
             f"الحالة: {'🟢 يتدرّب الحين' if S['training'] else ('⏳ يجمع عينات' if _offline.get('active') else '⏸️ ينتظر إغلاق السوق')}",
             f"عينات: {n:,} (انفجارات فعلية: {npos:,}) من {nsym:,} سهم | جلسات تطوّر: {sess}"]
    if not models:
        lines.append("لسه ما تدرّب نموذج — يحتاج عدة مئات أسهم وحالات انفجار كافية.")
    lines.extend(_pl_ladder_text(meta, short=short, S=S))     # السلّم أولاً: لو النص انقصّ بتيليجرام، ينقص من تفاصيل القواعد مو منه
    waiting = []
    for tid, _col, _thr, label in S["targets"]:
        m = models.get(tid)
        if not m:
            waiting.append(label.replace(" خلال يوم", "").replace(" خلال ساعة", "/س"))
            continue
        hold = m.get("hold") or {}
        lv = hold.get("levels") or []
        lines.append(f"\n• *{label}* — جيل {m.get('gen', 1)}")
        if lv:
            l1, l2 = lv[1], lv[2]
            verdict = "✅ ثابت بالتحقق" if (l1["lift"] >= 3 and l1["hits"] >= 5) else "⚠️ لم يثبت بعد"
            lines.append(f"  أحدث 20% (ما شافها): أعلى 1% لحظات → دقة {l1['prec'] * 100:.1f}% مقابل معدل عام {hold['base'] * 100:.2f}% (×{l1['lift']:.1f}) | التقط {l1['hits']} من {hold['npos']} انفجار — {verdict}")
            if not short:
                lines.append(f"  أعلى 2%: دقة {l2['prec'] * 100:.1f}% (×{l2['lift']:.1f}) | التقط {l2['hits']}")
        shown = 0
        for r in (rules.get(tid) or []):
            if r.get("hold_lift", 0) < 2 or r.get("hold_hits", 0) < 3:       # نخفي القواعد اللي ما ثبتت بالتحقق
                continue
            lines.append(f"  نمط: {_pl_rule_text(r, S['feat_ar'])}\n    تحقق: ×{r['hold_lift']:.1f} على {r['hold_n']} لحظة، التقط {r['hold_hits']} من {r['hold_pos_total']}")
            shown += 1
            if shown >= (1 if short else 2):
                break
    if waiting:
        lines.append(f"\n⏳ بانتظار أمثلة كافية للتدريب المستقل: {' · '.join(waiting)}")
    lines.append("\n" + S["footer"])
    return "\n".join(lines)[:3990]


def _pl_level_text(model, score):
    lv = (model.get("hold") or {}).get("levels") or []
    best = None
    for L in lv:
        if score >= L["thr"]:
            best = L; break
    if best is None:
        return "عادي (تحت أعلى 5% تاريخياً)"
    return f"ضمن أعلى {best['top'] * 100:.1f}% تاريخياً — دقة تاريخية ~{best['prec'] * 100:.1f}% (×{best['lift']:.1f})"


def _pl_score_symbols(symbols):
    out = []
    models = {tid: _pl["models"].get(tid) for tid in ("h1_25", "d1_25", "d1_50", "d1_80", "d1_100", "d1_170")}
    for sym in symbols:
        try:
            lines = [f"• *{sym}*"]
            try:
                dl = _dl_score_symbol(sym)
                if dl:
                    lines.append(dl)
            except Exception as error:
                lines.append(f"  📅 يومي: خطأ {error}")
            if any(models.values()):
                df = fetch_from_yahoo(sym, "30d", "5m", prepost=True)
                P = _pl_prepare(df)
                if P is not None:
                    x = P["X"][-1:]
                    r12 = float(P["r12"][-1]) if np.isfinite(P["r12"][-1]) else 0.0
                    lines.append(f"  ⏱️ شموع 5د (${P['c'][-1]:.4f} | آخر ساعة {r12:+.1f}%):")
                    for tid, lab in (("h1_25", "+25% خلال ساعة"), ("d1_25", "+25% خلال يوم"), ("d1_50", "+50% خلال يوم"),
                                     ("d1_80", "+80% خلال يوم"), ("d1_100", "+100% خلال يوم"), ("d1_170", "+170% خلال يوم")):
                        m = models.get(tid)
                        if m:
                            lines.append(f"   {lab}: {_pl_level_text(m, float(_pl_predict(m, x)[0]))}")
                    if r12 >= PL_MAX_PRIOR_MOVE:
                        lines.append("   تنبيه: السهم تحرّك فعلاً بالساعة الأخيرة، والنموذج تدرّب على اللحظات قبل الحركة.")
            out.append("\n".join(lines))
        except Exception as error:
            out.append(f"• {sym}: خطأ {error}")
    return "🧬 *تقييم النمط الحالي*\n━━━━━━━━━━━━━━━━\n" + "\n".join(out)


def _pl_score_send(symbols):
    try:
        send_telegram(_pl_score_symbols(symbols))
    except Exception as error:
        send_telegram(f"⚠️ ما قدرت أقيّم: {error}")


def _pl_session_end():
    try:
        sent_any = False
        if _dl["session_samples"] >= 100 or _dl["models"]:
            _pl_save(S=_dl)
            send_telegram(_pl_report_text(short=True, S=_dl))
            if _dl["models"]:
                send_telegram(_dl_setups_text(10))
            sent_any = True
        if _pl["session_samples"] >= 100:
            _pl_save(S=_pl)
            send_telegram(_pl_report_text(short=True, S=_pl))
        _pl["session_samples"] = 0; _dl["session_samples"] = 0
    except Exception as error:
        logger.debug(f"[Patterns] session end: {error}")


def _pl_all_reports():
    return [_pl_report_text(short=False, S=_dl), _pl_report_text(short=True, S=_pl)]


# ================================================================
# 📅 DAILY STRUCTURE LEARNER — يتعلّم "بنية" السهم قبل الانفجار على الشارت اليومي (سنوات، مو 55 يوم)
# ----------------------------------------------------------------
# نفس فكرة صايد CYCU: سهم نزل بقوة من قمة سابقة، وصل لدعم ارتدادي (خط صاعد تحت القيعان)، تضيّق وجفّ حجمه، ثم انفجر.
# الأنماط هذي ما تبان بشموع 5 دقائق، فهنا نقرأ الشارت اليومي ٣ سنوات لكل سهم ونحسب 32 خاصية بنيوية:
#   النزول عن قمة 60/120 يوم، البعد عن خط الدعم الصاعد وعدد لمساته، البعد عن قاع 60/120، الانضغاط، جفاف الحجم،
#   عمر وحجم آخر قفزة، قيعان أعلى، ذيل سفلي (ارتداد)، RSI، متوسطات…  (كلها من الماضي، فحص تسريب مستقبل ناجح بالاختبار)
# النتيجة المتعلَّمة: هل ارتفع السهم +25%/+50% في اليوم التالي أو +50%/+100% خلال 3 أيام؟
# نفس آلية التعلّم والتطوّر والتحقق (60/20/20 زمنياً) + أمر /picks يرتّب أسهم السوق الحالية بالنموذج (قائمة الاثنين).
# ================================================================
DL_ENABLED = os.getenv("DL_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
DL_PERIOD = os.getenv("DL_PERIOD", "3y")
DL_MAX_SAMPLES = int(os.getenv("DL_MAX_SAMPLES", "90000"))
DL_NEG_KEEP = float(os.getenv("DL_NEG_KEEP", "0.01"))
DL_HARD_KEEP = float(os.getenv("DL_HARD_KEEP", "0.15"))
DL_MAX_TODAY = float(os.getenv("DL_MAX_TODAY", "25"))              # نتجاهل اليوم اللي السهم ارتفع فيه فعلاً أكثر من هذا %
DL_MIN_VOL10 = float(os.getenv("DL_MIN_VOL10", "50000"))           # متوسط حجم 10 أيام (أسهم)
DL_MIN_LABEL_VOL = float(os.getenv("DL_MIN_LABEL_VOL", "20000"))   # يوم النتيجة لازم فيه حجم حقيقي
DL_REFETCH_DAYS = float(os.getenv("DL_REFETCH_DAYS", "5"))
DL_SAVE_EVERY = int(os.getenv("DL_SAVE_EVERY", "150"))
DL_TRAIN_EVERY = int(os.getenv("DL_TRAIN_EVERY", "300"))
DL_FILE_NPZ = os.path.join(_VOLUME_ROOT, "daily_pattern_samples.npz")
DL_FILE_JSON = os.path.join(_VOLUME_ROOT, "daily_pattern_models.json")

DL_FEATS = ["ret1", "ret5", "ret20", "ret60", "gap", "dd60", "dd120", "up60", "up120", "pos60", "tl_dist", "tl_slope",
            "tl_touch", "rng10", "squeeze", "vdry", "rvol", "vspike", "dvol", "spike120", "spike_age", "hl10", "green10",
            "clpos", "lwick", "rsi14", "sma20d", "sma50d", "logp", "downrun", "bigdn20", "range_today"]
DL_FEAT_AR = {
    "ret1": "تغير اليوم%", "ret5": "حركة 5 أيام%", "ret20": "حركة 20 يوم%", "ret60": "حركة 60 يوم%", "gap": "فجوة اليوم%",
    "dd60": "النزول عن قمة 60 يوم%", "dd120": "النزول عن قمة 120 يوم%", "up60": "فوق قاع 60 يوم%", "up120": "فوق قاع 120 يوم%",
    "pos60": "الموقع بنطاق 60 يوم", "tl_dist": "البعد عن خط الدعم الصاعد%", "tl_slope": "ميل خط الدعم %/يوم",
    "tl_touch": "لمسات خط الدعم", "rng10": "مدى 10 أيام%", "squeeze": "انضغاط التذبذب", "vdry": "جفاف الحجم",
    "rvol": "RVOL اليومي", "vspike": "طفرة حجم 5 أيام", "dvol": "سيولة (لوغ)", "spike120": "أكبر قفزة 120 يوم%",
    "spike_age": "عمر آخر قفزة (نسبة)", "hl10": "قيعان أعلى", "green10": "أيام خضراء", "clpos": "الإغلاق قرب القمة",
    "lwick": "ذيل سفلي (ارتداد)", "rsi14": "RSI", "sma20d": "البعد عن متوسط 20%", "sma50d": "البعد عن متوسط 50%",
    "logp": "السعر (لوغ)", "downrun": "أيام نزول متتالية", "bigdn20": "أكبر هبوط يوم/20 يوم%", "range_today": "مدى اليوم%"}
DL_SLOG = {"ret1", "ret5", "ret20", "ret60", "gap", "dd60", "dd120", "up60", "up120", "tl_dist", "rng10", "rvol", "vspike",
           "spike120", "sma20d", "sma50d", "bigdn20", "range_today"}
DL_TARGETS = [("n1_25", 3, 25.0, "+25% باليوم الجاي"), ("n1_50", 3, 50.0, "+50% باليوم الجاي"),
              ("n3_50", 4, 50.0, "+50% خلال 3 أيام"), ("n3_100", 4, 100.0, "+100% خلال 3 أيام")]
_DLI = {n: i for i, n in enumerate(DL_FEATS)}
_dl = _pl_new_state("daily", "📅 *تعلّم أنماط الدعم والانضغاط (شارت يومي — سنوات)*",
                    "ℹ️ يتعلّم من ~3 سنوات يومي لكل سهم، فعيّناته أكثر بكثير من نموذج 5د. الدقة التاريخية تقدير مو وعد، والقوائم تتغير بحسب آخر بيانات انسحبت.",
                    DL_FEATS, DL_SLOG, DL_TARGETS, DL_FEAT_AR, DL_FILE_NPZ, DL_FILE_JSON, DL_MAX_SAMPLES, DL_TRAIN_EVERY,
                    ladder_tids=("n1_25", "n3_50"), horizon="3 أيام")


def _dl_prepare(df):
    """شموع يومية → X (n × 32). كل خاصية عند يوم t تستخدم بيانات لحد إغلاق t فقط."""
    if df is None or df.empty or len(df) < 130:
        return None
    d = df[["open", "high", "low", "close", "volume"]].astype(float).dropna()
    d = d[(d["close"] > 0) & (d["high"] > 0) & (d["low"] > 0) & (d["high"] >= d["low"])]
    n = len(d)
    if n < 130:
        return None
    o = d["open"].to_numpy(); h = d["high"].to_numpy(); l = d["low"].to_numpy(); c = d["close"].to_numpy(); v = d["volume"].to_numpy()
    ts = ((d.index - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).to_numpy().astype("int64")
    S_ = pd.Series
    prevc = _pl_shift(c, 1)
    with np.errstate(divide="ignore", invalid="ignore"):
        def ret(k):
            return (c / _pl_shift(c, k) - 1.0) * 100.0
        hi60 = S_(h).rolling(60, min_periods=30).max().to_numpy(); lo60 = S_(l).rolling(60, min_periods=30).min().to_numpy()
        hi120 = S_(h).rolling(120, min_periods=60).max().to_numpy(); lo120 = S_(l).rolling(120, min_periods=60).min().to_numpy()
        dd60 = (c / hi60 - 1.0) * 100.0; dd120 = (c / hi120 - 1.0) * 100.0
        up60 = (c / lo60 - 1.0) * 100.0; up120 = (c / lo120 - 1.0) * 100.0
        pos60 = np.where(hi60 > lo60, (c - lo60) / (hi60 - lo60), np.nan)
        rng10 = (S_(h).rolling(10, min_periods=6).max().to_numpy() / S_(l).rolling(10, min_periods=6).min().to_numpy() - 1.0) * 100.0
        tr = np.maximum(h - l, np.maximum(np.abs(h - prevc), np.abs(l - prevc))) / c
        atr10 = S_(tr).rolling(10, min_periods=6).mean().to_numpy(); atr60 = S_(tr).rolling(60, min_periods=30).mean().to_numpy()
        squeeze = np.where(atr60 > 0, atr10 / atr60, np.nan)
        vs = S_(v)
        v5 = vs.rolling(5, min_periods=3).mean().to_numpy(); v60 = vs.rolling(60, min_periods=30).mean().to_numpy()
        vdry = np.where(v60 > 0, v5 / v60, np.nan)
        b20 = vs.shift(1).rolling(20, min_periods=10).mean().to_numpy()
        rvol = np.where(b20 > 0, v / b20, np.nan)
        med60 = vs.rolling(60, min_periods=30).median().to_numpy()
        vspike = vs.rolling(5, min_periods=3).max().to_numpy() / (med60 + 1.0)
        dvol = np.log10(1.0 + S_(c * v).rolling(10, min_periods=5).mean().to_numpy())
        upv = (h / prevc - 1.0) * 100.0
        spike120 = S_(upv).rolling(120, min_periods=60).max().to_numpy()
        hl10 = S_((l > _pl_shift(l, 1)).astype(float)).rolling(10, min_periods=6).mean().to_numpy()
        green10 = S_((c > o).astype(float)).rolling(10, min_periods=6).mean().to_numpy()
        rg = h - l
        clpos = np.where(rg > 0, (c - l) / rg, 0.5)
        lwick = np.where(rg > 0, (np.minimum(o, c) - l) / rg, 0.0)
        dlt = np.r_[np.nan, np.diff(c)]
        gn = S_(np.where(dlt > 0, dlt, 0.0)).rolling(14, min_periods=8).mean().to_numpy()
        ls = S_(np.where(dlt < 0, -dlt, 0.0)).rolling(14, min_periods=8).mean().to_numpy()
        rsi14 = np.where(ls > 0, 100.0 - 100.0 / (1.0 + gn / ls), np.where(gn > 0, 100.0, 50.0))
        sma20d = (c / S_(c).rolling(20, min_periods=12).mean().to_numpy() - 1.0) * 100.0
        sma50d = (c / S_(c).rolling(50, min_periods=30).mean().to_numpy() - 1.0) * 100.0
        bigdn20 = S_((c / prevc - 1.0) * 100.0).rolling(20, min_periods=10).min().to_numpy()
        range_today = (h - l) / c * 100.0
    # --- خط الدعم الصاعد من القيعان المؤكدة (قاع = أدنى سعر بنافذة 7 أيام، ويتأكد بعد 3 أيام) + عمر القفزة + أيام نزول ---
    lmin7 = S_(l).rolling(7, center=True, min_periods=7).min().to_numpy()
    piv_idx = np.flatnonzero(np.isfinite(lmin7) & (l <= lmin7 + 1e-12))
    ln_l = np.log(l)
    tl_dist = np.full(n, np.nan); tl_slope = np.full(n, np.nan); tl_touch = np.full(n, np.nan); spike_age = np.full(n, np.nan)
    downrun = np.zeros(n)
    for t in range(1, n):
        downrun[t] = min(10.0, downrun[t - 1] + 1.0) if c[t] < c[t - 1] else 0.0
    W = 120
    for t in range(60, n):
        a = int(np.searchsorted(piv_idx, t - W, "left")); b = int(np.searchsorted(piv_idx, t - 3, "right"))
        if b - a >= 3:
            px = piv_idx[a:b].astype(float); py = ln_l[piv_idx[a:b]]
            xm = px.mean(); ym = py.mean(); vx = ((px - xm) ** 2).sum()
            if vx > 0:
                bs = ((px - xm) * (py - ym)).sum() / vx; a0 = ym - bs * xm
                res = py - (a0 + bs * px); sh = res.min()
                tl_dist[t] = (c[t] / np.exp(a0 + bs * t + sh) - 1.0) * 100.0
                tl_slope[t] = (np.exp(bs) - 1.0) * 100.0
                tl_touch[t] = float(((res - sh) < 0.04).sum())
        seg = upv[max(0, t - 119):t + 1]
        if np.isfinite(seg).any():
            spike_age[t] = (len(seg) - 1 - int(np.nanargmax(seg))) / 120.0
    cols = {"ret1": ret(1), "ret5": ret(5), "ret20": ret(20), "ret60": ret(60), "gap": (o / prevc - 1.0) * 100.0,
            "dd60": dd60, "dd120": dd120, "up60": up60, "up120": up120, "pos60": pos60, "tl_dist": tl_dist,
            "tl_slope": tl_slope, "tl_touch": tl_touch, "rng10": rng10, "squeeze": squeeze, "vdry": vdry, "rvol": rvol,
            "vspike": vspike, "dvol": dvol, "spike120": spike120, "spike_age": spike_age, "hl10": hl10, "green10": green10,
            "clpos": clpos, "lwick": lwick, "rsi14": rsi14, "sma20d": sma20d, "sma50d": sma50d, "logp": np.log(c),
            "downrun": downrun, "bigdn20": bigdn20, "range_today": range_today}
    X = np.column_stack([cols[k] for k in DL_FEATS]).astype(np.float32)
    X[~np.isfinite(X)] = np.nan
    vol10 = vs.rolling(10, min_periods=5).mean().to_numpy()
    return {"n": n, "c": c, "h": h, "v": v, "ts": ts, "X": X, "vol10": vol10}


def _dl_row_list(x):
    return [None if not np.isfinite(val) else round(float(val), 4) for val in x]


def _dl_extract(symbol, df, since_ts, rng):
    """يرجع (meta, X, last_ts, lastrow). عيّنة = يوم t بنتيجة اليوم التالي/الثلاثة أيام التالية."""
    P = _dl_prepare(df)
    if P is None:
        return None
    n, c, h, v, ts, X, vol10 = P["n"], P["c"], P["h"], P["v"], P["ts"], P["X"], P["vol10"]
    nan_n = np.isnan(X).sum(axis=1)
    price_ok = (c >= DISCOVERY_PRICE_MIN) & (c <= DISCOVERY_PRICE_MAX) & (vol10 >= DL_MIN_VOL10)
    with np.errstate(invalid="ignore"):
        quiet_today = X[:, _DLI["ret1"]] < DL_MAX_TODAY
    base_ok = price_ok & (nan_n <= 5) & quiet_today & (np.arange(n) >= 100)
    lastrow = None
    if n >= 100 and price_ok[n - 1] and nan_n[n - 1] <= 5:
        lastrow = [int(ts[n - 1]), float(c[n - 1]), _dl_row_list(X[n - 1])]
    hv = np.where(v >= DL_MIN_LABEL_VOL, h, np.nan)
    n1 = np.r_[hv[1:], np.nan]; n2 = np.r_[hv[2:], np.nan, np.nan]; n3 = np.r_[hv[3:], np.nan, np.nan, np.nan]
    with np.errstate(invalid="ignore", divide="ignore"):
        g1 = (n1 / c - 1.0) * 100.0
        g3 = (np.fmax(np.fmax(n1, n2), n3) / c - 1.0) * 100.0
    g1[n - 1:] = np.nan
    g3[max(0, n - 3):] = np.nan
    last_ts = int(ts[n - 2]) if n >= 2 else 0
    ok = base_ok & (ts > since_ts) & (np.arange(n) <= n - 2)
    cand = np.flatnonzero(ok)
    empty = (np.zeros((0, 6)), np.zeros((0, len(DL_FEATS)), dtype=np.float32), last_ts, lastrow)
    if len(cand) == 0:
        return empty
    g1c, g3c, Xc = g1[cand], g3[cand], X[cand]
    with np.errstate(invalid="ignore"):
        glitch = (g1c > 5000.0) | (g3c > 5000.0)
        pos = (g1c >= 25.0) | (g3c >= 50.0)
        hard = ((np.nan_to_num(Xc[:, _DLI["dd60"]], nan=0.0) <= -30.0) & (np.nan_to_num(Xc[:, _DLI["up60"]], nan=999.0) <= 30.0)) \
            | (np.nan_to_num(Xc[:, _DLI["rvol"]], nan=0.0) >= 3.0)
    u = rng.random(len(cand))
    keep_pos = pos & ~glitch
    keep_hard = (~pos) & hard & (u < DL_HARD_KEEP) & ~glitch
    keep_neg = (~pos) & (~hard) & (u < DL_NEG_KEEP) & ~glitch
    keep = keep_pos | keep_hard | keep_neg
    if not keep.any():
        return empty
    w = np.where(keep_pos, 1.0, np.where(keep_hard, 1.0 / DL_HARD_KEEP, 1.0 / DL_NEG_KEEP))
    sel = np.flatnonzero(keep)
    meta = np.zeros((len(sel), 6), dtype=np.float64)
    meta[:, M_TS] = ts[cand[sel]]; meta[:, M_W] = w[sel]; meta[:, M_G1] = g1c[sel]; meta[:, M_GD] = g3c[sel]
    meta[:, M_POS] = pos[sel].astype(float)
    return meta, Xc[sel].astype(np.float32), last_ts, lastrow


def _dl_ingest(symbol, df):
    if not DL_ENABLED or not _pl_symbol_ok(symbol):
        return 0
    S = _dl
    since = int(S["sym_last"].get(symbol, 0))
    seed = (_zlib.crc32(symbol.encode()) ^ (int(S["symbols_ingested"]) * 2654435761)) & 0xFFFFFFFF
    out = _dl_extract(symbol, df, since, np.random.default_rng(seed))
    if out is None:
        return 0
    meta, X, last_ts, lastrow = out
    with S["lock"]:
        S["sym_last"][symbol] = max(since, last_ts)
        if lastrow is not None:
            S["last_feat"][symbol] = lastrow
        if len(meta):
            meta[:, M_SYM] = _pl_sym_id(symbol, S)
            S["chunks"].append((meta, X))
            S["session_samples"] += len(meta)
    return len(meta)


def _dl_after_symbol():
    S = _dl
    with S["lock"]:
        S["symbols_ingested"] += 1; S["since_train"] += 1
        cnt = S["symbols_ingested"]
    if cnt % 25 == 0:
        _pl_consolidate(S=S)
    if cnt % DL_SAVE_EVERY == 0:
        _pl_save(S=S)
    if (S["since_train"] >= S["train_every"] and not S["training"] and not _pl["training"]
            and time.time() - S["last_train_ts"] >= PL_TRAIN_MIN_GAP_SEC):
        S["since_train"] = 0
        threading.Thread(target=_pl_train_all, args=(False, S), daemon=True, name="DailyPatternTrainer").start()


def _dl_step(symbol):
    """يُستدعى من حلقة الأوفلاين: يسحب الشارت اليومي (مرة كل أسبوع تقريباً لكل سهم) ويغذّي المتعلّم."""
    if not DL_ENABLED or not _pl_symbol_ok(symbol):
        return
    S = _dl
    if time.time() - S["sym_fetch"].get(symbol, 0.0) < DL_REFETCH_DAYS * 86400:
        return
    df = fetch_from_yahoo(symbol, DL_PERIOD, "1d")
    if df is None or df.empty:
        return
    S["sym_fetch"][symbol] = time.time()
    _dl_ingest(symbol, df)
    _dl_after_symbol()


# ---------------- قائمة الأسهم المرشحة الحين + تقييم سهم محدد ----------------
def _dl_fact_text(row):
    f = {k: row[_DLI[k]] for k in ("dd120", "tl_dist", "tl_touch", "vdry", "up60", "squeeze")}
    parts = []
    if f["dd120"] is not None:
        parts.append(f"نازل {abs(f['dd120']):.0f}% عن قمة 4 شهور" if f["dd120"] < 0 else "قريب من قمة 4 شهور")
    if f["tl_dist"] is not None and f["tl_touch"] is not None:
        parts.append(f"{'فوق' if f['tl_dist'] >= 0 else 'تحت'} خط الدعم {abs(f['tl_dist']):.1f}% ({int(f['tl_touch'])} لمسات)")
    if f["vdry"] is not None:
        parts.append(f"حجم {f['vdry']:.1f}× المعتاد")
    return " | ".join(parts)


def _dl_models_ok():
    return {tid: _dl["models"].get(tid) for tid, _c, _t, _l in DL_TARGETS if _dl["models"].get(tid)}


def _pl_level_short(model, score):
    lv = (model.get("hold") or {}).get("levels") or []
    for L in lv:
        if score >= L["thr"]:
            return f"أعلى {L['top'] * 100:.1f}% (دقة تاريخية ~{L['prec'] * 100:.0f}%)"
    return "عادي"


def _dl_setups_text(top=10):
    models = _dl_models_ok()
    if not models:
        return "📅 لسه ما فيه نموذج يومي مدرّب. اترك الروبوت يشتغل (سبت/أحد/ليل) ثم جرّب /picks"
    with _dl["lock"]:
        items = list(_dl["last_feat"].items())
    if not items:
        return "📅 ما فيه أسهم محفوظة بعد."
    syms = [s for s, _r in items]; ts_arr = np.array([r[0] for _s, r in items], dtype=float)
    price = np.array([r[1] for _s, r in items], dtype=float)
    X = np.array([[np.nan if x is None else x for x in r[2]] for _s, r in items], dtype=np.float32)
    ok = np.isfinite(X[:, _DLI["ret1"]]) & (X[:, _DLI["ret1"]] < DL_MAX_TODAY)
    ok &= (time.time() - ts_arr) < 12 * 86400                  # بيانات أقدم من 12 يوم ما نعرضها
    if not ok.any():
        return "📅 ما فيه أسهم ببيانات حديثة بعد."
    idx = np.flatnonzero(ok)
    trusted = {t: m for t, m in models.items() if (m.get("hold") or {}).get("levels")
               and m["hold"]["levels"][1]["lift"] >= 3 and m["hold"]["levels"][1]["hits"] >= 5}
    rank_models = trusted or models                            # نرتّب بالنماذج اللي ثبتت فقط، وإلا استكشافي
    scores, key = {}, np.zeros(len(idx))
    for tid, m in models.items():
        scores[tid] = _pl_predict(m, X[idx])
    for tid, m in rank_models.items():
        key += np.argsort(np.argsort(scores[tid])) / max(1, len(idx) - 1)
    best = np.argsort(-key)[:top]
    date_txt = datetime.fromtimestamp(float(ts_arr[idx].max()), tz=_timezone.utc).strftime("%Y-%m-%d")
    head = "✅ النموذج ثبت على بيانات ما شافها" if trusted else "⚠️ النموذج لم يثبت بعد على بيانات الحجز — قائمة استكشافية فقط"
    lines = ["📅 *أقوى المرشحين بنمط الدعم/الانضغاط*", f"بيانات لغاية {date_txt} | {len(idx):,} سهم قيد التقييم", head, "━━━━━━━━━━━━━━━━"]
    show = [t for t in ("n1_25", "n3_50") if t in models] or list(models)[:2]
    for rank, b in enumerate(best, 1):
        i = idx[b]
        lines.append(f"{rank}) *{syms[i]}* ${price[i]:.3f}")
        parts = []
        for tid in show:
            lab = next(l for t, _c, _th, l in DL_TARGETS if t == tid)
            parts.append(f"{lab}: {_pl_level_short(models[tid], float(scores[tid][b]))}")
        lines.append("   " + "\n   ".join(parts))
        lines.append("   " + _dl_fact_text([None if not np.isfinite(x) else float(x) for x in X[i]]))
    lines.append("\n⚠️ ترتيب إحصائي من نمط تاريخي، مو توصية. لا تدخل بدون وقف خسارة.")
    return "\n".join(lines)[:3900]


def _dl_score_symbol(sym):
    df = fetch_from_yahoo(sym, "1y", "1d")
    P = _dl_prepare(df)
    if P is None:
        return None
    x = P["X"][-1:]
    row = [None if not np.isfinite(val) else float(val) for val in x[0]]
    lines = [f"  📅 يومي ({_dl_fact_text(row)}):"]
    models = _dl_models_ok()
    if not models:
        lines.append("   لسه ما فيه نموذج يومي مدرّب.")
    for tid, m in models.items():
        lab = next(l for t, _c, _th, l in DL_TARGETS if t == tid)
        lines.append(f"   {lab}: {_pl_level_text(m, float(_pl_predict(m, x)[0]))}")
    return "\n".join(lines)

if __name__ == "__main__":
    load_state()
    ensure_state_schema()
    # Batch high-frequency state writes, but persist the startup schema now.
    save_state(immediate=True)
    sle_spawn("StateFlusher", state_flusher)
    # النسخة الخارجية: من /data إلى GitHub كل 30 دقيقة افتراضيًا.
    if _github_configured():
        sle_spawn("GitHubStateSync", github_state_sync_loop)
    if not state.get("tickers"): state["tickers"] = ["AAPL", "TSLA", "NVDA", "AMD", "MSFT", "META", "GOOGL", "AMZN", "NFLX", "PYPL"]

    # 🧠 [SLE] تهيئة الروبوت أولًا: يحمّل الجينوم الذي طوّره بنفسه في آخر تشغيل،
    #    يطبّقه على الإعدادات الفعلية، ويركّب المراقبة الذاتية على مسار التنبيهات.
    sle_init()

    # ✅ شغل Telegram فوراً (قبل أي شيء آخر)
    logger.info("✅ Starting Telegram bot immediately...")
    if bot:
        sle_spawn("TelegramBot", run_telegram_bot)
        time.sleep(1)  # انتظر ثانية واحدة للتأكد من بدء Telegram
        send_telegram("✅ PENNY HUNTER BOT STARTING! جاري تحميل الأسهم...")
    else:
        logger.error("❌ Telegram bot not initialized!")

    # ===== شغل تحديث التيكرات في خيط منفصل (لا تحظر البوت الرئيسي) =====
    logger.info("📊 Loading ticker list in background...")
    sle_spawn("TickerLoader", update_all_tickers, supervise=False)   # مهمة مرة واحدة

    # أساسيات (إلزامية)
    sle_spawn("BackgroundMonitor", background_monitor)
    sle_spawn("MemoryMonitor", memory_monitor)
    sle_spawn("Cleaner", cleaner_loop)
    sle_spawn("PendingHaltsCleaner", pending_halts_cleaner_loop)
    sle_spawn("CacheCleaner", cache_cleaner_loop)

    # 🔥 ماسحات صيد البيني
    # 🔍 PRIMARY DISCOVERY ENGINE
    # (Full Rotation Discovery حُذفت 2026-09-23: كانت معطّلة افتراضيًا
    #  FULL_ROTATION_ENABLED=false وما تسوي شي غير نوم كل 5 دقايق — خيط بلا فايدة.)
    sle_spawn("AfterHoursDiscovery", after_hours_discovery_scanner)
    logger.info('🌙 After-Hours Discovery started')

    # 🏆 TOP GAINERS SCANNER
    sle_spawn("TopGainersDiscovery", top_gainers_scanner)
    logger.info('🏆 Top Gainers Discovery started')

    # 🚨 QUICK JUMP SCANNER
    sle_spawn("QuickJumpDiscovery", quick_jump_scanner)
    logger.info('🚨 Quick Jump Discovery started')

    # 🎯 RANKING ENGINE (يشمل الحين إثراء AUTO HUNTER داخليًا — بدون خيط/تحميل مكرر)
    sle_spawn("HotWatchlist", hot_watchlist_scanner)
    sle_spawn("ExplosiveSetupEngine", setup_scanner)
    logger.info('🎯 Hybrid pipeline started: Discovery → Ranking (+Hunter enrichment) → AI → Telegram')

    # 🚀 MEGA MOVER + ✨ DOJI REVERSAL — طبقات توعية إضافية، منفصلة عن صرامة الدخول
    sle_spawn("MegaMover", mega_mover_scanner)
    sle_spawn("DojiReversal", doji_reversal_scanner)
    sle_spawn("HighBreakout", high_breakout_scanner)
    sle_spawn("GapHunter", gap_hunter_scanner)
    sle_spawn("AccumulationTracker", accumulation_tracker_scanner)
    sle_spawn("PumpDumpHunter", pump_dump_scanner)
    sle_spawn("EarlyMover", early_mover_scanner)      # [FIX-12] إشارة قبل الانفجار
    sle_spawn("WeeklySwing", weekly_swing_scanner)
    sle_spawn("AnomalyDetector", anomaly_scanner)
    sle_spawn("YahooStream", yahoo_stream_worker)
    sle_spawn("InsiderBuy", insider_buy_scanner)
    sle_spawn("ClosedReview", closed_hours_review_scanner)
    sle_spawn("SignalJournal", signal_journal_worker)
    sle_spawn("OfflineTrainer", offline_trainer_loop)   # 🌙 يتدرّب على أسهم قديمة لما السوق مقفل (نهاية الأسبوع/الليل/العطل)
    logger.info('🚀 Mega Mover + ✨ Doji + 📈 Breakout + 🕳️ Gap Hunter + 🐋 Accumulation + 💊 Pump/Dump + 📅 Swing + 🔬 Anomaly + 🔴 Live Stream + 👔 Insider Buy + 📊 Closed Review started')

    # 📰 أخبار RSS والمحفزات
    sle_spawn("RSSNews", rss_news_scanner)
    start_safe_finnhub()
    sle_spawn("PriceAlertMonitor", price_alert_monitor)

    # 🧠 [SLE] حلقات الروبوت: تطوّر الجينوم، الأهداف، المراجعة الذاتية، والمشرف الذي
    #    يعيد تشغيل أي خيط مات (كل الخيوط أعلاه مسجّلة عنده، فيحميها كلها).
    sle_start_loops()

    logger.info("=" * 50)
    logger.info("🚀 PENNY HUNTER BOT STARTED!  |  🧠 SELF-LEARNING: " + ("ON" if SLE_ENABLED else "OFF"))
    logger.info("📊 Target: $0.3 - $30 penny stocks")
    logger.info("=" * 50)

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)

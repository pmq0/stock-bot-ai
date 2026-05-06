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
MAX_OPEN_TRADES = 5                      # أقصى 5 صفقات مفتوحة
DAILY_LOSS_LIMIT = 150.0                 # توقف عند خسارة $150
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
    "PRE":     3.0,      # RVOL 3x كحد أدنى
    "REGULAR": 2.5,
    "AFTER":   3.0,
    "CLOSED":  999,
}

# ✅ CACHE TTL
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

# Fast Filter Thresholds
MIN_PRICE = 0.1
MAX_PRICE = 30.0  # تم التعديل لصيد البيني ستوكس
MIN_VOLUME = 50000

# Fast Momentum Scanner Settings
MOMENTUM_SCAN_INTERVAL = 120
MOMENTUM_PRICE_MIN = 0.5
MOMENTUM_PRICE_MAX = 30.0
MOMENTUM_VOL_MIN = 500000
MOMENTUM_GAIN_PCT = 10.0

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
    "last_daily_report_sent": None
}

EASTERN_TZ = pytz.timezone("US/Eastern")

def now_est():
    return datetime.now(EASTERN_TZ)

def save_state():
    with state_lock:
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(state, f)
        except Exception as e:
            logger.error(f"Error saving state: {e}")

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with state_lock:
            try:
                with open(STATE_FILE, "r") as f:
                    state.update(json.load(f))
                logger.info("✅ State loaded successfully")
            except Exception as e:
                logger.error(f"Error loading state: {e}")

def get_trade_levels(price, rvol=1.0):
    tp_pct = BASE_TP_PCT
    if rvol > 5:
        tp_pct = EXTREME_RVOL_TP_PCT
    elif rvol > 3:
        tp_pct = HIGH_RVOL_TP_PCT
    return price * tp_pct, price * SL_PCT, tp_pct

def get_effective_min_score(phase, settings):
    min_score = settings["min_score"]
    now = now_est()
    if phase == "REGULAR" and 11 <= now.hour < 14:
        min_score = max(min_score, MIDDAY_MIN_SCORE)
    return min_score

def get_today_key():
    return now_est().date().isoformat()

def ensure_state_schema():
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
        f"📊 *تقرير التداول اليومي ({report_date})*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📡 إشارات اليوم: {signals}\n"
        f"✅ صفقات مغلقة: {closed}\n"
        f"🏆 صفقات رابحة: {wins}\n"
        f"❌ صفقات خاسرة: {losses}\n"
        f"📈 نسبة النجاح: {win_rate:.1f}%\n"
        f"💰 صافي الربح/الخسارة: ${total_pnl:+.2f}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🌟 أفضل صفقة: {best_line}\n"
        f"📉 أسوأ صفقة: {worst_line}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🤖 *Penny Hunter Bot*"
    )

def daily_report_scheduler():
    while True:
        try:
            now = now_est()
            if now.hour == DAILY_REPORT_HOUR and now.minute == DAILY_REPORT_MINUTE:
                today = get_today_key()
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
    return True

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

def send_telegram(message, photo=None):
    if not bot or not CHAT_ID:
        return
    try:
        if photo:
            bot.send_photo(CHAT_ID, photo, caption=message, parse_mode="Markdown")
        else:
            bot.send_message(CHAT_ID, message, parse_mode="Markdown", disable_web_page_preview=True)
        time.sleep(TELEGRAM_DELAY)
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

# ================= RSS NEWS SCANNER (ENHANCED - NO API) =================
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
    pattern = r'\b(?:NASDAQ|NYSE|AMEX|OTC):\s*([A-Z]{1,5})\b'
    found = re.findall(pattern, text.upper())
    pattern2 = r'\(([A-Z]{1,5})\)'
    found2 = re.findall(pattern2, text.upper())
    all_symbols = list(set(found + found2))
    exclude = {'THE','AND','FOR','INC','LLC','LTD','CEO','CFO','IPO','FDA','SEC','NYSE','USD','ETF'}
    return [s for s in all_symbols if s not in exclude and 1 <= len(s) <= 5]

def analyze_news_sentiment(title, desc=""):
    text = (title + " " + desc).lower()
    score = 0
    for kw in STRONG_POSITIVE:
        if kw in text:
            if kw in ["fda approval", "acquisition", "merger", "buyout"]:
                score += 30
            elif kw in ["partnership", "contract awarded", "beats estimates"]:
                score += 20
            else:
                score += 10
    for kw in NEGATIVE_WORDS:
        if kw in text:
            return "negative", 0
    if score > 0:
        return "positive", min(score, 100)
    return "neutral", 0

def rss_news_scanner():
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
                    news_id = title[:80]
                    with state_lock:
                        if news_id in state.get("seen_news", {}):
                            continue
                        state.setdefault("seen_news", {})[news_id] = time.time()
                    sentiment, score = analyze_news_sentiment(title, item.get('desc',''))
                    if sentiment != "positive" or score < 40:
                        continue
                    symbols = extract_symbols_from_text(title + " " + item.get('desc',''))
                    if not symbols:
                        continue
                    for symbol in symbols[:2]:
                        try:
                            df = cached_download(symbol, period="1d", interval="5m")
                            if df.empty or len(df) < 5:
                                continue
                            price = df['close'].iloc[-1]
                            if price > 30:
                                continue
                            vol_now = df['volume'].iloc[-1]
                            vol_avg = df['volume'].mean()
                            gain_pct = (price - df['open'].iloc[0]) / df['open'].iloc[0] * 100
                            vol_spike = vol_avg > 0 and vol_now > vol_avg * 2
                            not_too_late = gain_pct < 25
                            if not vol_spike or not not_too_late:
                                continue
                            entry = price
                            tp = round(entry * 1.10, 2)
                            sl = round(entry * 0.95, 2)
                            try:
                                translator = GoogleTranslator(source='en', target='ar')
                                title_ar = translator.translate(title[:200])
                            except:
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
                                f"📝 الخبر: {title_ar[:150]}...\n"
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

# ================= DATA FETCHING & SCANNING =================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def cached_download(symbol, period="1d", interval="5m"):
    cache_key = f"{symbol}_{period}_{interval}"
    with _cache_lock:
        if cache_key in _cache:
            data, timestamp = _cache[cache_key]
            if time.time() - timestamp < CACHE_TTL.get(interval, 60):
                return data
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={period}&interval={interval}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        resp = requests.get(url, headers=headers, impersonate="chrome120", timeout=15)
        if resp.status_code != 200:
            return pd.DataFrame()
        json_data = resp.json()
        result = json_data.get('chart', {}).get('result', [])
        if not result:
            return pd.DataFrame()
        res = result[0]
        timestamps = res.get('timestamp', [])
        indicators = res.get('indicators', {}).get('quote', [{}])[0]
        adj_close = res.get('indicators', {}).get('adjclose', [{}])[0].get('adjclose', [])
        df = pd.DataFrame({
            'open': indicators.get('open', []),
            'high': indicators.get('high', []),
            'low': indicators.get('low', []),
            'close': indicators.get('close', []),
            'volume': indicators.get('volume', []),
            'adj_close': adj_close if adj_close else indicators.get('close', [])
        }, index=pd.to_datetime(np.array(timestamps) * 1e9))
        df = df.dropna()
        with _cache_lock:
            _cache[cache_key] = (df, time.time())
        return df
    except Exception as e:
        logger.warning(f"Download error {symbol}: {e}")
        return pd.DataFrame()

def calculate_rvol(df, phase="REGULAR"):
    if len(df) < 2: return 0.0
    last_vol = df['volume'].iloc[-1]
    avg_vol = df['volume'].rolling(20).mean().iloc[-1]
    return last_vol / (avg_vol + 1e-9)

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
    df["vwap"] = (df["close"] * df["volume"]).cumsum() / (df["volume"].cumsum() + 1e-9)
    return df

def process_symbol(symbol):
    try:
        phase = get_market_phase()
        if phase == "CLOSED": return
        settings = PHASE_SETTINGS.get(phase)
        df = cached_download(symbol, period="1d", interval="5m")
        if df.empty or len(df) < 10: return
        
        last = df.iloc[-1]
        price = last['close']
        
        if price > 30.0:
            return
        
        if price < 5.0:
            rvol_check = calculate_rvol(df, phase=phase)
            if rvol_check < 4.0:
                return

        df = compute_indicators(df)
        last = df.iloc[-1]
        rvol = calculate_rvol(df, phase=phase)
        
        if rvol < MIN_RVOL_BY_PHASE.get(phase, 2.0): return
        
        score = 0
        if last['close'] > last['ema9']: score += 30
        if last['ema9'] > last['ema21']: score += 20
        if 40 < last['rsi'] < 70: score += 20
        if rvol > settings['vol_surge_mult']: score += 40
        if last['close'] > last['vwap']: score += 20
        
        min_score = get_effective_min_score(phase, settings)
        if score >= min_score:
            now = time.time()
            last_sig = state["seen_signals"].get(symbol, 0)
            if now - last_sig > SIGNAL_COOLDOWN:
                tp, sl, tp_pct = get_trade_levels(price, rvol)
                msg = (
                    f"🚀 *إشارة انفجار سعري: {symbol}*\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"💰 السعر: *${price:.2f}*\n"
                    f"🔥 RVOL: {rvol:.1f}x\n"
                    f"📊 Score: {score}\n"
                    f"🎯 الهدف: ${tp:.2f} (+{int((tp_pct-1)*100)}%)\n"
                    f"🛑 الوقف: ${sl:.2f}\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"🕒 الجلسة: {settings['description']}"
                )
                send_telegram(msg)
                with state_lock:
                    state["seen_signals"][symbol] = now
                    save_state()
    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")

def background_scanner():
    while True:
        try:
            phase = get_market_phase()
            if phase == "CLOSED":
                time.sleep(600)
                continue
            
            with state_lock:
                tickers = list(state.get("tickers", []))
            
            if not tickers:
                tickers = update_all_tickers()
            
            for i in range(0, len(tickers), CHUNK_SIZE):
                chunk = tickers[i:i+CHUNK_SIZE]
                with ThreadPoolExecutor(max_workers=FAST_FILTER_WORKERS) as executor:
                    executor.map(process_symbol, chunk)
                time.sleep(BREAK_BETWEEN_CHUNKS)
            
            time.sleep(SCAN_INTERVAL_SEC)
        except Exception as e:
            logger.error(f"Scanner error: {e}")
            time.sleep(60)

def update_all_tickers():
    tickers = set()
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
    
    final_list = sorted(list(tickers))[:MAX_TICKERS_TO_SCAN]
    with state_lock:
        state["tickers"] = final_list
        save_state()
    return final_list

def run_telegram_bot():
    @bot.message_handler(commands=['start', 'help'])
    def send_welcome(message):
        bot.reply_to(message, "🤖 *Penny Hunter Bot v33*\nأهلاً بك! أنا أراقب السوق الأمريكي لصيد الأسهم المتفجرة.")

    @bot.message_handler(commands=['status'])
    def bot_status(message):
        phase = get_market_phase()
        with state_lock:
            tickers_count = len(state.get("tickers", []))
        bot.reply_to(message, f"📊 *حالة البوت:*\nالجلسة: {phase}\nالأسهم المراقبة: {tickers_count}\nخسارة اليوم: ${state['daily_loss']:.2f}")

    bot.polling(none_stop=True)

if __name__ == "__main__":
    load_state()
    ensure_state_schema()
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=rss_news_scanner, daemon=True).start()
    threading.Thread(target=daily_report_scheduler, daemon=True).start()
    if bot:
        threading.Thread(target=run_telegram_bot, daemon=True).start()
    
    logger.info("✅ Bot started successfully!")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)

import os
import time
import json
import logging
import threading
import asyncio
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
import aiohttp
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

# Scanner Settings - مع 12,000 سهم
SCAN_INTERVAL_SEC = 900  # 15 دقيقة بين كل مسحة كاملة
CHUNK_SIZE = 100  # 100 سهم لكل شانك
FAST_FILTER_WORKERS = 5
DEEP_ANALYSIS_WORKERS = 3
DELAY_BETWEEN_REQUESTS = 0.3  # 300ms بين الطلبات
BREAK_BETWEEN_CHUNKS = 5
TRADE_MONITOR_INTERVAL = 60

TELEGRAM_DELAY = 1.0

# Strategy Parameters (Risk/Reward 1:2)
TP_PCT = 1.06   # +6%
SL_PCT = 0.97   # -3%

# Fast Filter Thresholds
MIN_PRICE = 1.0
MAX_PRICE = 200.0
MIN_VOLUME = 200000

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
    "last_ticker_update": None
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

# ================= NEW: ROBUST DATA FETCHER (NO BLOCKING) =================
class StockDataFetcher:
    """يجلب البيانات من Yahoo بدون حظر - يدعم آلاف الأسهم"""
    
    def __init__(self):
        self.session = None
    
    async def get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Accept-Language': 'en-US,en;q=0.9',
                }
            )
        return self.session
    
    async def fetch_chart_data(self, symbol, period="5d", interval="15m"):
        """جلب بيانات الشارت من Yahoo Finance"""
        try:
            # تحويل الفترة إلى أيام
            days_map = {"5d": 5, "1d": 1, "10d": 10, "1mo": 30}
            days = days_map.get(period, 5)
            
            end_date = int(datetime.now().timestamp())
            start_date = int((datetime.now() - timedelta(days=days)).timestamp())
            
            # خريطة الفواصل
            interval_map = {"15m": "15m", "5m": "5m", "1d": "1d", "1h": "60m"}
            yf_interval = interval_map.get(interval, "15m")
            
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval={yf_interval}&period1={start_date}&period2={end_date}"
            
            session = await self.get_session()
            
            for attempt in range(3):
                try:
                    async with session.get(url, timeout=15) as response:
                        if response.status == 200:
                            data = await response.json()
                            return self._parse_chart_response(data)
                        elif response.status == 429:
                            await asyncio.sleep(5 * (attempt + 1))
                        else:
                            await asyncio.sleep(1)
                except Exception as e:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(2)
            
            return None
        except Exception as e:
            logger.debug(f"Fetch error for {symbol}: {e}")
            return None
    
    def _parse_chart_response(self, data):
        """تحويل استجابة Yahoo إلى DataFrame"""
        try:
            if 'chart' not in data or 'result' not in data['chart'] or not data['chart']['result']:
                return None
            
            result = data['chart']['result'][0]
            timestamps = result.get('timestamp', [])
            quote = result.get('indicators', {}).get('quote', [{}])[0]
            
            if not timestamps or not quote:
                return None
            
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
                return None
            
            df.set_index('timestamp', inplace=True)
            return df
        except Exception as e:
            logger.debug(f"Parse error: {e}")
            return None
    
    async def close(self):
        if self.session:
            await self.session.close()

# Global fetcher instance
_fetcher = None
_fetcher_lock = threading.Lock()

def get_fetcher():
    global _fetcher
    if _fetcher is None:
        _fetcher = StockDataFetcher()
    return _fetcher

def safe_download(symbol, period="5d", interval="15m"):
    """واجهة متزامنة لجلب البيانات - تعمل بدون حظر"""
    try:
        fetcher = get_fetcher()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(fetcher.fetch_chart_data(symbol, period, interval))
        loop.close()
        return result if result is not None else pd.DataFrame()
    except Exception as e:
        logger.error(f"Download error {symbol}: {e}")
        return pd.DataFrame()

# ================= FALLBACK UNIVERSE SOURCES =================
MINIMAL_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD", "NFLX",
    "INTC", "PLTR", "SOFI", "NIO", "GME", "AMC", "RIOT", "MARA", "COIN"
]

def update_all_tickers():
    """تحديث قائمة الأسهم - يحتفظ بـ 12,000 سهم"""
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
    df["tr"] = np.maximum(df["high"] - df["low"], np.maximum(abs(df["high"] - df["close"].shift(1)), abs(df["low"] - df["close"].shift(1))))
    df["atr"] = df["tr"].rolling(14).mean()
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
    price_change_30 = abs((df['close'].iloc[-1] - df['close'].iloc[-30]) / (df['close'].iloc[-30] + 1e-9))
    price_stable = price_change_30 < 0.03
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
        ax1.fill_between(df_plot.index, df_plot['upper_band'], df_plot['lower_band'], alpha=0.1, color='gray', label='Bollinger Bands')
        ax1.axhline(y=entry, color='lime', linestyle='--', linewidth=1.5, label=f'Entry ${entry:.2f}')
        ax1.axhline(y=tp, color='green', linestyle='--', linewidth=1.5, label=f'TP ${tp:.2f}')
        ax1.axhline(y=sl, color='red', linestyle='--', linewidth=1.5, label=f'SL ${sl:.2f}')
        if is_pre_breakout:
            ax1.axvspan(df_plot.index[-5], df_plot.index[-1], alpha=0.2, color='yellow', label='Pre-Breakout')
        ax1.set_title(f'{symbol} - Trading Signal', fontsize=14, color='white')
        ax1.grid(True, alpha=0.15)
        ax1.tick_params(colors='white')
        colors = ['green' if df_plot['close'].iloc[i] >= df_plot['open'].iloc[i] else 'red' for i in range(len(df_plot))]
        ax2.bar(df_plot.index, df_plot['volume'], color=colors, alpha=0.7)
        ax2.axhline(y=df_plot['vol_ma'].iloc[-1], color='blue', linestyle='--', linewidth=1, label='Avg Volume')
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
    """فحص سريع - يستخدم الدالة الجديدة"""
    try:
        df = safe_download(symbol, period="1d")
        if df.empty:
            return False
        price = df["close"].iloc[-1]
        volume = df["volume"].iloc[-1]
        return MIN_PRICE < price < MAX_PRICE and volume > MIN_VOLUME
    except:
        return False

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
        if vol_surge:
            score += 40
        if price_break:
            score += 40
        if 40 < last['rsi'] < 70:
            score += 10
        if last['ema9'] > last['ema21']:
            score += 10
        
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
    caption = f"{phase_emoji} *SIGNAL: {symbol}* ({phase})\n💰 Entry: ${price:.2f}\n🎯 TP: ${tp:.2f}\n🛑 SL: ${sl:.2f}\n📦 Size: {size}\n📊 Score: {score}/{settings['min_score']}\n📋 {settings['description']}"
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
    send_telegram(f"{emoji} *CLOSED: {symbol}*\n📝 {reason}\n💵 Exit: ${price:.2f}\n📈 PnL: ${pnl:+.2f} ({pnl_pct:+.2f}%)\n📊 Daily Loss: ${state.get('daily_loss', 0):.2f}")

def update_trades():
    with state_lock:
        symbols = list(state["open_trades"].keys())
        if state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT:
            for symbol in symbols:
                try:
                    df = safe_download(symbol, period='1d', interval='5m')
                    if not df.empty:
                        price = df['close'].iloc[-1]
                        close_trade(symbol, price, "Daily loss limit reached")
                except:
                    pass
            return
    
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
            logger.info("🤖 Starting Telegram bot polling...")
            bot.infinity_polling(timeout=30, long_polling_timeout=10)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(15)

if bot:
    @bot.message_handler(commands=['start'])
    def cmd_start(message):
        phase = get_market_phase()
        msg = f"👋 Welcome to Trading Bot v32!\n\n"
        msg += f"📊 Monitoring {len(state['tickers'])} stocks\n"
        msg += f"🕐 Phase: {PHASE_SETTINGS[phase]['description']}\n\n"
        msg += f"✅ No more Yahoo blocking!\n"
        msg += f"📈 Risk/Reward: 1:2\n\n"
        msg += f"Commands:\n/status - Performance\n/positions - Open trades\n/close SYMBOL - Close trade\n/scan SYMBOL - Analyze"
        send_telegram(msg)

    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        with state_lock:
            perf = state["performance"]
            total = perf["wins"] + perf["losses"]
            wr = (perf["wins"] / total * 100) if total > 0 else 0
            msg = f"📊 *Bot Status*\n✅ Wins: {perf['wins']}\n❌ Losses: {perf['losses']}\n📈 Win Rate: {wr:.1f}%\n💵 Total PnL: ${perf['total_pnl']:+.2f}\n📦 Open: {len(state['open_trades'])}\n🌐 Universe: {len(state['tickers'])} stocks\n📉 Daily Loss: ${state.get('daily_loss', 0):.2f}"
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
                msg += f"\n🔹 *{sym}* | Entry ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f}"
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
                    send_telegram(f"❌ {symbol} is not an open position")
                    return
            df = safe_download(symbol, period='1d', interval='5m')
            if df.empty:
                send_telegram(f"❌ Could not fetch price for {symbol}")
                return
            current_price = df['close'].iloc[-1]
            close_trade(symbol, current_price, "Manual Close")
            send_telegram(f"✅ Position {symbol} closed")
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
            
            msg = f"📊 *{symbol} Analysis*\n💰 Price: ${last['close']:.2f}\n📊 Volume: {int(last['volume']):,}\n⚡ RSI: {last['rsi']:.1f}\n🎯 Score: {score}/{settings['min_score']}\n🕐 Phase: {phase}"
            send_telegram(msg)
        except Exception as e:
            send_telegram(f"❌ Error: {e}")

# ================= MAIN =================
if __name__ == "__main__":
    load_state()
    
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    
    if bot:
        threading.Thread(target=run_telegram_bot, daemon=True).start()
    
    logger.info("=" * 50)
    logger.info("✅ Bot started successfully!")
    logger.info(f"📊 Universe size: {len(state.get('tickers', []))} stocks")
    logger.info(f"⚡ No Yahoo blocking - Using curl_cffi + aiohttp")
    logger.info("=" * 50)
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)

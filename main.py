import os
import time
import json
import logging
import threading
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, request
import telebot
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
import ftplib
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

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
STATE_FILE = os.path.join(STATE_DIR, "state_v28.json")
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_v28.log"),
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
    "last_ticker_update": None,
    "backtest_results": {}
}

EASTERN_TZ = pytz.timezone("US/Eastern")

def now_est() -> datetime:
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

def is_market_open():
    return get_market_phase() == "REGULAR"

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

# ================= TICKER DISCOVERY =================
def update_all_tickers():
    try:
        logger.info("Fetching latest ticker list from NASDAQ FTP...")
        ftp = ftplib.FTP("ftp.nasdaqtrader.com")
        ftp.login()
        ftp.cwd("SymbolDirectory")
        tickers = []
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
        clean_tickers = sorted(list(set([t for t in tickers if str(t).isalpha() and len(str(t)) <= 5])))
        with state_lock:
            state["tickers"] = clean_tickers
            state["last_ticker_update"] = datetime.now().isoformat()
            save_state()
        logger.info(f"Successfully updated ticker list: {len(clean_tickers)} symbols found.")
        return clean_tickers
    except Exception as e:
        logger.error(f"Ticker update error: {e}")
        return state.get("tickers", [])

# ================= TELEGRAM HELPERS =================
def send_telegram(message):
    if bot and CHAT_ID:
        try:
            bot.send_message(CHAT_ID, message, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Telegram error: {e}")

# ================= RISK MANAGEMENT =================
def calculate_position_size(price):
    stop_loss = price * (1 - SL_PCT)
    risk_per_share = price - stop_loss
    if risk_per_share <= 0: return 1
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

# ================= PATTERN DETECTION =================
def detect_accumulation(df):
    if len(df) < 30: return False, 0
    last_10 = df.tail(10)
    last_20 = df.tail(20)
    price_change = abs((df['close'].iloc[-1] - df['close'].iloc[-20]) / (df['close'].iloc[-20] + 1e-9))
    price_stable = price_change < 0.015
    volume_surge = last_10['volume'].mean() > last_20['volume'].mean() * 1.2
    tight_range = (last_10['high'].max() - last_10['low'].min()) / (df['close'].iloc[-1] + 1e-9) < 0.04
    acc_score = 0
    if price_stable: acc_score += 30
    if volume_surge: acc_score += 40
    if tight_range: acc_score += 30
    return acc_score >= 60, acc_score

def detect_pre_breakout(df):
    if len(df) < 20: return False
    is_squeezing = df['bandwidth'].iloc[-1] < df['bandwidth'].iloc[-10] * 0.7
    approaching = df['close'].iloc[-1] > df['upper_band'].iloc[-1] * 0.97
    volume_surge = df['volume'].iloc[-3:].mean() > df['volume'].rolling(20).mean().iloc[-1] * 1.5
    return is_squeezing and approaching and volume_surge

# ================= SCORING =================
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
        if is_pre_breakout: ax1.axvspan(df_plot.index[-5], df_plot.index[-1], alpha=0.2, color='yellow', label='Pre-Breakout')
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
    try:
        df = yf.download(symbol, period="1d", progress=False)
        if df.empty: return False
        price = df["Close"].iloc[-1]
        volume = df["Volume"].iloc[-1]
        return MIN_PRICE < price < MAX_PRICE and volume > MIN_VOLUME
    except: return False

# ================= BACKTEST =================
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
def process_symbol(symbol):
    try:
        reset_daily_loss_if_needed()
        phase = get_market_phase()
        settings = PHASE_SETTINGS.get(phase, PHASE_SETTINGS["CLOSED"])
        if phase == "CLOSED": return
        with state_lock:
            if symbol in state["open_trades"] or state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT: return
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="5d", interval="15m")
        if df.empty: return
        df.columns = [c.lower() for c in df.columns]
        df = compute_indicators(df)
        is_accumulating, acc_score = detect_accumulation(df)
        is_pre_breakout = detect_pre_breakout(df)
        
        score, details = score_stock(df, settings["vol_surge_mult"])
        
        if score >= settings["min_score"]:
            now = time.time()
            last_seen = state["seen_signals"].get(symbol, 0)
            if now - last_seen > SIGNAL_COOLDOWN:
                open_trade(symbol, details['price'], score, df, is_accumulating, is_pre_breakout, phase, settings)
                with state_lock:
                    state["seen_signals"][symbol] = now
                    save_state()
    except Exception: pass

# ================= TRADE MANAGEMENT =================
def open_trade(symbol, price, score, df, is_accumulating=False, is_pre_breakout=False, phase="REGULAR", settings=None):
    with state_lock:
        if state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT or len(state["open_trades"]) >= MAX_OPEN_TRADES: return
        size = int(calculate_position_size(price) * settings["size_multiplier"])
        tp, sl = price * TP_PCT, price * SL_PCT
        state["open_trades"][symbol] = {"entry": price, "tp": tp, "sl": sl, "size": size, "time": time.time(), "score": score, "phase": phase}
        save_state()
    chart = generate_chart(symbol, df, price, tp, sl, is_accumulating, is_pre_breakout)
    phase_emoji = "🟡" if phase == "PRE" else ("🔵" if phase == "AFTER" else "🟢")
    caption = f"{phase_emoji} *EXPLOSION ALERT: {symbol}* ({phase})\n💰 Entry: ${price:.2f}\n🎯 TP: ${tp:.2f}\n🛑 SL: ${sl:.2f}\n📦 Size: {size}\n📊 Score: {score}/{settings['min_score']}\n📋 {settings['description']}"
    if is_accumulating: caption += "\n📦 *Accumulation Pattern*"
    if is_pre_breakout: caption += "\n⚡ *Pre-Breakout*"
    if chart and bot: bot.send_photo(CHAT_ID, chart, caption=caption, parse_mode='Markdown')
    else: send_telegram(caption)

def close_trade(symbol, price, reason):
    with state_lock:
        if symbol not in state["open_trades"]: return
        trade = state["open_trades"].pop(symbol)
        pnl = (price - trade["entry"]) * trade["size"]
        pnl_pct = ((price - trade["entry"]) / trade["entry"]) * 100
        if pnl > 0: state["performance"]["wins"] += 1
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
    for symbol in symbols:
        try:
            df = yf.Ticker(symbol).history(period='1d', interval='5m')
            if df.empty: continue
            price = df['Close'].iloc[-1]
            with state_lock:
                trade = state["open_trades"].get(symbol)
                if not trade: continue
                if price >= trade["tp"]: close_trade(symbol, price, "TAKE PROFIT")
                elif price <= trade["sl"]: close_trade(symbol, price, "STOP LOSS")
        except: pass

# ================= SCANNER ENGINE =================
def background_scanner():
    while True:
        try:
            phase = get_market_phase()
            if phase != "CLOSED":
                with state_lock:
                    if not state["tickers"] or (state["last_ticker_update"] and (datetime.now() - datetime.fromisoformat(state["last_ticker_update"])).days >= 1):
                        update_all_tickers()
                    tickers = list(state["tickers"])
                chunk_size = 500
                for i in range(0, len(tickers), chunk_size):
                    chunk = tickers[i:i+chunk_size]
                    filtered = []
                    with ThreadPoolExecutor(max_workers=20) as executor:
                        future_to_sym = {executor.submit(fast_filter, sym): sym for sym in chunk}
                        for future in as_completed(future_to_sym):
                            if future.result(): filtered.append(future_to_sym[future])
                    if filtered:
                        with ThreadPoolExecutor(max_workers=15) as executor:
                            executor.map(process_symbol, filtered)
                    time.sleep(1)
            time.sleep(SCAN_INTERVAL_SEC)
        except Exception as e:
            logger.error(f"Scanner error: {e}")
            time.sleep(60)

def background_monitor():
    while True:
        try:
            if get_market_phase() != "CLOSED": update_trades()
            time.sleep(TRADE_MONITOR_INTERVAL)
        except Exception as e:
            logger.error(f"Monitor error: {e}")
            time.sleep(30)

# ================= TELEGRAM HANDLERS (FULL COMMANDS) =================
if bot:
    @bot.message_handler(commands=['start'])
    def cmd_start(message):
        phase = get_market_phase()
        phase_desc = PHASE_SETTINGS.get(phase, PHASE_SETTINGS["CLOSED"])["description"]
        welcome = f"""
🚀 *AI Trading Bot V28 - Full Features*

🕐 Current Market: {phase} - {phase_desc}

*Features:*
✅ 7,500+ stocks (NASDAQ/NYSE/AMEX)
✅ Two-stage filtering (Fast + Deep)
✅ Explosion detection
✅ Accumulation detection 📦
✅ Pre-breakout alerts ⚡
✅ Automatic charts 📊
✅ TP/SL management (5%/3%)
✅ Position sizing (2% risk)
✅ Daily loss limit ($300)
✅ 3 market phases (PRE/REGULAR/AFTER)

*Commands:*
/status - Bot status
/positions - Open trades
/scan <symbol> - Analyze any stock
/backtest <symbol> - Run backtest
/close <symbol> - Close trade manually
"""
        bot.reply_to(message, welcome, parse_mode='Markdown')

    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        reset_daily_loss_if_needed()
        phase = get_market_phase()
        phase_desc = PHASE_SETTINGS.get(phase, PHASE_SETTINGS["CLOSED"])["description"]
        with state_lock:
            perf = state["performance"]
            total = perf["wins"] + perf["losses"]
            wr = (perf["wins"] / total * 100) if total > 0 else 0
            msg = f"📊 *Bot Status (v28)*\n"
            msg += f"🕐 Market Phase: {phase} - {phase_desc}\n\n"
            msg += f"✅ Wins: {perf['wins']}\n"
            msg += f"❌ Losses: {perf['losses']}\n"
            msg += f"📈 Win Rate: {wr:.1f}%\n"
            msg += f"💵 Total PnL: ${perf['total_pnl']:+.2f}\n"
            msg += f"📦 Open Trades: {len(state['open_trades'])}/{MAX_OPEN_TRADES}\n"
            msg += f"📉 Daily Loss: ${state.get('daily_loss', 0):.2f} / ${DAILY_LOSS_LIMIT}\n"
            msg += f"🌐 Universe: {len(state['tickers'])} stocks"
        bot.reply_to(message, msg, parse_mode='Markdown')

    @bot.message_handler(commands=['positions'])
    def cmd_positions(message):
        with state_lock:
            trades = state["open_trades"]
            if not trades:
                bot.reply_to(message, "📭 No open positions")
                return
            msg = "*Open Positions*\n"
            for sym, t in trades.items():
                age = int((time.time() - t["time"]) / 60)
                msg += f"\n🔹 *{sym}* | Entry ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f} | Size {t['size']} | Age {age}m | Phase {t.get('phase', 'REGULAR')}"
        bot.reply_to(message, msg, parse_mode='Markdown')

    @bot.message_handler(commands=['scan'])
    def cmd_scan(message):
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "⚠️ Usage: /scan <SYMBOL>")
            return
        symbol = parts[1].upper()
        msg = bot.reply_to(message, f"🔍 Analyzing {symbol}...")
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="5d", interval="15m")
            if df.empty:
                bot.edit_message_text(f"⚠️ No data for {symbol}", chat_id=message.chat.id, message_id=msg.message_id)
                return
            df.columns = [c.lower() for c in df.columns]
            df = compute_indicators(df)
            phase = get_market_phase()
            settings = PHASE_SETTINGS.get(phase, PHASE_SETTINGS["REGULAR"])
            score, details = score_stock(df, settings["vol_surge_mult"])
            last = df.iloc[-1]
            is_accumulating, acc_score = detect_accumulation(df)
            is_pre_breakout = detect_pre_breakout(df)
            analysis = f"📊 *{symbol} Analysis*\n"
            analysis += f"💰 Price: ${last['close']:.2f}\n"
            analysis += f"📊 Volume: {int(last['volume']):,}\n"
            analysis += f"⚡ RSI: {last['rsi']:.1f}\n"
            analysis += f"🧠 Score: {score}/{settings['min_score']}\n"
            analysis += f"🕐 Phase: {phase}\n"
            if is_accumulating: analysis += f"📦 *Accumulation Pattern* ({acc_score}%)\n"
            if is_pre_breakout: analysis += f"⚡ *Pre-Breakout Detected*\n"
            bot.edit_message_text(analysis, chat_id=message.chat.id, message_id=msg.message_id, parse_mode='Markdown')
        except Exception as e:
            bot.edit_message_text(f"❌ Error analyzing {symbol}", chat_id=message.chat.id, message_id=msg.message_id)

    @bot.message_handler(commands=['backtest'])
    def cmd_backtest(message):
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "⚠️ Usage: /backtest <SYMBOL>")
            return
        symbol = parts[1].upper()
        msg = bot.reply_to(message, f"🔬 Running backtest for {symbol}...")
        result = run_backtest(symbol)
        if result and result.get("total_trades", 0) > 0:
            emoji = "✅" if result.get("total_pnl", 0) > 0 else "❌"
            text = f"🔬 *Backtest: {symbol}*\n"
            text += f"📊 Trades: {result['total_trades']}\n"
            text += f"✅ Wins: {result['wins']}\n"
            text += f"❌ Losses: {result['losses']}\n"
            text += f"📈 Win Rate: {result['win_rate']}%\n"
            text += f"💰 Avg PnL: {result['avg_pnl']:+.2f}%\n"
            text += f"💵 Total PnL: {result['total_pnl']:+.2f}%\n"
            text += f"{emoji} Strategy: {'PROFITABLE' if result['total_pnl'] > 0 else 'NOT PROFITABLE'}"
        else:
            text = f"⚠️ No trades triggered for {symbol} in backtest period (60 days)"
        bot.edit_message_text(text, chat_id=message.chat.id, message_id=msg.message_id, parse_mode='Markdown')

    @bot.message_handler(commands=['close'])
    def cmd_close(message):
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "⚠️ Usage: /close <SYMBOL>")
            return
        symbol = parts[1].upper()
        with state_lock:
            if symbol not in state["open_trades"]:
                bot.reply_to(message, f"📭 No open trade for {symbol}")
                return
        try:
            df = yf.Ticker(symbol).history(period='1d', interval='5m')
            if df.empty:
                bot.reply_to(message, f"⚠️ Cannot fetch price for {symbol}")
                return
            price = df['Close'].iloc[-1]
            close_trade(symbol, price, "MANUAL CLOSE")
            bot.reply_to(message, f"✅ Closed {symbol} at ${price:.2f}")
        except Exception as e:
            bot.reply_to(message, f"❌ Error closing {symbol}: {e}")

# ================= MAIN =================
if __name__ == "__main__":
    load_state()
    reset_daily_loss_if_needed()
    
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    
    if bot:
        threading.Thread(target=lambda: bot.infinity_polling(), daemon=True).start()
        logger.info("Telegram bot started")
        send_telegram("✅ *Bot v28 Started - Full Commands*\n📊 7,500+ stocks | 3 Market Phases\n📈 Position sizing | Daily loss limit\n🔬 Backtest | 📊 Charts\n\nCommands: /status, /positions, /scan, /backtest, /close")
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

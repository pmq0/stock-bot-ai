import os
import time
import json
import logging
import threading
import gc
import numpy as np
import pandas as pd
from datetime import datetime
from flask import Flask, request
import telebot
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
import ftplib
import io

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
STATE_DIR = os.getenv("STATE_DIR", ".")

# Trading Parameters
CAPITAL = 10000.0
RISK_PER_TRADE = 0.02
MAX_OPEN_TRADES = 5
MIN_SCORE_REGULAR = 70
MIN_SCORE_PENNY = 75
SIGNAL_COOLDOWN = 3600
DAILY_LOSS_LIMIT = 300.0
STATE_FILE = os.path.join(STATE_DIR, "state_v33.json")
SCAN_INTERVAL_SEC = 1800  # 30 minutes
TRADE_MONITOR_INTERVAL = 120

TP_PCT = 1.05
SL_PCT = 0.97

# Fast Filter Thresholds
MIN_PRICE = 2.0
MAX_PRICE = 100.0
MIN_VOLUME = 100000

PHASE_SETTINGS = {
    "PRE": {"min_score": 80, "size_multiplier": 0.5, "vol_surge_mult": 2.5, "description": "🟡 Pre-Market"},
    "REGULAR": {"min_score": 70, "size_multiplier": 1.0, "vol_surge_mult": 2.0, "description": "🟢 Regular Hours"},
    "AFTER": {"min_score": 85, "size_multiplier": 0.3, "vol_surge_mult": 3.0, "description": "🔵 After-Hours"},
    "CLOSED": {"min_score": 999, "size_multiplier": 0, "vol_surge_mult": 0, "description": "⚫ Closed"}
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

app = Flask(__name__)
bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

state_lock = threading.RLock()
state = {
    "open_trades": {},
    "performance": {"wins": 0, "losses": 0, "total_pnl": 0.0},
    "seen_signals": {},
    "daily_loss": 0.0,
    "last_reset": None,
    "tickers": [],
    "last_ticker_update": None,
}

EASTERN_TZ = pytz.timezone("US/Eastern")

def now_est(): return datetime.now(EASTERN_TZ)

def get_market_phase():
    now = now_est()
    hour = now.hour
    weekday = now.weekday()
    if weekday >= 5: return "CLOSED"
    if 4 <= hour < 9: return "PRE"
    elif 9 <= hour < 16: return "REGULAR"
    elif 16 <= hour < 20: return "AFTER"
    return "CLOSED"

def save_state():
    with state_lock:
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(state, f, indent=2, default=str)
        except: pass

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                loaded = json.load(f)
                with state_lock: state.update(loaded)
        except: pass

def reset_daily_loss_if_needed():
    today = now_est().date().isoformat()
    with state_lock:
        if state.get("last_reset") != today:
            state["daily_loss"] = 0.0
            state["last_reset"] = today
            save_state()

def update_all_tickers():
    try:
        logger.info("Fetching tickers from NASDAQ FTP...")
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
        
        # ✅ التعديل: 100 سهم فقط
        clean_tickers = [t for t in tickers if str(t).isalpha() and 2 <= len(str(t)) <= 4]
        clean_tickers = list(dict.fromkeys(clean_tickers))[:100]
        
        with state_lock:
            state["tickers"] = clean_tickers
            state["last_ticker_update"] = datetime.now().isoformat()
            save_state()
            
        logger.info(f"✅ Universe: {len(clean_tickers)} symbols")
        return clean_tickers
    except Exception as e:
        logger.error(f"Ticker update error: {e}")
        return state.get("tickers", [])

def send_telegram(message):
    if bot and CHAT_ID:
        try:
            bot.send_message(CHAT_ID, message, parse_mode='Markdown')
        except: pass

def calculate_position_size(price):
    stop_loss = price * (1 - SL_PCT)
    risk_per_share = price - stop_loss
    if risk_per_share <= 0: return 1
    risk_amount = CAPITAL * RISK_PER_TRADE
    size = int(risk_amount / risk_per_share)
    return max(1, min(size, 30))

def compute_indicators(df):
    df = df.copy()
    df["ema9"] = df["close"].ewm(9).mean()
    df["ema21"] = df["close"].ewm(21).mean()
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / (loss + 1e-9)
    df["rsi"] = 100 - (100 / (1 + rs))
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["atr"] = (df["high"] - df["low"]).rolling(14).mean()
    return df

def fast_filter(symbol):
    try:
        df = yf.download(symbol, period="1d", progress=False)
        if df.empty: return False
        price = df["Close"].iloc[-1]
        volume = df["Volume"].iloc[-1]
        del df
        gc.collect()
        return MIN_PRICE < price < MAX_PRICE and volume > MIN_VOLUME
    except: 
        return False

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
        
        last = df.iloc[-1]
        vol_surge = last['volume'] > df['vol_ma'].iloc[-1] * settings["vol_surge_mult"]
        price_break = last['close'] > df['high'].iloc[-20:-1].max()
        
        score = 0
        if vol_surge: score += 40
        if price_break: score += 40
        if 40 < last['rsi'] < 70: score += 10
        if last['ema9'] > last['ema21']: score += 10
        
        del df
        gc.collect()
        
        if score >= settings["min_score"]:
            now = time.time()
            last_seen = state["seen_signals"].get(symbol, 0)
            if now - last_seen > SIGNAL_COOLDOWN:
                open_trade(symbol, last['close'], score, phase, settings)
                with state_lock:
                    state["seen_signals"][symbol] = now
                    save_state()
    except Exception as e:
        pass

def open_trade(symbol, price, score, phase, settings):
    with state_lock:
        if state.get("daily_loss", 0) >= DAILY_LOSS_LIMIT or len(state["open_trades"]) >= MAX_OPEN_TRADES: return
        size = int(calculate_position_size(price) * settings["size_multiplier"])
        tp, sl = price * TP_PCT, price * SL_PCT
        state["open_trades"][symbol] = {"entry": price, "tp": tp, "sl": sl, "size": size, "time": time.time(), "score": score}
        save_state()
    
    phase_emoji = "🟡" if phase == "PRE" else ("🔵" if phase == "AFTER" else "🟢")
    msg = f"{phase_emoji} *EXPLOSION: {symbol}*\n💰 Entry: ${price:.2f}\n🎯 TP: ${tp:.2f}\n🛑 SL: ${sl:.2f}\n📦 Size: {size}\n📊 Score: {score}"
    send_telegram(msg)

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
    msg = f"{emoji} *CLOSED: {symbol}*\n📝 {reason}\n💵 Exit: ${price:.2f}\n📈 PnL: ${pnl:+.2f}"
    send_telegram(msg)

def update_trades():
    with state_lock:
        symbols = list(state["open_trades"].keys())
    for symbol in symbols:
        try:
            df = yf.Ticker(symbol).history(period='1d', interval='5m')
            if df.empty: continue
            price = df['Close'].iloc[-1]
            del df
            gc.collect()
            with state_lock:
                trade = state["open_trades"].get(symbol)
                if not trade: continue
                if price >= trade["tp"]: close_trade(symbol, price, "TAKE PROFIT")
                elif price <= trade["sl"]: close_trade(symbol, price, "STOP LOSS")
        except: pass

def background_scanner():
    while True:
        try:
            phase = get_market_phase()
            if phase != "CLOSED":
                with state_lock:
                    if not state["tickers"] or (state["last_ticker_update"] and (datetime.now() - datetime.fromisoformat(state["last_ticker_update"])).days >= 1):
                        update_all_tickers()
                    tickers = list(state["tickers"])
                
                chunk_size = 50
                for i in range(0, len(tickers), chunk_size):
                    chunk = tickers[i:i+chunk_size]
                    
                    filtered = []
                    with ThreadPoolExecutor(max_workers=3) as executor:
                        future_to_sym = {executor.submit(fast_filter, sym): sym for sym in chunk}
                        for future in as_completed(future_to_sym):
                            if future.result():
                                filtered.append(future_to_sym[future])
                    
                    if filtered:
                        with ThreadPoolExecutor(max_workers=2) as executor:
                            executor.map(process_symbol, filtered)
                    
                    gc.collect()
                    time.sleep(5)
                
                logger.info(f"✅ Scan complete: {len(tickers)} stocks")
            time.sleep(SCAN_INTERVAL_SEC)
        except Exception as e:
            logger.error(f"Scanner error: {e}")
            time.sleep(60)

def background_monitor():
    while True:
        try:
            if get_market_phase() != "CLOSED":
                update_trades()
            time.sleep(TRADE_MONITOR_INTERVAL)
        except: pass

def generate_analysis_message(symbol):
    for attempt in range(2):
        try:
            df = yf.download(symbol, period="10d", interval="15m", progress=False)
            if df.empty:
                if attempt == 0:
                    time.sleep(1)
                    continue
                return f"⚠️ No data for {symbol}"
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
            msg = f"📊 *{symbol} Analysis*\n💰 Price: ${last['close']:.2f}\n📊 Volume: {int(last['volume']):,}\n⚡ RSI: {last['rsi']:.1f}\n🧠 Score: {score}/{settings['min_score']}\n🕐 Phase: {phase}"
            return msg
        except Exception as e:
            if attempt == 0:
                time.sleep(1)
                continue
            return f"❌ Error: {e}"

if bot:
    @bot.message_handler(commands=['start'])
    def cmd_start(message):
        welcome = "🚀 *AI Trading Bot V33 - Stable*\n📊 100 active stocks\n\n/status - Bot status\n/positions - Open trades\n/scan <symbol> - Analyze\n/close <symbol> - Close trade"
        bot.reply_to(message, welcome, parse_mode='Markdown')

    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        reset_daily_loss_if_needed()
        phase = get_market_phase()
        with state_lock:
            perf = state["performance"]
            total = perf["wins"] + perf["losses"]
            wr = (perf["wins"] / total * 100) if total > 0 else 0
            msg = f"📊 *Bot Status (V33)*\n🕐 Phase: {phase}\n✅ Wins: {perf['wins']}\n❌ Losses: {perf['losses']}\n📈 Win Rate: {wr:.1f}%\n💵 PnL: ${perf['total_pnl']:+.2f}\n📦 Open: {len(state['open_trades'])}/{MAX_OPEN_TRADES}\n📉 Daily Loss: ${state.get('daily_loss', 0):.2f}\n🌐 Universe: {len(state['tickers'])} stocks"
        bot.reply_to(message, msg, parse_mode='Markdown')

    @bot.message_handler(commands=['positions'])
    def cmd_positions(message):
        with state_lock:
            trades = state["open_trades"]
            if not trades: bot.reply_to(message, "📭 No open positions"); return
            msg = "*Open Positions*\n"
            for sym, t in trades.items():
                msg += f"\n🔹 *{sym}* | Entry ${t['entry']:.2f} | TP ${t['tp']:.2f} | SL ${t['sl']:.2f} | Size {t['size']}"
        bot.reply_to(message, msg, parse_mode='Markdown')

    @bot.message_handler(commands=['scan'])
    def cmd_scan(message):
        parts = message.text.split()
        if len(parts) != 2: bot.reply_to(message, "⚠️ Usage: /scan <SYMBOL>"); return
        symbol = parts[1].upper()
        msg = bot.reply_to(message, f"🔍 Analyzing {symbol}...")
        analysis = generate_analysis_message(symbol)
        bot.edit_message_text(analysis, chat_id=message.chat.id, message_id=msg.message_id, parse_mode='Markdown')

    @bot.message_handler(commands=['close'])
    def cmd_close(message):
        parts = message.text.split()
        if len(parts) != 2: bot.reply_to(message, "⚠️ Usage: /close <SYMBOL>"); return
        symbol = parts[1].upper()
        with state_lock:
            if symbol not in state["open_trades"]: bot.reply_to(message, f"📭 No open trade for {symbol}"); return
        try:
            df = yf.download(symbol, period='1d', interval='5m', progress=False)
            if df.empty: bot.reply_to(message, f"⚠️ Cannot fetch price"); return
            price = df['Close'].iloc[-1]
            close_trade(symbol, price, "MANUAL")
            bot.reply_to(message, f"✅ Closed {symbol} at ${price:.2f}")
        except: bot.reply_to(message, f"❌ Error closing {symbol}")

if __name__ == "__main__":
    load_state()
    reset_daily_loss_if_needed()
    
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    
    if bot:
        threading.Thread(target=lambda: bot.infinity_polling(), daemon=True).start()
        send_telegram("✅ *Bot V33 Started - STABLE MODE*\n📊 100 active stocks\n✅ /scan, /status, /positions, /close")
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

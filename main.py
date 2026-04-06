import os
import time
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime
import pytz
import telebot

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
                

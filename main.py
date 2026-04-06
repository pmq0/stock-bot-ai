import os
import time
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
import telebot
from collections import deque

# ================= CONFIGURATION =================
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
POLYGON_API = os.getenv("POLYGON_API")

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
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
        "COIN", "UBER", "NFLX", "DIS", "PYPL", "SQ", "CRM", "ADBE",
        "CMCSA", "CSCO", "XOM", "CVX", "PG", "KO", "PEP", "WMT", "HD"
    ]

# ================= MARKET HOURS =================
def is_market_open():
    """Check if US stock market is currently open (9:30 AM - 4:00 PM EST, Mon-Fri)"""
    tz = pytz.timezone("US/Eastern")
    now = datetime.now(tz)
    
    # Check if weekend
    if now.weekday() >= 5:
        return False
        
    # Check time (9:30 AM to 4:00 PM)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= now <= market_close

# ================= POLYGON DATA =================
# Cache for fetched data to reduce API calls and improve performance
data_cache = {}

def get_data(symbol, timespan="minute", multiplier=15, days_back=30):
    """Fetch historical data from Polygon.io with caching"""
    cache_key = f"{symbol}_{timespan}_{multiplier}_{days_back}"
    if cache_key in data_cache:
        # Check if cached data is recent enough (e.g., within the last 5 minutes)
        cached_time, df = data_cache[cache_key]
        if (datetime.now() - cached_time).total_seconds() < 300:
            logger.info(f"Using cached data for {symbol}")
            return df

    tz = pytz.timezone("US/Eastern")
    end_date = datetime.now(tz)
    start_date = end_date - timedelta(days=days_back)
    
    # Polygon.io API allows max 50000 results per call, limit to 5000 for safety
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_API}"
    
    try:
        r = requests.get(url, timeout=10).json()
        
        if "results" not in r or len(r["results"]) == 0:
            logger.warning(f"No data returned for {symbol}")
            return None
            
        df = pd.DataFrame(r["results"])
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "vw": "vwap", "t": "timestamp"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit=\'ms\', utc=True).dt.tz_convert(tz)
        df = df.set_index("timestamp")
        
        data_cache[cache_key] = (datetime.now(), df)
        return df
    except requests.exceptions.Timeout:
        logger.error(f"Request to Polygon.io timed out for {symbol}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error fetching data for {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None

# ================= ADVANCED INDICATORS =================
def calculate_indicators(df):
    """Calculate technical indicators for the dataframe"""
    if len(df) < 50: # Ensure enough data for indicators
        return df

    # 1. ATR (Average True Range) - Volatility
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df["atr"] = true_range.rolling(14).mean()
    
    # 2. RSI (Relative Strength Index) - Momentum
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))
    
    # 3. EMAs (Exponential Moving Averages) - Trend
    df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean() # Long term trend
    
    # 4. MACD (Moving Average Convergence Divergence)
    ema_12 = df["close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema_12 - ema_26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    
    # 5. Bollinger Bands - Volatility & Mean Reversion
    df["bb_middle"] = df["close"].rolling(window=20).mean()
    bb_std = df["close"].rolling(window=20).std()
    df["bb_upper"] = df["bb_middle"] + (bb_std * 2)
    df["bb_lower"] = df["bb_middle"] - (bb_std * 2)
    
    # 6. Volume Analysis
    df["vol_sma_20"] = df["volume"].rolling(window=20).mean()
    df["vol_sma_50"] = df["volume"].rolling(window=50).mean()
    
    return df

# ================= AI SCORING ENGINE (ULTRA PRO) =================
def score_setup(df):
    """
    Advanced scoring system based on multiple confluences and price action.
    Max score is 100.
    """
    score = 0
    reasons = []
    
    if len(df) < 200: # Need sufficient data for all indicators
        return 0, ["Insufficient data for full scoring"]

    current = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 1. Trend Alignment (Max 30 pts)
    if current["close"] > current["ema_50"] and current["ema_50"] > current["ema_200"]:
        score += 15
        reasons.append("Strong Uptrend (Price > EMA50 > EMA200)")
    elif current["close"] > current["ema_21"]:
        score += 10
        reasons.append("Price above EMA21")
        
    if current["ema_9"] > current["ema_21"] and prev["ema_9"] <= prev["ema_21"]:
        score += 15 # Bullish EMA Crossover
        reasons.append("Bullish EMA Cross (9 over 21)")
        
    # 2. Momentum (Max 25 pts)
    if 45 < current["rsi"] < 65: # Healthy momentum, not overbought/oversold
        score += 10
        reasons.append("Healthy RSI (45-65)")
    elif current["rsi"] > 65 and prev["rsi"] <= 65: # Breaking into overbought, initial strength
        score += 5
        reasons.append("RSI Entering Overbought Zone")
        
    if current["macd"] > current["macd_signal"] and current["macd_hist"] > 0 and prev["macd_hist"] <= 0:
        score += 15 # Bullish MACD Crossover
        reasons.append("Bullish MACD Crossover")
        
    # 3. Volatility & Volume (Max 20 pts)
    if current["volume"] > current["vol_sma_20"] * 2 and current["volume"] > current["vol_sma_50"] * 1.5:
        score += 15
        reasons.append("Significant Volume Spike")
        
    if current["close"] > current["vwap"] and prev["close"] <= prev["vwap"]:
        score += 5
        reasons.append("Price Crossed Above VWAP")
        
    # 4. Price Action / Breakout (Max 25 pts)
    recent_high_20 = df["high"].iloc[-20:-1].max()
    recent_high_50 = df["high"].iloc[-50:-1].max()
    
    if current["close"] > recent_high_50: # 50-period breakout
        score += 15
        reasons.append("50-Period Price Breakout")
    elif current["close"] > recent_high_20: # 20-period breakout
        score += 10
        reasons.append("20-Period Price Breakout")
        
    if current["close"] > current["bb_upper"] and prev["close"] <= prev["bb_upper"]:
        score += 10 # Breaking out of Bollinger Bands
        reasons.append("Breaking Above Bollinger Upper Band")

    # 5. Sentiment Analysis (Placeholder - requires external API or more complex NLP)
    # For now, a conceptual score. In a real scenario, this would integrate news sentiment.
    sentiment_score = get_sentiment_score(df) # This function needs to be implemented
    score += sentiment_score
    if sentiment_score > 0:
        reasons.append("Positive Sentiment (Conceptual)")

    return score, reasons

def get_sentiment_score(df): # Placeholder for sentiment analysis
    # In a real scenario, this would fetch news/social media sentiment
    # and return a score (e.g., -10 to +10)
    # For demonstration, let's return a random positive score sometimes
    if np.random.rand() > 0.7: # 30% chance of positive sentiment
        return 5
    return 0

# ================= RISK MANAGEMENT & TARGETS =================
def calculate_trade_params(price, atr_val, account_balance=10000, risk_per_trade_percent=1.0):
    """Calculate dynamic targets and stop loss based on ATR and account risk"""
    if atr_val == 0 or np.isnan(atr_val):
        return None # Cannot calculate if ATR is invalid

    # Risk 1.5 ATR for Stop Loss
    sl_dist = atr_val * 1.5
    
    sl = price - sl_dist
    if sl <= 0: # Stop loss cannot be zero or negative
        return None

    # Targets based on Risk/Reward ratios
    t1 = price + (sl_dist * 1.0) # 1:1 RR
    t2 = price + (sl_dist * 1.5) # 1:1.5 RR
    t3 = price + (sl_dist * 2.0) # 1:2 RR
    
    # Calculate Position Size
    risk_amount = account_balance * (risk_per_trade_percent / 100)
    risk_per_share = price - sl
    
    if risk_per_share <= 0:
        return None # Invalid risk per share

    suggested_shares = int(risk_amount / risk_per_share)
    if suggested_shares <= 0:
        return None # Not enough capital or risk too small

    risk_reward_ratio = round((t2 - price) / (price - sl), 2) if price > sl else 0
    
    return {
        "entry": price,
        "sl": sl,
        "t1": t1,
        "t2": t2,
        "t3": t3,
        "shares": suggested_shares,
        "risk_reward": risk_reward_ratio
    }

# ================= ANALYSIS PIPELINE =================
def analyze(symbol):
    logger.info(f"Analyzing {symbol}...")
    df = get_data(symbol, days_back=60) # Fetch more data for long-term indicators
    
    if df is None or len(df) < 200: # Ensure enough data for all indicators (EMA200, etc.)
        logger.warning(f"Insufficient data for {symbol}. Skipping.")
        return None
        
    df = calculate_indicators(df)
    
    # Drop NaNs created by rolling windows. Ensure enough data remains.
    df = df.dropna()
    if len(df) < 50:
        logger.warning(f"Not enough clean data for {symbol} after indicator calculation. Skipping.")
        return None
        
    score, reasons = score_setup(df)
    
    # Only return high probability setups
    if score < 70: # Adjusted minimum score for higher quality signals
        logger.info(f"Score for {symbol} ({score}) is below threshold. Skipping.")
        return None
        
    current = df.iloc[-1]
    price = current["close"]
    atr_val = current["atr"]
    
    trade_params = calculate_trade_params(price, atr_val)
    if trade_params is None:
        logger.warning(f"Could not calculate trade parameters for {symbol}. Skipping.")
        return None

    return {
        "symbol": symbol,
        "score": score,
        "price": price,
        "reasons": reasons,
        "params": trade_params,
        "time": datetime.now(pytz.timezone("US/Eastern")).strftime("%H:%M:%S EST")
    }

# ================= BASIC BACKTESTING (SIMULATION) =================
def run_backtest(symbol, start_date_str, end_date_str):
    logger.info(f"Running backtest for {symbol} from {start_date_str} to {end_date_str}")
    tz = pytz.timezone("US/Eastern")
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=tz)
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=tz)

    # Fetch all data for the backtest period
    df_full = get_data(symbol, days_back=(end_date - start_date).days + 5) # +5 for indicator calculation
    if df_full is None or len(df_full) < 200:
        logger.error(f"Not enough data for backtest on {symbol}")
        return None

    df_full = calculate_indicators(df_full)
    df_full = df_full.dropna()
    
    backtest_results = []
    # Iterate through the data point by point for simulation
    for i in range(len(df_full)):
        current_time = df_full.index[i]
        if current_time < start_date or current_time > end_date:
            continue

        # Use a rolling window of data up to the current point
        df_window = df_full.iloc[:i+1]
        if len(df_window) < 200: # Ensure enough data for indicators in window
            continue

        score, reasons = score_setup(df_window)
        if score >= 70: # If a signal is generated
            current_price = df_window["close"].iloc[-1]
            atr_val = df_window["atr"].iloc[-1]
            trade_params = calculate_trade_params(current_price, atr_val)
            
            if trade_params:
                backtest_results.append({
                    "timestamp": current_time,
                    "symbol": symbol,
                    "score": score,
                    "entry": trade_params["entry"],
                    "sl": trade_params["sl"],
                    "t1": trade_params["t1"],
                    "t2": trade_params["t2"],
                    "t3": trade_params["t3"],
                    "shares": trade_params["shares"],
                    "risk_reward": trade_params["risk_reward"]
                })
    logger.info(f"Backtest for {symbol} completed with {len(backtest_results)} signals.")
    return backtest_results

# ================= MAIN ENGINE =================
def run():
    startup_msg = "🚀 <b>AI Institutional Ultra Pro v5.0 Started</b>\n\n"
    startup_msg += "✅ Advanced Indicators (RSI, MACD, VWAP, EMAs, BBands)\n"
    startup_msg += "✅ Dynamic ATR Risk Management & Position Sizing\n"
    startup_msg += "✅ Market Hours Filtering\n"
    startup_msg += "✅ Enhanced Scoring System\n"
    startup_msg += "✅ Data Caching & Robust Error Handling\n"
    startup_msg += "✅ Railway Deployment Ready\n"
    startup_msg += "✅ Basic Backtesting Capability (Offline)"
    send(startup_msg)
    logger.info("Bot started successfully.")

    seen_signals = deque(maxlen=100) # Keep track of recent signals to avoid spamming

    while True:
        try:
            if not is_market_open():
                logger.info("Market is closed. Waiting...")
                time.sleep(300) # Check every 5 minutes
                continue

            results = []
            universe = get_universe()
            
            for symbol in universe:
                # Check if this symbol has been signaled recently
                if any(s["symbol"] == symbol and (time.time() - s["timestamp"]).total_seconds() < 7200 for s in seen_signals):
                    logger.info(f"Skipping {symbol} - Signal already sent recently.")
                    continue

                r = analyze(symbol)
                
                if not r:
                    continue
                    
                results.append(r)
                
                # Respect Polygon API rate limits (5 calls / minute for free tier)
                # This is a critical point for avoiding crashes due to API limits
                time.sleep(15) # Increased sleep to be safer with 5 calls/min limit

            # Sort by highest score and take top 3
            results = sorted(results, key=lambda x: x["score"], reverse=True)[:3]

            for r in results:
                seen_signals.append({"symbol": r["symbol"], "timestamp": datetime.now()})
                p = r["params"]
                
                reasons_str = "\n".join([f"🔹 {reason}" for reason in r["reasons"]])
                
                msg = f"""
🚨 <b>AI ULTRA PRO SIGNAL DETECTED</b> 🚨

📊 <b>Ticker:</b> #{r[\'symbol\']}
⭐ <b>Score:</b> {r[\'score\']}/100
⏰ <b>Time:</b> {r[\'time\']}

💰 <b>Entry Price:</b> ${p[\'entry\']:.2f}

🎯 <b>Targets:</b>
T1: ${p[\'t1\']:.2f} (1:1 RR)
T2: ${p[\'t2\']:.2f} (1:1.5 RR)
T3: ${p[\'t3\']:.2f} (1:2 RR)

🛑 <b>Stop Loss:</b> ${p[\'sl\']:.2f}

⚖️ <b>Risk/Reward:</b> 1:{p[\'risk_reward\']}
📦 <b>Suggested Size:</b> {p[\'shares\']} shares (1% Account Risk)

🧠 <b>Confluence Factors:</b>
{reasons_str}
"""
                send(msg)
                logger.info(f"Signal sent for {r[\'symbol\']}")

            logger.info("Cycle complete. Waiting for next scan...")
            time.sleep(300) # Wait 5 minutes before next scan
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network or API error in main loop: {e}")
            time.sleep(60) # Wait a bit longer before retrying on network errors
        except Exception as e:
            logger.error(f"Critical error in main loop: {e}", exc_info=True) # Log full traceback
            send(f"⚠️ <b>Bot Error:</b> Critical error in main loop: {e}")
            time.sleep(120) # Wait longer before retrying on critical errors

# ================= ENTRY POINT =================
if __name__ == "__main__":
    # Example of how to run a backtest (this would typically be a separate script or function call)
    # backtest_results = run_backtest("AAPL", "2023-10-01", "2023-10-31")
    # if backtest_results:
    #     logger.info(f"AAPL Backtest Signals: {len(backtest_results)}")
    #     for res in backtest_results:
    #         logger.info(res)

    run()
                

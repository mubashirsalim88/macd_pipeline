import asyncio
import time
import pandas as pd
import queue
from datetime import datetime, timedelta
import pytz
from nsepython import nse_quote
from src.data_pipeline.fyers_websocket import FyersWebSocketClient
from src.data_pipeline.resampler import Resampler
from src.data_pipeline.storage import Storage
from src.data_pipeline.backfill import Backfill
from src.utils.logger import get_logger
from config.config import SYMBOLS_FILE

logger = get_logger(__name__)

async def is_market_open():
    now = datetime.now(pytz.timezone("Asia/Kolkata"))
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    is_open = market_open <= now <= market_close
    logger.info(f"Market status: {'Open' if is_open else 'Closed'} at {now}")
    return is_open

async def wait_for_market_open():
    now = datetime.now(pytz.timezone("Asia/Kolkata"))
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now < market_open:
        seconds_to_wait = (market_open - now).total_seconds()
        logger.info(f"Waiting {seconds_to_wait:.0f} seconds until market open at {market_open}")
        while seconds_to_wait > 0:
            print(f"Starting in {int(seconds_to_wait)} seconds...", end="\r")
            await asyncio.sleep(min(seconds_to_wait, 1))
            seconds_to_wait -= 1
        print(" " * 50, end="\r")
    logger.info("Market open. Starting pipeline.")

async def validate_historical_data(backfill, symbols, timeframes, lookback_days=7):
    market_open = pd.Timestamp.now(tz="Asia/Kolkata").replace(hour=9, minute=15, second=0)
    market_close = pd.Timestamp.now(tz="Asia/Kolkata").replace(hour=15, minute=30, second=0)
    yesterday = market_close - pd.Timedelta(days=1)
    for symbol in symbols:
        for tf in timeframes:
            gaps = backfill.check_data_gaps(
                symbol, tf,
                (yesterday - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S%z"),
                yesterday.strftime("%Y-%m-%d %H:%M:%S%z")
            )
            if gaps:
                logger.warning(f"Found {len(gaps)} gaps for {symbol} ({tf}). Triggering backfill.")
                await backfill.backfill_gaps(symbol, tf, gaps)

def validate_ohlcv(symbol, ohlcv_df):
    try:
        nse_data = nse_quote(symbol.split(":")[1].replace("-EQ", ""))
        if not isinstance(nse_data, dict):
            logger.warning(f"Invalid NSE data for {symbol}: {nse_data}")
            return False
        nse_ltp = nse_data.get("lastPrice")
        if nse_ltp and not ohlcv_df.empty:
            latest_ohlcv = ohlcv_df.iloc[-1]
            discrepancy = abs(latest_ohlcv["close"] - nse_ltp) / nse_ltp * 100
            logger.info(f"Validation for {symbol}: Discrepancy = {discrepancy:.4f}%")
            return discrepancy < 0.01
        logger.warning(f"No NSE LTP or OHLCV data for {symbol}")
        return False
    except Exception as e:
        logger.error(f"Validation failed for {symbol}: {e}")
        return False

async def test_pipeline(override_market_check: bool = False, test_symbol: str = "NSE:RELIANCE-EQ"):
    storage = Storage()
    backfill = Backfill()
    symbols = [test_symbol] if test_symbol else pd.read_csv(SYMBOLS_FILE)["symbol"].tolist()

    # Pre-market historical validation (9:00 AM)
    now = datetime.now(pytz.timezone("Asia/Kolkata"))
    pre_market_check = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now >= pre_market_check and now < now.replace(hour=9, minute=15, second=0):
        logger.info("Validating historical data before market open.")
        await validate_historical_data(backfill, symbols, ["15s", "30s", "1min", "3min", "5min"])

    # Wait for market open
    if not override_market_check:
        await wait_for_market_open()

    # Check if market is open
    if not await is_market_open() and not override_market_check:
        logger.error("Market is closed. Running backfill instead.")
        await backfill.backfill_all(lookback_days=7)
        return

    ws = FyersWebSocketClient()
    resampler = Resampler(ws.tick_queues, storage)

    try:
        # Start WebSocket in a separate thread
        ws_task = asyncio.create_task(asyncio.to_thread(ws.start))
        # Start Resampler
        resampler_task = asyncio.create_task(resampler.start())
        
        start_time = time.perf_counter()
        for i in range(300):  # Run for 5 minutes
            await asyncio.sleep(1)
            for symbol in symbols:
                queue_size = ws.tick_queues[symbol].qsize()
                ohlcv_df = resampler.ohlcv_data[symbol]["1s"]
                if queue_size > 0:
                    queue_contents = []
                    temp_queue = queue.Queue()
                    while not ws.tick_queues[symbol].empty():
                        item = ws.tick_queues[symbol].get()
                        queue_contents.append(item)
                        temp_queue.put(item)
                    while not temp_queue.empty():
                        ws.tick_queues[symbol].put(temp_queue.get())
                    logger.info(f"{symbol}: Queue size = {queue_size}, OHLCV rows = {len(ohlcv_df)}, Queue contents = {queue_contents}")
                
                if i % 30 == 0:
                    if queue_size == 0:
                        batch_size = 10
                        batch_index = ws.symbols.index(symbol) // batch_size
                        batch = ws.symbols[batch_index * batch_size:(batch_index + 1) * batch_size]
                        quotes = ws.fetch_quote_fallback(batch)
                        for quote in quotes:
                            if quote and quote["symbol"] == symbol:
                                ws.tick_queues[symbol].put(quote)
                                logger.info(f"Fallback quote for {symbol}: {quote}")
                    if not ohlcv_df.empty and validate_ohlcv(symbol, ohlcv_df):
                        logger.info(f"{symbol} validated successfully")
                    else:
                        logger.warning(f"{symbol} validation failed or no data")

        total_rows = sum(len(resampler.ohlcv_data[symbol]["1s"]) for symbol in ws.tick_queues)
        if total_rows == 0:
            logger.warning("No real-time data received. Running backfill.")
            await backfill.backfill_all(lookback_days=1)

        for symbol in ws.tick_queues:
            for tf in ["15s", "30s", "1min", "3min", "5min"]:
                ohlcv_df = resampler.ohlcv_data[symbol][tf]
                if not ohlcv_df.empty:
                    try:
                        start = time.perf_counter()
                        storage.save_ohlcv(symbol, ohlcv_df, tf)
                        latency = (time.perf_counter() - start) * 1000
                        logger.info(f"Storage latency for {symbol} ({tf}): {latency:.2f}ms")
                        if latency > 100:
                            logger.warning(f"Storage latency for {symbol} ({tf}) exceeds 100ms")
                    except Exception as e:
                        logger.error(f"Error saving OHLCV for {symbol} ({tf}): {e}", exc_info=True)

        elapsed = time.perf_counter() - start_time
        logger.info(f"Test completed in {elapsed:.2f} seconds")

    except Exception as e:
        logger.error(f"Test pipeline failed: {e}", exc_info=True)

    finally:
        resampler.stop()
        ws.stop()
        logger.info("Pipeline shutdown complete")

if __name__ == "__main__":
    asyncio.run(test_pipeline())
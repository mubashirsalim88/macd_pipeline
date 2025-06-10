import time
import pandas as pd
import queue
from datetime import datetime
import pytz
import asyncio
from typing import List
from nsepython import nse_quote
from src.data_pipeline.fyers_websocket import FyersWebSocketClient
from src.data_pipeline.resampler import Resampler
from src.data_pipeline.storage import Storage
from src.data_pipeline.backfill import Backfill
from src.utils.logger import get_logger

logger = get_logger(__name__)

def is_market_open():
    now = datetime.now(pytz.timezone("Asia/Kolkata"))
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    is_open = market_open <= now <= market_close
    logger.info(f"Market status: {'Open' if is_open else 'Closed'} at {now}")
    return is_open

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
        else:
            logger.warning(f"No NSE LTP or OHLCV data for {symbol}")
            return False
    except Exception as e:
        logger.error(f"Validation failed for {symbol}: {e}")
        return False

def test_pipeline(override_market_check: bool = False):
    storage = Storage()
    if not is_market_open() and not override_market_check:
        logger.error("Market is closed. Running backfill instead.")
        backfill = Backfill()
        asyncio.run(backfill.backfill_all(lookback_days=1))
        return
    
    ws = FyersWebSocketClient()
    resampler = Resampler(ws.tick_queues, storage)
    
    try:
        ws.start()
        resampler.start()
        start_time = time.perf_counter()
        
        for i in range(300):
            time.sleep(1)
            for symbol in ws.tick_queues:
                queue_size = ws.tick_queues[symbol].qsize()
                ohlcv_df = resampler.ohlcv_data[symbol]["1s"]
                # Log queue contents only if non-empty
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
                    if queue_size == 0 and hasattr(ws, 'fetch_quote_fallback'):
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
            backfill = Backfill()
            asyncio.run(backfill.backfill_all(lookback_days=1))
        
        for symbol in ws.tick_queues:
            ohlcv_df = resampler.ohlcv_data[symbol]["1s"]
            if not ohlcv_df.empty:
                try:
                    start = time.perf_counter()
                    storage.save_ohlcv(symbol, ohlcv_df, "1s")
                    latency = (time.perf_counter() - start) * 1000
                    logger.info(f"Storage latency for {symbol}: {latency:.2f}ms")
                    if latency > 100:
                        logger.warning(f"Storage latency for {symbol} exceeds 100ms")
                except Exception as e:
                    logger.error(f"Error saving OHLCV for {symbol}: {e}")
        
        elapsed = time.perf_counter() - start_time
        logger.info(f"Test completed in {elapsed:.2f} seconds")
    
    except Exception as e:
        logger.error(f"Test pipeline failed: {e}")
    
    finally:
        resampler.stop()
        ws.stop()
        logger.info("Pipeline shutdown complete")

if __name__ == "__main__":
    test_pipeline()
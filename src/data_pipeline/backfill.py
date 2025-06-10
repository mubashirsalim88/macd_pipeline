import asyncio
import pandas as pd
import logging
import time
import os
from typing import List, Dict, Any, cast
from pathlib import Path
from fyers_apiv3 import fyersModel
import stat
from src.utils.config_loader import load_config
from src.utils.fyers_auth_ngrok import load_tokens
from src.data_pipeline.storage import Storage
from config.config import SYMBOLS_FILE

logger = logging.getLogger(__name__)

class Backfill:
    def __init__(self):
        self.config = load_config('config/config.yaml')
        self.client_id = self.config['fyers']['client_id']
        self.access_token = load_tokens()
        log_dir = Path("data/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        self.fyers = fyersModel.FyersModel(
            client_id=self.client_id,
            token=self.access_token,
            log_path=str(log_dir)
        )
        self.storage = Storage()
        self.symbols = pd.read_csv(SYMBOLS_FILE)['symbol'].tolist()
        self.base_path = Path(r"C:\Users\mubas\OneDrive\Desktop\macd_pipeline")
        self.storage_path = self.base_path / 'data/ticks/historical'
        self.data_pipeline_path = self.base_path / 'data/ticks/data_pipeline'
        try:
            os.makedirs(self.storage_path, exist_ok=True)
            os.makedirs(self.data_pipeline_path, exist_ok=True)
            logger.info(f"Ensured directories: {self.storage_path}, {self.data_pipeline_path}")
        except Exception as e:
            logger.error(f"Failed to create directories: {e}")
        # Clean up stray NSE files
        for path in [self.storage_path / "NSE", self.data_pipeline_path / "NSE"]:
            if path.exists():
                try:
                    if path.is_file():
                        path.unlink()
                        logger.info(f"Deleted stray NSE file at {path}")
                    elif path.is_dir():
                        import shutil
                        shutil.rmtree(path)
                        logger.info(f"Deleted stray NSE directory at {path}")
                except Exception as e:
                    logger.error(f"Failed to delete NSE entry: {e}")
        self.blacklist = {'NSE:UNITEDSPIRITS-EQ', 'NSE:ZOMATO-EQ'}
        valid_symbols = []
        for symbol in self.symbols:
            if symbol in self.blacklist:
                logger.warning(f"Skipping {symbol}")
                continue
            try:
                quote_response = self.fyers.quotes({"symbols": [symbol]})
                if isinstance(quote_response, dict) and quote_response.get('s') == 'ok':
                    valid_symbols.append(symbol)
                else:
                    logger.warning(f"Invalid symbol: {symbol}")
            except Exception as e:
                logger.error(f"Error validating {symbol}: {e}")
        self.symbols = valid_symbols
        logger.info(f"Validated {len(self.symbols)} symbols")

    def check_data_gaps(self, symbol: str, timeframe: str, expected_start: str, expected_end: str) -> List[Dict[str, str]]:
        try:
            df = self.storage.load_historical(symbol, timeframe)
            if df.empty:
                logger.warning(f"No data for {symbol} ({timeframe}). Full gap detected")
                return [{"start": expected_start, "end": expected_end}]
            df["timestamp"] = pd.to_datetime(df['timestamp'])
            start_ts = pd.Timestamp(expected_start, tz="Asia/Kolkata")
            end_ts = pd.Timestamp(expected_end, tz="Asia/Kolkata")
            gaps = []
            current_time = start_ts
            while current_time < end_ts:
                current_time += pd.Timedelta(timeframe)
                if not ((df['timestamp'] >= current_time) & (df['timestamp'] < current_time + pd.Timedelta(timeframe))).any():
                    gaps.append({
                        "start": current_time.strftime("%Y-%m-%d %H:%M:%S%z"),
                        "end": (current_time + pd.Timedelta(timeframe)).strftime("%Y-%m-%d %H:%M:%S%z")
                    })
            if gaps:
                logger.warning(f"Found {len(gaps)} gaps for {symbol} ({timeframe})")
            return gaps
        except Exception as e:
            logger.error(f"Error checking gaps for {symbol} ({timeframe}): {e}")
            return [{"start": expected_start, "end": expected_end}]

    async def fetch_historical_data(self, symbol: str, interval: int, lookback: int, today_only: bool = False) -> List[Dict[str, Any]]:
        try:
            if today_only:
                from_date = to_date = pd.Timestamp.now().strftime('%Y-%m-%d')
                periods = [(from_date, to_date)]
            else:
                to_date = pd.Timestamp.now(tz="Asia/Kolkata")
                from_date = to_date - pd.Timedelta(days=lookback)
                periods = []
                current_start = from_date
                while current_start < to_date:
                    current_end = min(current_start + pd.Timedelta(days=60), to_date)
                    periods.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                    current_start = current_end + pd.Timedelta(days=1)
            all_candles = []
            for start, end in periods:
                data = {
                    "symbol": symbol,
                    "resolution": str(interval),
                    "date_format": "1",
                    "range_from": start,
                    "range_to": end,
                    "cont_flag": True
                }
                response = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.fyers.history(data)
                )
                if isinstance(response, dict) and response.get('s') == 'ok':
                    candles = response.get('candles', [])
                    logger.info(f"Fetched {len(candles)} candles for {symbol} ({interval}) from {start} to {end}")
                    all_candles.extend(candles)
                else:
                    logger.error(f"Failed to fetch {symbol}: {response}")
                await asyncio.sleep(2)
            if all_candles:
                df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                duplicates = df['timestamp'].duplicated().sum()
                if duplicates:
                    logger.warning(f"Removed {duplicates} duplicate timestamps for {symbol}")
                    df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
                return cast(List[Dict[str, Any]], df.to_dict('records'))
            return []
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return []

    def save_to_h5(self, symbol: str, interval: int, candles: List[Dict[str, Any]]):
        if not candles:
            logger.warning(f"No data to save for {symbol} ({interval})")
            return
        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        interval_str = f"{interval}min" if interval >= 1 else f"{int(interval*60)}s"
        try:
            logger.debug(f"Saving {symbol} ({interval_str})")
            self.storage.save_historical(symbol, df, interval_str)
            file_path = self.storage_path / f"{interval_str}.h5"
            if os.path.exists(file_path):
                logger.info(f"Size: {os.path.getsize(file_path)} bytes")
            else:
                logger.error(f"File not found after save: {file_path}")
        except Exception as e:
            logger.error(f"Error saving {symbol} ({interval_str}): {e}")

    async def backfill_symbol(self, symbol: str, interval: int, lookback_days: int, today_only: bool = True, max_attempts: int = 3):
        for attempt in range(1, max_attempts + 1):
            try:
                candles = await self.fetch_historical_data(symbol, interval, lookback_days, today_only)
                if candles:
                    self.save_to_h5(symbol, interval, candles)
                    return
                logger.warning(f"No data to backfill for {symbol} ({interval})")
                return
            except Exception as e:
                logger.error(f"Attempt {attempt}/{max_attempts} failed for {symbol}: {e}")
                if attempt == max_attempts:
                    logger.error(f"Failed to backfill {symbol} after {max_attempts} attempts")
                    return
                else:
                    await asyncio.sleep(2 ** attempt)

    async def backfill_gaps(self, symbol: str, timeframe: str, gaps: List[Dict[str, str]]):
        try:
            if timeframe in ["15s", "30s"]:
                for gap in gaps:
                    start = pd.Timestamp(gap["start"]).strftime("%Y-%m-%d")
                    end = pd.Timestamp(gap["end"]).strftime("%Y-%m-%d")
                    data = {
                        "symbol": symbol,
                        "resolution": "1",
                        "date_format": "1",
                        "range_from": start,
                        "range_to": end,
                        "cont_flag": True
                    }
                    response = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: self.fyers.history(data)
                    )
                    if isinstance(response, dict) and response.get('s') == 'ok':
                        candles = response.get('candles', [])
                        if candles:
                            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                            df["timestamp"] = pd.to_datetime(df['timestamp'], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
                            duplicates = df['timestamp'].duplicated().sum()
                            if duplicates:
                                logger.warning(f"Found {duplicates} duplicates for {symbol} ({timeframe}): {df[df['timestamp'].duplicated()]['timestamp'].tolist()}")
                                df = df.drop_duplicates(subset=['timestamp'])
                            try:
                                df = df.set_index("timestamp").resample(timeframe).ffill().reset_index()
                                self.storage.save_historical(symbol, df, timeframe)
                                logger.info(f"Backfilled {symbol} ({timeframe}) for gap {gap['start']} to {gap['end']}")
                            except Exception as e:
                                logger.error(f"Failed to resample {symbol} ({timeframe}): {e}")
                        else:
                            logger.warning(f"No 1min data for {symbol} ({timeframe})")
                    else:
                        logger.error(f"Failed to fetch 1min data for {symbol}: {response}")
            else:
                interval = int(pd.Timedelta(timeframe).total_seconds() / 60)
                for gap in gaps:
                    start = pd.Timestamp(gap["start"]).strftime("%Y-%m-%d")
                    end = pd.Timestamp(gap["end"]).strftime("%Y-%m-%d")
                    data = {
                        "symbol": symbol,
                        "resolution": str(interval),
                        "date_format": "1",
                        "range_from": start,
                        "range_to": end,
                        "cont_flag": True
                    }
                    response = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: self.fyers.history(data)
                    )
                    if isinstance(response, dict) and response.get('s') == 'ok':
                        candles = response.get('candles', [])
                        if candles:
                            self.save_to_h5(symbol, interval, candles)
                            logger.info(f"Backfilled {symbol} ({timeframe}) for gap {gap['start']} to {gap['end']}")
                        else:
                            logger.warning(f"No data for {symbol} ({timeframe})")
                    else:
                        logger.error(f"Failed to backfill {symbol} ({timeframe}): {response}")
        except Exception as e:
            logger.error(f"Error backfilling gaps for {symbol} ({timeframe}): {e}")

    async def backfill_all(self, interval: int = 1, lookback_days: int = 1, today_only: bool = False):
        api_calls = 0
        start_time = time.time()
        market_open = pd.Timestamp.now(tz="Asia/Kolkata").replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = pd.Timestamp.now(tz="Asia/Kolkata").replace(hour=15, minute=30, second=0, microsecond=0)
        test_symbols = ["NSE:RELIANCE-EQ"]
        for symbol in test_symbols:
            await self.validate_token()
            for tf in ["15s", "30s", "1min", "3min", "5min"]:
                gaps = self.check_data_gaps(symbol, tf, market_open.strftime("%Y-%m-%d %H:%M:%S%z"), market_close.strftime("%Y-%m-%d %H:%M:%S%z"))
                if gaps:
                    await self.backfill_gaps(symbol, tf, gaps)
            if not today_only:
                for interval in [1, 3, 5]:
                    await self.backfill_symbol(symbol, interval, lookback_days)
            api_calls += 3
            if api_calls >= 30:
                elapsed = time.time() - start_time
                logger.info(f"Processed {api_calls} API calls in {elapsed:.2f}s")
                if elapsed < 60:
                    logger.warning("Approaching rate limit. Sleeping for 60s")
                    await asyncio.sleep(60)
                api_calls = 0
                start_time = time.time()
            await asyncio.sleep(5)

    async def validate_token(self, max_attempts: int = 3):
        for attempt in range(1, max_attempts + 1):
            try:
                quote_response = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.fyers.quotes({"symbols": ["NSE:RELIANCE-EQ"]})
                )
                if isinstance(quote_response, dict) and quote_response.get('s') == 'ok':
                    logger.info("Fetching quotes successful")
                    return
                logger.error(f"Token validation failed: {quote_response}")
                self.access_token = load_tokens()
                log_dir = Path("data/logs")
                log_dir.mkdir(parents=True, exist_ok=True)
                self.fyers = fyersModel.FyersModel(
                    client_id=self.client_id,
                    token=self.access_token,
                    log_path=str(log_dir)
                )
                quote_response = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.fyers.quotes({"symbols": ["NSE:RELIANCE-EQ"]})
                )
                if isinstance(quote_response, dict) and quote_response.get('s') == 'ok':
                    logger.info("Token refreshed successfully")
                    return
                logger.error(f"Token refresh failed: {quote_response}")
                raise RuntimeError("Unable to validate or refresh token")
            except Exception as e:
                logger.error(f"Error validating token: {e}")
                if attempt == max_attempts:
                    raise RuntimeError(f"Token validation failed after {max_attempts} attempts: {e}")
                await asyncio.sleep(2 ** attempt)

if __name__ == "__main__":
    backfill = Backfill()
    try:
        asyncio.run(backfill.backfill_all(lookback_days=600))
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
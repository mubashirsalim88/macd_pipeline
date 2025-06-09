# src/data_pipeline/backfill.py
import asyncio
import logging
import time
import os
import stat
from typing import List, Dict, Any
import pandas as pd
from fyers_apiv3 import fyersModel
from src.utils.config_loader import load_config
from src.utils.fyers_auth_ngrok import load_tokens
from pathlib import Path
from src.data_pipeline.storage import Storage

logging.basicConfig(level=logging.INFO)
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
            log_path=str(log_dir) + "/"
        )
        self.storage = Storage()
        self.symbols = pd.read_csv(self.config['symbols']['file'])["symbol"].tolist()
        self.base_path = Path(r"C:\Users\mubas\OneDrive\Desktop\macd_pipeline")
        self.storage_path = self.base_path / 'data/ticks/historical/'
        self.storage_path.mkdir(parents=True, exist_ok=True)
        # Clean up stray NSE file
        nse_path = self.storage_path / "NSE"
        if nse_path.exists() and nse_path.is_file():
            try:
                nse_path.unlink()
                logger.info("Deleted stray NSE file")
            except Exception as e:
                logger.error(f"Failed to delete NSE file: {e}")
        logger.info(f"Created storage directory: {self.storage_path}")
        self.blacklist = {'NSE:UNITEDSPIRITS-EQ', 'NSE:ZOMATO-EQ'}
        valid_symbols = []
        for symbol in self.symbols:
            if symbol in self.blacklist:
                logger.warning(f"Skipping blacklisted symbol: {symbol}")
                continue
            try:
                quote_response = self.fyers.quotes({"symbols": [symbol]})
                if isinstance(quote_response, dict) and quote_response.get('s') == 'ok':
                    valid_symbols.append(symbol)
                else:
                    logger.warning(f"Invalid symbol: {symbol}")
            except Exception as e:
                logger.error(f"Error validating symbol {symbol}: {e}")
        self.symbols = valid_symbols
        logger.info(f"Validated {len(self.symbols)} symbols")

    async def fetch_historical_data(self, symbol: str, interval: int, lookback_days: int, today_only: bool = False) -> List[Dict[str, Any]]:
        try:
            if today_only:
                from_date = to_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            else:
                to_date = pd.Timestamp.now().strftime('%Y-%m-%d')
                from_date = (pd.Timestamp.now() - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            data = {
                "symbol": symbol,
                "resolution": str(interval),
                "date_format": "1",
                "range_from": from_date,
                "range_to": to_date,
                "cont_flag": "1"
            }
            response = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.fyers.history(data)
            )
            if isinstance(response, dict) and response.get('s') == 'ok':
                logger.info(f"Fetched {len(response.get('candles', []))} historical candles for {symbol} ({interval})")
                return response.get('candles', [])
            else:
                logger.error(f"Failed to fetch historical data for {symbol}: {response}")
                return []
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
            return []

    def save_to_h5(self, symbol: str, interval: int, candles: List[Dict[str, Any]]):
        if not candles:
            logger.warning(f"No data to save for {symbol} ({interval})")
            return
        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        interval_str = f"{interval}min"
        try:
            logger.debug(f"Attempting to save {symbol}_{interval_str}.h5 to {self.storage_path}")
            self.storage.save_historical(symbol, df, interval_str)
            file_path = self.storage_path / f"{symbol}_{interval_str}.h5"
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                file_stat = os.stat(file_path)
                logger.info(f"Verified {file_path}: Size {file_size} bytes, Mode {stat.filemode(file_stat.st_mode)}")
            else:
                logger.warning(f"File {file_path} not found after save")
            dir_contents = [f.name for f in self.storage_path.glob("*.h5")]
            logger.info(f"Directory contents (glob): {dir_contents}")
            all_entries = os.listdir(self.storage_path)
            logger.info(f"Directory contents (listdir): {all_entries}")
            nse_path = self.storage_path / "NSE"
            if nse_path.exists():
                logger.warning(f"NSE entry found: File={nse_path.is_file()}, Dir={nse_path.is_dir()}")
        except Exception as e:
            logger.error(f"Error saving historical data for {symbol}: {e}")

    async def backfill_symbol(self, symbol: str, interval: int, lookback_days: int, today_only: bool = False, max_attempts: int = 3):
        for attempt in range(1, max_attempts + 1):
            try:
                candles = await self.fetch_historical_data(symbol, interval, lookback_days, today_only)
                if candles:
                    self.save_to_h5(symbol, interval, candles)
                    return
                else:
                    logger.warning(f"No data to backfill for {symbol} ({interval})")
                    return
            except Exception as e:
                logger.error(f"Attempt {attempt}/{max_attempts} failed for {symbol}: {e}")
                if attempt == max_attempts:
                    logger.error(f"Failed to backfill {symbol} after {max_attempts} attempts")
                    logger.warning(f"No data to backfill for {symbol} ({interval})")
                else:
                    await asyncio.sleep(2 ** attempt)

    async def backfill_all(self, interval: int = 1, lookback_days: int = 90, today_only: bool = False):
        api_calls = 0
        start_time = time.time()
        for symbol in self.symbols:
            await self.validate_token()
            await self.backfill_symbol(symbol, interval, lookback_days, today_only)
            api_calls += 1
            if api_calls % 30 == 0:  # Adjusted for potential stricter Fyers API limits
                elapsed = time.time() - start_time
                logger.info(f"Processed {api_calls} symbols in {elapsed:.2f}s")
                if elapsed < 60 and api_calls >= 30:
                    logger.warning("Approaching rate limit. Sleeping for 60s.")
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
                    logger.info("Token validated successfully")
                    return
                else:
                    logger.error(f"Token validation failed: {quote_response}")
                    logger.info("Attempting to refresh token...")
                    self.access_token = load_tokens()
                    log_dir = Path("data/logs")
                    log_dir.mkdir(parents=True, exist_ok=True)
                    self.fyers = fyersModel.FyersModel(
                        client_id=self.client_id,
                        token=self.access_token,
                        log_path=str(log_dir) + "/"
                    )
                    quote_response = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: self.fyers.quotes({"symbols": ["NSE:RELIANCE-EQ"]})
                    )
                    if isinstance(quote_response, dict) and quote_response.get('s') == 'ok':
                        logger.info("Token refreshed and validated successfully")
                        return
                    else:
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
        asyncio.run(backfill.backfill_all(lookback_days=1))
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
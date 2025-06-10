import pandas as pd
import os
import time
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)

class Storage:
    def __init__(self):
        self.base_path = Path(r"C:\macd_pipeline")
        self.tick_path = self.base_path / 'data/ticks/data_pipeline'
        self.historical_path = self.base_path / 'data/ticks/historical'
        self.indicators_path = self.base_path / 'data/indicators'
        self.tick_path.mkdir(parents=True, exist_ok=True)
        self.historical_path.mkdir(parents=True, exist_ok=True)
        self.indicators_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using local storage. Tick path: {self.tick_path}, Historical path: {self.historical_path}, Indicators path: {self.indicators_path}")
        logger.debug(f"Current working directory: {os.getcwd()}")
        self._log_directory_contents()

    def _log_directory_contents(self):
        """Log contents of historical, tick, and indicators directories for debugging."""
        try:
            historical_files = [f.name for f in self.historical_path.glob("*")]
            tick_files = [f.name for f in self.tick_path.glob("*")]
            indicators_files = [f.name for f in self.indicators_path.glob("*")]
            logger.debug(f"Historical directory contents (glob): {historical_files}")
            logger.debug(f"Tick directory contents (glob): {tick_files}")
            logger.debug(f"Indicators directory contents (glob): {indicators_files}")
            historical_listdir = os.listdir(self.historical_path)
            tick_listdir = os.listdir(self.tick_path)
            indicators_listdir = os.listdir(self.indicators_path)
            logger.debug(f"Historical directory contents (listdir): {historical_listdir}")
            logger.debug(f"Tick directory contents (listdir): {tick_listdir}")
            logger.debug(f"Indicators directory contents (listdir): {indicators_listdir}")
        except Exception as e:
            logger.error(f"Error logging directory contents: {e}")

    def save_historical(self, symbol: str, df: pd.DataFrame, timeframe: str):
        try:
            file_path = self.historical_path / f"{symbol}_{timeframe}.h5"
            logger.debug(f"Writing to {file_path}, exists before: {file_path.exists()}")
            test_path = file_path.with_suffix('.txt')
            with open(test_path, 'w') as f:
                f.write(f"Test write for {symbol}_{timeframe} at {pd.Timestamp.now()}")
            logger.debug(f"Wrote test file: {test_path}")
            for attempt in range(1, 4):
                try:
                    df.to_hdf(file_path, key='ohlcv', mode='w', format='table')
                    with open(file_path, 'a') as f:
                        os.fsync(f.fileno())
                    logger.info(f"Saved historical for {symbol} to {file_path}")
                    if file_path.exists():
                        file_size = os.path.getsize(file_path)
                        logger.debug(f"Verified {file_path}: Size {file_size} bytes")
                    else:
                        logger.warning(f"File {file_path} not found after save")
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/3 failed for {file_path}: {e}")
                    if attempt == 3:
                        raise
                    time.sleep(2)
            self._log_directory_contents()
        except Exception as e:
            logger.error(f"Error saving historical data for {symbol}: {e}")
            self._log_directory_contents()

    def save_ohlcv(self, symbol: str, df: pd.DataFrame, timeframe: str):
        try:
            file_path = self.tick_path / f"{symbol}_{timeframe}.h5"
            logger.debug(f"Writing OHLCV to {file_path}, exists before: {file_path.exists()}")
            test_path = file_path.with_suffix('.txt')
            with open(test_path, 'w') as f:
                f.write(f"Test write for {symbol}_{timeframe} at {pd.Timestamp.now()}")
            logger.debug(f"Wrote test file: {test_path}")
            for attempt in range(1, 4):
                try:
                    df.to_hdf(file_path, key='ohlcv', mode='w', format='table')
                    with open(file_path, 'a') as f:
                        os.fsync(f.fileno())
                    logger.info(f"Saved OHLCV for {symbol} to {file_path}")
                    if file_path.exists():
                        file_size = os.path.getsize(file_path)
                        logger.debug(f"Verified {file_path}: Size {file_size} bytes")
                    else:
                        logger.warning(f"File {file_path} not found after save")
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/3 failed for {file_path}: {e}")
                    if attempt == 3:
                        raise
                    time.sleep(2)
            self._log_directory_contents()
        except Exception as e:
            logger.error(f"Error saving OHLCV for {symbol}: {e}")
            self._log_directory_contents()

    def save_indicators(self, symbol: str, df: pd.DataFrame, timeframe: str, indicator_type: str):
        try:
            date = pd.Timestamp.now().strftime("%Y%m%d")
            indicator_dir = self.indicators_path / indicator_type
            indicator_dir.mkdir(parents=True, exist_ok=True)
            file_path = indicator_dir / f"{symbol}_{timeframe}_{date}.h5"
            logger.debug(f"Writing indicators to {file_path}, exists before: {file_path.exists()}")
            test_path = file_path.with_suffix('.txt')
            with open(test_path, 'w') as f:
                f.write(f"Test write for {symbol}_{timeframe}_{indicator_type} at {pd.Timestamp.now()}")
            logger.debug(f"Wrote test file: {test_path}")
            for attempt in range(1, 4):
                try:
                    df.to_hdf(file_path, key=indicator_type, mode='w', format='table')
                    with open(file_path, 'a') as f:
                        os.fsync(f.fileno())
                    logger.info(f"Saved {indicator_type} for {symbol} ({timeframe}) to {file_path}")
                    if file_path.exists():
                        file_size = os.path.getsize(file_path)
                        logger.debug(f"Verified {file_path}: Size {file_size} bytes")
                    else:
                        logger.warning(f"File {file_path} not found after save")
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/3 failed for {file_path}: {e}")
                    if attempt == 3:
                        raise
                    time.sleep(2)
            self._log_directory_contents()
        except Exception as e:
            logger.error(f"Error saving {indicator_type} for {symbol}: {e}")
            self._log_directory_contents()

    def load_historical(self, symbol: str, timeframe: str) -> pd.DataFrame:
        try:
            file_path = self.historical_path / f"{symbol}_{timeframe}.h5"
            if file_path.exists():
                df = pd.read_hdf(file_path, key='ohlcv')
                if isinstance(df, pd.Series):
                    df = df.to_frame().T
                logger.debug(f"Loaded historical data for {symbol} ({timeframe}) from {file_path}")
                return df
            else:
                logger.error(f"Error loading historical data for {symbol} ({timeframe}): File {file_path} does not exist")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error loading historical data for {symbol} ({timeframe}): {e}")
            return pd.DataFrame()

    def trim_old_data(self, symbol: str, timeframe: str, retention_days: int):
        try:
            file_path = self.historical_path / f"{symbol}_{timeframe}.h5"
            if file_path.exists():
                df = pd.read_hdf(file_path, key='ohlcv')
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                cutoff = pd.Timestamp.now(tz=df["timestamp"].iloc[0].tz) - pd.Timedelta(days=retention_days)
                df = df[df["timestamp"] > cutoff]
                for attempt in range(1, 4):
                    try:
                        df.to_hdf(file_path, key='ohlcv', mode='w', format='table')
                        with open(file_path, 'a') as f:
                            os.fsync(f.fileno())
                        logger.info(f"Trimmed data for {symbol} ({timeframe}) before {cutoff}")
                        break
                    except Exception as e:
                        logger.warning(f"Attempt {attempt}/3 failed for {file_path}: {e}")
                        if attempt == 3:
                            raise
                        time.sleep(2)
                self._log_directory_contents()
            else:
                logger.warning(f"No data to trim for {symbol} ({timeframe})")
        except Exception as e:
            logger.error(f"Error trimming data for {symbol}: {e}")
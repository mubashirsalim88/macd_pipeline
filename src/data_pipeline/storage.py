import pandas as pd
import os
import time
from pathlib import Path
from threading import Lock
from src.utils.logger import get_logger

logger = get_logger(__name__)

class Storage:
    def __init__(self, csv_debug=False):
            self.base_path = Path(r"C:\Users\mubas\OneDrive\Desktop\macd_pipeline")
            self.tick_path = self.base_path / 'data/ticks/data_pipeline'
            self.historical_path = self.base_path / 'data/ticks/historical'
            self.indicators_path = self.base_path / 'data/indicators'
            for path in [self.tick_path, self.historical_path, self.indicators_path]:
                os.makedirs(path, exist_ok=True)
                logger.info(f"Ensured directory exists: {path}")
            self.lock = Lock()
            self.csv_debug = csv_debug

    def save_historical(self, symbol: str, df: pd.DataFrame, timeframe: str):
        if df.empty:
            logger.warning(f"Empty DataFrame for {symbol} ({timeframe}). Skipping save.")
            return
        file_path = self.historical_path / f"{timeframe}.h5"
        key = symbol.replace(":", "_")
        resolved_path = file_path.resolve()
        logger.debug(f"Saving {symbol} ({timeframe}) to {resolved_path}")

        # Validate and convert timestamps
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True, errors='coerce').dt.tz_convert('Asia/Kolkata')
            if df['timestamp'].isna().any():
                logger.error(f"Invalid timestamps found in {symbol} ({timeframe})")
                return
            logger.debug(f"New data timestamp range: {df['timestamp'].min()} to {df['timestamp'].max()}")

        # Write test file for debugging
        test_path = file_path.with_suffix('.txt')
        try:
            with open(test_path, 'w') as f:
                f.write(f"Test write for {symbol}_{timeframe} at {pd.Timestamp.now()}")
            logger.debug(f"Wrote test file: {test_path}")
        except Exception as e:
            logger.error(f"Failed to write test file {test_path}: {e}")

        # Fallback to CSV if enabled
        if self.csv_debug:
            csv_path = self.historical_path / f"{timeframe}.csv"
            try:
                df.to_csv(csv_path, mode='a', index=False)
                logger.info(f"Saved CSV for {symbol} ({timeframe}) to {csv_path}")
            except Exception as e:
                logger.error(f"Failed to save CSV {csv_path}: {e}")
            return

        # Save to HDF5
        with self.lock:
            for attempt in range(1, 4):
                try:
                    with pd.HDFStore(resolved_path, mode='a') as store:
                        if f"/{key}" in store:
                            existing_df = store[key]
                            if 'timestamp' in existing_df.columns:
                                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], utc=True, errors='coerce').dt.tz_convert('Asia/Kolkata')
                                logger.debug(f"Existing data timestamp range: {existing_df['timestamp'].min()} to {existing_df['timestamp'].max()}")
                                # Skip merge if new data fully covers existing range
                                if (df['timestamp'].min() <= existing_df['timestamp'].min() and 
                                    df['timestamp'].max() >= existing_df['timestamp'].max()):
                                    logger.info(f"New data covers existing range for {symbol} ({timeframe}). Overwriting.")
                                else:
                                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                                    duplicates = combined_df['timestamp'].duplicated().sum()
                                    if duplicates:
                                        logger.warning(f"Removed {duplicates} duplicates for {symbol} ({timeframe})")
                                        combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp')
                                    df = combined_df
                                    if df.empty:
                                        logger.info(f"No data after deduplication for {symbol} ({timeframe})")
                                        return
                            else:
                                logger.warning(f"No timestamp column in existing data for {symbol} ({timeframe})")
                        # Save (overwrite existing key)
                        store.put(key, df, format='table', data_columns=True)
                    logger.info(f"Saved historical for {symbol} ({timeframe}) to {resolved_path}, rows: {len(df)}")
                    if file_path.exists():
                        file_size = os.path.getsize(file_path)
                        logger.info(f"Verified {resolved_path}: Size {file_size} bytes")
                    else:
                        logger.error(f"File {resolved_path} not found after save")
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/3 failed for {resolved_path}: {e}")
                    if attempt == 3:
                        logger.error(f"Failed to save historical data for {symbol}: {e}")
                    time.sleep(2)

    def load_historical(self, symbol: str, timeframe: str) -> pd.DataFrame:
        file_path = self.historical_path / f"{timeframe}.h5"
        key = symbol.replace(":", "_")
        resolved_path = file_path.resolve()
        try:
            if file_path.exists():
                with pd.HDFStore(resolved_path, mode='r') as store:
                    if f"/{key}" in store:
                        df = store[key]
                        if isinstance(df, pd.Series):
                            logger.debug(f"Converting Series to DataFrame for {symbol} ({timeframe})")
                            df = df.to_frame().T
                        elif not isinstance(df, pd.DataFrame):
                            logger.warning(f"Unexpected data type {type(df)} for {symbol} ({timeframe})")
                            return pd.DataFrame()
                        # Ensure timestamp is timezone-aware
                        if 'timestamp' in df.columns:
                            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce').dt.tz_convert('Asia/Kolkata')
                            if df['timestamp'].isna().any():
                                logger.warning(f"Invalid timestamps in {symbol} ({timeframe})")
                        logger.debug(f"Loaded historical data for {symbol} ({timeframe}), rows: {len(df)}")
                        return df
                    else:
                        logger.debug(f"No data for {symbol} ({timeframe}) in {resolved_path}")
                        return pd.DataFrame()
            else:
                logger.debug(f"File {resolved_path} does not exist for {symbol} ({timeframe})")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error loading data for {symbol} ({timeframe}): {e}")
            return pd.DataFrame()

    def save_ohlcv(self, symbol: str, df: pd.DataFrame, timeframe: str):
        if df.empty:
            logger.warning(f"Empty OHLCV DataFrame for {symbol} ({timeframe}). Skipping save.")
            return
        file_path = self.historical_path / f"{timeframe}.h5"
        key = symbol.replace(":", "_")
        resolved_path = file_path.resolve()
        logger.debug(f"Saving OHLCV {symbol} ({timeframe}) to {resolved_path}, rows: {len(df)}")
        
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce').dt.tz_convert('Asia/Kolkata')
            if df['timestamp'].isna().any():
                logger.error(f"Invalid timestamps in OHLCV for {symbol} ({timeframe})")
                return
            logger.debug(f"Timestamp range: {df['timestamp'].min()} to {df['timestamp'].max()}")

        with self.lock:
            for attempt in range(1, 4):
                try:
                    with pd.HDFStore(resolved_path, mode='a') as store:
                        if f"/{key}" in store:
                            existing_df = store[key]
                            if 'timestamp' in existing_df.columns:
                                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], utc=True, errors='coerce').dt.tz_convert('Asia/Kolkata')
                                logger.debug(f"Existing data rows: {len(existing_df)}, Timestamp range: {existing_df['timestamp'].min()} to {existing_df['timestamp'].max()}")
                                combined_df = pd.concat([existing_df, df], ignore_index=True)
                                duplicates = combined_df['timestamp'].duplicated().sum()
                                if duplicates:
                                    logger.warning(f"Removed {duplicates} duplicates for {symbol} ({timeframe})")
                                    combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp')
                                df = combined_df
                        store.put(key, df, format='table', data_columns=True)
                    logger.info(f"Saved OHLCV for {symbol} ({timeframe}) to {resolved_path}, rows: {len(df)}")
                    if file_path.exists():
                        file_size = os.path.getsize(file_path)
                        logger.info(f"Verified {resolved_path}: Size {file_size} bytes")
                    break
                except Exception as e:
                    logger.error(f"Attempt {attempt}/3 failed for {resolved_path}: {e}", exc_info=True)
                    if attempt == 3:
                        logger.error(f"Failed to save OHLCV for {symbol}: {e}")

    def save_indicators(self, symbol: str, df: pd.DataFrame, timeframe: str, indicator_type: str):
        # Placeholder: Implement if needed
        pass

    def trim_old_data(self, symbol: str, timeframe: str, retention_days: int):
        file_path = self.historical_path / f"{timeframe}.h5"
        key = symbol.replace(":", "_")
        resolved_path = file_path.resolve()
        try:
            if file_path.exists():
                with pd.HDFStore(resolved_path, mode='a') as store:
                    if f"/{key}" in store:
                        df = store[key]
                        if df is not None and not df.empty and "timestamp" in df.columns:
                            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors='coerce').dt.tz_convert('Asia/Kolkata')
                            cutoff = pd.Timestamp.now(tz='Asia/Kolkata') - pd.Timedelta(days=retention_days)
                            df = df[df["timestamp"] > cutoff]
                            store.put(key, df, format='table', data_columns=True)
                            logger.info(f"Trimmed data for {symbol} ({timeframe}) before {cutoff}")
                        else:
                            logger.warning(f"No valid timestamp data for {symbol} ({timeframe})")
                    else:
                        logger.debug(f"No data to trim for {symbol} ({timeframe})")
            else:
                logger.debug(f"File {resolved_path} does not exist")
        except Exception as e:
            logger.error(f"Error trimming data for {symbol}: {e}")
# src/data_pipeline/storage.py
import pandas as pd
import boto3
import tempfile
from pathlib import Path
from filelock import FileLock
from src.utils.config_loader import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

class Storage:
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = load_config(config_path)
        self.storage_type = self.config.get("storage", {}).get("type", "local")
        self.base_path = Path(r"C:\Users\mubas\OneDrive\Desktop\macd_pipeline")
        self.local_dir = self.base_path / "data"
        self.s3_client = None
        self.bucket = None
        if self.storage_type == "s3":
            s3_config = self.config.get("storage", {}).get("s3", {})
            required_keys = ["access_key", "secret_key", "bucket", "region"]
            if not s3_config or not all(
                key in s3_config and isinstance(s3_config[key], str) and s3_config[key]
                for key in required_keys
            ):
                logger.warning("Incomplete or invalid S3 configuration. Falling back to local storage.")
                self.storage_type = "local"
            else:
                try:
                    region = s3_config.get("region", "us-east-1")
                    self.s3_client = boto3.client(
                        "s3",
                        aws_access_key_id=s3_config["access_key"],
                        aws_secret_access_key=s3_config["secret_key"],
                        region_name=region
                    )
                    self.bucket = s3_config["bucket"]
                    logger.info(f"Initialized S3 storage with bucket: {self.bucket}")
                except Exception as e:
                    logger.error(f"Failed to initialize S3 storage: {e}. Falling back to local storage.")
                    self.storage_type = "local"
        if self.storage_type == "local":
            logger.info("Using local storage.")

    def save_ohlcv(self, symbol: str, df: pd.DataFrame, timeframe: str | None = None):
        try:
            date = df["timestamp"].iloc[-1].strftime("%Y%m%d") if not df.empty else pd.Timestamp.now().strftime("%Y%m%d")
            key = f"ticks/data_pipeline/{symbol}_{timeframe or '1s'}_{date}.h5"
            if self.storage_type == "local":
                file_path = self.local_dir / key
                file_path.parent.mkdir(parents=True, exist_ok=True)
                lock_path = file_path.with_suffix('.lock')
                with FileLock(lock_path):
                    df.to_hdf(file_path, key="ohlcv", mode="w")
                logger.info(f"Saved OHLCV for {symbol} to {file_path}")
            elif self.s3_client and self.bucket:
                with tempfile.NamedTemporaryFile(suffix=".h5") as temp_file:
                    df.to_hdf(temp_file.name, key="ohlcv", mode="w")
                    self.s3_client.upload_file(temp_file.name, self.bucket, key)
                logger.info(f"Saved OHLCV for {symbol} to s3://{self.bucket}/{key}")
            else:
                logger.error("S3 storage not initialized. Cannot save OHLCV.")
        except Exception as e:
            logger.error(f"Error saving OHLCV for {symbol}: {e}")

    def save_historical(self, symbol: str, df: pd.DataFrame, timeframe: str):
        try:
            key = f"ticks/historical/{symbol}_{timeframe}.h5"
            file_path = self.local_dir / key
            file_path.parent.mkdir(parents=True, exist_ok=True)
            lock_path = file_path.with_suffix('.lock')
            with FileLock(lock_path):
                df.to_hdf(file_path, key="ohlcv", mode="w")
            logger.info(f"Saved historical for {symbol} to {file_path}")
            if file_path.exists():
                file_size = file_path.stat().st_size
                logger.debug(f"Verified {file_path}: Size {file_size} bytes")
            else:
                logger.warning(f"File {file_path} not found after save")
            if self.storage_type == "s3" and self.s3_client and self.bucket:
                with tempfile.NamedTemporaryFile(suffix=".h5") as temp_file:
                    df.to_hdf(temp_file.name, key="ohlcv", mode="w")
                    self.s3_client.upload_file(temp_file.name, self.bucket, key)
                logger.info(f"Saved historical for {symbol} to s3://{self.bucket}/{key}")
        except Exception as e:
            logger.error(f"Error saving historical for {symbol}: {e}")

    def save_indicators(self, symbol: str, df: pd.DataFrame, timeframe: str, indicator_type: str):
        try:
            date = pd.Timestamp.now().strftime("%Y%m%d")
            key = f"indicators/{indicator_type}/{symbol}_{timeframe}_{date}.h5"
            if self.storage_type == "local":
                file_path = self.local_dir / key
                file_path.parent.mkdir(parents=True, exist_ok=True)
                lock_path = file_path.with_suffix('.lock')
                with FileLock(lock_path):
                    df.to_hdf(file_path, key=indicator_type, mode="w")
                logger.info(f"Saved {indicator_type} for {symbol} to {file_path}")
            elif self.s3_client and self.bucket:
                with tempfile.NamedTemporaryFile(suffix=".h5") as temp_file:
                    df.to_hdf(temp_file.name, key=indicator_type, mode="w")
                    self.s3_client.upload_file(temp_file.name, self.bucket, key)
                logger.info(f"Saved {indicator_type} for {symbol} to s3://{self.bucket}/{key}")
            else:
                logger.error("S3 storage not initialized. Cannot save indicators.")
        except Exception as e:
            logger.error(f"Error saving {indicator_type} for {symbol}: {e}")

    def load_historical(self, symbol: str, timeframe: str) -> pd.DataFrame:
        try:
            key = f"ticks/historical/{symbol}_{timeframe}.h5"
            file_path = self.local_dir / key
            lock_file = file_path.with_suffix('.lock')
            with FileLock(lock_file):
                df = pd.read_hdf(file_path, key='ohlcv')  # type: ignore
            if not isinstance(df, pd.DataFrame):
                logger.error(f"Loaded data for {symbol} ({timeframe}) is not a DataFrame")
                return pd.DataFrame()
            logger.info(f"Loaded historical data for {symbol} ({timeframe}) from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading historical data for {symbol} ({timeframe}): {e}")
            return pd.DataFrame()
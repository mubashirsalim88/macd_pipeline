import pandas as pd
from pathlib import Path
from datetime import datetime
import pytz
from src.data_pipeline.storage import Storage
from src.utils.logger import get_logger
from config.config import TIMEFRAMES

logger = get_logger(__name__)

def download_stock_data(symbol: str, timeframes: list = TIMEFRAMES + ["1s"], output_dir: str = "data/exports"):
    """
    Download all OHLCV data (historical and live) for a single stock across all timeframes into separate sheets in an Excel file.
    Timestamps are saved as strings in IST with timezone (e.g., '2025-06-12 09:15:00+05:30').
    
    Args:
        symbol (str): Stock symbol, e.g., "NSE:RELIANCE-EQ".
        timeframes (list): List of timeframes, e.g., ["1s", "15s", "30s", "1min", "3min", "5min"].
        output_dir (str): Directory to save the Excel file.
    
    Returns:
        None: Saves data to an Excel file with one sheet per timeframe.
    """
    try:
        # Initialize storage
        storage = Storage()
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_path}")

        # Initialize dictionary to store DataFrames for each timeframe
        timeframe_data = {}
        
        # Load data for each timeframe
        for tf in timeframes:
            logger.info(f"Loading data for {symbol} ({tf})")
            df = storage.load_historical(symbol, tf)
            
            if df.empty:
                logger.warning(f"No data found for {symbol} ({tf})")
                continue
                
            # Ensure required columns
            required_cols = ["timestamp", "open", "high", "low", "close", "volume"]
            if not all(col in df.columns for col in required_cols):
                logger.error(f"Missing required columns in {symbol} ({tf}): {df.columns}")
                continue
                
            # Select only required columns
            df = df[required_cols].copy()
            
            # Ensure timestamps are Asia/Kolkata and convert to string with timezone
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_convert("Asia/Kolkata")
            if df["timestamp"].isna().any():
                logger.warning(f"Dropping {df['timestamp'].isna().sum()} rows with invalid timestamps for {symbol} ({tf})")
                df = df.dropna(subset=["timestamp"])
            
            # Convert timestamps to strings with +05:30
            df["timestamp"] = df["timestamp"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S%z"))
            
            # Remove duplicates and sort by timestamp
            duplicates = df.duplicated(subset=["timestamp"]).sum()
            if duplicates:
                logger.warning(f"Removed {duplicates} duplicate timestamps for {symbol} ({tf})")
                df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
            
            timeframe_data[tf] = df
            logger.info(f"Loaded {len(df)} rows for {symbol} ({tf})")
        
        if not timeframe_data:
            logger.error(f"No data loaded for {symbol} across any timeframe")
            return
            
        # Generate output filename with timestamp
        today = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d")
        output_filename = output_path / f"{symbol.replace(':', '_')}_{today}.xlsx"
        
        # Write to Excel with separate sheets
        with pd.ExcelWriter(output_filename, engine="openpyxl") as writer:
            for tf, df in timeframe_data.items():
                df.to_excel(writer, sheet_name=tf, index=False)
                logger.info(f"Wrote {len(df)} rows to sheet '{tf}' in {output_filename}")
        
        # Verify file
        file_size = output_filename.stat().st_size
        logger.info(f"Saved Excel file: {output_filename} (Size: {file_size} bytes)")
        
        # Log timestamp range for verification
        for tf in timeframes:
            if tf in timeframe_data and not timeframe_data[tf].empty:
                # Convert strings back to datetime for range calculation
                temp_timestamps = pd.to_datetime(timeframe_data[tf]["timestamp"], format="%Y-%m-%d %H:%M:%S%z")
                logger.info(f"{symbol} ({tf}) timestamp range: {temp_timestamps.min()} to {temp_timestamps.max()}")
    
    except Exception as e:
        logger.error(f"Error downloading data for {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    # Example usage
    stock_symbol = "NSE:RELIANCE-EQ"
    download_stock_data(stock_symbol)
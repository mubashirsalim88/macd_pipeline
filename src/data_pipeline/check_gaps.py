import asyncio
import pandas as pd
from datetime import datetime
import pytz
from src.data_pipeline.backfill import Backfill
from src.utils.logger import get_logger
from config.config import SYMBOLS_FILE

logger = get_logger(__name__)

async def check_todays_gaps():
    now = datetime.now(tz=pytz.timezone("Asia/Kolkata"))
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now < market_close:
        logger.warning("Market is still open. Checking gaps for yesterday instead.")
        check_date = now - pd.Timedelta(days=1)
    else:
        check_date = now
    backfill = Backfill()
    symbols = pd.read_csv(SYMBOLS_FILE)["symbol"].tolist()
    market_open = check_date.replace(hour=9, minute=15, second=0)
    market_close = check_date.replace(hour=15, minute=30, second=0)
    for symbol in symbols:
        for tf in ["15s", "30s", "1min", "3min", "5min"]:
            gaps = backfill.check_data_gaps(
                symbol, tf,
                market_open.strftime("%Y-%m-%d %H:%M:%S%z"),
                market_close.strftime("%Y-%m-%d %H:%M:%S%z")
            )
            if gaps:
                logger.info(f"Found {len(gaps)} gaps for {symbol} ({tf}). Backfilling.")
                await backfill.backfill_gaps(symbol, tf, gaps)

if __name__ == "__main__":
    asyncio.run(check_todays_gaps())
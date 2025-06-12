import time
import pandas as pd
import asyncio
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.indicators.macd import MACD
from src.indicators.cal_input import CalInput
from config.config import TIMEFRAMES, MACD_PARAMS, HIGHER_TIMEFRAMES

logger = get_logger(__name__)

class Resampler:
    def __init__(self, tick_queues, storage):
        self.tick_queues = tick_queues
        self.storage = storage
        self.config = load_config("config/config.yaml")
        self.timeframes = TIMEFRAMES
        self.ohlcv_data = {
            symbol: {
                tf: pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
                for tf in self.timeframes + ["1s"]
            }
            for symbol in tick_queues.keys()
        }
        self.running = False

    async def aggregate_ticks(self):
        try:
            for symbol, queue in self.tick_queues.items():
                ticks = []
                while not queue.empty():
                    ticks.append(queue.get())
                if ticks:
                    df = pd.DataFrame(ticks)
                    if 'timestamp' not in df.columns or df['timestamp'].isna().any():
                        logger.error(f"Invalid timestamps in ticks for {symbol}: {df.head()}")
                        continue
                    timestamp = df["timestamp"].max().floor("1s")
                    ohlcv = {
                        "timestamp": timestamp,
                        "open": df["ltp"].iloc[0],
                        "high": df["ltp"].max(),
                        "low": df["ltp"].min(),
                        "close": df["ltp"].iloc[-1],
                        "volume": df["volume"].iloc[-1]
                    }
                    self.ohlcv_data[symbol]["1s"] = pd.concat(
                        [self.ohlcv_data[symbol]["1s"], pd.DataFrame([ohlcv])],
                        ignore_index=True
                    ).drop_duplicates(subset=["timestamp"], keep="last")
                    self.storage.save_ohlcv(symbol, self.ohlcv_data[symbol]["1s"], "1s")
                    logger.debug(f"Aggregated 1s OHLCV for {symbol}: {ohlcv}")
                else:
                    logger.debug(f"No ticks for {symbol} in this interval")
        except Exception as e:
            logger.error(f"Error in aggregate_ticks: {e}", exc_info=True)

    async def resample_to_timeframe(self, symbol: str, timeframe: str) -> pd.DataFrame:
        try:
            df = self.ohlcv_data[symbol]["1s"].set_index("timestamp")
            if df.empty:
                logger.warning(f"No 1s OHLCV data for {symbol} to resample to {timeframe}")
                return pd.DataFrame()
            resampled = df.resample(timeframe).agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum"
            }).dropna()
            self.ohlcv_data[symbol][timeframe] = resampled.reset_index()
            self.storage.save_ohlcv(symbol, self.ohlcv_data[symbol][timeframe], timeframe)
            logger.info(f"Resampled {symbol} to {timeframe} with {len(resampled)} candles")
            return resampled
        except Exception as e:
            logger.error(f"Error resampling {symbol} to {timeframe}: {e}", exc_info=True)
            return pd.DataFrame()

    def compute_indicators(self, symbol: str, timeframe: str):
        try:
            df = self.ohlcv_data[symbol][timeframe]
            if df.empty:
                logger.warning(f"No OHLCV data for {symbol} ({timeframe}) to compute indicators")
                return
            params = MACD_PARAMS.get(timeframe, [])
            if not params:
                logger.error(f"No MACD parameters defined for {timeframe}")
                return
            macd = MACD(df, timeframe)
            macd_df = macd.compute_macd()
            macd.save(symbol)
            higher_tfs = HIGHER_TIMEFRAMES.get(timeframe, [])
            cal_input = CalInput(macd_df, timeframe)
            cal_0, cal_1 = cal_input.compute_cal_input()
            cal_input.save(symbol)
            logger.info(f"Computed indicators for {symbol} ({timeframe})")
        except Exception as e:
            logger.error(f"Error computing indicators for {symbol} ({timeframe}): {e}", exc_info=True)

    async def process(self):
        self.running = True
        last_resample = {tf: 0.0 for tf in self.timeframes}  # fix here
        while self.running:
            try:
                await self.aggregate_ticks()
                now = time.time()
                for tf in self.timeframes:
                    seconds = pd.Timedelta(tf).total_seconds()
                    if now - last_resample[tf] >= seconds:
                        for symbol in self.tick_queues:
                            await self.resample_to_timeframe(symbol, tf)
                            self.compute_indicators(symbol, tf)
                        last_resample[tf] = now
                        logger.debug(f"Processed resampling for {tf}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in process loop: {e}", exc_info=True)

    async def start(self):
        try:
            logger.info("Resampler started")
            await self.process()
        except Exception as e:
            logger.error(f"Error starting resampler: {e}", exc_info=True)

    def stop(self):
        self.running = False
        logger.info("Resampler stopped")
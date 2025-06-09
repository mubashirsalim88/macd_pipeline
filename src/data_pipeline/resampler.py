import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.indicators.macd import MACD
from src.indicators.cal_input import CalInput

logger = get_logger(__name__)

class Resampler:
    def __init__(self, tick_queues, storage):
        """
        Initialize Resampler for tick aggregation and indicator computation.
        tick_queues: Dict[str, Queue] of symbol:Queue for tick data
        storage: Storage instance for saving data
        """
        self.tick_queues = tick_queues
        self.storage = storage
        self.config = load_config("config/config.yaml")
        self.timeframes = self.config["timeframes"]  # ["15s", "30s", "1min", "3min", "5min"]
        self.scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
        self.ohlcv_data = {
            symbol: {
                tf: pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
                for tf in self.timeframes + ["1s"]
            }
            for symbol in tick_queues.keys()
        }
        # Schedule 1-second aggregation
        self.scheduler.add_job(self.aggregate_ticks, "interval", seconds=1)
        # Schedule resampling and indicator computation
        for tf in self.timeframes:
            seconds = pd.Timedelta(tf).total_seconds()
            self.scheduler.add_job(
                lambda tf=tf: self.resample_and_compute_indicators(tf),
                "interval",
                seconds=seconds
            )

    def aggregate_ticks(self):
        """
        Aggregate ticks into 1-second OHLCV.
        """
        for symbol, queue in self.tick_queues.items():
            ticks = []
            while not queue.empty():
                ticks.append(queue.get())
            if ticks:
                df = pd.DataFrame(ticks)
                timestamp = df["timestamp"].max().floor("1s")
                ohlcv = {
                    "timestamp": timestamp,
                    "open": df["ltp"].iloc[0],
                    "high": df["ltp"].max(),
                    "low": df["ltp"].min(),
                    "close": df["ltp"].iloc[-1],
                    "volume": df["volume"].iloc[-1]  # Assume cumulative volume
                }
                self.ohlcv_data[symbol]["1s"] = pd.concat(
                    [self.ohlcv_data[symbol]["1s"], pd.DataFrame([ohlcv])],
                    ignore_index=True
                ).drop_duplicates(subset=["timestamp"], keep="last")
                self.storage.save_ohlcv(symbol, self.ohlcv_data[symbol]["1s"], "1s")
                logger.debug(f"Aggregated 1s OHLCV for {symbol}: {ohlcv}")
            else:
                logger.debug(f"No ticks for {symbol} in this interval")

    def resample_to_timeframe(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """
        Resample 1-second OHLCV to target timeframe.
        """
        df = self.ohlcv_data[symbol]["1s"].set_index("timestamp")
        if df.empty:
            logger.warning(f"No 1s OHLCV data for {symbol} to resample to {timeframe}")
            return pd.DataFrame()
        
        try:
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
            logger.error(f"Error resampling {symbol} to {timeframe}: {e}")
            return pd.DataFrame()

    def compute_indicators(self, symbol: str, timeframe: str):
        """
        Compute MACD and CAL INPUT for the timeframe.
        """
        df = self.ohlcv_data[symbol][timeframe]
        if df.empty:
            logger.warning(f"No OHLCV data for {symbol} ({timeframe}) to compute indicators")
            return
        
        try:
            # Compute MACD
            params = self.config["macd_params"].get(timeframe, [])
            if not params:
                logger.error(f"No MACD parameters defined for {timeframe}")
                return
            macd = MACD(df, params, timeframe)
            macd_df = macd.compute_macd()
            macd.save(symbol)
            
            # Compute CAL INPUT
            higher_tfs = self.config["higher_timeframes"].get(timeframe, [])
            cal_input = CalInput(macd_df, timeframe, higher_tfs)
            cal_0, cal_1 = cal_input.compute_cal_input()
            cal_input.save(symbol)
            logger.info(f"Computed indicators for {symbol} ({timeframe})")
        except Exception as e:
            logger.error(f"Error computing indicators for {symbol} ({timeframe}): {e}")

    def resample_and_compute_indicators(self, timeframe: str):
        """
        Resample and compute indicators for all symbols.
        """
        for symbol in self.tick_queues:
            try:
                self.resample_to_timeframe(symbol, timeframe)
                self.compute_indicators(symbol, timeframe)
            except Exception as e:
                logger.error(f"Error processing {symbol} for {timeframe}: {e}")

    def start(self):
        """
        Start the scheduler.
        """
        try:
            self.scheduler.start()
            logger.info("Resampler started")
        except Exception as e:
            logger.error(f"Error starting resampler: {e}")

    def stop(self):
        """
        Stop the scheduler.
        """
        try:
            self.scheduler.shutdown()
            logger.info("Resampler stopped")
        except Exception as e:
            logger.error(f"Error stopping resampler: {e}")
import pandas as pd
import numpy as np
from src.utils.logger import get_logger
from src.data_pipeline.storage import Storage
from config.config import HIGHER_TIMEFRAMES

logger = get_logger(__name__)

class CalInput:
    def __init__(self, macd_df, timeframe, storage_config="config/config.yaml"):
        self.macd_df = macd_df.copy()
        self.timeframe = timeframe
        self.higher_timeframes = HIGHER_TIMEFRAMES.get(timeframe, [])
        self.storage = Storage(config_path=storage_config)
        self.cal_0 = pd.DataFrame()
        self.cal_1 = pd.DataFrame()

    def compute_cal_input(self):
        try:
            if self.macd_df.empty or "Sr.No." not in self.macd_df.columns:
                logger.warning(f"Invalid or empty MACD DataFrame for {self.timeframe}. Skipping CAL INPUT.")
                return pd.DataFrame(), pd.DataFrame()

            cal_0_data = []
            cal_1_data = []
            
            for sr_no in self.macd_df["Sr.No."].unique():
                df = self.macd_df[self.macd_df["Sr.No."] == sr_no][["timestamp", "FL", "SL"]]
                if df.empty:
                    logger.warning(f"No data for Sr.No. {sr_no} in {self.timeframe}")
                    continue
                row_0 = {"Sr.No.": sr_no}
                row_1 = {"Sr.No.": sr_no}
                
                for tf in [self.timeframe] + self.higher_timeframes:
                    if tf == self.timeframe:
                        fl_ch = ((df["FL"] - df["FL"].shift(1)) / df["FL"].shift(1) * 100).iloc[-1]
                        sl_ch = ((df["SL"] - df["SL"].shift(1)) / df["SL"].shift(1) * 100).iloc[-1]
                        fl_cg = (abs(df["FL"] - df["FL"].shift(1)) / df["FL"].shift(1) * 100).iloc[-1]
                        sl_cg = (abs(df["SL"] - df["SL"].shift(1)) / df["SL"].shift(1) * 100).iloc[-1]
                    else:
                        resampled = df.set_index("timestamp").resample(tf).last().dropna()
                        if resampled.empty:
                            logger.warning(f"No resampled data for {tf} in Sr.No. {sr_no}")
                            fl_ch = sl_ch = fl_cg = sl_cg = 0
                        else:
                            fl_ch = ((resampled["FL"] - resampled["FL"].shift(1)) / resampled["FL"].shift(1) * 100).iloc[-1]
                            sl_ch = ((resampled["SL"] - resampled["SL"].shift(1)) / resampled["SL"].shift(1) * 100).iloc[-1]
                            fl_cg = (abs(resampled["FL"] - resampled["FL"].shift(1)) / resampled["FL"].shift(1) * 100).iloc[-1]
                            sl_cg = (abs(resampled["SL"] - resampled["SL"].shift(1)) / resampled["SL"].shift(1) * 100).iloc[-1]
                    
                    row_0[f"{sr_no}CH-FL_{tf}"] = fl_ch if not np.isinf(fl_ch) else 0
                    row_0[f"{sr_no}CH-SL_{tf}"] = sl_ch if not np.isinf(sl_ch) else 0
                    row_1[f"{sr_no}CG_{tf}"] = fl_cg if not np.isinf(fl_cg) else 0
                    
                    logger.debug(f"Computed CAL INPUT for Sr.No. {sr_no}, TF: {tf}")
                
                cal_0_data.append(row_0)
                cal_1_data.append(row_1)
            
            self.cal_0 = pd.DataFrame(cal_0_data)
            self.cal_1 = pd.DataFrame(cal_1_data)
            return self.cal_0, self.cal_1
        except Exception as e:
            logger.error(f"Error computing CAL INPUT for {self.timeframe}: {e}")
            return pd.DataFrame(), pd.DataFrame()

    def save(self, symbol):
        try:
            if not self.cal_0.empty:
                self.storage.save_indicators(symbol, self.cal_0, self.timeframe, "cal_input_0")
                logger.info(f"Saved CAL INPUT {self.timeframe}.0 for {symbol}")
            if not self.cal_1.empty:
                self.storage.save_indicators(symbol, self.cal_1, self.timeframe, "cal_input_1")
                logger.info(f"Saved CAL INPUT {self.timeframe}.1 for {symbol}")
        except Exception as e:
            logger.error(f"Error saving CAL INPUT for {symbol}: {e}")
import pandas as pd
import numpy as np
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.data_pipeline.storage import Storage
from config.config import TIMEFRAMES
from typing import Tuple

logger = get_logger(__name__)

class RuleEngine:
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = load_config(config_path)
        self.storage = Storage()
        self.timeframes = TIMEFRAMES

    def load_indicators(self, symbol: str, timeframe: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        try:
            date = pd.Timestamp.now().strftime("%Y%m%d")
            macd_path = self.storage.indicators_path / 'macd' / f"{symbol}_{timeframe}_{date}.h5"
            cal_0_path = self.storage.indicators_path / 'cal_input_0' / f"{symbol}_{timeframe}_{date}.h5"
            cal_1_path = self.storage.indicators_path / 'cal_input_1' / f"{symbol}_{timeframe}_{date}.h5"
            macd_df = pd.read_hdf(macd_path, key="macd") if macd_path.exists() else pd.DataFrame()
            cal_0_df = pd.read_hdf(cal_0_path, key="cal_input_0") if cal_0_path.exists() else pd.DataFrame()
            cal_1_df = pd.read_hdf(cal_1_path, key="cal_input_1") if cal_1_path.exists() else pd.DataFrame()
            macd_df = pd.DataFrame(macd_df) if not isinstance(macd_df, pd.DataFrame) else macd_df
            cal_0_df = pd.DataFrame(cal_0_df) if not isinstance(cal_0_df, pd.DataFrame) else cal_0_df
            cal_1_df = pd.DataFrame(cal_1_df) if not isinstance(cal_1_df, pd.DataFrame) else cal_1_df
            return macd_df, cal_0_df, cal_1_df
        except Exception as e:
            logger.error(f"Error loading indicators for {symbol} ({timeframe}): {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def apply_layer(self, symbol: str, timeframe: str, layer_num: int) -> pd.DataFrame:
        try:
            macd_df, cal_0_df, cal_1_df = self.load_indicators(symbol, timeframe)
            if macd_df.empty or cal_0_df.empty or cal_1_df.empty:
                logger.warning(f"No indicator data for {symbol} ({timeframe}) in layer {layer_num}")
                return pd.DataFrame()

            result = macd_df.copy()
            if layer_num == 1:
                result = result[result["FL_Dir"] == "up"]
            elif layer_num == 2:
                result = result[result["FL_Xng_Up"] == "XING-UP"]
            elif layer_num == 3:
                if not cal_0_df.empty:
                    for col in cal_0_df.columns:
                        if col.startswith("CH-FL") and col.endswith(timeframe):
                            result = result[cal_0_df[col] > 0]
            elif layer_num == 4:
                result = result[result["PP_BC_Up"] == "PP_BC_CL_UP"]
            elif layer_num == 5:
                result = result[result["FL"] > result["SL"]]

            logger.info(f"Applied layer {layer_num} rules for {symbol} ({timeframe})")
            return result
        except Exception as e:
            logger.error(f"Error applying layer {layer_num} for {symbol} ({timeframe}): {e}")
            return pd.DataFrame()

    def orchestrate_layers(self, symbol: str) -> pd.DataFrame:
        try:
            filtered_data = None
            for layer in range(1, 6):
                for timeframe in self.timeframes:
                    layer_result = self.apply_layer(symbol, timeframe, layer)
                    if filtered_data is None:
                        filtered_data = layer_result
                    else:
                        filtered_data = pd.concat([filtered_data, layer_result], ignore_index=True)
            logger.info(f"Completed orchestration for {symbol}")
            return filtered_data if filtered_data is not None else pd.DataFrame()
        except Exception as e:
            logger.error(f"Error in orchestration for {symbol}: {e}")
            return pd.DataFrame()

    def generate_signals(self, symbol: str, filtered_data: pd.DataFrame) -> pd.DataFrame:
        try:
            if filtered_data.empty:
                logger.warning(f"No filtered data for {symbol} to generate signals")
                return pd.DataFrame()
            signals = filtered_data.copy()
            signals["Signal"] = np.where(
                (signals["FL"] > signals["SL"]) & (signals["PP_BC_Up"] == "PP_BC_CL_UP"),
                "BUY",
                "HOLD"
            )
            logger.info(f"Generated signals for {symbol}")
            return signals
        except Exception as e:
            logger.error(f"Error generating signals for {symbol}: {e}")
            return pd.DataFrame()
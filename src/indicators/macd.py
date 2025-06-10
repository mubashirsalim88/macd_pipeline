import pandas as pd
import numpy as np
from src.utils.logger import get_logger
from src.data_pipeline.storage import Storage
from config.config import MACD_PARAMS

logger = get_logger(__name__)

class MACD:
    def __init__(self, df, timeframe, storage_config="config/config.yaml"):
        self.df = df.copy()
        self.params_list = MACD_PARAMS.get(timeframe, [])
        self.timeframe = timeframe
        self.storage = Storage(config_path=storage_config)
        self.result = pd.DataFrame()

    def compute_macd(self):
        try:
            valid_params = []
            for fast, slow, signal in self.params_list:
                if not all(isinstance(x, (int, float)) for x in [fast, slow, signal]):
                    logger.warning(f"Invalid MACD params: ({fast}, {slow}, {signal}). Skipping.")
                    continue
                if fast <= 0 or slow <= 0 or signal <= 0 or fast >= slow:
                    logger.warning(f"Invalid MACD params: ({fast}, {slow}, {signal}). Must be positive and fast < slow.")
                    continue
                valid_params.append((fast, slow, signal))
            
            if not valid_params:
                logger.error(f"No valid MACD parameters for {self.timeframe}")
                return pd.DataFrame()

            results = []
            for i, (fast, slow, signal) in enumerate(valid_params, 1):
                logger.info(f"Computing MACD for Sr.No. {i}: ({fast}, {slow}, {signal})")
                df = self.df.copy()
                
                ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
                ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
                df["FL"] = ema_fast - ema_slow
                df["SL"] = df["FL"].ewm(span=signal, adjust=False).mean()
                
                df["FL_Dir"] = np.where(df["FL"] > df["FL"].shift(1), "up", "down")
                df["SL_Dir"] = np.where(df["SL"] > df["SL"].shift(1), "up", "down")
                
                df["FL_Pos"] = np.where(df["FL"] > 0, "above", "below")
                df["SL_Pos"] = np.where(df["SL"] > 0, "above", "below")
                
                df["FL_Xng_Up"] = np.where(
                    (df["FL"] > df["SL"]) & (df["FL"].shift(1) <= df["SL"].shift(1)),
                    "XING-UP", "NOT"
                )
                df["SL_Xng_Up"] = np.where(
                    (df["SL"] > df["SL"].shift(1)) & (df["FL"] > df["SL"]),
                    "XING-UP", "NOT"
                )
                df["FL_Xng_Dn"] = np.where(
                    (df["FL"] < df["SL"]) & (df["FL"].shift(1) >= df["SL"].shift(1)),
                    "XING-DN", "NOT"
                )
                df["SL_Xng_Dn"] = np.where(
                    (df["SL"] < df["SL"].shift(1)) & (df["FL"] < df["SL"]),
                    "XING-DN", "NOT"
                )
                
                df["PP_BC_CL_Up"] = np.where(
                    (df["FL"] > 0) & (df["SL"] > 0) & (df["FL_Dir"] == "up"),
                    "PP_BC_CL_UP", "NOT"
                )
                df["PP_BC_CL_Dn"] = np.where(
                    (df["FL"] > 0) & (df["SL"] > 0) & (df["FL_Dir"] == "down"),
                    "PP_BC_CL_DN", "NOT"
                )
                df["NN_BC_CL_Up"] = np.where(
                    (df["FL"] < 0) & (df["SL"] < 0) & (df["FL_Xng_Up"] == "XING-UP"),
                    "NN_BC_CL_UP", "NOT"
                )
                df["NN_BC_CL_Dn"] = np.where(
                    (df["FL"] < 0) & (df["SL"] < 0) & (df["FL_Dir"] == "down"),
                    "NN_BC_CL_Dn", "NOT"
                )
                df["PP_BC_Up"] = df["PP_BC_CL_Up"]
                df["PP_BC_Dn"] = df["PP_BC_CL_Dn"]
                df["NN_BC_Up"] = df["NN_BC_CL_Up"]
                df["NN_BC_Dn"] = df["NN_BC_CL_Dn"]
                df["NP_BC_Up"] = np.where(
                    (df["FL"] < 0) & (df["SL"] > 0) & (df["FL_Xng_Up"] == "XING-UP"),
                    "NP_BC_UP", "NOT"
                )
                df["PN_BC_Dn"] = np.where(
                    (df["FL"] > 0) & (df["SL"] < 0) & (df["FL_Xng_Dn"] == "XING-DN"),
                    "PN_BC_DN", "NOT"
                )
                
                df["Sr.No."] = i
                df["MACD"] = f"{fast},{slow},{signal}"
                results.append(df)
            
            if results:
                self.result = pd.concat(results, ignore_index=True)
                return self.result
            else:
                logger.warning(f"No MACD results for {self.timeframe}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error computing MACD for {self.timeframe}: {e}")
            return pd.DataFrame()

    def save(self, symbol):
        try:
            if not self.result.empty:
                self.storage.save_indicators(symbol, self.result, self.timeframe, "macd")
                logger.info(f"Saved MACD data for {symbol} ({self.timeframe})")
            else:
                logger.warning(f"No MACD data to save for {symbol} ({self.timeframe})")
        except Exception as e:
            logger.error(f"Error saving MACD for {symbol}: {e}")
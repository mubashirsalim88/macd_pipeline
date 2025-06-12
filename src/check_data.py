import pandas as pd
from pathlib import Path

historical_path = Path(r"C:\Users\mubas\OneDrive\Desktop\macd_pipeline\data\ticks\historical")
timeframes = ["1min", "3min", "5min"]
symbol = "NSE:RELIANCE-EQ"  # Example symbol
key = symbol.replace(":", "_")

for tf in timeframes:
    file_path = historical_path / f"{tf}.h5"
    if file_path.exists():
        with pd.HDFStore(file_path, mode='r') as store:
            if f"/{key}" in store:
                df = store[key]
                print(f"{tf} data for {symbol}: {len(df)} rows")
                print(f"Timestamp range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            else:
                print(f"No data for {symbol} in {tf}.h5")
    else:
        print(f"{tf}.h5 does not exist")
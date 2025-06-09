MACD Pipeline
 A Python-based pipeline for fetching real-time stock market data from the Fyers API, processing it for Moving Average Convergence Divergence (MACD) analysis, and storing results. This project uses WebSocket for live tick data and REST API for fallback quotes.

 ## Features
 - Connects to Fyers API WebSocket for real-time market data.
 - Subscribes to 97 NSE symbols (from `nse_100.csv`).
 - Fetches fallback quotes if no ticks are received for 2 minutes.
 - Logs data to `data/logs` for debugging.
 - Configurable via `config/config.yaml`.

 ## Installation
 1. Clone the repository:
    ```bash
    git clone https://github.com/your-username/macd_pipeline.git
    cd macd_pipeline
    ```
 2. Create and activate a virtual environment:
    ```bash
    python -m venv .venv
    .\venv\Scripts\activate  # Windows
    ```
 3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
 4. Configure `config/config.yaml` (not tracked):
    ```yaml
    fyers:
      client_id: your-client-id
      api_key: your-api-key
      access_token_secret: your-access-token-secret
    symbols:
      file: path/to/nse_100.csv
    ```

 ## Usage
 Run the WebSocket client:
 ```bash
 python -m src.data_pipeline.fyers_websocket
 ```
 - Logs are saved in `data/logs`.
 - Test during NSE market hours (9:15 AM–3:30 PM IST) for live ticks.

 ## Project Structure
 ```
 macd_pipeline/
 ├── src/
 │   ├── data_pipeline/
 │   │   └── fyers_websocket.py
 │   └── utils/
 │       ├── config_loader.py
 │       ├── fyers_auth_token.py
 │       └── logger.py
 ├── config/
 │   └── config.yaml
 ├── data/
 │   ├── logs/
 │   └── nse_100.csv
 ├── .gitignore
 ├── pyproject.toml
 ├── README.md
 └── requirements.txt
 ```

 ## Dependencies
 - Python 3.9+
 - `fyers-apiv3==3.1.7`
 - `pandas`
 - See `requirements.txt` for full list.

 ## Notes
 - Test during NSE market hours to receive WebSocket ticks.
 - Fyers API may limit WebSocket subscriptions to ~50–100 symbols. Batch subscription may be needed for large symbol lists.
 - Sensitive files (`config.yaml`, logs) are excluded via `.gitignore`.

 ## License
 MIT License (to be added)


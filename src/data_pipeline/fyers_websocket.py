from fyers_apiv3.FyersWebsocket import data_ws
import pandas as pd
import queue
import time
from pathlib import Path
from src.utils.config_loader import load_config
from src.utils.fyers_auth_ngrok import load_tokens
from src.utils.logger import get_logger
from typing import Dict, Any, cast
from fyers_apiv3 import fyersModel
from config.config import SYMBOLS_FILE

logger = get_logger(__name__)

class FyersWebSocketClient:
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize WebSocket client with Fyers API credentials and symbol list."""
        self.config = load_config(config_path)
        self.client_id = self.config["fyers"]["client_id"]
        self.access_token = load_tokens()
        self.symbols = pd.read_csv(SYMBOLS_FILE)["symbol"].tolist()
        self.blacklist = {'NSE:UNITEDSPIRITS-EQ', 'NSE:ZOMATO-EQ'}
        self.symbols = [s for s in self.symbols if s not in self.blacklist]
        logger.info(f"Subscribing to {len(self.symbols)} symbols")
        self.tick_queues = {symbol: queue.Queue() for symbol in self.symbols}
        self.last_tick_time = time.time()
        log_dir = Path("data/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        self.fyers = fyersModel.FyersModel(
            client_id=self.client_id,
            token=self.access_token,
            log_path=str(log_dir) + "/"
        )
        self.ws = data_ws.FyersDataSocket(
            access_token=f"{self.client_id}:{self.access_token}",
            log_path=str(log_dir) + "/",
            write_to_file=True,
            on_connect=self._on_connect,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            litemode=False,
            reconnect=True
        )
        test_symbol = self.symbols[0] if self.symbols else "NSE:RELIANCE-EQ"
        try:
            quote_response = self.fyers.quotes({"symbols": test_symbol})
            if isinstance(quote_response, dict) and quote_response.get("s") == "ok":
                logger.info(f"Token validation quote for {test_symbol}: Success")
            else:
                logger.error(f"Invalid token or API issue: {quote_response}")
                raise RuntimeError("Token validation failed")
        except Exception as e:
            logger.error(f"Token validation failed: {e}")
            raise

    def _on_message(self, message: Dict[str, Any]) -> None:
        """Handle incoming WebSocket messages."""
        try:
            if not isinstance(message, dict):
                logger.error(f"Unexpected message type: {type(message)}")
                return
            message_dict = cast(Dict[str, Any], message)
            symbol = message_dict.get("symbol")
            ltp = message_dict.get("ltp")
            vol = message_dict.get("vol_traded")
            timestamp = pd.Timestamp.now(tz="Asia/Kolkata")
            if symbol in self.tick_queues and ltp is not None:
                self.tick_queues[symbol].put({"timestamp": timestamp, "ltp": ltp, "volume": vol})
                self.last_tick_time = time.time()
                logger.info(f"Received tick for {symbol}: LTP={ltp}, Volume={vol}")
            else:
                logger.debug(f"Non-tick or invalid message")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _on_error(self, message: Dict[str, Any]) -> None:
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {message}")
        try:
            self.ws.subscribe(symbols=self.symbols, data_type="SymbolData")
            logger.info("Resubscribed to symbols due to error")
        except Exception as e:
            logger.error(f"Failed to resubscribe: {e}")

    def _on_connect(self) -> None:
        """Handle WebSocket connection establishment."""
        logger.info(f"WebSocket connected. Subscribing to {len(self.symbols)} symbols")
        try:
            self.ws.subscribe(symbols=self.symbols, data_type="SymbolData")
            self.ws.keep_running()
            logger.info("Subscription request sent")
        except Exception as e:
            logger.error(f"Subscription failed: {e}")

    def _on_close(self, message: Dict[str, Any]) -> None:
        """Handle WebSocket closure."""
        logger.warning(f"WebSocket closed: {message}")

    def start(self):
        """Start the WebSocket client with retry logic."""
        max_retries = 5
        attempt = 0
        while attempt < max_retries:
            try:
                self.ws.connect()
                logger.info("WebSocket connection initiated")
                while True:
                    time.sleep(60)
                    if time.time() - self.last_tick_time > 120:
                        logger.warning("No ticks received for 2 minutes. Fetching fallback quotes.")
                        for symbol in self.symbols:
                            quote = self.fetch_quote_fallback(symbol)
                            if quote:
                                self.tick_queues[symbol].put(quote)
                                logger.info(f"Fallback quote for {symbol}")
                    try:
                        self.ws.subscribe(symbols=self.symbols, data_type="SymbolData")
                        logger.debug("Refreshed subscription")
                    except Exception as e:
                        logger.error(f"Subscription refresh failed: {e}")
            except Exception as e:
                attempt += 1
                logger.error(f"Attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(5 * attempt)
                else:
                    raise RuntimeError("Failed to connect WebSocket after retries")

    def stop(self):
        """Stop the WebSocket client."""
        try:
            self.ws.unsubscribe(symbols=self.symbols)
            self.ws.close_connection()
            logger.info("WebSocket stopped")
        except Exception as e:
            logger.error(f"Error during WebSocket stop: {e}")

    def fetch_quote_fallback(self, symbol: str) -> Dict[str, Any]:
        """Fetch fallback quote for a symbol via REST API."""
        try:
            response = self.fyers.quotes({"symbols": symbol})
            logger.debug(f"Fallback quote response for {symbol}")
            if isinstance(response, dict) and response.get("s") == "ok" and response.get("d"):
                quote = response["d"][0]["v"]
                timestamp = pd.Timestamp.now(tz="Asia/Kolkata")
                return {"timestamp": timestamp, "ltp": quote.get("lp"), "volume": quote.get("volume")}
            else:
                logger.warning(f"Failed to fetch quote for {symbol}: {response}")
                return {}
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return {}

if __name__ == "__main__":
    ws = FyersWebSocketClient()
    try:
        ws.start()
        while True:
            time.sleep(60)
            for symbol in ws.tick_queues:
                if ws.tick_queues[symbol].qsize() == 0:
                    quote = ws.fetch_quote_fallback(symbol)
                    if quote:
                        ws.tick_queues[symbol].put(quote)
                        logger.info(f"Fallback quote for {symbol}")
    except KeyboardInterrupt:
        ws.stop()
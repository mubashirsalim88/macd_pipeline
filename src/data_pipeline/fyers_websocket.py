from fyers_apiv3.FyersWebsocket import data_ws
import pandas as pd
import queue
import time
from typing import Dict, Any, List, cast
from pathlib import Path
from src.utils.config_loader import load_config
from src.utils.fyers_auth_ngrok import load_tokens
from src.utils.logger import get_logger
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
        self.subscribed_symbols = set()  # Track symbols with received ticks
        self.last_volume = {symbol: 0 for symbol in self.symbols}  # Track last known volume
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
        # Validate symbols using REST API
        valid_symbols = []
        test_symbol = self.symbols[0] if self.symbols else "NSE:RELIANCE-EQ"
        try:
            quote_response = self.fyers.quotes({"symbols": test_symbol})
            logger.info(f"Quote response for {test_symbol}: {quote_response}")
            if isinstance(quote_response, dict) and quote_response.get("s") == "ok":
                logger.info(f"Token validation quote for {test_symbol}: Success")
                valid_symbols.append(test_symbol)
            else:
                logger.error(f"Invalid token or API issue for {test_symbol}: {quote_response}")
                raise RuntimeError("Token validation failed")
            # Validate remaining symbols
            for symbol in self.symbols[1:]:
                response = self.fyers.quotes({"symbols": symbol})
                if isinstance(response, dict) and response.get("s") == "ok":
                    valid_symbols.append(symbol)
                else:
                    logger.warning(f"Invalid symbol {symbol}: {response}")
            self.symbols = valid_symbols
            logger.info(f"Validated {len(self.symbols)} symbols")
        except Exception as e:
            logger.error(f"Token validation failed: {e}")
            raise

    def _on_message(self, message: Dict[str, Any]) -> None:
        """Handle incoming WebSocket messages."""
        logger.debug(f"Raw WebSocket message: {message}")
        try:
            if not isinstance(message, dict):
                logger.error(f"Unexpected message type: {type(message)}")
                return
            message_dict = cast(Dict[str, Any], message)
            # Log system messages (e.g., full mode, subscription status)
            if message_dict.get("type") == "ful":
                logger.info(f"Mode switch: {message_dict}")
                if message_dict.get("code") != 200:
                    logger.error(f"Full mode failed: {message_dict}")
            elif message_dict.get("type") == "sub":
                logger.info(f"Subscription status: {message_dict}")
                if message_dict.get("code") == 11011:
                    logger.error(f"Subscription failed: {message_dict}")
            # Process tick messages
            symbol = message_dict.get("symbol")
            ltp = message_dict.get("ltp")
            vol = message_dict.get("vol_traded_today", self.last_volume.get(symbol, 0))
            last_qty = message_dict.get("last_traded_qty", 0)
            timestamp = pd.Timestamp.now(tz="Asia/Kolkata")
            if symbol in self.tick_queues and ltp is not None:
                tick = {"timestamp": timestamp, "ltp": ltp, "volume": vol, "last_qty": last_qty}
                self.tick_queues[symbol].put(tick)
                self.last_tick_time = time.time()
                self.subscribed_symbols.add(symbol)
                self.last_volume[symbol] = vol
                logger.info(f"Received tick for {symbol}: LTP={ltp}, Volume={vol}, LastQty={last_qty}")
            else:
                logger.debug(f"Non-tick or invalid message for symbol: {symbol}, ltp: {ltp}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _on_connect(self) -> None:
        """Handle WebSocket connection establishment."""
        logger.info(f"WebSocket connected. Subscribing to {len(self.symbols)} symbols")
        self._subscribe()

    def _subscribe(self) -> None:
        """Subscribe to symbols in batches with individual retries."""
        max_attempts = 3
        batch_size = 50
        for attempt in range(1, max_attempts + 1):
            try:
                for i in range(0, len(self.symbols), batch_size):
                    batch = self.symbols[i:i + batch_size]
                    response = self.ws.subscribe(symbols=batch, data_type="SymbolUpdate")
                    logger.info(f"Subscription response for batch {i//batch_size + 1}: {response}")
                    if response is None:
                        for symbol in batch:
                            try:
                                response = self.ws.subscribe(symbols=[symbol], data_type="SymbolUpdate")
                                logger.info(f"Individual subscription for {symbol}: {response}")
                            except Exception as e:
                                logger.error(f"Failed to subscribe to {symbol}: {e}")
                self.ws.keep_running()
                logger.info("Subscription request sent")
                break
            except Exception as e:
                logger.error(f"Subscription attempt {attempt}/{max_attempts} failed: {e}")
                if attempt < max_attempts:
                    time.sleep(2 ** attempt)
                else:
                    raise RuntimeError("Failed to subscribe after retries")
        # Log missing symbols after subscription
        missing_symbols = set(self.symbols) - self.subscribed_symbols
        if missing_symbols:
            logger.warning(f"Subscribed symbols missing ticks: {missing_symbols}")

    def _on_error(self, message: Dict[str, Any]) -> None:
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {message}")
        try:
            self._subscribe()
            logger.info("Resubscribed to symbols")
        except Exception as e:
            logger.error(f"Failed to resubscribe: {e}")

    def _on_close(self, message: Dict[str, Any]) -> None:
        """Handle WebSocket closure."""
        logger.warning(f"WebSocket closed: {message}")

    def start(self):
        """Start the WebSocket client with retry logic."""
        max_retries = 5
        attempt = 0
        last_subscription_time = time.time()
        while attempt < max_retries:
            try:
                self.ws.connect()
                logger.info("WebSocket connection initiated")
                while True:
                    time.sleep(60)
                    # Periodic re-subscription every 5 minutes
                    if time.time() - last_subscription_time > 300:
                        try:
                            self._subscribe()
                            last_subscription_time = time.time()
                            logger.info("Periodic re-subscription completed")
                        except Exception as e:
                            logger.error(f"Periodic re-subscription failed: {e}")
                    # Check for ticks
                    if time.time() - self.last_tick_time > 120:
                        logger.warning("No ticks received for 2 minutes. Fetching fallback quotes.")
                        batch_size = 10
                        for i in range(0, len(self.symbols), batch_size):
                            batch = self.symbols[i:i + batch_size]
                            quotes = self.fetch_quote_fallback(batch)
                            for quote in quotes:
                                if quote and quote["symbol"] in self.tick_queues:
                                    self.tick_queues[quote["symbol"]].put(quote)
                                    logger.info(f"Fallback quote for {quote['symbol']}")
                    # Log missing symbols
                    missing_symbols = set(self.symbols) - self.subscribed_symbols
                    if missing_symbols:
                        logger.warning(f"Symbols missing ticks: {missing_symbols}")
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

    def fetch_quote_fallback(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Fetch fallback quotes for symbols via REST API."""
        try:
            response = self.fyers.quotes({"symbols": ",".join(symbols)})
            logger.debug(f"Batch fallback quote response for {len(symbols)} symbols")
            results = []
            if isinstance(response, dict) and response.get("s") == "ok" and response.get("d"):
                for quote in response["d"]:
                    timestamp = pd.Timestamp.now(tz="Asia/Kolkata")
                    if quote["v"].get("lp") is not None:
                        results.append({
                            "symbol": quote["n"],
                            "timestamp": timestamp,
                            "ltp": quote["v"].get("lp"),
                            "volume": quote["v"].get("volume")
                        })
                    else:
                        logger.warning(f"No LTP in quote for {quote['n']}: {quote}")
            else:
                logger.warning(f"Failed to fetch batch quotes: {response}")
            return results
        except Exception as e:
            logger.error(f"Error fetching batch quotes: {e}")
            return []

if __name__ == "__main__":
    ws = FyersWebSocketClient()
    try:
        ws.start()
        while True:
            time.sleep(60)
            for symbol in ws.tick_queues:
                if ws.tick_queues[symbol].qsize() == 0:
                    quote = ws.fetch_quote_fallback([symbol])
                    if quote:
                        ws.tick_queues[symbol].put(quote[0])
                        logger.info(f"Fallback quote for {symbol}: {quote[0]}")
    except KeyboardInterrupt:
        ws.stop()
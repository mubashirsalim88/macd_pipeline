from typing import Optional, Tuple
import os
import json
from datetime import datetime, timedelta
import pytz
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from fyers_apiv3 import fyersModel
from pyngrok import ngrok
import webbrowser
import threading
import time
import yaml

logger = get_logger(__name__)

class PreserveFormatDumper(yaml.SafeDumper):
    """Custom YAML dumper to preserve formatting."""
    def ignore_aliases(self, data):
        return True

    def represent_str(self, data):
        if data.startswith('http'):
            return self.represent_scalar('tag:yaml.org,2002:str', data, style='plain')
        return super().represent_str(data)

class OAuthHandler(BaseHTTPRequestHandler):
    auth_code = None

    def do_GET(self):
        try:
            parsed_path = urlparse(self.path)
            if parsed_path.path == "/callback":
                query_params = parse_qs(parsed_path.query)
                auth_code = query_params.get("auth_code", [None])[0]
                if auth_code:
                    OAuthHandler.auth_code = auth_code
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"Auth code received. You can close this window.")
                    logger.info(f"Captured auth_code: {auth_code[:10]}...")
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                else:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"Missing auth_code parameter.")
                    logger.error("No auth_code in callback.")
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            logger.error(f"Error in OAuthHandler: {e}")

def load_tokens() -> str:
    token_path = "data/tokens.json"
    try:
        if os.path.isfile(token_path):
            with open(token_path, "r") as f:
                data = json.load(f)
            access_token = data.get("access_token")
            issued_at = data.get("issued_at", datetime.now(pytz.UTC).timestamp())
            expires_in = data.get("expires_in", 86400)
            expiry_time = datetime.fromtimestamp(issued_at, tz=pytz.UTC) + timedelta(seconds=expires_in)
            if datetime.now(pytz.UTC) < expiry_time:
                logger.info("Valid access token loaded")
                return access_token
            logger.warning("Access token expired. Refreshing.")
        else:
            logger.info("No tokens.json found. Initiating OAuth flow.")
        
        access_token, _, _ = get_access_token()
        return access_token
    except Exception as e:
        logger.error(f"Error loading tokens: {e}")
        raise RuntimeError("Failed to load or refresh tokens")

def update_config_yaml(redirect_url: str):
    config_path = "config/config.yaml"
    try:
        cfg = load_config(config_path)
        if cfg["fyers"]["redirect_url"] != redirect_url:
            cfg["fyers"]["redirect_url"] = redirect_url
            with open(config_path, "w") as f:
                yaml.dump(cfg, f, Dumper=PreserveFormatDumper, sort_keys=False, allow_unicode=True)
            logger.info(f"Updated {config_path} with new redirect_url: {redirect_url}")
        else:
            logger.info("No update needed for redirect_url")
    except Exception as e:
        logger.error(f"Error updating config.yaml: {e}")
        raise

def get_access_token() -> Tuple[str, Optional[str], Optional[int]]:
    try:
        cfg = load_config("config/config.yaml")
        client_id = cfg["fyers"]["client_id"]
        secret_key = cfg["fyers"]["secret_key"]
        ngrok_auth_token = cfg["ngrok"]["auth_token"]
        port = cfg["ngrok"]["port"]

        token_path = "data/tokens.json"
        last_redirect_url = None
        if os.path.isfile(token_path):
            with open(token_path, "r") as f:
                data = json.load(f)
                last_redirect_url = data.get("redirect_url")

        ngrok.set_auth_token(ngrok_auth_token)
        tunnel = ngrok.connect(port, "http")
        if not tunnel.public_url:
            raise RuntimeError("Ngrok failed to provide a public URL")
        redirect_url = f"{tunnel.public_url}/callback"

        if last_redirect_url == redirect_url:
            logger.info("Reusing cached redirect_url")
        else:
            update_config_yaml(redirect_url)
            logger.warning(f"\n*** ACTION REQUIRED ***\nUpdate redirect URL at https://developer.fyers.in to: {redirect_url}\n")
            input("Press Enter after updating the redirect URL...")

        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_url,
            response_type="code",
            grant_type="authorization_code"
        )

        auth_url = session.generate_authcode()
        logger.info(f"Generated auth URL: {auth_url}")

        server_address = ("", port)
        httpd = HTTPServer(server_address, OAuthHandler)
        logger.info(f"Starting HTTP server at http://localhost:{port}")
        webbrowser.open(auth_url)
        while OAuthHandler.auth_code is None:
            httpd.handle_request()
        auth_code = OAuthHandler.auth_code
        httpd.server_close()

        if not auth_code:
            raise RuntimeError("No auth_code provided.")

        logger.info(f"Received auth_code: {auth_code[:10]}...")
        session.set_token(auth_code)
        response = session.generate_token()
        if response.get("s") != "ok" or "access_token" not in response:
            raise RuntimeError(f"Token generation failed: {response}")
        access_token = response["access_token"]
        refresh_token = response.get("refresh_token")
        expires_in = response.get("expires_in", 86400)
        logger.info("Access token generated successfully.")

        os.makedirs("data", exist_ok=True)
        with open("data/tokens.json", "w") as f:
            json.dump({
                "access_token": access_token,
                "refresh_token": refresh_token,
                "expires_in": expires_in,
                "issued_at": datetime.now(pytz.UTC).timestamp(),
                "redirect_url": redirect_url
            }, f)
        return access_token, refresh_token, expires_in
    except Exception as e:
        logger.error(f"Error in get_access_token: {e}")
        raise

if __name__ == "__main__":
    get_access_token()
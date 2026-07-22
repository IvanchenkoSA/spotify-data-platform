import json
import os
import secrets
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv()

CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]
REDIRECT_URI = os.environ["SPOTIFY_REDIRECT_URI"]

AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"

SCOPE = "user-read-recently-played"
TOKEN_FILE = Path(".spotify-token.json")


def build_authorization_url(state: str) -> str:
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPE,
        "state": state,
        "show_dialog": "true",
    }

    return f"{AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code_for_tokens(code: str) -> dict:
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        },
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=30,
    )

    response.raise_for_status()
    return response.json()


def save_tokens(tokens: dict) -> None:
    TOKEN_FILE.write_text(
        json.dumps(tokens, indent=2),
        encoding="utf-8",
    )


def wait_for_callback(expected_state: str) -> str:
    result: dict[str, str] = {}

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed_url = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(parsed_url.query)

            if "error" in query:
                result["error"] = query["error"][0]
                message = "Spotify authorization failed."
            elif query.get("state", [None])[0] != expected_state:
                result["error"] = "state_mismatch"
                message = "Invalid OAuth state."
            elif "code" not in query:
                result["error"] = "missing_code"
                message = "Authorization code is missing."
            else:
                result["code"] = query["code"][0]
                message = "Authorization completed. You can close this tab."

            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(message.encode("utf-8"))

        def log_message(self, format: str, *args: object) -> None:
            return

    server = HTTPServer(("127.0.0.1", 8888), CallbackHandler)
    server.handle_request()
    server.server_close()

    if "error" in result:
        raise RuntimeError(f"OAuth error: {result['error']}")

    return result["code"]


def main() -> None:
    state = secrets.token_urlsafe(32)
    authorization_url = build_authorization_url(state)

    print("Opening Spotify authorization page...")
    print(authorization_url)

    webbrowser.open(authorization_url)

    code = wait_for_callback(state)
    tokens = exchange_code_for_tokens(code)
    save_tokens(tokens)

    print("Authorization successful.")
    print(f"Token saved to {TOKEN_FILE}")


if __name__ == "__main__":
    main()

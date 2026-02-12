import requests
import os
import time
import subprocess
import json

TOKEN_URL = "https://portail-api.meteofrance.fr/token"
_token_cache = None


def get_valid_token():
    global _token_cache

    # Token encore valide ?
    if _token_cache and time.time() < _token_cache["expires_at"]:
        return _token_cache["access_token"]

    print(os.getenv('METEOFRANCE_BASIC_AUTH')  )
    headers = {
        "Authorization": f"Basic {os.getenv('METEOFRANCE_BASIC_AUTH')}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    body = "grant_type=client_credentials"

    r = requests.post(
        TOKEN_URL,
        headers=headers,
        data=body,
        timeout=10
    )
    r.raise_for_status()

    data = r.json()

    _token_cache = {
        "access_token": data["access_token"],
        "expires_at": time.time() + data["expires_in"] - 60
    }

    return _token_cache["access_token"]


def get_valid_token_debugwindows():
    global _token_cache

    # Cache 1h
    if _token_cache and time.time() < _token_cache["expires_at"]:
        return _token_cache["access_token"]

    basic_auth = os.getenv("METEOFRANCE_BASIC_AUTH")
    if not basic_auth:
        raise ValueError("METEOFRANCE_BASIC_AUTH manquant")

    cmd = [
        "curl.exe",
        "-s",
        "-X", "POST",
        TOKEN_URL,
        "-H", f"Authorization: Basic {basic_auth}",
        "-H", "Content-Type: application/x-www-form-urlencoded",
        "-d", "grant_type=client_credentials"
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Erreur curl : {result.stderr}")

    data = json.loads(result.stdout)

    _token_cache = {
        "access_token": data["access_token"],
        "expires_at": time.time() + data["expires_in"] - 60
    }

    return _token_cache["access_token"]
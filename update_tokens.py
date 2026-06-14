import requests
import json
import os
import sys
import time

UIDPASS_FILE = "uidpass.json"
TOKEN_FILE = "tokens.json"
API_URL = os.getenv("TOKEN_API_URL", "http://127.0.0.1:5001/token").strip()
TOKEN_RETRY_ATTEMPTS = 10
TOKEN_RETRY_DELAY_SECONDS = 0.7

def read_uidpass():
    uidpass_json = os.getenv("UIDPASS_JSON", "").strip()
    uid = os.getenv("FREEFIRE_UID", "").strip()
    password = os.getenv("FREEFIRE_PASSWORD", "").strip()

    if uidpass_json:
        try:
            records = json.loads(uidpass_json)
        except json.JSONDecodeError as e:
            print(f"UIDPASS_JSON is not valid JSON: {e}")
            sys.exit(1)
        if isinstance(records, dict):
            records = [records]
        if not isinstance(records, list):
            print("UIDPASS_JSON must be a JSON object or list.")
            sys.exit(1)
        return records

    if uid and password:
        return [{"uid": uid, "password": password}]

    if not os.path.exists(UIDPASS_FILE):
        print(
            "uidpass.json not found. Add it locally, or set GitHub secret UIDPASS_JSON "
            "with your UID/password JSON."
        )
        sys.exit(1)

    with open(UIDPASS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def fetch_token(uid, password):
    url = f"{API_URL}?uid={uid}&password={password}"
    last_error = None
    for attempt in range(1, TOKEN_RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            data = response.json()
            token = data.get("token")
            if token:
                return token
            last_error = "token missing in response"
        except Exception as e:
            last_error = e

        if attempt < TOKEN_RETRY_ATTEMPTS:
            time.sleep(TOKEN_RETRY_DELAY_SECONDS)

    print(f"Error fetching token for UID {uid} after {TOKEN_RETRY_ATTEMPTS} attempts: {last_error}")
    return None

def update_token_file(token_list):
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(token_list, f, ensure_ascii=False, indent=4)

def main():
    uidpass_list = read_uidpass()
    new_tokens = []
    for item in uidpass_list:
        uid = str(item["uid"])
        token = fetch_token(uid, item["password"])
        if token:
            new_tokens.append({"uid": uid, "token": token})
    if new_tokens:
        update_token_file(new_tokens)
        print(f"tokens.json updated successfully. valid={len(new_tokens)} total={len(uidpass_list)}")
    else:
        print("No tokens updated.")
        sys.exit(1)

if __name__ == "__main__":
    main()

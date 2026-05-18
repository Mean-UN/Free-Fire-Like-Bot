import requests
import json
import os
import sys

UIDPASS_FILE = "uidpass.json"
TOKEN_FILE = "tokens.json"
API_URL = "https://xtytdtyj-jwt.up.railway.app/token"

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
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        return data.get("token")
    except Exception as e:
        print(f"Error fetching token for UID {uid}: {e}")
        return None

def update_token_file(token_list):
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(token_list, f, ensure_ascii=False, indent=4)

def main():
    uidpass_list = read_uidpass()
    new_tokens = []
    for item in uidpass_list:
        token = fetch_token(item["uid"], item["password"])
        if token:
            new_tokens.append({"token": token})
    if new_tokens:
        update_token_file(new_tokens)
        print(f"tokens.json updated successfully. valid={len(new_tokens)} total={len(uidpass_list)}")
    else:
        print("No tokens updated.")
        sys.exit(1)

if __name__ == "__main__":
    main()

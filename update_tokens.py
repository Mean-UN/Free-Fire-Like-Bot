import requests
import json
import os
import sys

UIDPASS_FILE = "uidpass.json"
TOKEN_FILE = "tokens.json"
API_URL = "https://xtytdtyj-jwt.up.railway.app/token"

def read_uidpass():
    uid = os.getenv("FREEFIRE_UID", "").strip()
    password = os.getenv("FREEFIRE_PASSWORD", "").strip()

    if uid and password:
        return [{"uid": uid, "password": password}]

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

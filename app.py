from flask import Flask, request, jsonify
import asyncio
import os
import threading
import time
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import binascii
import aiohttp
import requests
import json
import like_pb2
import like_count_pb2
import uid_generator_pb2
from google.protobuf.message import DecodeError
import base64
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKENS_FILE = os.path.join(BASE_DIR, "tokens.json")
UIDPASS_FILE = os.path.join(BASE_DIR, "uidpass.json")
TOKEN_API_URL = "https://xtytdtyj-jwt.up.railway.app/token"
UPSTREAM_TIMEOUT_SECONDS = int(os.getenv("UPSTREAM_TIMEOUT_SECONDS", "20"))
FFINFO_API_URL = os.getenv("FFINFO_API_URL", "https://info-ob49.onrender.com/api/account/").strip()
FFINFO_API_KEY = os.getenv("FFINFO_API_KEY", "").strip()
AUTO_TOKEN_REFRESH_HOURS = float(os.getenv("AUTO_TOKEN_REFRESH_HOURS", "7"))
ENABLE_AUTO_TOKEN_REFRESH = os.getenv("ENABLE_AUTO_TOKEN_REFRESH", "true").strip().lower() in {"1", "true", "yes", "on"}
FFINFO_DEFAULT_REGIONS = [
    region.strip().upper()
    for region in os.getenv("FFINFO_DEFAULT_REGIONS", "SG,IND,BD,BR,US,SAC,NA,EU,ME,TH,VN,ID,TW,RU").split(",")
    if region.strip()
]

def read_uidpass_records():
    uid = os.getenv("FREEFIRE_UID", "").strip()
    password = os.getenv("FREEFIRE_PASSWORD", "").strip()

    if uid and password:
        return [{"uid": uid, "password": password}]

    with open(UIDPASS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def refresh_tokens_from_uidpass():
    try:
        uidpass_list = read_uidpass_records()
        new_tokens = []
        failed_uids = []

        for item in uidpass_list:
            uid = str(item.get("uid", "")).strip()
            password = str(item.get("password", "")).strip()
            if not uid or not password:
                continue
            try:
                response = requests.get(
                    TOKEN_API_URL,
                    params={"uid": uid, "password": password},
                    timeout=20
                )
                response.raise_for_status()
                token = response.json().get("token")
            except Exception:
                token = None
            if token and isinstance(token, str):
                new_tokens.append({"token": token})
            else:
                failed_uids.append(uid)

        if new_tokens:
            with open(TOKENS_FILE, "w", encoding="utf-8") as f:
                json.dump(new_tokens, f, ensure_ascii=False, indent=4)
            return True, len(new_tokens), len(uidpass_list), failed_uids

        return False, 0, len(uidpass_list), failed_uids
    except Exception as e:
        app.logger.error(f"Auto token refresh failed: {e}")
        return False, 0, 0, []


def load_tokens(auto_refresh=True):
    try:
        with open(TOKENS_FILE, "r", encoding="utf-8") as f:
            tokens = json.load(f)
        if isinstance(tokens, list) and tokens:
            return tokens
        raise ValueError("tokens.json is empty or invalid")
    except Exception as e:
        app.logger.error(f"Error loading tokens: {e}")
        if auto_refresh:
            ok, count, total, failed_uids = refresh_tokens_from_uidpass()
            if ok:
                app.logger.info(
                    f"Auto-refreshed tokens.json from uidpass.json. total={total} valid={count} failed={len(failed_uids)}"
                )
                return load_tokens(auto_refresh=False)
            app.logger.error(
                f"Token auto-refresh did not produce valid tokens. total={total} failed={len(failed_uids)}"
            )
        return None

def refresh_and_load_tokens():
    ok, count, total, failed_uids = refresh_tokens_from_uidpass()
    if not ok:
        app.logger.error(
            f"Token refresh failed. total={total} valid={count} failed={len(failed_uids)}"
        )
        return None
    app.logger.info(
        f"Refreshed tokens.json from uidpass.json. total={total} valid={count} failed={len(failed_uids)}"
    )
    return load_tokens(auto_refresh=False)

def auto_refresh_tokens_loop():
    interval_seconds = max(300, int(AUTO_TOKEN_REFRESH_HOURS * 60 * 60))
    while True:
        try:
            app.logger.info(f"Auto token refresh running. interval_hours={AUTO_TOKEN_REFRESH_HOURS}")
            refresh_and_load_tokens()
        except Exception as e:
            app.logger.error(f"Scheduled token refresh failed: {e}")
        time.sleep(interval_seconds)

if ENABLE_AUTO_TOKEN_REFRESH:
    threading.Thread(target=auto_refresh_tokens_loop, daemon=True).start()

def get_region_from_token(token):
    try:
        payload = token.split('.')[1]
        payload += '=' * (-len(payload) % 4)
        decoded_payload = base64.urlsafe_b64decode(payload).decode('utf-8')
        parsed_payload = json.loads(decoded_payload)
        return parsed_payload.get('lock_region', '').upper()
    except Exception as e:
        app.logger.error(f"Error decoding token payload: {e}")
        return ""

def normalize_region(region):
    region = str(region or "").strip().upper()
    if region in {"EU", "EUR"}:
        return "EUROPE"
    return region

def ffinfo_api_region(region):
    region = str(region or "").strip().upper()
    if region == "EUROPE":
        return "eu"
    return region.lower()

def token_region(token_item):
    token = token_item.get("token", "") if isinstance(token_item, dict) else str(token_item)
    return normalize_region(get_region_from_token(token))

def select_tokens_for_region(tokens, server_name):
    requested_region = normalize_region(server_name)
    return [item for item in tokens if token_region(item) == requested_region]

def select_token_for_region(tokens, server_name):
    matching_tokens = select_tokens_for_region(tokens, server_name)
    if matching_tokens:
        return matching_tokens[0]["token"]
    return None

def fetch_external_ffinfo(uid, server_name):
    if not FFINFO_API_URL:
        return None, "", {"error": "FFINFO_API_URL is disabled"}

    regions = [server_name] if server_name else FFINFO_DEFAULT_REGIONS
    last_error = {"error": "No region returned data"}
    for region in regions:
        headers = {}
        params = {"uid": uid, "region": ffinfo_api_region(region)}
        if FFINFO_API_KEY:
            headers["x-api-key"] = FFINFO_API_KEY

        try:
            response = requests.get(
                FFINFO_API_URL,
                params=params,
                headers=headers,
                timeout=UPSTREAM_TIMEOUT_SECONDS,
            )
            if response.status_code != 200:
                last_error = {
                    "region": normalize_region(region),
                    "status_code": response.status_code,
                    "body": response.text[:180],
                }
                app.logger.info(
                    f"FF info API returned {response.status_code} for region {region}: {response.text[:180]}"
                )
                continue
            data = response.json()
            basic_info = data.get("basicInfo", {}) if isinstance(data, dict) else {}
            resolved_region = normalize_region(basic_info.get("region") or region)
            return data, resolved_region, None
        except Exception as e:
            last_error = {"region": normalize_region(region), "error": str(e)}
            app.logger.info(f"External FF info request failed for region {region}: {e}")
    return None, "", last_error

def encrypt_message(plaintext):
    try:
        key = b'Yg&tc%DEuh6%Zc^8'
        iv = b'6oyZDr22E3ychjM%'
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded_message = pad(plaintext, AES.block_size)
        encrypted_message = cipher.encrypt(padded_message)
        return binascii.hexlify(encrypted_message).decode('utf-8')
    except Exception as e:
        app.logger.error(f"Error encrypting message: {e}")
        return None

def create_protobuf_message(user_id, region):
    try:
        message = like_pb2.like()
        message.uid = int(user_id)
        message.region = region
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Error creating protobuf message: {e}")
        return None

async def send_request(encrypted_uid, token, url):
    try:
        edata = bytes.fromhex(encrypted_uid)
        headers = {
            'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
            'Connection': "Keep-Alive",
            'Accept-Encoding': "gzip",
            'Authorization': f"Bearer {token}",
            'Content-Type': "application/x-www-form-urlencoded",
            'Expect': "100-continue",
            'X-Unity-Version': "2018.4.11f1",
            'X-GA': "v1 1",
            'ReleaseVersion': "OB53"
        }
        timeout = aiohttp.ClientTimeout(total=UPSTREAM_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, data=edata, headers=headers) as response:
                if response.status != 200:
                    body = await response.text()
                    app.logger.error(f"Like request failed with status {response.status}: {body[:100]}")
                    return response.status
                return await response.text()
    except Exception as e:
        app.logger.error(f"Exception in send_request: {e}")
        return None

async def send_multiple_requests(uid, server_name, url, tokens=None):
    try:
        region = server_name
        protobuf_message = create_protobuf_message(uid, region)
        if protobuf_message is None:
            app.logger.error("Failed to create protobuf message.")
            return None
        encrypted_uid = encrypt_message(protobuf_message)
        if encrypted_uid is None:
            app.logger.error("Encryption failed.")
            return None
        tasks = []
        if tokens is None:
            tokens = load_tokens()
        if tokens is None:
            app.logger.error("Failed to load tokens.")
            return None
        tokens = select_tokens_for_region(tokens, server_name)
        if not tokens:
            app.logger.error(f"No tokens found for region {server_name}.")
            return None
        for i in range(100):
            token = tokens[i % len(tokens)]["token"]
            tasks.append(send_request(encrypted_uid, token, url))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    except Exception as e:
        app.logger.error(f"Exception in send_multiple_requests: {e}")
        return None

def create_protobuf(uid):
    try:
        message = uid_generator_pb2.uid_generator()
        message.saturn_ = int(uid)
        message.garena = 1
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Error creating uid protobuf: {e}")
        return None

def enc(uid):
    protobuf_data = create_protobuf(uid)
    if protobuf_data is None:
        return None
    encrypted_uid = encrypt_message(protobuf_data)
    return encrypted_uid

def make_request(encrypt, server_name, token):
    try:
        if server_name == "IND":
            url = "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            url = "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
        else:
            url = "https://clientbp.ggpolarbear.com/GetPlayerPersonalShow"
        
        if not encrypt:
            app.logger.error("Invalid encrypt parameter (empty or None)")
            return None
            
        edata = bytes.fromhex(encrypt)
        headers = {
            'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
            'Connection': "Keep-Alive",
            'Accept-Encoding': "gzip",
            'Authorization': f"Bearer {token}",
            'Content-Type': "application/x-www-form-urlencoded",
            'Expect': "100-continue",
            'X-Unity-Version': "2018.4.11f1",
            'X-GA': "v1 1",
            'ReleaseVersion': "OB53"
        }
        response = requests.post(
            url,
            data=edata,
            headers=headers,
            verify=False,
            timeout=UPSTREAM_TIMEOUT_SECONDS,
        )
        
        if response.status_code != 200:
            app.logger.error(f"Server returned status {response.status_code}: {response.text[:100]}")
            return None
            
        hex_data = response.content.hex()
        binary = bytes.fromhex(hex_data)
        decode = decode_protobuf(binary)
        if decode is None:
            app.logger.error("Protobuf decoding returned None.")
            return None
        return decode
    except ValueError as e:
        app.logger.error(f"Error converting encrypt to hex: {e}")
        return None
    except Exception as e:
        app.logger.error(f"Error in make_request: {e}")
        return None

def decode_protobuf(binary):
    try:
        items = like_count_pb2.Info()
        items.ParseFromString(binary)
        return items
    except DecodeError as e:
        app.logger.error(f"Error decoding Protobuf data: {e}")
        return None
    except Exception as e:
        app.logger.error(f"Unexpected error during protobuf decoding: {e}")
        return None

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "credit": "https://t.me/mean_un",
        "message": "Welcome to the Free Fire Like API",
        "status": "API is running",
        "endpoints": "/like?uid=<uid> or /like?uid=<uid>&server_name=<server_name>",
        "example": "/like?uid=123456789 or /like?uid=123456789&server_name=bd"
})


@app.route('/like', methods=['GET'])
def handle_requests():
    uid = request.args.get("uid")
    if not uid:
        return jsonify({"error": "UID is required"}), 400

    try:
        tokens = load_tokens()
        if tokens is None or not tokens:
            return jsonify({"error": "Failed to load tokens."}), 500
        
        # Extract server_name (lock_region) from token if not provided
        server_name = request.args.get("server_name", "").upper()
        if not server_name:
            token = tokens[0]['token']
            server_name = get_region_from_token(token)
        
        if not server_name:
            return jsonify({"error": "server_name could not be determined from token or input"}), 400
        server_name = normalize_region(server_name)

        token = select_token_for_region(tokens, server_name)
        if token is None:
            app.logger.info(f"No token for {server_name}. Refreshing tokens and retrying selection once.")
            tokens = refresh_and_load_tokens()
            token = select_token_for_region(tokens or [], server_name)
            if token is None:
                available_regions = sorted({token_region(item) for item in (tokens or []) if token_region(item)})
                return jsonify({
                    "error": f"No valid token found for region {server_name}. Add a {server_name} UID/password in uidpass.json and refresh tokens.",
                    "available_regions": available_regions,
                }), 500
        
        encrypted_uid = enc(uid)
        if encrypted_uid is None:
            return jsonify({"error": "Encryption of UID failed."}), 500

        # Get before likes count
        before = make_request(encrypted_uid, server_name, token)
        if before is None:
            app.logger.info("Player info failed before likes. Refreshing tokens and retrying once.")
            tokens = refresh_and_load_tokens()
            if tokens is None or not tokens:
                return jsonify({"error": "Token expired and auto-refresh failed. Check uidpass.json."}), 500
            token = select_token_for_region(tokens, server_name)
            if token is None:
                return jsonify({"error": f"Token refresh succeeded, but no token exists for region {server_name}."}), 500
            before = make_request(encrypted_uid, server_name, token)
            if before is None:
                return jsonify({"error": "Failed to retrieve player info. There are no valid token found! please update tokens.json with valid tokens"}), 500
        
        data_before = json.loads(MessageToJson(before))
        before_like = int(data_before.get('AccountInfo', {}).get('Likes', 0) or 0)
        app.logger.info(f"Likes before: {before_like}")

        # Determine URL based on server
        if server_name == "IND":
            url = "https://client.ind.freefiremobile.com/LikeProfile"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            url = "https://client.us.freefiremobile.com/LikeProfile"
        else:
            url = "https://clientbp.ggpolarbear.com/LikeProfile"

        # Send like requests
        requests_sent = asyncio.run(send_multiple_requests(uid, server_name, url, tokens=tokens))
        app.logger.info(f"Requests sent: {requests_sent}")
        if requests_sent is None:
            return jsonify({"error": "Failed to send like requests."}), 500

        # Get after likes count
        after = make_request(encrypted_uid, server_name, token)
        if after is None:
            app.logger.info("Player info failed after likes. Refreshing tokens and retrying once.")
            tokens = refresh_and_load_tokens()
            if tokens is None or not tokens:
                return jsonify({"error": "Token expired and auto-refresh failed after likes. Check uidpass.json."}), 500
            token = select_token_for_region(tokens, server_name)
            if token is None:
                return jsonify({"error": f"Token refresh succeeded, but no token exists for region {server_name}."}), 500
            after = make_request(encrypted_uid, server_name, token)
            if after is None:
                return jsonify({"error": "Failed to retrieve player info after likes."}), 500
        
        data_after = json.loads(MessageToJson(after))
        account_info = data_after.get('AccountInfo', {})
        after_like = int(account_info.get('Likes', 0) or 0)
        player_uid = str(account_info.get('UID', uid) or uid)  # Keep as string
        player_name = str(account_info.get('PlayerNickname', 'Unknown')).strip()
        
        like_given = after_like - before_like
        
        return jsonify({
            "credit": "https://t.me/mean_un",
            "LikesGivenByAPI": like_given,
            "LikesafterCommand": after_like,
            "LikesbeforeCommand": before_like,
            "PlayerNickname": player_name,
            "Region": server_name,
            "UID": player_uid,
            "status": 1 if like_given > 0 else 2
        })
    except Exception as e:
        app.logger.error(f"Error processing request: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/ffinfo', methods=['GET'])
def handle_ffinfo():
    uid = request.args.get("uid")
    if not uid:
        return jsonify({"error": "UID is required"}), 400

    try:
        server_name = request.args.get("server_name", "").upper()

        external_data, resolved_region, external_error = fetch_external_ffinfo(uid, normalize_region(server_name))
        if external_data:
            return jsonify({
                "source": "external",
                "Region": resolved_region,
                "data": external_data,
                "status": 1,
            })
        if server_name:
            return jsonify({
                "error": f"Full ffinfo API did not return data for region {normalize_region(server_name)}.",
                "details": external_error,
            }), 502

        tokens = load_tokens()
        if tokens is None or not tokens:
            return jsonify({"error": "Failed to load tokens and full ffinfo API did not return data."}), 500

        if not server_name:
            token = tokens[0]['token']
            server_name = get_region_from_token(token)
        if not server_name:
            return jsonify({"error": "server_name could not be determined from token or input"}), 400
        server_name = normalize_region(server_name)

        token = select_token_for_region(tokens, server_name)
        if token is None:
            app.logger.info(f"No token for {server_name}. Refreshing tokens and retrying selection once.")
            tokens = refresh_and_load_tokens()
            token = select_token_for_region(tokens or [], server_name)
            if token is None:
                available_regions = sorted({token_region(item) for item in (tokens or []) if token_region(item)})
                return jsonify({
                    "error": f"Full ffinfo API did not return data, and no local token exists for region {server_name}.",
                    "available_regions": available_regions,
                }), 500

        encrypted_uid = enc(uid)
        if encrypted_uid is None:
            return jsonify({"error": "Encryption of UID failed."}), 500

        player_info = make_request(encrypted_uid, server_name, token)
        if player_info is None:
            app.logger.info("FF info failed. Refreshing tokens and retrying once.")
            tokens = refresh_and_load_tokens()
            if tokens is None or not tokens:
                return jsonify({"error": "Token expired and auto-refresh failed. Check uidpass.json."}), 500
            token = select_token_for_region(tokens, server_name)
            if token is None:
                return jsonify({"error": f"Token refresh succeeded, but no token exists for region {server_name}."}), 500
            player_info = make_request(encrypted_uid, server_name, token)
            if player_info is None:
                return jsonify({"error": "Failed to retrieve player info after token refresh."}), 500

        data = json.loads(MessageToJson(player_info))
        account_info = data.get('AccountInfo', {})
        return jsonify({
            "source": "local",
            "Region": server_name,
            "data": {
                "basicInfo": {
                    "accountId": str(account_info.get('UID', uid) or uid),
                    "nickname": str(account_info.get('PlayerNickname', 'Unknown')).strip(),
                    "liked": int(account_info.get('Likes', 0) or 0),
                    "region": server_name,
                }
            },
            "status": 1,
        })
    except Exception as e:
        app.logger.error(f"Error processing ffinfo request: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv("PORT", "5001"))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)

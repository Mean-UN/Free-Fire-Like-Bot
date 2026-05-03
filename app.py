from flask import Flask, request, jsonify
import asyncio
import os
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
FFINFO_API_URL = os.getenv("FFINFO_API_URL", "").strip()
FFINFO_API_KEY = os.getenv("FFINFO_API_KEY", "").strip()

def refresh_tokens_from_uidpass():
    try:
        with open(UIDPASS_FILE, "r", encoding="utf-8") as f:
            uidpass_list = json.load(f)
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

def fetch_external_ffinfo(uid, server_name):
    if not FFINFO_API_URL:
        return None

    try:
        headers = {}
        params = {"uid": uid}
        if server_name:
            params["region"] = server_name.lower()
        if FFINFO_API_KEY:
            headers["x-api-key"] = FFINFO_API_KEY

        response = requests.get(
            FFINFO_API_URL,
            params=params,
            headers=headers,
            timeout=UPSTREAM_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            app.logger.error(f"FF info API returned {response.status_code}: {response.text[:180]}")
            return None
        return response.json()
    except Exception as e:
        app.logger.error(f"External FF info request failed: {e}")
        return None

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

async def send_multiple_requests(uid, server_name, url):
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
        tokens = load_tokens()
        if tokens is None:
            app.logger.error("Failed to load tokens.")
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
        token = tokens[0]['token']
        
        # Extract server_name (lock_region) from token if not provided
        server_name = request.args.get("server_name", "").upper()
        if not server_name:
            server_name = get_region_from_token(token)
        
        if not server_name:
            return jsonify({"error": "server_name could not be determined from token or input"}), 400
        
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
            token = tokens[0]['token']
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
        requests_sent = asyncio.run(send_multiple_requests(uid, server_name, url))
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
            token = tokens[0]['token']
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
        tokens = load_tokens()
        if tokens is None or not tokens:
            return jsonify({"error": "Failed to load tokens."}), 500

        token = tokens[0]['token']
        server_name = request.args.get("server_name", "").upper()
        if not server_name:
            server_name = get_region_from_token(token)
        if not server_name:
            return jsonify({"error": "server_name could not be determined from token or input"}), 400

        external_data = fetch_external_ffinfo(uid, server_name)
        if external_data:
            return jsonify({
                "source": "external",
                "Region": server_name,
                "data": external_data,
                "status": 1,
            })

        encrypted_uid = enc(uid)
        if encrypted_uid is None:
            return jsonify({"error": "Encryption of UID failed."}), 500

        player_info = make_request(encrypted_uid, server_name, token)
        if player_info is None:
            app.logger.info("FF info failed. Refreshing tokens and retrying once.")
            tokens = refresh_and_load_tokens()
            if tokens is None or not tokens:
                return jsonify({"error": "Token expired and auto-refresh failed. Check uidpass.json."}), 500
            token = tokens[0]['token']
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

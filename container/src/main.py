import logging
from http import HTTPStatus

from connector import TelegramConnector
from telegram import get_telegram_client
from timescale import get_timescale_client
from flask import Flask, request, jsonify

logging.getLogger().setLevel(logging.INFO)

app = Flask(__name__)

@app.route("/", methods=["POST"])
def run_connector():
    logging.info("Container initialized")

    try:
        request_data = request.get_json()
        if not request_data:
            return jsonify({"error": "Missing request body with account credentials"}), HTTPStatus.BAD_REQUEST

        account_id = request_data.get("accountId")
        api_id = request_data.get("apiId")
        api_hash = request_data.get("apiHash")
        session_str = request_data.get("sessionStr")

        if not all([account_id, api_id, api_hash, session_str]):
            return jsonify({"error": "Missing required fields: accountId, apiId, apiHash, sessionStr"}), HTTPStatus.BAD_REQUEST

        logging.info(f"Processing account {account_id}")

        telegram = get_telegram_client(api_id=api_id, api_hash=api_hash, session_str=session_str)
        timescale = get_timescale_client()

        connector = TelegramConnector(timescale, telegram, account_id=account_id)
        logging.info(f"TelegramConnector instance created for account {account_id}")
        connector.start()
        return jsonify({"status": "Connector finished", "accountId": account_id}), HTTPStatus.OK
    except Exception as e:
        logging.error(f"Error in run_connector: {e}")
        return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR

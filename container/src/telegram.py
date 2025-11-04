import env

from telethon import TelegramClient
from telethon.sessions import StringSession


def get_telegram_client(api_id=None, api_hash=None, session_str=None):
    """
    Creates a TelegramClient instance with provided credentials or falls back to env vars.
    
    Args:
        api_id: Telegram API ID (optional, falls back to env.TELEGRAM_API_ID)
        api_hash: Telegram API hash (optional, falls back to env.TELEGRAM_API_HASH)
        session_str: Telegram session string (optional, falls back to env.TELEGRAM_SESSION_STR)
    
    Returns:
        TelegramClient instance
    """
    if api_id is None:
        api_id = env.TELEGRAM_API_ID
    if api_hash is None:
        api_hash = env.TELEGRAM_API_HASH
    if session_str is None:
        session_str = env.TELEGRAM_SESSION_STR

    if not all([api_id, api_hash, session_str]):
        raise ValueError("Missing Telegram credentials. Provide api_id, api_hash, and session_str or set env vars.")

    string_session = StringSession(session_str)
    return TelegramClient(
        session=string_session, api_id=int(api_id), api_hash=api_hash
    )

import env
import psycopg2
import logging
import json
from typing import List, Dict
from psycopg2.extras import execute_values
from retry import retry_with_backoff
from errors import ErrorType


class TimescaleClient:
    def __init__(self, url: str):
        self.connection_url = url
        self.connection = None
        self._connect()

    def _connect(self):
        try:
            self.connection = psycopg2.connect(self.connection_url)
            self.connection.autocommit = True
        except Exception as e:
            logging.error(f"Unable to connect to TimescaleDB: {e}")
            raise

    def insert_messages_batch(self, messages: List[Dict], batch_size: int = 1000):
        """
        Insert messages in batches to avoid memory issues and connection timeouts.
        
        Args:
            messages: List of message dictionaries to insert
            batch_size: Maximum number of messages to insert per batch
        """
        if not messages:
            return

        total_messages = len(messages)
        logging.info(f"Inserting {total_messages} messages in batches of {batch_size}...")

        for batch_start in range(0, total_messages, batch_size):
            batch_end = min(batch_start + batch_size, total_messages)
            batch = messages[batch_start:batch_end]
            
            logging.info(f"Processing batch {batch_start//batch_size + 1}: messages {batch_start+1}-{batch_end} of {total_messages}")
            self._insert_batch(batch)

    def _insert_batch(self, messages: List[Dict]):
        """Insert a single batch of messages with retry logic."""
        def _do_insert():
            sql_insert_unique_messages = """
            INSERT INTO unique_messages (content, embedding)
            VALUES %s
            ON CONFLICT (content) DO NOTHING;
            """

            sql_fetch_message_ids = """
            SELECT content, id FROM unique_messages
            WHERE content IN %s;
            """

            sql_insert_message_feed = """
            INSERT INTO message_feed (
                timestamp, platform_name, platform_user_id, platform_user_name,
                platform_message_id, platform_message_url, source_account_id, source_channel_name,
                source_channel_id, platform_specific, message_id
            )
            VALUES %s
            ON CONFLICT (timestamp, platform_name, platform_message_id, source_account_id) DO NOTHING;
            """

            if not self.connection or self.connection.closed:
                self._connect()

            with self.connection.cursor() as cursor:
                try:
                    unique_message_values = [
                        (msg["message"]["text"], None) for msg in messages
                    ]

                    # Insert unique messages
                    execute_values(cursor, sql_insert_unique_messages, unique_message_values)

                    # Fetch message IDs for unique messages
                    unique_message_contents = tuple(
                        [msg["message"]["text"] for msg in messages]
                    )
                    cursor.execute(sql_fetch_message_ids, (unique_message_contents,))
                    unique_message_map = dict(cursor.fetchall())

                    message_feed_values = []
                    failed_rows = []

                    for msg in messages:
                        try:
                            message_text = msg["message"]["text"]
                            unique_message_id = unique_message_map[message_text]

                            message_feed_data = (
                                msg["timestamp"],
                                msg["source"]["platform"],
                                msg["user"]["id"],
                                msg["user"]["name"],
                                msg["message"]["id"],
                                msg["message"].get("url", None),
                                msg["source"].get("account_id"),
                                msg["source"]["channel"].get("name", None),
                                msg["source"]["channel"].get("id", None),
                                json.dumps(msg.get("platform_specific", {})),
                                unique_message_id,
                            )
                            message_feed_values.append(message_feed_data)

                        except Exception as row_error:
                            failed_rows.append((msg, str(row_error)))
                            logging.error(
                                f"Failed to process row for message {msg['message']['id']}: {row_error}"
                            )

                    if message_feed_values:
                        execute_values(cursor, sql_insert_message_feed, message_feed_values)

                    self.connection.commit()
                    logging.info(f"Batch inserted {len(message_feed_values)} messages into message feed.")

                    if failed_rows:
                        logging.warning(
                            f"Failed to process {len(failed_rows)} rows in this batch."
                        )

                except Exception as e:
                    self.connection.rollback()
                    logging.error(f"Failed to batch insert messages: {e}")
                    raise
        
        # Retry with exponential backoff for transient errors
        try:
            retry_with_backoff(
                _do_insert,
                max_retries=3,
                initial_delay=1.0,
                max_delay=10.0,
                error_type_filter=ErrorType.RETRYABLE
            )
        except Exception as e:
            from errors import classify_error
            error_type = classify_error(e)
            if error_type == ErrorType.FATAL:
                raise
            # For connection errors, try reconnecting once
            if error_type == ErrorType.CONNECTION:
                logging.warning("Connection error detected. Attempting to reconnect...")
                try:
                    self._connect()
                    _do_insert()
                except Exception as retry_error:
                    logging.error(f"Retry after reconnect failed: {retry_error}")
                    raise
            else:
                raise

    def close(self):
        """Close the database connection. Should only be called when done with the client."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logging.info("Database connection closed.")


def get_timescale_client():
    timescale_connection_url = env.TIMESCALE_CONNECTION
    return TimescaleClient(timescale_connection_url)

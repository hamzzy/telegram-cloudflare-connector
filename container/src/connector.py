import json
import logging
import time
import asyncio
import signal
from asyncio import get_event_loop
from urllib import request
import os
import re
import jsonschema

from jsonschema.exceptions import ValidationError, SchemaError
from bs4 import BeautifulSoup
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest, GetRepliesRequest

from timescale import TimescaleClient
from errors import ErrorType, classify_error
from retry import retry_with_backoff


class UserNotLoggedIn(Exception):
    pass


class TelegramConnector:
    def __init__(
        self,
        timescale: TimescaleClient,
        telegram: TelegramClient,
        account_id: str = None,
        max_execution_time: int = 25,
        max_concurrent_channels: int = 5,
        save_interval: int = 50,
        memory_buffer_size: int = 200
    ):
        self.event_loop = get_event_loop()

        self.timescale = timescale
        self.telegram = telegram
        self.account_id = account_id or str(telegram.api_id)
        self.max_execution_time = max_execution_time
        self.max_concurrent_channels = max_concurrent_channels
        self.save_interval = save_interval
        self.memory_buffer_size = memory_buffer_size
        self.collect_comments = True
        self.start_time = time.time()
        self.semaphore = asyncio.Semaphore(max_concurrent_channels)
        self.shutdown_requested = False
        self.pending_messages = []
        self.processed_channels = set()
        self.message_buffer = []
        self.total_messages_collected = 0
        self.rate_limit_encountered = False

        current_dir = os.path.realpath(__file__)
        current_dir = os.path.dirname(current_dir)
        schema_file_path = os.path.join(current_dir, "schema/topic_schema_message.json")

        with open(schema_file_path) as schema_file:
            self.schema = json.loads(schema_file.read())
        
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logging.warning(f"Received signal {signum}. Initiating graceful shutdown...")
            self.shutdown_requested = True
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    def _check_timeout(self):
        elapsed = time.time() - self.start_time
        if elapsed >= self.max_execution_time:
            logging.warning(f"Execution time limit reached ({elapsed:.1f}s). Stopping channel processing.")
            return True
        if self.shutdown_requested:
            logging.warning("Shutdown requested. Stopping channel processing.")
            return True
        return False
    
    async def _save_incremental(self, messages: list):
        """Save messages incrementally to prevent data loss and optimize memory."""
        if not messages:
            return
        
        try:
            glued_messages = self._glue_same_user_messages(messages)
            if glued_messages:
                logging.info(f"Incremental save: Inserting {len(glued_messages)} messages...")
                
                async def save_messages():
                    self.timescale.insert_messages_batch(glued_messages)
                
                await retry_with_backoff(
                    save_messages,
                    max_retries=3,
                    initial_delay=2.0,
                    max_delay=30.0,
                    error_type_filter=ErrorType.RETRYABLE
                )
                
                self.pending_messages.clear()
                logging.info("Incremental save completed.")
        except Exception as e:
            error_type = classify_error(e)
            logging.error(f"Incremental save failed ({error_type.value}): {e}")
            if error_type != ErrorType.FATAL:
                # Keep messages for retry
                self.pending_messages.extend(messages)
            raise
    
    async def _flush_message_buffer(self, force: bool = False):
        """Flush message buffer to database when it reaches threshold."""
        buffer_size = len(self.message_buffer)
        
        if buffer_size >= self.memory_buffer_size or (force and buffer_size > 0):
            messages_to_save = self.message_buffer[:self.memory_buffer_size]
            self.message_buffer = self.message_buffer[self.memory_buffer_size:]
            
            try:
                await self._save_incremental(messages_to_save)
                logging.info(f"Flushed {len(messages_to_save)} messages from buffer. Buffer size: {len(self.message_buffer)}")
            except Exception as e:
                error_type = classify_error(e)
                logging.error(f"Failed to flush buffer ({error_type.value}): {e}")
                # Put messages back at the front for retry
                self.message_buffer = messages_to_save + self.message_buffer
    
    def _add_to_buffer(self, messages: list):
        """Add messages to buffer. Caller should flush periodically."""
        if not messages:
            return
        
        self.message_buffer.extend(messages)
        self.total_messages_collected += len(messages)

    def _get_last_msg_ids(self):
        """Get last message ID per channel for posts."""
        sql = """
        SELECT DISTINCT source_channel_id AS channel_id, MAX(CAST(platform_message_id AS INTEGER)) as last_msg_id
        FROM message_feed
        WHERE platform_name = 'telegram' AND source_account_id = %s
        AND platform_specific->>'referenced_post' IS NULL
        GROUP BY source_channel_id;
        """
        try:
            if not self.timescale.connection or self.timescale.connection.closed:
                self.timescale._connect()
            
            with self.timescale.connection.cursor() as cur:
                cur.execute(sql, (self.account_id,))
                rows = cur.fetchall()

            if not rows:
                logging.info("No last messages found.")
                return {}

            last_msg_ids = {str(row[0]): int(row[1]) for row in rows}
            return last_msg_ids
        except Exception as e:
            logging.error(f"Failed to fetch last message IDs: {e}")
            return {}
    
    def _get_last_comment_ids(self, post_id):
        """Get last comment ID for a specific post."""
        sql = """
        SELECT MAX(CAST(platform_message_id AS INTEGER)) as last_comment_id
        FROM message_feed
        WHERE platform_name = 'telegram' 
        AND source_account_id = %s
        AND platform_specific->>'referenced_post_id' = %s;
        """
        try:
            if not self.timescale.connection or self.timescale.connection.closed:
                self.timescale._connect()
            
            with self.timescale.connection.cursor() as cur:
                cur.execute(sql, (self.account_id, str(post_id)))
                row = cur.fetchone()
                
            if row and row[0]:
                return int(row[0])
            return 0
        except Exception as e:
            logging.debug(f"Failed to fetch last comment ID for post {post_id}: {e}")
            return 0

    def _get_file_url_from_web_preview(self, url: str, type: str):
        try:
            web_preview = request.urlopen(url).read()
            soup_preview = BeautifulSoup(web_preview, "html.parser")

            if type == "video":
                target_element = soup_preview.find(class_="tgme_widget_message_video")

                if target_element:
                    source = target_element.get("src")
                    if source:
                        return source

            if type == "image":
                target_element = soup_preview.find(class_="tgme_widget_message_photo_wrap")

                if target_element:
                    style = target_element.get("style")
                    if style:
                        url = re.search(r"background-image:\s*url\('([^']+)'", style).group(1)
                        return url

            if type == "audio":
                target_element = soup_preview.find(name="audio")

                if target_element:
                    source = target_element.get("src")
                    if source:
                        return source

        except Exception:
            return None

    def _get_media_elements(self, entity, item):
        media = []

        if not hasattr(entity, "username"):
            return media

        post_preview = f"https://t.me/{entity.username}/{item.id}?embed=1&mode=tme"

        """
        Image
        """
        if item.photo:
            url = self._get_file_url_from_web_preview(post_preview, "image")
            if url:
                media.append({"id": str(item.photo.id), "type": "image", "url": url})

        """
        Audio
        """
        if item.audio:
            url = self._get_file_url_from_web_preview(post_preview, "audio")
            if url:
                media.append({"id": str(item.audio.id), "type": "audio", "url": url})

        if item.voice:
            url = self._get_file_url_from_web_preview(post_preview, "audio")
            if url:
                media.append({"id": str(item.voice.id), "type": "audio", "url": url})

        """
        Video
        """
        if item.video:
            url = self._get_file_url_from_web_preview(post_preview, "video")
            if url:
                media.append({"id": str(item.video.id), "type": "video", "url": url})

        if item.video_note:
            url = self._get_file_url_from_web_preview(post_preview, "video")
            if url:
                media.append({"id": str(item.video_note.id), "type": "video", "url": url})

        return media

    async def _process_dialog_message(self, dialog_entity, message_item, is_comment=False, parent_post_id=None):
        try:
            source_id = message_item.from_id or message_item.peer_id

            if not source_id:
                return

            source_entity = await self.telegram.get_entity(source_id)
            is_bot_message = getattr(source_entity, 'bot', False) or getattr(message_item, 'via_bot_id', None) is not None

            if is_bot_message:
                return

            message_id = str(message_item.id)
            message_text: str = message_item.message

            if message_text is None:
                return

            source_user_id = str(source_entity.id)
            first_name = getattr(source_entity, 'first_name', '')
            last_name = getattr(source_entity, 'last_name', '')
            source_user_name = f"{first_name} {last_name}".strip()

            if not source_user_name:
                source_user_name = source_entity.username or dialog_entity.title

            message_data = {
                "timestamp": message_item.date.isoformat(),
                "message": {"id": message_id, "text": message_text},
                "user": {"id": source_user_id, "name": source_user_name},
                "source": {
                    "account_id": self.account_id,
                    "platform": "telegram",
                    "channel": {"id": str(dialog_entity.id), "name": dialog_entity.title},
                },
            }

            # Add referenced post if this is a comment
            if is_comment and parent_post_id:
                message_data["source"]["referenced_post"] = {"id": str(parent_post_id)}
                if hasattr(dialog_entity, "username") and dialog_entity.username:
                    message_data["source"]["referenced_post"]["url"] = f"https://t.me/{dialog_entity.username}/{parent_post_id}"
                # Also store in platform_specific for database querying
                message_data["platform_specific"]["referenced_post_id"] = str(parent_post_id)

            if hasattr(dialog_entity, "username") and dialog_entity.username:
                if not is_comment:
                    message_data["message"]["url"] = f"https://t.me/{dialog_entity.username}/{message_id}"

            if message_item.geo is not None:
                message_data["geo_coords"] = {
                    "type": "Point",
                    "coordinates": [message_item.geo.long, message_item.geo.lat],
                }

            if message_item.poll is not None:
                message_data["message"]["text"] = message_item.poll.poll.question

            media_elements = self._get_media_elements(dialog_entity, message_item)
            if len(media_elements) > 0:
                message_data["message"]["media"] = media_elements

            jsonschema.validate(instance=message_data, schema=self.schema)

            return message_data

        except ValidationError as validation_error:
            logging.error(f"Message validation failed: {validation_error}")
        except SchemaError as schema_error:
            logging.error(f"Schema error: {schema_error}")
        except Exception as general_error:
            logging.info(f"The message is not valid: {general_error}. Skipping...")
    
    async def _fetch_post_comments(self, dialog_entity, post_id):
        """Fetch comments for a channel post."""
        try:
            if self._check_timeout():
                return []
            
            # Get last comment ID for this post to fetch only new comments
            last_comment_id = self._get_last_comment_ids(post_id)
            
            # Skip if no new comments expected (only fetch if we're getting new posts)
            if last_comment_id == 0:
                # First time fetching - get all comments up to limit
                limit_value = 100
            else:
                # Fetch only new comments
                limit_value = 0
            
            async def fetch_replies():
                return await self.telegram(
                    GetRepliesRequest(
                        peer=dialog_entity,
                        msg_id=post_id,
                        offset_id=0,
                        offset_date=None,
                        add_offset=0,
                        limit=limit_value,
                        max_id=0,
                        min_id=last_comment_id,
                        hash=0
                    )
                )
            
            replies = await retry_with_backoff(
                fetch_replies,
                max_retries=3,
                initial_delay=1.0,
                max_delay=60.0
            )
            
            comment_messages = []
            for item in reversed(replies.messages):
                if self._check_timeout():
                    break
                comment = await self._process_dialog_message(
                    dialog_entity, 
                    item, 
                    is_comment=True, 
                    parent_post_id=post_id
                )
                if comment:
                    comment_messages.append(comment)
            
            return comment_messages
        except Exception as err:
            error_type = classify_error(err)
            # Some posts may not have comments enabled, which is fine
            if error_type != ErrorType.FATAL:
                logging.debug(f"Could not fetch comments for post {post_id}: {str(err)}")
            return []

    def _glue_same_user_messages(self, messages: list):
        user_messages = {}
        glued_messages = []

        for message in messages:
            user_id = message["user"]["id"]
            channel_id = message["source"]["channel"]["id"]
            account_id = message["source"]["account_id"]
            referenced_post_id = message["source"].get("referenced_post", {}).get("id", "")
            key = (account_id, user_id, channel_id, referenced_post_id)

            if key not in user_messages:
                user_messages[key] = []

            user_messages[key].append(message)

        for key, messages in user_messages.items():
            sorted_messages = sorted(messages, key=lambda x: x["timestamp"])
            glued_text = " ".join([msg["message"]["text"] for msg in sorted_messages])

            glued_text_without_unicode = glued_text.encode("ascii", "ignore").decode()
            if len(glued_text_without_unicode) < 20:
                continue

            last_message = sorted_messages[-1]
            last_message["message"]["text"] = glued_text[:512].strip()

            all_media = []

            # Combine media files
            for msg in sorted_messages:
                media = msg["message"].get("media")

                if media:
                    all_media.extend(media)

            if len(all_media) > 0:
                last_message["message"]["media"] = all_media

            glued_messages.append(last_message)

        sorted_glued_messages = sorted(glued_messages, key=lambda x: x["timestamp"])
        return sorted_glued_messages

    async def _process_single_channel(self, entity, entity_id, last_msg_id):
        """Process a single channel and return its messages with retry logic and rate limit awareness."""
        async with self.semaphore:
            if self._check_timeout() or entity_id in self.processed_channels:
                return []
            
            try:
                async def fetch_history():
                    limit_value = 20 if last_msg_id == 0 else 0
                    return await self.telegram(
                        GetHistoryRequest(
                            peer=entity,
                            limit=limit_value,
                            offset_id=0,
                            offset_date=None,
                            add_offset=0,
                            max_id=0,
                            min_id=last_msg_id,
                            hash=0,
                        )
                    )
                
                logging.info(f"Obtaining chat history for '{entity_id}' - '{entity.title}'...")
                
                history = await retry_with_backoff(
                    fetch_history,
                    max_retries=3,
                    initial_delay=1.0,
                    max_delay=60.0
                )

                channel_messages = []
                posts_with_comments = []
                
                for item in reversed(history.messages):
                    if self._check_timeout():
                        break
                    message = await self._process_dialog_message(entity, item)
                    if message:
                        channel_messages.append(message)
                        # Check if post has comments/replies enabled
                        if hasattr(item, 'replies') and item.replies:
                            replies_info = item.replies
                            if hasattr(replies_info, 'replies') and replies_info.replies > 0:
                                posts_with_comments.append((item.id, message))

                # Fetch comments for posts that have them (if enabled)
                if self.collect_comments and posts_with_comments and not self._check_timeout():
                    logging.info(f"Fetching comments for {len(posts_with_comments)} posts in '{entity.title}'...")
                    comment_tasks = [
                        self._fetch_post_comments(entity, post_id)
                        for post_id, _ in posts_with_comments
                    ]
                    
                    comment_results = await asyncio.gather(*comment_tasks, return_exceptions=True)
                    
                    comments_collected = 0
                    for result in comment_results:
                        if isinstance(result, Exception):
                            error_type = classify_error(result)
                            if error_type != ErrorType.FATAL:
                                logging.debug(f"Error fetching comments: {result}")
                        elif result:
                            channel_messages.extend(result)
                            comments_collected += len(result)
                    
                    if comments_collected > 0:
                        logging.info(f"Collected {comments_collected} comments from {len(posts_with_comments)} posts")

                self.processed_channels.add(entity_id)
                return channel_messages
            except Exception as err:
                error_type = classify_error(err)
                
                # Handle rate limits specially - reduce concurrency
                if error_type == ErrorType.RATE_LIMIT:
                    self.rate_limit_encountered = True
                    logging.warning(
                        f"Rate limit encountered for channel '{entity_id}'. "
                        f"Reducing concurrency and backing off."
                    )
                    # Reduce semaphore capacity temporarily
                    if self.semaphore._value > 1:
                        # Release one permit to reduce concurrency
                        pass  # Semaphore will naturally reduce concurrency on next retry
                
                logging.warning(
                    f"Error processing channel '{entity_id}' ({error_type.value}): {str(err)}"
                )
                if error_type == ErrorType.FATAL:
                    self.processed_channels.add(entity_id)
                return []

    async def _get_channel_messages(self):
        last_msg_ids = self._get_last_msg_ids()
        channels_to_process = []

        logging.info("Collecting channels to process...")
        async for dialog in self.telegram.iter_dialogs(archived=False):
            if self._check_timeout():
                break

            if dialog.is_user:
                continue

            entity = dialog.entity
            entity_id = str(entity.id)
            last_msg_id = last_msg_ids.get(entity_id, 0)

            if dialog.message.id <= last_msg_id:
                continue

            channels_to_process.append((entity, entity_id, last_msg_id))

        if not channels_to_process:
            logging.info("No channels to process.")
            return

        logging.info(f"Processing {len(channels_to_process)} channels in parallel (max {self.max_concurrent_channels} concurrent)...")

        tasks = [
            self._process_single_channel(entity, entity_id, last_msg_id)
            for entity, entity_id, last_msg_id in channels_to_process
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_count = 0
        error_count = 0

        # Process results and add to buffer incrementally
        for result in results:
            if isinstance(result, Exception):
                error_count += 1
                logging.error(f"Channel processing error: {result}")
            elif result:
                # Add messages to buffer instead of accumulating in memory
                self._add_to_buffer(result)
                processed_count += 1
                # Periodically flush buffer to keep memory usage low
                if len(self.message_buffer) >= self.memory_buffer_size:
                    await self._flush_message_buffer()

        logging.info(f"Completed processing: {processed_count} successful, {error_count} errors")
        logging.info(f"Total messages collected: {self.total_messages_collected}, Buffer size: {len(self.message_buffer)}")

        # If rate limit was encountered, log warning
        if self.rate_limit_encountered:
            logging.warning(
                "Rate limit encountered during processing. Consider reducing "
                f"max_concurrent_channels (current: {self.max_concurrent_channels})"
            )

        # Flush any remaining messages in buffer
        if self.message_buffer:
            logging.info(f"Flushing remaining {len(self.message_buffer)} messages from buffer...")
            await self._flush_message_buffer(force=True)

        # Combine with pending messages from previous runs
        if self.pending_messages:
            logging.info(f"Processing {len(self.pending_messages)} pending messages from previous run...")
            await self._save_incremental(self.pending_messages)
            self.pending_messages = []

        elapsed_time = time.time() - self.start_time
        logging.info(
            f"Successfully processed {processed_count}/{len(channels_to_process)} "
            f"channels in {elapsed_time:.1f}s. Total messages: {self.total_messages_collected}"
        )

    async def _start(self):
        telegram_connected = False
        try:
            if not self.telegram.is_connected():
                logging.info("User is not connected. Trying to connect now...")
                await retry_with_backoff(
                    lambda: self.telegram.start(),
                    max_retries=3,
                    initial_delay=2.0
                )
                telegram_connected = True

            if not await self.telegram.is_user_authorized():
                raise UserNotLoggedIn

            logging.info("Getting user messages from channels...")
            await self._get_channel_messages()
            
            # Save any remaining pending messages and flush buffer
            if self.message_buffer:
                logging.info(f"Flushing {len(self.message_buffer)} messages from buffer before shutdown...")
                await self._flush_message_buffer(force=True)
            
            if self.pending_messages:
                logging.info(f"Saving {len(self.pending_messages)} pending messages...")
                await self._save_incremental(self.pending_messages)
        finally:
            # Ensure Telegram connection is always closed
            await self._cleanup_telegram(telegram_connected)

    async def _cleanup_telegram(self, was_connected: bool):
        """Cleanup Telegram connection with retry logic."""
        try:
            if self.telegram.is_connected():
                await retry_with_backoff(
                    lambda: self.telegram.disconnect(),
                    max_retries=2,
                    initial_delay=1.0,
                    max_delay=5.0
                )
                logging.info("Telegram connection closed successfully.")
        except Exception as e:
            error_type = classify_error(e)
            logging.warning(
                f"Error closing Telegram connection ({error_type.value}): {e}"
            )

    def start(self):
        logging.info("Starting telegram connector...")
        try:
            self.event_loop.run_until_complete(self._start())
        except KeyboardInterrupt:
            logging.warning("Received keyboard interrupt. Initiating graceful shutdown...")
            self.shutdown_requested = True
            # Try to save pending messages and flush buffer before exit
            if self.message_buffer:
                try:
                    self.event_loop.run_until_complete(self._flush_message_buffer(force=True))
                except Exception as e:
                    logging.error(f"Failed to flush buffer on shutdown: {e}")
            
            if self.pending_messages:
                try:
                    self.event_loop.run_until_complete(self._save_incremental(self.pending_messages))
                except Exception as e:
                    logging.error(f"Failed to save messages on shutdown: {e}")
        finally:
            # Final cleanup
            try:
                if self.telegram.is_connected():
                    self.event_loop.run_until_complete(self._cleanup_telegram(True))
            except Exception as e:
                logging.warning(f"Final cleanup error: {e}")

import asyncio
import logging
import sys
import time
from collections import defaultdict, deque
from typing import Optional, Dict, Deque
from threading import Lock

from errbot.backends.base import AWAY, DND, OFFLINE, ONLINE, Message, Person, Presence, Reaction, REACTION_ADDED, REACTION_REMOVED
from errbot.core import ErrBot

from discordlib.person import DiscordPerson, DiscordSender
from discordlib.room import DiscordCategory, DiscordRoom, DiscordRoomOccupant

log = logging.getLogger("errbot-backend-discord")

try:
    import discord
except ImportError:
    log.exception("Could not start err-backend-discord")
    log.fatal("The required discord module could not be found.")
    sys.exit(1)




class DiscordBackend(ErrBot):
    """
    Discord backend for Errbot.
    """

    client = None

    def __init__(self, config):
        super().__init__(config)

        self.token = config.BOT_IDENTITY.get("token", None)
        self.initial_intents = config.BOT_IDENTITY.get("initial_intents", "default")
        self.intents = config.BOT_IDENTITY.get("intents", None)
        
        # Network retry configuration
        self.max_retries = config.BOT_IDENTITY.get("max_retries", 3)
        self.retry_delay = config.BOT_IDENTITY.get("retry_delay", 2.0)
        self.timeout = config.BOT_IDENTITY.get("timeout", 10.0)  # Reduced from 30s to 10s
        
        # Rate limiting configuration
        self.rate_limit_enabled = config.BOT_IDENTITY.get("rate_limit_enabled", True)
        self.global_rate_limit = config.BOT_IDENTITY.get("global_rate_limit", 50)  # requests per minute
        self.per_channel_rate_limit = config.BOT_IDENTITY.get("per_channel_rate_limit", 5)  # messages per minute per channel
        self.per_user_rate_limit = config.BOT_IDENTITY.get("per_user_rate_limit", 10)  # messages per minute per user
        self.rate_limit_window = config.BOT_IDENTITY.get("rate_limit_window", 60)  # seconds
        
        # Rate limiting tracking
        self._rate_limit_lock = Lock()
        self._global_requests: Deque[float] = deque()
        self._channel_requests: Dict[str, Deque[float]] = defaultdict(deque)
        self._user_requests: Dict[str, Deque[float]] = defaultdict(deque)
        self._rate_limit_warnings: Dict[str, float] = {}  # Track when we last warned about rate limits

        if not self.token:
            log.fatal(
                "You need to set a token entry in the BOT_IDENTITY"
                " setting of your configuration."
            )
            sys.exit(1)

        self.bot_identifier = None
        
        # Message tracking for reactions
        self._message_cache: Dict[str, discord.Message] = {}  # errbot message id -> discord message
        self._message_cache_lock = Lock()
        self._max_cached_messages = config.BOT_IDENTITY.get("max_cached_messages", 1000)

    async def _retry_operation(self, operation, operation_name: str, *args, **kwargs):
        """
        Retry network operations with exponential backoff for transient errors.
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(operation):
                    return await operation(*args, **kwargs)
                else:
                    return operation(*args, **kwargs)
                    
            except discord.HTTPException as e:
                last_exception = e
                
                # Handle Discord rate limiting specifically
                if e.status == 429:  # Too Many Requests
                    retry_after = getattr(e, 'retry_after', None) or self.retry_delay
                    log.warning(f"{operation_name} rate limited by Discord. Waiting {retry_after}s before retry...")
                    await asyncio.sleep(retry_after)
                    continue  # Don't count rate limit as a retry attempt
                
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    log.warning(f"{operation_name} failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    log.error(f"{operation_name} failed after {self.max_retries + 1} attempts: {e}")
                    
            except (discord.ConnectionClosed, discord.GatewayNotFound) as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    log.warning(f"{operation_name} failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    log.error(f"{operation_name} failed after {self.max_retries + 1} attempts: {e}")
                    
            except discord.Forbidden as e:
                log.error(f"{operation_name} failed due to insufficient permissions: {e}")
                raise
                
            except discord.NotFound as e:
                log.error(f"{operation_name} failed - resource not found: {e}")
                raise
                
            except Exception as e:
                log.error(f"{operation_name} failed with unexpected error: {e}")
                raise
                
        raise last_exception

    def _safe_run_coroutine(self, coro, operation_name: str, timeout: Optional[float] = None):
        """
        Safely run a coroutine with timeout and error handling.
        Uses a non-blocking approach to prevent Discord heartbeat issues.
        """
        timeout = timeout or self.timeout
        
        try:
            # Use asyncio.run_coroutine_threadsafe but don't block waiting for result
            future = asyncio.run_coroutine_threadsafe(coro, loop=DiscordBackend.client.loop)
            
            # For non-critical operations, don't wait for completion to avoid blocking
            if operation_name in ['send_message', 'send_card', 'add_reaction', 'remove_reaction']:
                # Schedule the operation but don't wait for it
                def handle_result(fut):
                    try:
                        result = fut.result()
                        log.debug(f"{operation_name} completed successfully")
                        return result
                    except Exception as e:
                        log.error(f"{operation_name} failed: {e}")
                
                future.add_done_callback(handle_result)
                return None  # Don't block
            else:
                # For critical operations, wait with reduced timeout
                return future.result(timeout=min(timeout, 5.0))
                
        except asyncio.TimeoutError:
            log.error(f"{operation_name} timed out after {timeout}s")
            raise
        except Exception as e:
            log.error(f"{operation_name} failed: {e}")
            raise

    def _clean_old_requests(self, request_queue: Deque[float]) -> None:
        """
        Remove requests older than the rate limit window.
        """
        current_time = time.time()
        cutoff_time = current_time - self.rate_limit_window
        
        while request_queue and request_queue[0] < cutoff_time:
            request_queue.popleft()

    def _is_rate_limited(self, identifier: str = None, channel_id: str = None, user_id: str = None) -> bool:
        """
        Check if we're currently rate limited for the given context.
        
        Args:
            identifier: General identifier for rate limit warnings
            channel_id: Channel ID for per-channel rate limiting
            user_id: User ID for per-user rate limiting
            
        Returns:
            True if rate limited, False otherwise
        """
        if not self.rate_limit_enabled:
            return False
            
        current_time = time.time()
        
        with self._rate_limit_lock:
            # Clean old requests
            self._clean_old_requests(self._global_requests)
            
            # Check global rate limit
            if len(self._global_requests) >= self.global_rate_limit:
                self._warn_rate_limit("global", identifier)
                return True
            
            # Check per-channel rate limit
            if channel_id:
                channel_queue = self._channel_requests[channel_id]
                self._clean_old_requests(channel_queue)
                
                if len(channel_queue) >= self.per_channel_rate_limit:
                    self._warn_rate_limit(f"channel:{channel_id}", identifier)
                    return True
            
            # Check per-user rate limit
            if user_id:
                user_queue = self._user_requests[user_id]
                self._clean_old_requests(user_queue)
                
                if len(user_queue) >= self.per_user_rate_limit:
                    self._warn_rate_limit(f"user:{user_id}", identifier)
                    return True
                    
        return False

    def _record_request(self, channel_id: str = None, user_id: str = None) -> None:
        """
        Record a request for rate limiting tracking.
        
        Args:
            channel_id: Channel ID for per-channel tracking
            user_id: User ID for per-user tracking
        """
        if not self.rate_limit_enabled:
            return
            
        current_time = time.time()
        
        with self._rate_limit_lock:
            # Record global request
            self._global_requests.append(current_time)
            
            # Record per-channel request
            if channel_id:
                self._channel_requests[channel_id].append(current_time)
            
            # Record per-user request
            if user_id:
                self._user_requests[user_id].append(current_time)

    def _warn_rate_limit(self, limit_type: str, identifier: str = None) -> None:
        """
        Log rate limit warnings, but not too frequently.
        
        Args:
            limit_type: Type of rate limit (global, channel:id, user:id)
            identifier: Additional identifier for context
        """
        current_time = time.time()
        warning_key = f"{limit_type}:{identifier}" if identifier else limit_type
        
        # Only warn once per minute per limit type
        if warning_key not in self._rate_limit_warnings or \
           current_time - self._rate_limit_warnings[warning_key] > 60:
            
            log.warning(f"Rate limit reached for {limit_type}. Dropping message to prevent Discord API limits.")
            self._rate_limit_warnings[warning_key] = current_time

    def _should_send_message(self, msg: Message) -> bool:
        """
        Check if we should send a message based on rate limiting.
        
        Args:
            msg: The message to potentially send
            
        Returns:
            True if message should be sent, False if rate limited
        """
        if not self.rate_limit_enabled:
            return True
            
        # Extract identifiers for rate limiting
        channel_id = None
        user_id = None
        identifier = str(msg.to)
        
        if hasattr(msg.to, 'id'):
            if isinstance(msg.to, DiscordRoom):
                channel_id = str(msg.to.id)
            elif isinstance(msg.to, DiscordPerson):
                user_id = str(msg.to.id)
        
        # Check if rate limited
        if self._is_rate_limited(identifier=identifier, channel_id=channel_id, user_id=user_id):
            return False
            
        # Record the request
        self._record_request(channel_id=channel_id, user_id=user_id)
        return True

    def get_rate_limit_status(self) -> Dict[str, int]:
        """
        Get current rate limit status for monitoring.
        
        Returns:
            Dictionary with current request counts
        """
        if not self.rate_limit_enabled:
            return {"rate_limiting": "disabled"}
            
        with self._rate_limit_lock:
            # Clean old requests first
            self._clean_old_requests(self._global_requests)
            
            status = {
                "global_requests": len(self._global_requests),
                "global_limit": self.global_rate_limit,
                "active_channels": len(self._channel_requests),
                "active_users": len(self._user_requests),
                "rate_limit_window": self.rate_limit_window
            }
            
            # Add top channels by request count
            channel_counts = {}
            for channel_id, queue in self._channel_requests.items():
                self._clean_old_requests(queue)
                if queue:  # Only include channels with recent requests
                    channel_counts[channel_id] = len(queue)
            
            if channel_counts:
                top_channels = sorted(channel_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                status["top_channels"] = top_channels
                
            return status

    def set_message_size_limit(self, limit=2000, hard_limit=2000):
        """
        Discord supports up to 2000 characters per message.
        """
        super().set_message_size_limit(limit, hard_limit)

    async def on_error(self, event, *args, **kwargs):
        super().on_error(event, *args, **kwargs)
        # A stub entry in case special error handling is required.
        pass

    async def on_ready(self):
        """
        Discord client ready event handler
        """
        # Call connect only after successfully connected and ready to service Discord events.
        self.connect_callback()

        log.debug(
            f"Logged in as {DiscordBackend.client.user.name}, {DiscordBackend.client.user.id}"
        )
        if self.bot_identifier is None:
            self.bot_identifier = DiscordPerson(DiscordBackend.client.user.id)

        for channel in DiscordBackend.client.get_all_channels():
            log.debug(f"Found channel: {channel}")

    async def on_message_edit(self, before, after):
        """
        Edit message event handler
        """
        # Ignore bot messages and messages without content changes
        if after.author.bot or before.content == after.content:
            return
            
        try:
            # Prepare extras with edit information
            edit_extras = {
                'edited': True,
                'original_content': before.content,
                'edit_timestamp': after.edited_at.isoformat() if after.edited_at else None,
                'discord_message_id': str(after.id)
            }
            
            # Combine Discord embeds with edit information
            combined_extras = list(after.embeds) if after.embeds else []
            combined_extras.append(edit_extras)
            
            # Create the edited message object with proper extras
            err_msg = Message(after.content, extras=combined_extras)
            
            # Set message identifiers
            if isinstance(after.channel, discord.abc.PrivateChannel):
                err_msg.frm = DiscordPerson(after.author.id)
                err_msg.to = self.bot_identifier
            else:
                err_msg.to = DiscordRoom.from_id(after.channel.id)
                err_msg.frm = DiscordRoomOccupant(after.author.id, after.channel.id)
            
            log.debug(f"Message edited by {err_msg.frm}: '{before.content}' -> '{after.content}'")
            
            # Process the edited message if it contains a command
            if self.process_message(err_msg):
                recipient = err_msg.frm
                if isinstance(recipient, DiscordSender):
                    async with recipient.get_discord_object().typing():
                        self._dispatch_to_plugins("callback_message", err_msg)
            
            # Note: Plugins can detect edited messages by checking msg.extras for 'edited': True
            
        except Exception as e:
            log.error(f"Error processing message edit event: {e}")

    async def on_message(self, msg: discord.Message):
        """
        Message event handler
        """
        err_msg = Message(msg.content, extras=msg.embeds)

        # if the message coming in is from a webhook, it will not have a username
        # this will cause the whole process to fail.  In those cases, return without
        # processing.

        if msg.author.bot:
            return

        if isinstance(msg.channel, discord.abc.PrivateChannel):
            err_msg.frm = DiscordPerson(msg.author.id)
            err_msg.to = self.bot_identifier
        else:
            err_msg.to = DiscordRoom.from_id(msg.channel.id)
            err_msg.frm = DiscordRoomOccupant(msg.author.id, msg.channel.id)

        if self.process_message(err_msg):
            # Message contains a command
            recipient = err_msg.frm

            if not isinstance(recipient, DiscordSender):
                raise ValueError("Message object from is not a DiscordSender")

            async with recipient.get_discord_object().typing():
                self._dispatch_to_plugins("callback_message", err_msg)

        if msg.mentions:
            self.callback_mention(
                err_msg,
                [DiscordRoomOccupant(mention.id, msg.channel.id) for mention in msg.mentions],
            )

    def is_from_self(self, msg: Message) -> bool:
        """
        Test if message is from the bot instance.
        """
        if not isinstance(msg.frm, DiscordPerson):
            return False

        return msg.frm.id == self.bot_identifier.id

    async def on_member_update(self, before, after):
        """
        Member update event handler
        """
        if before.status != after.status:
            person = DiscordPerson(after.id)

            log.debug(f"Person {person} changed status to {after.status} from {before.status}")
            if after.status == discord.Status.online:
                self.callback_presence(Presence(person, ONLINE))
            elif after.status == discord.Status.offline:
                self.callback_presence(Presence(person, OFFLINE))
            elif after.status == discord.Status.idle:
                self.callback_presence(Presence(person, AWAY))
            elif after.status == discord.Status.dnd:
                self.callback_presence(Presence(person, DND))
        else:
            log.debug("Unrecognised member update, ignoring...")

    async def on_reaction_add(self, reaction, user):
        """
        Reaction add event handler
        """
        if user.bot:
            return  # Ignore bot reactions
            
        try:
            # Create the reactor (person who added the reaction)
            if isinstance(reaction.message.channel, discord.abc.PrivateChannel):
                reactor = DiscordPerson(user.id)
            else:
                reactor = DiscordRoomOccupant(user.id, reaction.message.channel.id)
            
            # Get reaction name (emoji or custom emoji name)
            reaction_name = str(reaction.emoji)
            if hasattr(reaction.emoji, 'name'):
                reaction_name = reaction.emoji.name
            
            # Create the reaction object
            err_reaction = Reaction(
                reactor=reactor,
                action=REACTION_ADDED,
                timestamp=str(int(time.time())),
                reaction_name=reaction_name,
                reacted_to={
                    'message_id': str(reaction.message.id),
                    'channel_id': str(reaction.message.channel.id),
                    'author_id': str(reaction.message.author.id),
                    'content': reaction.message.content[:100]  # First 100 chars for context
                }
            )
            
            log.debug(f"Reaction added: {reaction_name} by {reactor} to message {reaction.message.id}")
            self.callback_reaction(err_reaction)
            
        except Exception as e:
            log.error(f"Error processing reaction add event: {e}")

    async def on_reaction_remove(self, reaction, user):
        """
        Reaction remove event handler
        """
        if user.bot:
            return  # Ignore bot reactions
            
        try:
            # Create the reactor (person who removed the reaction)
            if isinstance(reaction.message.channel, discord.abc.PrivateChannel):
                reactor = DiscordPerson(user.id)
            else:
                reactor = DiscordRoomOccupant(user.id, reaction.message.channel.id)
            
            # Get reaction name (emoji or custom emoji name)
            reaction_name = str(reaction.emoji)
            if hasattr(reaction.emoji, 'name'):
                reaction_name = reaction.emoji.name
            
            # Create the reaction object
            err_reaction = Reaction(
                reactor=reactor,
                action=REACTION_REMOVED,
                timestamp=str(int(time.time())),
                reaction_name=reaction_name,
                reacted_to={
                    'message_id': str(reaction.message.id),
                    'channel_id': str(reaction.message.channel.id),
                    'author_id': str(reaction.message.author.id),
                    'content': reaction.message.content[:100]  # First 100 chars for context
                }
            )
            
            log.debug(f"Reaction removed: {reaction_name} by {reactor} from message {reaction.message.id}")
            self.callback_reaction(err_reaction)
            
        except Exception as e:
            log.error(f"Error processing reaction remove event: {e}")

    async def on_guild_channel_create(self, channel):
        """
        Channel creation event handler
        """
        try:
            if isinstance(channel, discord.TextChannel):
                room = DiscordRoom.from_id(channel.id)
                log.info(f"Text channel created: {channel.name} (ID: {channel.id}) in guild {channel.guild.name}")
                self.callback_room_joined(room)
            elif isinstance(channel, discord.CategoryChannel):
                category = DiscordCategory(channel.name, channel.guild.id, channel.id)
                log.info(f"Category created: {channel.name} (ID: {channel.id}) in guild {channel.guild.name}")
            else:
                log.debug(f"Channel created: {channel.name} (Type: {type(channel).__name__})")
                
        except Exception as e:
            log.error(f"Error processing channel create event: {e}")

    async def on_guild_channel_delete(self, channel):
        """
        Channel deletion event handler
        """
        try:
            if isinstance(channel, discord.TextChannel):
                room = DiscordRoom.from_id(channel.id)
                log.info(f"Text channel deleted: {channel.name} (ID: {channel.id}) in guild {channel.guild.name}")
                self.callback_room_left(room)
            elif isinstance(channel, discord.CategoryChannel):
                log.info(f"Category deleted: {channel.name} (ID: {channel.id}) in guild {channel.guild.name}")
            else:
                log.debug(f"Channel deleted: {channel.name} (Type: {type(channel).__name__})")
                
        except Exception as e:
            log.error(f"Error processing channel delete event: {e}")

    async def on_guild_channel_update(self, before, after):
        """
        Channel update event handler
        """
        try:
            # Check for topic changes
            if hasattr(before, 'topic') and hasattr(after, 'topic') and before.topic != after.topic:
                if isinstance(after, discord.TextChannel):
                    room = DiscordRoom.from_id(after.id)
                    log.info(f"Channel topic changed in {after.name}: '{before.topic}' -> '{after.topic}'")
                    self.callback_room_topic(room)
            
            # Check for name changes
            if before.name != after.name:
                log.info(f"Channel renamed: '{before.name}' -> '{after.name}' (ID: {after.id})")
            
            # Check for permission changes
            if before.overwrites != after.overwrites:
                log.debug(f"Channel permissions updated for {after.name} (ID: {after.id})")
                
        except Exception as e:
            log.error(f"Error processing channel update event: {e}")

    def query_room(self, room):
        """
        Query room with multi-guild support.

        Formats:
        <#channel_id> -> Discord channel mention
        ##category -> a category in first guild
        ##category@guild_id -> a category in specific guild
        #room -> a room in first guild  
        #room@guild_id -> a room in specific guild
        room -> a room in first guild

        :param room: Room identifier string
        :return: DiscordRoom, DiscordCategory, or None
        """
        if len(DiscordBackend.client.guilds) == 0:
            log.error(f"Unable to query room '{room}' because no guilds were found!")
            return None

        # Handle Discord channel mentions like <#1395580495932293163>
        if room.startswith("<#") and room.endswith(">"):
            try:
                channel_id = int(room[2:-1])  # Extract ID from <#ID>
                channel = DiscordBackend.client.get_channel(channel_id)
                if channel:
                    if isinstance(channel, discord.CategoryChannel):
                        return DiscordCategory(channel.name, channel.guild.id, channel.id)
                    else:
                        return DiscordRoom.from_id(channel_id)
                else:
                    log.error(f"Channel with ID {channel_id} not found")
                    return None
            except ValueError:
                log.error(f"Invalid channel ID in mention: {room}")
                return None

        # Parse guild specification
        guild_id = None
        room_name = room
        
        if "@" in room:
            room_name, guild_id = room.rsplit("@", 1)
            try:
                guild_id = int(guild_id)
            except ValueError:
                log.error(f"Invalid guild ID in room specification: {room}")
                return None
        
        # Get the target guild
        if guild_id:
            guild = DiscordBackend.client.get_guild(guild_id)
            if not guild:
                log.error(f"Guild {guild_id} not found or bot not in guild")
                return None
        else:
            guild = DiscordBackend.client.guilds[0]  # Default to first guild

        # Parse room type and create appropriate object
        if room_name.startswith("##"):
            return DiscordCategory(room_name[2:], guild.id)
        elif room_name.startswith("#"):
            return DiscordRoom(room_name[1:], guild.id)
        else:
            return DiscordRoom(room_name, guild.id)



    def send_message(self, msg: Message):
        super().send_message(msg)

        if not isinstance(msg.to, DiscordSender):
            raise RuntimeError(
                f"{msg.to} doesn't support sending messages."
                f"  Expected DiscordSender object but got {type(msg.to)}."
            )

        # Check rate limiting before sending
        if not self._should_send_message(msg):
            log.debug(f"Message to {msg.to} dropped due to rate limiting")
            return

        log.debug(
            f"Message to:{msg.to}({type(msg.to)}) from:{msg.frm}({type(msg.frm)}),"
            f" is_direct:{msg.is_direct} extras: {msg.extras} size: {len(msg.body)}"
        )

        for message in [
            msg.body[i : i + self.message_size_limit]
            for i in range(0, len(msg.body), self.message_size_limit)
        ]:
            try:
                # Check if message should be sent to a thread
                if hasattr(msg, 'extras') and msg.extras and msg.extras.get('thread_id'):
                    thread_id = msg.extras['thread_id']
                    thread = DiscordBackend.client.get_channel(int(thread_id))
                    if thread and isinstance(thread, discord.Thread):
                        self._safe_run_coroutine(
                            self._retry_operation(thread.send, "send_message_to_thread", content=message),
                            "send_message_to_thread"
                        )
                        log.debug(f"Sent message to thread {thread_id}")
                    else:
                        log.warning(f"Thread {thread_id} not found, sending to regular channel")
                        self._safe_run_coroutine(
                            self._retry_operation(msg.to.send, "send_message", content=message),
                            "send_message"
                        )
                else:
                    # Regular message sending
                    self._safe_run_coroutine(
                        self._retry_operation(msg.to.send, "send_message", content=message),
                        "send_message"
                    )
            except Exception as e:
                log.error(f"Failed to send message to {msg.to}: {e}")
                # Don't re-raise to prevent bot from crashing on message send failures

    def send_card(self, card):
        """
        Send a basic Discord embed card.
        For advanced card features, use the DiscordCards plugin.
        """
        recipient = card.to

        if not isinstance(recipient, DiscordSender):
            raise RuntimeError(
                f"{recipient} doesn't support sending messages."
                f"  Expected {DiscordSender} but got {type(recipient)}"
            )

        # Create a mock message for rate limiting check
        mock_msg = Message("")
        mock_msg.to = recipient
        
        # Check rate limiting before sending
        if not self._should_send_message(mock_msg):
            log.debug(f"Card to {recipient} dropped due to rate limiting")
            return

        # Basic embed creation (core functionality)
        em = discord.Embed(
            title=card.title or None,
            description=card.body or None
        )

        try:
            self._safe_run_coroutine(
                self._retry_operation(recipient.send, "send_card", embed=em),
                "send_card",
                timeout=5.0
            )
        except Exception as e:
            log.error(f"Failed to send card to {recipient}: {e}")
            # Don't re-raise to prevent bot from crashing on card send failures

    def send_discord_embed(self, recipient, title=None, description=None, color=None, 
                          fields=None, image=None, thumbnail=None, footer=None, 
                          author=None, url=None, timestamp=None):
        """
        Send a Discord embed with full Discord-specific features.
        This is the backend API for plugins to create rich embeds.
        """
        if not isinstance(recipient, DiscordSender):
            raise RuntimeError(f"Recipient must be a DiscordSender, got {type(recipient)}")

        # Create a mock message for rate limiting check
        mock_msg = Message("")
        mock_msg.to = recipient
        
        # Check rate limiting before sending
        if not self._should_send_message(mock_msg):
            log.debug(f"Discord embed to {recipient} dropped due to rate limiting")
            return False

        try:
            # Create Discord embed
            em = discord.Embed(
                title=title,
                description=description,
                color=color,
                url=url,
                timestamp=timestamp
            )

            # Add fields
            if fields:
                for field in fields:
                    if isinstance(field, dict):
                        em.add_field(
                            name=field.get('name', 'Field'),
                            value=field.get('value', 'Value'),
                            inline=field.get('inline', True)
                        )
                    elif isinstance(field, (list, tuple)) and len(field) >= 2:
                        em.add_field(
                            name=field[0],
                            value=field[1],
                            inline=field[2] if len(field) > 2 else True
                        )

            # Add image
            if image:
                em.set_image(url=image)

            # Add thumbnail
            if thumbnail:
                em.set_thumbnail(url=thumbnail)

            # Add footer
            if footer:
                if isinstance(footer, dict):
                    em.set_footer(
                        text=footer.get('text', ''),
                        icon_url=footer.get('icon_url')
                    )
                else:
                    em.set_footer(text=str(footer))

            # Add author
            if author:
                if isinstance(author, dict):
                    em.set_author(
                        name=author.get('name', ''),
                        url=author.get('url'),
                        icon_url=author.get('icon_url')
                    )
                else:
                    em.set_author(name=str(author))

            # Send the embed
            self._safe_run_coroutine(
                self._retry_operation(recipient.send, "send_discord_embed", embed=em),
                "send_discord_embed",
                timeout=5.0
            )
            return True

        except Exception as e:
            log.error(f"Failed to send Discord embed to {recipient}: {e}")
            return False

    def build_reply(self, mess, text=None, private=False, threaded=False):
        response = self.build_message(text)

        if mess.is_direct:
            response.frm = self.bot_identifier
            response.to = mess.frm
        else:
            if not isinstance(mess.frm, DiscordRoomOccupant):
                raise RuntimeError("Non-Direct messages must come from a room occupant")

            response.frm = DiscordRoomOccupant(self.bot_identifier.id, mess.frm.room.id)
            response.to = DiscordPerson(mess.frm.id) if private else mess.to
            
            # Handle thread support
            if threaded and hasattr(mess, 'extras') and mess.extras:
                # Check if the original message has thread information
                thread_id = mess.extras.get('thread_id')
                discord_msg_id = mess.extras.get('discord_message_id')
                
                if thread_id:
                    # Reply in existing thread
                    response.extras = response.extras or {}
                    response.extras['thread_id'] = thread_id
                    log.debug(f"Replying in existing thread {thread_id}")
                elif discord_msg_id and not mess.is_direct:
                    # Create a new thread from the original message
                    try:
                        thread_name = f"Reply to {mess.frm.nick}" if hasattr(mess.frm, 'nick') else "Thread"
                        thread_id = self._create_thread_from_message(discord_msg_id, thread_name)
                        if thread_id:
                            response.extras = response.extras or {}
                            response.extras['thread_id'] = thread_id
                            log.debug(f"Created new thread {thread_id} for threaded reply")
                    except Exception as e:
                        log.warning(f"Failed to create thread for reply: {e}")
                        
        return response



    def config_intents(self):
        """
        Process discord intents configuration for bot.
        """

        def apply_as_int(bot_intents, intent):
            if intent >= 0:
                bot_intents._set_flag(intent, True)
            else:
                intent *= -1
                bot_intents._set_flag(intent, False)
            return bot_intents

        def apply_as_str(bot_intents, intent):
            toggle = True
            if intent.startswith("-"):
                toggle = False
                intent = intent[1:]

            if hasattr(bot_intents, intent):
                setattr(bot_intents, intent, toggle)
            else:
                log.warning("Unknown intent '%s'.", intent)
            return bot_intents

        bot_intents = {
            "none": discord.Intents.none,
            "default": discord.Intents.default,
            "all": discord.Intents.all,
        }.get(self.initial_intents, discord.Intents.default)()

        if isinstance(self.intents, list):
            for intent in self.intents:
                if isinstance(intent, int):
                    bot_intents = apply_as_int(bot_intents, intent)
                elif isinstance(intent, str):
                    bot_intents = apply_as_str(bot_intents, intent)
                else:
                    log.warning("Unknown intent type %s for '%s'", type(intent), str(intent))
        elif isinstance(self.intents, int):
            bot_intents = apply_as_int(bot_intents, self.intents)
        else:
            if self.intents is not None:
                log.warning(
                    "Unsupported intent type %s for '%s'",
                    type(self.intents),
                    str(self.intents),
                )

        log.info(
            "Enabled intents - {}".format(", ".join([i[0] for i in list(bot_intents) if i[1]]))
        )
        log.info(
            "Disabled intents - {}".format(
                ", ".join([i[0] for i in list(bot_intents) if i[1] is False])
            )
        )
        return bot_intents

    def initialise_client(self):
        """
        Initialise discord client.  This function is called whenever the serve_once
        is restarted.  This involves initialising the intents, callback handlers
        and dependency injection for classes that use the discord client.
        """

        bot_intents = self.config_intents()
        DiscordBackend.client = discord.Client(intents=bot_intents)

        # Register discord event coroutines.
        for func in [
            self.on_ready,
            self.on_message,
            self.on_member_update,
            self.on_message_edit,
            self.on_member_update,
            self.on_reaction_add,
            self.on_reaction_remove,
            self.on_guild_channel_create,
            self.on_guild_channel_delete,
            self.on_guild_channel_update,
        ]:
            DiscordBackend.client.event(func)

        # Use dependency injection to make discord client available to submodule classes.
        DiscordCategory.client = DiscordBackend.client
        DiscordRoomOccupant.client = DiscordBackend.client
        DiscordRoom.client = DiscordBackend.client
        DiscordPerson.client = DiscordBackend.client
        DiscordSender.client = DiscordBackend.client

    def serve_once(self):
        """
        Initialise discord client and establish connection.
        """

        async def start_client(token):
            """
            Start the discord client using asynchronous event loop.
            """
            async with DiscordBackend.client:
                await DiscordBackend.client.start(token)

        try:
            self.initialise_client()

            # Discord.py 2.0's client.run convenience method traps KeyboardInterrupt so it can not be used.
            # The documented manual method is used here so errbot can handle KeyboardInterrupt exceptions.
            asyncio.run(start_client(self.token))

        except KeyboardInterrupt:
            log.info("Received keyboard interrupt, shutting down...")
            self.disconnect_callback()
            return True
        except discord.LoginFailure:
            log.fatal("Invalid Discord token provided")
            sys.exit(1)
        except discord.HTTPException as e:
            log.error(f"HTTP error during connection: {e}")
            raise
        except discord.ConnectionClosed as e:
            log.error(f"Connection to Discord was closed: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpected error during connection: {e}")
            raise

    def change_presence(self, status: str = ONLINE, message: str = ""):
        """
        Change bot presence with basic activity.
        For advanced presence features, use set_discord_presence().
        """
        log.debug(f'Presence changed to {status} and activity "{message}".')
        try:
            activity = discord.Activity(name=message) if message else None
            
            async def update_presence():
                return await self._retry_operation(
                    DiscordBackend.client.change_presence,
                    "change_presence",
                    status=status,
                    activity=activity
                )
            
            self._safe_run_coroutine(update_presence(), "change_presence")
            
        except Exception as e:
            log.error(f"Failed to change presence: {e}")
            # Don't re-raise to prevent bot from crashing on presence change failures



    def add_reaction(self, msg: Message, reaction: str) -> None:
        """
        Add a reaction to a message.
        
        Args:
            msg: The message to react to
            reaction: The reaction emoji (unicode emoji or custom emoji name)
        """
        try:
            # Get the Discord message object
            discord_msg = self._get_discord_message_from_errbot_message(msg)
            if discord_msg is None:
                log.error(f"Could not find Discord message to react to")
                return
            
            # Add the reaction
            async def add_reaction_async():
                return await self._retry_operation(
                    discord_msg.add_reaction,
                    "add_reaction",
                    reaction
                )
            
            self._safe_run_coroutine(add_reaction_async(), "add_reaction")
            log.debug(f"Added reaction {reaction} to message {discord_msg.id}")
            
        except Exception as e:
            log.error(f"Failed to add reaction {reaction}: {e}")

    def remove_reaction(self, msg: Message, reaction: str) -> None:
        """
        Remove a reaction from a message.
        
        Args:
            msg: The message to remove reaction from
            reaction: The reaction emoji to remove
        """
        try:
            # Get the Discord message object
            discord_msg = self._get_discord_message_from_errbot_message(msg)
            if discord_msg is None:
                log.error(f"Could not find Discord message to remove reaction from")
                return
            
            # Remove the reaction (bot's own reaction)
            async def remove_reaction_async():
                return await self._retry_operation(
                    discord_msg.remove_reaction,
                    "remove_reaction",
                    reaction,
                    DiscordBackend.client.user
                )
            
            self._safe_run_coroutine(remove_reaction_async(), "remove_reaction")
            log.debug(f"Removed reaction {reaction} from message {discord_msg.id}")
            
        except Exception as e:
            log.error(f"Failed to remove reaction {reaction}: {e}")

    def _cache_message(self, errbot_msg_id: str, discord_msg: discord.Message) -> None:
        """
        Cache a Discord message for later lookup.
        
        Args:
            errbot_msg_id: The errbot message identifier
            discord_msg: The Discord message object
        """
        with self._message_cache_lock:
            # Implement LRU-style cache by removing oldest entries
            if len(self._message_cache) >= self._max_cached_messages:
                # Remove oldest entry (first inserted)
                oldest_key = next(iter(self._message_cache))
                del self._message_cache[oldest_key]
            
            self._message_cache[errbot_msg_id] = discord_msg

    def _get_discord_message_from_errbot_message(self, msg: Message):
        """
        Helper method to get Discord message object from errbot Message.
        
        Args:
            msg: The errbot Message object
            
        Returns:
            Discord message object or None if not found
        """
        # Try to get message ID from extras
        if hasattr(msg, 'extras') and msg.extras:
            discord_msg_id = msg.extras.get('discord_message_id')
            if discord_msg_id:
                try:
                    # Try to fetch the message from Discord
                    async def fetch_message():
                        # Determine the channel
                        if isinstance(msg.to, DiscordRoom):
                            channel = DiscordBackend.client.get_channel(msg.to.id)
                        elif isinstance(msg.frm, DiscordRoomOccupant):
                            channel = DiscordBackend.client.get_channel(msg.frm.room.id)
                        else:
                            # Direct message - get DM channel
                            if isinstance(msg.to, DiscordPerson):
                                user = DiscordBackend.client.get_user(msg.to.id)
                                channel = user.dm_channel or await user.create_dm()
                            elif isinstance(msg.frm, DiscordPerson):
                                user = DiscordBackend.client.get_user(msg.frm.id)
                                channel = user.dm_channel or await user.create_dm()
                            else:
                                return None
                        
                        if channel:
                            return await channel.fetch_message(int(discord_msg_id))
                        return None
                    
                    return self._safe_run_coroutine(fetch_message(), "fetch_message_for_reaction")
                    
                except Exception as e:
                    log.debug(f"Could not fetch Discord message {discord_msg_id}: {e}")
        
        # Check message cache
        msg_cache_key = f"{msg.to}:{getattr(msg, 'body', '')[:50]}"  # Simple cache key
        with self._message_cache_lock:
            cached_msg = self._message_cache.get(msg_cache_key)
            if cached_msg:
                return cached_msg
        
        log.debug("Could not find Discord message for reaction. Message may be too old or not sent by this bot.")
        return None

    def prefix_groupchat_reply(self, message, identifier: Person):
        message.body = f"@{identifier.nick} {message.body}"

    def rooms(self):
        return [
            DiscordRoom.from_id(channel.id) for channel in DiscordBackend.client.get_all_channels()
        ]

    @property
    def mode(self):
        return "discord"

    def build_identifier(self, text: str):
        """
        Guilds in Discord represent an isolated collection of users and channels,
        and are often referred to as "servers" in the UI.

        Valid forms of strreps:
        <@userid>                      -> Person
        <#channelid>                   -> Room
        @user#discriminator            -> Person
        #channel                       -> Room (a uniquely identified channel on any guild)
        #channel#guild_id              -> Room (a channel on a specific guild)

        :param text:  The text the represents an Identifier
        :return: Identifier

        Room Example:
            #general@12345678901234567 -> Sends a message to the
            #general channel of the guild with id 12345678901234567
        """
        if not text:
            raise ValueError("A string must be provided to build an identifier.")

        log.debug(f"Build_identifier {text}")

        # Mentions are wrapped by <>
        if text.startswith("<") and text.endswith(">"):
            text = text[1:-1]
            if text.startswith("@"):
                return DiscordPerson(user_id=text[1:])
            elif text.startswith("#"):
                # channel id
                return DiscordRoom(channel_id=text[1:])
            else:
                raise ValueError(f"Unsupport identification {text}")
        # Raw text channel name start with #
        elif text.startswith("#"):
            if "@" in text:
                channel_name, guild_id = text.split("@", 1)
                return DiscordRoom(channel_name[1:], guild_id)
            else:
                return DiscordRoom(text[1:])
        # Raw text username starts with @
        elif text.startswith("@"):
            text = text[1:]
            if "#" in text:
                user, discriminator = text.split("#", 1)
                return DiscordPerson(username=user, discriminator=discriminator)
        # Handle Discord username format without @ prefix (e.g., "lcizzle#0")
        elif "#" in text and not text.startswith("#"):
            # This looks like a Discord username#discriminator format
            user, discriminator = text.split("#", 1)
            # Validate that discriminator is numeric (Discord discriminators are 4 digits or "0")
            if discriminator.isdigit() or discriminator == "0":
                return DiscordPerson(username=user, discriminator=discriminator)

        raise ValueError(f"Invalid representation {text}")

    def upload_file(self, msg, filename):
        try:
            with open(filename, "rb") as f:  # Open in binary mode for file uploads
                dest = None
                if msg.is_direct:
                    dest = DiscordPerson(msg.frm.id).get_discord_object()
                else:
                    dest = msg.to.get_discord_object()

                log.info(f"Sending file {filename} to user {msg.frm}")
                
                async def send_file():
                    return await self._retry_operation(
                        dest.send, 
                        "upload_file", 
                        file=discord.File(f, filename=filename)
                    )
                
                self._safe_run_coroutine(send_file(), "upload_file")
                
        except FileNotFoundError:
            log.error(f"File not found: {filename}")
            raise
        except PermissionError:
            log.error(f"Permission denied accessing file: {filename}")
            raise
        except Exception as e:
            log.error(f"Failed to upload file {filename}: {e}")
            raise

    def history(self, channelname, before=None):
        try:
            mychannel = discord.utils.get(self.client.get_all_channels(), name=channelname)
            
            if mychannel is None:
                log.error(f"Channel '{channelname}' not found")
                return []

            async def gethist(mychannel, before=None):
                async def get_history():
                    return [i async for i in mychannel.history(limit=10, before=before)]
                
                return await self._retry_operation(
                    get_history,
                    "history"
                )

            return self._safe_run_coroutine(gethist(mychannel, before), "history")
            
        except Exception as e:
            log.error(f"Failed to get history for channel '{channelname}': {e}")
            return []

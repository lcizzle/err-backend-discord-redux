import asyncio
import logging
import sys
import time
from typing import Optional

from errbot.backends.base import AWAY, DND, OFFLINE, ONLINE, Message, Person, Presence
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

COLOURS = {
    "red": 0xFF0000,
    "green": 0x008000,
    "yellow": 0xFFA500,
    "blue": 0x0000FF,
    "white": 0xFFFFFF,
    "cyan": 0x00FFFF,
}


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
        self.timeout = config.BOT_IDENTITY.get("timeout", 30.0)

        if not self.token:
            log.fatal(
                "You need to set a token entry in the BOT_IDENTITY"
                " setting of your configuration."
            )
            sys.exit(1)

        self.bot_identifier = None

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
                    
            except (discord.HTTPException, discord.ConnectionClosed, discord.GatewayNotFound) as e:
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
        """
        timeout = timeout or self.timeout
        
        try:
            future = asyncio.run_coroutine_threadsafe(coro, loop=DiscordBackend.client.loop)
            return future.result(timeout=timeout)
        except asyncio.TimeoutError:
            log.error(f"{operation_name} timed out after {timeout}s")
            raise
        except Exception as e:
            log.error(f"{operation_name} failed: {e}")
            raise

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
        log.warning("Message editing not supported.")

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

    def query_room(self, room):
        """
        Query room.

        This method implicitly assume the bot is in one guild server.

        ##category -> a category
        #room -> Creates a room

        :param room:
        :return:
        """
        if len(DiscordBackend.client.guilds) == 0:
            log.error(f"Unable to join room '{room}' because no guilds were found!")
            return None

        guild = DiscordBackend.client.guilds[0]

        room_name = room
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

        log.debug(
            f"Message to:{msg.to}({type(msg.to)}) from:{msg.frm}({type(msg.frm)}),"
            f" is_direct:{msg.is_direct} extras: {msg.extras} size: {len(msg.body)}"
        )

        for message in [
            msg.body[i : i + self.message_size_limit]
            for i in range(0, len(msg.body), self.message_size_limit)
        ]:
            try:
                self._safe_run_coroutine(
                    self._retry_operation(msg.to.send, "send_message", content=message),
                    "send_message"
                )
            except Exception as e:
                log.error(f"Failed to send message to {msg.to}: {e}")
                # Don't re-raise to prevent bot from crashing on message send failures

    def send_card(self, card):
        recipient = card.to

        if not isinstance(recipient, DiscordSender):
            raise RuntimeError(
                f"{recipient} doesn't support sending messages."
                f"  Expected {DiscordSender} but got {type(recipient)}"
            )

        if card.color:
            color = COLOURS.get(card.color, int(card.color.replace("#", "0x"), 16))
        else:
            color = None

        # Create Embed object
        em = discord.Embed(title=card.title, description=card.body, color=color)

        if card.image:
            em.set_image(url=card.image)

        if card.thumbnail:
            em.set_thumbnail(url=card.thumbnail)

        if card.fields:
            for key, value in card.fields:
                em.add_field(name=key, value=value, inline=True)

        try:
            self._safe_run_coroutine(
                self._retry_operation(recipient.send, "send_card", embed=em),
                "send_card",
                timeout=5.0
            )
        except Exception as e:
            log.error(f"Failed to send card to {recipient}: {e}")
            # Don't re-raise to prevent bot from crashing on card send failures

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
        log.debug(f'Presence changed to {status} and activity "{message}".')
        try:
            activity = discord.Activity(name=message)
            
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
                user, discriminator = text.split("#",1)
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

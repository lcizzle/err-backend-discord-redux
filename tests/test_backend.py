import asyncio
import importlib
import logging
import os
import sys
from tempfile import mkdtemp

import discord
import pytest
from discordlib.person import DiscordPerson
from discordlib.room import DiscordRoom
from errbot.backends.base import Message
from errbot.bootstrap import bot_config_defaults
from mock import MagicMock

log = logging.getLogger(__name__)

DiscordBackend = importlib.import_module("err-backend-discord").DiscordBackend


class MockedDiscordBackend(DiscordBackend):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_msgs = []
        self.bot_identifier = MagicMock()


@pytest.fixture
def mock_discord_client():
    client = MagicMock()
    mock_user = MagicMock()
    mock_user.id = 123456789012345678
    mock_user.name = "someone"
    mock_user.discriminator = "0"
    client.get_user.return_value = mock_user
    client.get_all_members.return_value = [mock_user]

    mock_channel = MagicMock()
    mock_channel.id = 123456789012345678
    mock_channel.name = "general"
    client.get_channel.return_value = mock_channel

    mock_guild = MagicMock()
    mock_guild.id = 123456789012345678
    mock_guild.name = "testguild"
    mock_guild.channels = [mock_channel]
    client.get_guild.return_value = mock_guild

    orig_backend_client = DiscordBackend.client
    orig_person_client = DiscordPerson.client
    orig_room_client = DiscordRoom.client

    DiscordBackend.client = client
    DiscordPerson.client = client
    DiscordRoom.client = client

    yield client

    DiscordBackend.client = orig_backend_client
    DiscordPerson.client = orig_person_client
    DiscordRoom.client = orig_room_client


@pytest.fixture
def backend(mock_discord_client):
    tempdir = mkdtemp()
    sys.modules.pop("errbot.config-template", None)
    __import__("errbot.config-template")
    config = sys.modules["errbot.config-template"]
    bot_config_defaults(config)
    config.BOT_DATA_DIR = tempdir
    config.BOT_LOG_FILE = os.path.join(tempdir, "log.txt")
    config.BOT_EXTRA_PLUGIN_DIR = []
    config.BOT_LOG_LEVEL = logging.DEBUG
    config.BOT_IDENTITY = {
        "token": "token_abcd",
        "initial_intents": "default",
        "intents": [],
    }
    config.BOT_ASYNC = False
    config.BOT_PREFIX = "!"
    config.CHATROOM_FN = "test_room"

    discord_backend = MockedDiscordBackend(config)
    return discord_backend


def test_build_identifier_user_mention(backend):
    ident = backend.build_identifier("<@123456789012345678>")
    assert isinstance(ident, DiscordPerson)
    assert ident.id == 123456789012345678


def test_build_identifier_channel_mention(backend):
    ident = backend.build_identifier("<#123456789012345678>")
    assert isinstance(ident, DiscordRoom)
    assert ident.id == 123456789012345678


def test_build_identifier_username(backend):
    ident = backend.build_identifier("@someone#0")
    assert isinstance(ident, DiscordPerson)
    assert ident.id == 123456789012345678


def test_build_identifier_room_with_guild(backend):
    ident = backend.build_identifier("#general@123456789012345678")
    assert isinstance(ident, DiscordRoom)
    assert ident.id == 123456789012345678


def test_build_identifier_empty(backend):
    with pytest.raises(ValueError, match="A string must be provided"):
        backend.build_identifier("")


def test_build_identifier_invalid(backend):
    with pytest.raises(ValueError, match="Invalid representation"):
        backend.build_identifier("random_invalid_string")


def test_mode(backend):
    assert backend.mode == "discord"


def test_on_message_caching_and_extras(backend):
    import asyncio

    mock_discord_msg = MagicMock()
    mock_discord_msg.id = 998877665544332211
    mock_discord_msg.content = "hello bot"
    mock_discord_msg.embeds = []
    mock_discord_msg.author.bot = False
    mock_discord_msg.author.id = 111122223333444455
    mock_discord_msg.mentions = []

    mock_channel = MagicMock()
    mock_channel.id = 555566667777888899
    mock_discord_msg.channel = mock_channel

    backend.process_message = MagicMock(return_value=False)

    asyncio.run(backend.on_message(mock_discord_msg))

    with backend._message_cache_lock:
        cached = backend._message_cache.get(str(mock_discord_msg.id))
    assert cached == mock_discord_msg

    assert backend.process_message.called
    err_msg = backend.process_message.call_args[0][0]
    assert err_msg.extras["discord_message_id"] == "998877665544332211"
    assert err_msg.extras["channel_id"] == "555566667777888899"


def test_create_thread_from_message(backend):
    mock_msg = MagicMock()
    mock_msg.id = 123456

    backend._cache_message("123456", mock_msg)
    backend._safe_run_coroutine = MagicMock(return_value="7891011")

    thread_id = backend._create_thread_from_message("123456", "Test Thread")
    assert thread_id == "7891011"
    assert backend._safe_run_coroutine.called
    backend._safe_run_coroutine.call_args[0][0].close()


def test_on_raw_reaction_add_and_remove(backend):
    import asyncio

    backend.callback_reaction = MagicMock()

    payload_add = MagicMock()
    payload_add.user_id = 123456789012345678
    payload_add.channel_id = 123456789012345678
    payload_add.guild_id = 123456789012345678
    payload_add.message_id = 123456789012345678
    payload_add.member = None
    payload_add.emoji.name = "thumbsup"

    asyncio.run(backend.on_raw_reaction_add(payload_add))
    assert backend.callback_reaction.called
    rxn = backend.callback_reaction.call_args[0][0]
    assert rxn.reaction_name == "thumbsup"
    assert rxn.action == "added"
    assert rxn.reacted_to["message_id"] == "123456789012345678"

    backend.callback_reaction.reset_mock()

    payload_remove = MagicMock()
    payload_remove.user_id = 123456789012345678
    payload_remove.channel_id = 123456789012345678
    payload_remove.guild_id = 123456789012345678
    payload_remove.message_id = 123456789012345678
    payload_remove.emoji.name = "thumbsup"

    asyncio.run(backend.on_raw_reaction_remove(payload_remove))
    assert backend.callback_reaction.called
    rxn_remove = backend.callback_reaction.call_args[0][0]
    assert rxn_remove.reaction_name == "thumbsup"
    assert rxn_remove.action == "removed"
    assert rxn_remove.reacted_to["message_id"] == "123456789012345678"


def test_on_raw_reaction_fallback_uncached_channel(backend):
    import asyncio

    backend.callback_reaction = MagicMock()

    # Uncached channel where get_channel returns None
    orig_get_channel = DiscordBackend.client.get_channel
    DiscordBackend.client.get_channel.return_value = None

    payload = MagicMock()
    payload.user_id = 123456789012345678
    payload.channel_id = 999999999999999999
    payload.guild_id = 123456789012345678
    payload.message_id = 123456789012345678
    payload.member = None
    payload.emoji.name = "thumbsup"

    asyncio.run(backend.on_raw_reaction_add(payload))
    assert backend.callback_reaction.called
    rxn = backend.callback_reaction.call_args[0][0]
    assert isinstance(rxn.reactor, DiscordPerson)

    DiscordBackend.client.get_channel = orig_get_channel


def test_on_message_delete(backend):
    import asyncio

    backend._dispatch_to_plugins = MagicMock()

    mock_msg = MagicMock(spec=discord.Message)
    mock_msg.id = 998877665512345678
    mock_msg.content = "Secret message"
    mock_msg.author.id = 112233445566778899
    mock_msg.author.bot = False
    mock_msg.channel = MagicMock(spec=discord.TextChannel)
    mock_msg.channel.id = 445566778899112233

    # Put it in cache first
    backend._cache_message("998877665512345678", mock_msg)
    assert backend._message_cache.get("998877665512345678") is not None

    asyncio.run(backend.on_message_delete(mock_msg))

    assert backend._dispatch_to_plugins.called
    dispatch_call = backend._dispatch_to_plugins.call_args
    assert dispatch_call[0][0] == "callback_message_deleted"
    err_msg = dispatch_call[0][1]
    assert err_msg.body == "Secret message"
    assert err_msg.extras["discord_message_id"] == "998877665512345678"
    assert err_msg.extras["channel_id"] == "445566778899112233"
    assert err_msg.extras["deleted"] is True
    assert err_msg.extras["cached"] is True

    # Check that it was evicted from cache
    assert backend._message_cache.get("998877665512345678") is None


def test_on_raw_message_delete(backend):
    backend._dispatch_to_plugins = MagicMock()

    payload = MagicMock(spec=discord.RawMessageDeleteEvent)
    payload.message_id = 112233445566778899
    payload.channel_id = 667788990011223344
    payload.guild_id = 998877665544332211
    del payload.thread_id

    DiscordBackend.client.get_channel = MagicMock(return_value=None)
    asyncio.run(backend.on_raw_message_delete(payload))

    assert backend._dispatch_to_plugins.called
    dispatch_call = backend._dispatch_to_plugins.call_args
    assert dispatch_call[0][0] == "callback_raw_message_deleted"
    err_msg = dispatch_call[0][1]
    assert err_msg.extras["discord_message_id"] == "112233445566778899"
    assert err_msg.extras["channel_id"] == "667788990011223344"
    assert err_msg.extras["deleted"] is True
    assert err_msg.extras["cached"] is False

    # Test when deleted inside a thread
    mock_thread = MagicMock(spec=discord.Thread)
    mock_thread.id = 667788990011223344
    DiscordBackend.client.get_channel = MagicMock(return_value=mock_thread)
    asyncio.run(backend.on_raw_message_delete(payload))
    err_msg = backend._dispatch_to_plugins.call_args[0][1]
    assert err_msg.extras["thread_id"] == "667788990011223344"


def test_on_thread_events(backend):
    import asyncio

    backend._dispatch_to_plugins = MagicMock()
    backend.callback_room_joined = MagicMock()
    backend.callback_room_left = MagicMock()

    mock_thread = MagicMock(spec=discord.Thread)
    mock_thread.id = 5566778899
    mock_thread.name = "Support Thread"
    mock_thread.parent_id = 112233
    mock_thread.guild.name = "Test Guild"
    mock_thread.guild.id = 999
    mock_thread.archived = False
    mock_thread.locked = False

    orig_get_channel = DiscordBackend.client.get_channel
    DiscordBackend.client.get_channel.return_value = mock_thread

    # Test thread create
    asyncio.run(backend.on_thread_create(mock_thread))
    assert backend._dispatch_to_plugins.called
    assert backend._dispatch_to_plugins.call_args[0][0] == "callback_thread_created"
    assert backend.callback_room_joined.called

    # Test thread delete
    backend._dispatch_to_plugins.reset_mock()
    asyncio.run(backend.on_thread_delete(mock_thread))
    assert backend._dispatch_to_plugins.called
    assert backend._dispatch_to_plugins.call_args[0][0] == "callback_thread_deleted"
    assert backend.callback_room_left.called

    # Test thread update
    backend._dispatch_to_plugins.reset_mock()
    mock_thread_updated = MagicMock(spec=discord.Thread)
    mock_thread_updated.id = 5566778899
    mock_thread_updated.name = "Support Thread (Closed)"
    mock_thread_updated.archived = True
    mock_thread_updated.locked = True

    asyncio.run(backend.on_thread_update(mock_thread, mock_thread_updated))
    assert backend._dispatch_to_plugins.called
    assert backend._dispatch_to_plugins.call_args[0][0] == "callback_thread_updated"

    DiscordBackend.client.get_channel = orig_get_channel


def test_safe_dispatch_to_plugins(backend):
    """
    Verify that _dispatch_to_plugins safely skips plugins that do not implement
    the callback method without raising AttributeError, while calling plugins that do.
    """
    plugin_without_cb = MagicMock(spec=["name"])
    plugin_without_cb.name = "ACLs"

    plugin_with_cb = MagicMock()
    plugin_with_cb.name = "DiscordTest"

    backend.plugin_manager = MagicMock()
    backend.plugin_manager.get_all_active_plugins.return_value = [
        plugin_without_cb,
        plugin_with_cb,
    ]

    mock_room = MagicMock()
    backend._dispatch_to_plugins("callback_thread_created", mock_room)

    assert not hasattr(plugin_without_cb, "callback_thread_created")
    plugin_with_cb.callback_thread_created.assert_called_once_with(mock_room)


def test_create_thread_with_channel_id_and_cache(backend):
    """
    Verify thread creation fetches directly using channel_id if provided,
    and caches the created thread in _thread_cache.
    """
    mock_msg = MagicMock()
    mock_thread = MagicMock(spec=discord.Thread)
    mock_thread.id = 778899001122

    async def async_create_thread(name):
        return mock_thread

    mock_msg.create_thread = async_create_thread
    backend._fetch_message_by_id = MagicMock(return_value=mock_msg)
    backend._safe_run_coroutine = lambda coro, *a, **k: asyncio.run(coro)

    thread_id = backend._create_thread_from_message(
        "123456789", "Test Thread", channel_id="987654321"
    )

    backend._fetch_message_by_id.assert_called_once_with("123456789", channel_id="987654321")
    assert thread_id == "778899001122"
    assert "778899001122" in backend._thread_cache
    assert backend._thread_cache["778899001122"] == mock_thread

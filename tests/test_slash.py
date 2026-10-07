import asyncio
import importlib
import logging
import os
import sys
from tempfile import mkdtemp
from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest
from discord import app_commands
from discordlib.commands import message_context_menu, slash_command, user_context_menu
from discordlib.person import DiscordPerson
from discordlib.room import DiscordRoom, DiscordRoomOccupant
from errbot import BotPlugin
from errbot.backends.base import Message
from errbot.bootstrap import bot_config_defaults

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

    client._connection._command_tree = None
    client.loop = asyncio.new_event_loop()

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
        "sync_commands": True,
        "guild_sync_id": 123456789012345678,
    }
    config.BOT_ASYNC = False
    config.BOT_PREFIX = "!"
    config.CHATROOM_FN = "test_room"

    discord_backend = MockedDiscordBackend(config)
    discord_backend.rate_limit_enabled = False
    discord_backend.bot_identifier = DiscordPerson("123456789012345678")
    discord_backend.plugin_manager = MagicMock()
    discord_backend.plugin_manager.get_all_active_plugins.return_value = []
    discord_backend.repo_manager = MagicMock()
    discord_backend.repo_manager.plugin_dir = tempdir

    # Initialize CommandTree
    discord_backend.tree = app_commands.CommandTree(mock_discord_client)
    DiscordBackend.tree = discord_backend.tree

    return discord_backend


# =========================================================================
# Decorators Tests
# =========================================================================


def test_slash_command_decorator_attributes():
    @slash_command(name="ping", description="Ping test", guild=999)
    async def sample_ping(interaction: discord.Interaction):
        """Sample ping docstring."""
        pass

    assert getattr(sample_ping, "_is_slash_command") is True
    assert getattr(sample_ping, "_slash_name") == "ping"
    assert getattr(sample_ping, "_slash_description") == "Ping test"
    assert getattr(sample_ping, "_slash_guilds") == [999]


def test_message_context_menu_decorator_attributes():
    @message_context_menu(name="Quote Message", guilds=[111, 222])
    async def sample_quote(interaction: discord.Interaction, message: discord.Message):
        pass

    assert getattr(sample_quote, "_is_message_context_menu") is True
    assert getattr(sample_quote, "_context_menu_name") == "Quote Message"
    assert getattr(sample_quote, "_context_menu_guilds") == [111, 222]


def test_user_context_menu_decorator_attributes():
    @user_context_menu(name="Inspect User", guild=333)
    async def sample_inspect(interaction: discord.Interaction, user: discord.Member):
        pass

    assert getattr(sample_inspect, "_is_user_context_menu") is True
    assert getattr(sample_inspect, "_context_menu_name") == "Inspect User"
    assert getattr(sample_inspect, "_context_menu_guilds") == [333]


# =========================================================================
# Backend Slash Command Registration & Removal Tests
# =========================================================================


def test_backend_add_and_remove_slash_command_global(backend):
    async def my_cmd(interaction: discord.Interaction):
        pass

    cmd = backend.add_slash_command(my_cmd, name="global_test", description="Global test")
    assert cmd.name == "global_test"
    assert backend.tree.get_command("global_test") is not None

    removed = backend.remove_slash_command("global_test")
    assert removed is True
    assert backend.tree.get_command("global_test") is None


def test_backend_add_and_remove_slash_command_guild(backend):
    async def guild_cmd(interaction: discord.Interaction):
        pass

    guild_id = 987654321
    cmd = backend.add_slash_command(
        guild_cmd, name="guild_test", description="Guild test", guild=guild_id
    )
    assert cmd.name == "guild_test"

    guild_obj = discord.Object(id=guild_id)
    assert backend.tree.get_command("guild_test", guild=guild_obj) is not None

    removed = backend.remove_slash_command("guild_test", guild=guild_id)
    assert removed is True
    assert backend.tree.get_command("guild_test", guild=guild_obj) is None


def test_backend_add_context_menu(backend):
    async def msg_menu(interaction: discord.Interaction, message: discord.Message):
        pass

    menu = backend.add_context_menu(
        msg_menu, name="Test Quote", menu_type=discord.AppCommandType.message
    )
    assert menu.name == "Test Quote"
    assert menu.type == discord.AppCommandType.message
    assert backend.tree.get_command("Test Quote", type=discord.AppCommandType.message) is not None


# =========================================================================
# Plugin Scanning & Unregistering Tests
# =========================================================================


def test_register_and_unregister_plugin_commands(backend):
    class SamplePlugin(BotPlugin):
        name = "SamplePlugin"

        @slash_command(name="plugin_cmd", description="Plugin slash command")
        async def plugin_slash(self, interaction: discord.Interaction):
            pass

        @message_context_menu(name="Plugin Quote")
        async def plugin_quote(self, interaction: discord.Interaction, message: discord.Message):
            pass

        @user_context_menu(name="Plugin Inspect")
        async def plugin_inspect(self, interaction: discord.Interaction, user: discord.Member):
            pass

    plugin = SamplePlugin(backend)
    backend.register_plugin_commands(plugin)

    assert backend.tree.get_command("plugin_cmd") is not None
    assert backend.tree.get_command("Plugin Quote", type=discord.AppCommandType.message) is not None
    assert backend.tree.get_command("Plugin Inspect", type=discord.AppCommandType.user) is not None

    # Unregister
    backend.unregister_plugin_commands(plugin)
    assert backend.tree.get_command("plugin_cmd") is None
    assert backend.tree.get_command("Plugin Quote", type=discord.AppCommandType.message) is None
    assert backend.tree.get_command("Plugin Inspect", type=discord.AppCommandType.user) is None


# =========================================================================
# Auto-Bridging Tests
# =========================================================================


def test_bridge_errbot_commands(backend):
    def sample_status(msg, args):
        """Show current bot status."""
        return "All systems nominal"

    backend.commands = {
        "status": sample_status,
        "invalid command with spaces": sample_status,
    }
    backend.re_commands = {}

    backend._bridge_errbot_commands()

    # 'status' should be bridged into tree
    status_cmd = backend.tree.get_command("status")
    assert status_cmd is not None
    assert status_cmd.description == "Show current bot status."
    assert "status" in backend._bridged_commands

    # 'invalid_command_with_spaces' -> normalized to 'invalid_command_with_spaces'
    norm_cmd = backend.tree.get_command("invalid_command_with_spaces")
    assert norm_cmd is not None


def test_bridge_errbot_callback_dispatches_message(backend):
    dispatched_msg = None

    def capture_callback_message(msg):
        nonlocal dispatched_msg
        dispatched_msg = msg

    backend.callback_message = capture_callback_message

    def sample_hello(msg, args):
        return "Hello"

    backend.commands = {"hello": sample_hello}
    backend.re_commands = {}
    backend._bridge_errbot_commands()

    hello_cmd = backend.tree.get_command("hello")
    assert hello_cmd is not None

    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.id = 998877665511223344
    mock_interaction.user = MagicMock()
    mock_interaction.user.id = 111222333444555666
    mock_interaction.channel_id = 123456789012345678
    mock_interaction.guild = MagicMock()
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False
    mock_interaction.response.defer = AsyncMock()

    # Invoke bridge callback directly
    asyncio.run(hello_cmd.callback(mock_interaction, args="world"))

    assert mock_interaction.response.defer.called
    assert dispatched_msg is not None
    assert dispatched_msg.body == "!hello world"
    assert dispatched_msg.extras.get("discord_message_id") == "998877665511223344"
    assert dispatched_msg.extras.get("interaction") == mock_interaction


# =========================================================================
# Sync Application Commands Tests
# =========================================================================


def test_sync_application_commands_guild(backend):
    backend.tree.copy_global_to = MagicMock()
    backend.tree.sync = AsyncMock(return_value=[MagicMock(), MagicMock()])

    asyncio.run(backend._sync_application_commands())

    # Configured with guild_sync_id = 123456789012345678
    assert backend.tree.copy_global_to.called
    assert backend.tree.sync.called
    assert backend.tree.sync.call_args[1]["guild"].id == 123456789012345678


def test_sync_application_commands_global(backend):
    backend.bot_config.BOT_IDENTITY["guild_sync_id"] = None
    backend.tree.sync = AsyncMock(return_value=[MagicMock()])

    asyncio.run(backend._sync_application_commands())

    assert backend.tree.sync.called
    # When guild_sync_id is None, sync() is called without guild argument
    assert backend.tree.sync.call_args[1].get("guild") is None


def test_manual_sync_slash_commands(backend):
    backend.tree.sync = AsyncMock(return_value=[MagicMock()])
    backend._safe_run_coroutine = MagicMock(return_value=["cmd1", "cmd2"])

    res = backend.sync_slash_commands(guild_id=999)
    assert backend._safe_run_coroutine.called
    assert res == ["cmd1", "cmd2"]


# =========================================================================
# Send Message Interaction Routing Tests
# =========================================================================


def test_send_message_routes_to_interaction_response(backend):
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False
    mock_interaction.response.send_message = AsyncMock()

    msg = Message("Hello from slash command!")
    msg.frm = DiscordPerson("123456789012345678")
    msg.to = DiscordPerson("123456789012345678")
    msg.extras["interaction"] = mock_interaction

    backend._safe_run_coroutine = MagicMock(side_effect=lambda coro, *a, **kw: asyncio.run(coro))
    backend.send_message(msg)

    assert mock_interaction.response.send_message.called
    assert (
        mock_interaction.response.send_message.call_args[1]["content"]
        == "Hello from slash command!"
    )


def test_send_message_routes_to_interaction_followup(backend):
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = True
    mock_interaction.followup = MagicMock()
    mock_interaction.followup.send = AsyncMock()

    msg = Message("Deferred reply from slash command!")
    msg.frm = DiscordPerson("123456789012345678")
    msg.to = DiscordPerson("123456789012345678")
    msg.extras["interaction"] = mock_interaction

    backend._safe_run_coroutine = MagicMock(side_effect=lambda coro, *a, **kw: asyncio.run(coro))
    backend.send_message(msg)

    assert mock_interaction.followup.send.called
    assert (
        mock_interaction.followup.send.call_args[1]["content"]
        == "Deferred reply from slash command!"
    )

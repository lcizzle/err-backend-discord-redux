import importlib
import logging
import os
import sys
from tempfile import mkdtemp

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

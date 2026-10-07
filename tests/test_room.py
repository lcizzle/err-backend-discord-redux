import logging

import pytest
from discordlib.room import DiscordRoom
from mock import MagicMock

log = logging.getLogger(__name__)


@pytest.fixture
def discord_room():
    room = DiscordRoom
    setattr(room, "client", MagicMock())
    return room


def test_create_room_without_arguments():
    with pytest.raises(ValueError) as excinfo:
        DiscordRoom()
    assert "A channel id or channel name + guild id is required for a Room." in str(excinfo.value)


def test_create_room_with_name_only():
    with pytest.raises(ValueError) as excinfo:
        DiscordRoom(channel_name="#testing_ground")
    assert "A channel id or channel name + guild id is required for a Room." in str(excinfo.value)


def test_create_room_with_guild_only():
    with pytest.raises(ValueError) as excinfo:
        DiscordRoom(guild_id="1234567890123456789")
    assert "A channel id or channel name + guild id is required for a Room." in str(excinfo.value)


def test_create_room_with_id(discord_room):
    mock_channel = MagicMock()
    mock_channel.id = 1234567890132456789
    discord_room.client.get_channel.return_value = mock_channel
    room = discord_room(channel_id="1234567890132456789")
    assert room.id == 1234567890132456789


def test_create_room_with_name_and_guild_id(discord_room):
    mock_channel = MagicMock()
    mock_channel.id = 1234567890132456789
    mock_channel.name = "#testing_ground"
    mock_guild = MagicMock()
    mock_guild.channels = [mock_channel]
    discord_room.client.get_guild.return_value = mock_guild
    room = discord_room(channel_name="#testing_ground", guild_id="2345678901234567890")
    assert room.id == 1234567890132456789


def test_channel_name_to_id_with_thread_and_forum(discord_room):
    import asyncio

    import discord

    mock_forum = MagicMock(spec=getattr(discord, "ForumChannel", discord.TextChannel))
    mock_forum.id = 987654321
    mock_forum.name = "announcements-forum"
    mock_forum.guild.id = 111222

    mock_channel = MagicMock(spec=discord.TextChannel)
    mock_channel.id = 123456789
    mock_channel.name = "announcements-forum"
    mock_channel.guild.id = 111222

    discord_room.client.get_all_channels.return_value = [mock_channel]

    room = MagicMock(spec=DiscordRoom)
    room._channel_name = "announcements-forum"
    room._guild_id = 111222
    room.channel_name_to_id = DiscordRoom.channel_name_to_id.__get__(room, DiscordRoom)

    res_id = room.channel_name_to_id()
    assert res_id == 123456789


def test_room_send_to_forum_channel(discord_room):
    import asyncio

    import discord

    mock_forum = MagicMock(spec=getattr(discord, "ForumChannel", discord.TextChannel))
    mock_forum.id = 5544332211
    mock_forum.name = "ideas-forum"

    async def mock_create_thread(**kwargs):
        return MagicMock()

    mock_forum.create_thread = MagicMock(side_effect=mock_create_thread)
    discord_room.client.get_channel.return_value = mock_forum

    room = discord_room(channel_id="5544332211")
    # Emulate discord_channel being ForumChannel if available
    if hasattr(discord, "ForumChannel"):
        mock_forum.__class__ = discord.ForumChannel
        room.discord_channel = mock_forum

        asyncio.run(room.send(content="New Feature Proposal\nDetails here"))
        assert mock_forum.create_thread.called
        call_kwargs = mock_forum.create_thread.call_args[1]
        assert call_kwargs["name"] == "New Feature Proposal"


def test_deleted_room_name_and_guild(discord_room):
    """
    Verify that when a room/thread is deleted and client.get_channel returns None,
    accessing .name, .guild, and .exists does not raise AttributeError.
    """
    discord_room.client.get_channel.return_value = None

    room = discord_room(
        channel_name="deleted-thread", guild_id="123456789", channel_id="9988776655"
    )

    assert room.name == "deleted-thread"
    assert room.guild == 123456789
    assert room.id == 9988776655
    assert room.exists is False

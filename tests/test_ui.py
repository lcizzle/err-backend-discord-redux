import asyncio
import importlib
import logging
import os
import sys
from tempfile import mkdtemp
from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest
from discordlib.person import DiscordPerson
from discordlib.room import DiscordRoom, DiscordRoomOccupant
from discordlib.ui import ActionRowView, SimpleButton, SimpleModal, SimpleSelect
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
    }
    config.BOT_ASYNC = False
    config.BOT_PREFIX = "!"
    config.CHATROOM_FN = "test_room"

    discord_backend = MockedDiscordBackend(config)
    discord_backend.rate_limit_enabled = False
    discord_backend.bot_identifier = DiscordPerson("123456789012345678")
    discord_backend.plugin_manager = MagicMock()
    discord_backend.plugin_manager.get_all_active_plugins.return_value = []
    return discord_backend


def test_simple_button_init_and_properties():
    btn = SimpleButton(label="Click Me", custom_id="test_btn", style=discord.ButtonStyle.success)
    assert btn.label == "Click Me"
    assert btn.custom_id == "test_btn"
    assert btn.style == discord.ButtonStyle.success
    assert btn.disabled is False


def test_simple_button_sync_callback():
    callback_called = False

    def on_click(interaction):
        nonlocal callback_called
        callback_called = True

    btn = SimpleButton(label="Click", callback=on_click)
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False

    asyncio.run(btn.callback(mock_interaction))
    assert callback_called is True


def test_simple_button_async_callback():
    callback_called = False

    async def on_click(interaction):
        nonlocal callback_called
        callback_called = True

    btn = SimpleButton(label="Click", callback=on_click)
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False

    asyncio.run(btn.callback(mock_interaction))
    assert callback_called is True


def test_simple_button_no_callback_defers():
    btn = SimpleButton(label="Click", callback=None)
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False
    mock_interaction.response.defer = AsyncMock()

    asyncio.run(btn.callback(mock_interaction))
    mock_interaction.response.defer.assert_awaited_once()


def test_simple_button_callback_error_handling():
    def on_click(interaction):
        raise ValueError("Boom")

    btn = SimpleButton(label="Click", callback=on_click)
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False
    mock_interaction.response.send_message = AsyncMock()

    asyncio.run(btn.callback(mock_interaction))
    mock_interaction.response.send_message.assert_awaited_once()
    assert "Boom" in mock_interaction.response.send_message.call_args[0][0]


def test_action_row_view_fluent_builder():
    view = ActionRowView()
    view.add_button(label="Btn 1", custom_id="b1")
    view.add_select(
        placeholder="Choose option",
        custom_id="s1",
        options=[("Option A", "val_a", "Description A"), "Option B"],
    )

    assert len(view.children) == 2
    assert isinstance(view.children[0], SimpleButton)
    assert view.children[0].label == "Btn 1"
    assert isinstance(view.children[1], SimpleSelect)
    assert view.children[1].placeholder == "Choose option"
    assert len(view.children[1].options) == 2
    assert view.children[1].options[0].label == "Option A"
    assert view.children[1].options[0].value == "val_a"
    assert view.children[1].options[1].label == "Option B"


def test_simple_select_async_callback():
    received_values = []

    async def on_select(interaction, values):
        nonlocal received_values
        received_values = values

    select = SimpleSelect(
        placeholder="Pick",
        options=[discord.SelectOption(label="1", value="one")],
        callback=on_select,
    )
    select._values = ["one"]
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False

    asyncio.run(select.callback(mock_interaction))
    assert received_values == ["one"]


def test_simple_modal_builder_and_on_submit():
    modal = SimpleModal(title="Test Modal", custom_id="modal_1")
    modal.add_text_input(label="Name", custom_id="name_field", placeholder="Enter name")

    assert modal.title == "Test Modal"
    assert modal.custom_id == "modal_1"
    assert "name_field" in modal.inputs
    assert len(modal.children) == 1


def test_simple_modal_async_on_submit():
    submitted_data = {}

    async def on_submit(interaction, values):
        nonlocal submitted_data
        submitted_data = values

    modal = SimpleModal(title="Survey", on_submit=on_submit)
    modal.add_text_input(label="Feedback", custom_id="fb")
    modal.inputs["fb"]._value = "Great job!"

    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False

    asyncio.run(modal.on_submit(mock_interaction))
    assert submitted_data == {"fb": "Great job!"}


def test_send_ui_interaction_new_response(backend):
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False
    mock_interaction.response.send_message = AsyncMock()

    view = ActionRowView().add_button("Click")
    backend._safe_run_coroutine = MagicMock()
    backend.send_ui(mock_interaction, content="Hello Interaction", view=view, ephemeral=True)

    assert backend._safe_run_coroutine.called
    backend._safe_run_coroutine.call_args[0][0].close()


def test_send_ui_interaction_followup(backend):
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = True
    mock_interaction.followup = MagicMock()
    mock_interaction.followup.send = AsyncMock()

    view = ActionRowView().add_button("Click")
    backend._safe_run_coroutine = MagicMock()
    backend.send_ui(mock_interaction, content="Followup message", view=view, ephemeral=False)

    assert backend._safe_run_coroutine.called
    backend._safe_run_coroutine.call_args[0][0].close()


def test_send_ui_discord_room(backend):
    room = DiscordRoom("test-channel", 111111111111111111, 222222222222222222)
    view = ActionRowView().add_button("Button")
    backend._safe_run_coroutine = MagicMock()

    backend.send_ui(room, content="Test UI in Room", view=view)
    assert backend._safe_run_coroutine.called
    backend._safe_run_coroutine.call_args[0][0].close()


def test_send_ui_message_with_thread(backend):
    msg = Message("Help", extras={"thread_id": "333333333333333333"})

    mock_thread_channel = MagicMock(spec=discord.Thread)
    mock_thread_channel.id = 333333333333333333
    mock_thread_channel.name = "thread-name"
    mock_thread_channel.guild = MagicMock(id=111111111111111111)
    backend.client.get_channel.return_value = mock_thread_channel

    view = ActionRowView().add_button("Thread Button")
    backend._safe_run_coroutine = MagicMock()
    backend.send_ui(msg, content="Posting to thread", view=view)
    assert backend._safe_run_coroutine.called
    backend._safe_run_coroutine.call_args[0][0].close()


def test_send_modal(backend):
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False
    mock_interaction.response.send_modal = AsyncMock()

    modal = SimpleModal(title="Test Modal")
    backend._safe_run_coroutine = MagicMock()
    backend.send_modal(mock_interaction, modal)

    assert backend._safe_run_coroutine.called
    backend._safe_run_coroutine.call_args[0][0].close()


def test_send_modal_already_done_fails(backend):
    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = True
    mock_interaction.response.send_modal = AsyncMock()

    modal = SimpleModal(title="Test Modal")
    res = backend.send_modal(mock_interaction, modal)
    assert res is None
    mock_interaction.response.send_modal.assert_not_called()


def test_on_interaction_dispatches_callback(backend):
    mock_plugin = MagicMock()
    mock_plugin.callback_interaction = MagicMock()
    mock_plugin_manager = MagicMock()
    mock_plugin_manager.get_all_active_plugins.return_value = [mock_plugin]
    backend.plugin_manager = mock_plugin_manager

    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.type = discord.InteractionType.component
    mock_interaction.user = MagicMock(id=999)
    mock_interaction.data = {"custom_id": "btn_test"}

    asyncio.run(backend.on_interaction(mock_interaction))
    mock_plugin.callback_interaction.assert_called_once_with(mock_interaction)


def test_send_message_with_view_in_extras(backend):
    room = DiscordRoom("test-channel", 111111111111111111, 222222222222222222)
    view = ActionRowView().add_button("My Button")
    msg = Message("Click the button below:", extras={"view": view})
    msg.to = room
    msg.frm = DiscordPerson("999999999999999999")

    backend._safe_run_coroutine = MagicMock()
    backend.send_message(msg)
    assert backend._safe_run_coroutine.called
    backend._safe_run_coroutine.call_args[0][0].close()


def test_send_card_with_view(backend):
    room = DiscordRoom("test-channel", 111111111111111111, 222222222222222222)
    view = ActionRowView().add_button("Card Button")
    card = MagicMock()
    card.to = room
    card.title = "Card Title"
    card.body = "Card Description"
    card.view = view

    backend._safe_run_coroutine = MagicMock()
    backend.send_card(card)
    assert backend._safe_run_coroutine.called
    backend._safe_run_coroutine.call_args[0][0].close()


def test_send_discord_embed_with_view(backend):
    room = DiscordRoom("test-channel", 111111111111111111, 222222222222222222)
    view = ActionRowView().add_button("Embed Button")
    backend._safe_run_coroutine = MagicMock()
    res = backend.send_discord_embed(room, title="Rich Embed", view=view)
    assert res is True
    assert backend._safe_run_coroutine.called
    backend._safe_run_coroutine.call_args[0][0].close()


def test_build_reply_with_view(backend):
    parent_msg = Message("Hello bot")
    parent_msg.frm = DiscordRoomOccupant("999999999999999999", "222222222222222222")
    parent_msg.to = DiscordRoom("test-channel", 111111111111111111, 222222222222222222)

    view = ActionRowView().add_button("Reply Button")
    reply = backend.build_reply(parent_msg, "Here is a reply", view=view)

    assert reply.body == "Here is a reply"
    assert reply.extras.get("view") == view


def test_register_view_components_and_fallback_dispatch(backend):
    button_clicked = False

    def on_click(interaction):
        nonlocal button_clicked
        button_clicked = True

    view = ActionRowView()
    view.add_button("Click", custom_id="test_click_btn", callback=on_click)
    backend._register_view_components(view)

    assert "test_click_btn" in backend._active_component_items

    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.id = 11110001
    mock_interaction.type = discord.InteractionType.component
    mock_interaction.user = MagicMock(id=999)
    mock_interaction.data = {"custom_id": "test_click_btn"}
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False

    asyncio.run(backend.on_interaction(mock_interaction))
    assert button_clicked is True
    from discordlib.ui import is_interaction_handled

    assert is_interaction_handled(mock_interaction) is True


def test_modal_fallback_dispatch(backend):
    modal_submitted = False
    submitted_values = {}

    def on_submit(interaction, values):
        nonlocal modal_submitted, submitted_values
        modal_submitted = True
        submitted_values = values

    modal = SimpleModal(title="Test Modal", custom_id="test_modal_123", on_submit=on_submit)
    modal.add_text_input(label="Name", custom_id="name_field")
    backend._active_modals["test_modal_123"] = modal

    mock_interaction = MagicMock(spec=discord.Interaction)
    mock_interaction.id = 22220002
    mock_interaction.type = discord.InteractionType.modal_submit
    mock_interaction.user = MagicMock(id=999)
    mock_interaction.data = {
        "custom_id": "test_modal_123",
        "components": [{"components": [{"custom_id": "name_field", "value": "Alice"}]}],
    }
    mock_interaction.response = MagicMock()
    mock_interaction.response.is_done.return_value = False

    asyncio.run(backend.on_interaction(mock_interaction))
    assert modal_submitted is True
    assert submitted_values.get("name_field") == "Alice"

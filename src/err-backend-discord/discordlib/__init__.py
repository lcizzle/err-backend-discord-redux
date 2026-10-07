"""
discordlib - Discord backend support classes for Errbot.
"""

from discordlib.commands import message_context_menu, slash_command, user_context_menu
from discordlib.person import DiscordPerson, DiscordSender
from discordlib.room import DiscordCategory, DiscordRoom, DiscordRoomOccupant
from discordlib.ui import (
    ActionRowView,
    SimpleButton,
    SimpleModal,
    SimpleSelect,
    is_interaction_handled,
    mark_interaction_handled,
)

__all__ = [
    "DiscordPerson",
    "DiscordRoomOccupant",
    "DiscordSender",
    "DiscordCategory",
    "DiscordRoom",
    "ActionRowView",
    "SimpleButton",
    "SimpleModal",
    "SimpleSelect",
    "is_interaction_handled",
    "mark_interaction_handled",
    "slash_command",
    "message_context_menu",
    "user_context_menu",
]

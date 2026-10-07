"""
Discord Native Application Commands (Slash Commands & Context Menus) support for Errbot.
Provides decorators and helpers for plugins to declare slash commands and context menus.
"""

from typing import Callable, List, Optional, Union

import discord
from discord import app_commands


def slash_command(
    name: Optional[str] = None,
    description: Optional[str] = None,
    guild: Optional[Union[int, str, discord.abc.Snowflake]] = None,
    guilds: Optional[List[Union[int, str, discord.abc.Snowflake]]] = None,
):
    """
    Decorator to declare a plugin method as a native Discord Slash Command (/command).

    Usage:
        class MyPlugin(BotPlugin):
            @slash_command(name="ping", description="Check bot latency")
            async def ping_cmd(self, interaction: discord.Interaction):
                await interaction.response.send_message("Pong!")

            @slash_command(name="echo", description="Echoes back text")
            async def echo_cmd(self, interaction: discord.Interaction, message: str):
                await interaction.response.send_message(f"Echo: {message}")
    """

    def decorator(func: Callable):
        func._is_slash_command = True
        func._slash_name = (name or func.__name__).lower()
        doc = (func.__doc__ or "Slash command").strip().split("\n")[0][:100]
        func._slash_description = description or doc
        func._slash_guilds = [guild] if guild else (guilds or [])
        return func

    return decorator


def message_context_menu(
    name: Optional[str] = None,
    guild: Optional[Union[int, str, discord.abc.Snowflake]] = None,
    guilds: Optional[List[Union[int, str, discord.abc.Snowflake]]] = None,
):
    """
    Decorator to declare a plugin method as a Discord Message Context Menu item
    (Right-click message -> Apps -> [name]).

    Usage:
        class MyPlugin(BotPlugin):
            @message_context_menu(name="Quote Message")
            async def quote_msg(self, interaction: discord.Interaction, message: discord.Message):
                await interaction.response.send_message(f"Quote: {message.content}")
    """

    def decorator(func: Callable):
        func._is_message_context_menu = True
        func._context_menu_name = name or func.__name__
        func._context_menu_guilds = [guild] if guild else (guilds or [])
        return func

    return decorator


def user_context_menu(
    name: Optional[str] = None,
    guild: Optional[Union[int, str, discord.abc.Snowflake]] = None,
    guilds: Optional[List[Union[int, str, discord.abc.Snowflake]]] = None,
):
    """
    Decorator to declare a plugin method as a Discord User Context Menu item
    (Right-click user/member -> Apps -> [name]).

    Usage:
        class MyPlugin(BotPlugin):
            @user_context_menu(name="Inspect User")
            async def inspect_user(self, interaction: discord.Interaction, user: discord.Member):
                await interaction.response.send_message(f"User: {user.name} ({user.id})")
    """

    def decorator(func: Callable):
        func._is_user_context_menu = True
        func._context_menu_name = name or func.__name__
        func._context_menu_guilds = [guild] if guild else (guilds or [])
        return func

    return decorator

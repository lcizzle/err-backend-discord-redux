"""
Discord UI helper components for Errbot plugins.
Provides easy creation of Discord Buttons, Select Menus, Views, and Modals.
"""

import asyncio
import inspect
import logging
from collections import deque
from typing import Callable, List, Optional, Union

import discord

log = logging.getLogger(__name__)

# Bounded tracking for handled interactions to prevent duplicate execution
# without modifying discord.Interaction's fixed __slots__.
_HANDLED_INTERACTIONS = set()
_HANDLED_QUEUE = deque(maxlen=2000)


def mark_interaction_handled(interaction: discord.Interaction) -> bool:
    """
    Mark an interaction as handled.
    Returns True if it was already marked, False if marked now.
    """
    inter_id = getattr(interaction, "id", None)
    if inter_id is None:
        return False
    if inter_id in _HANDLED_INTERACTIONS:
        return True
    if len(_HANDLED_QUEUE) == _HANDLED_QUEUE.maxlen:
        oldest = _HANDLED_QUEUE.popleft()
        _HANDLED_INTERACTIONS.discard(oldest)
    _HANDLED_INTERACTIONS.add(inter_id)
    _HANDLED_QUEUE.append(inter_id)
    return False


def is_interaction_handled(interaction: discord.Interaction) -> bool:
    """
    Check if an interaction has already been marked handled or responded to.
    """
    inter_id = getattr(interaction, "id", None)
    if inter_id is not None and inter_id in _HANDLED_INTERACTIONS:
        return True
    if hasattr(interaction, "response") and interaction.response.is_done():
        return True
    return False


class SimpleButton(discord.ui.Button):
    """
    A button component supporting synchronous or asynchronous callbacks.
    """

    def __init__(
        self,
        label: str,
        custom_id: Optional[str] = None,
        style: discord.ButtonStyle = discord.ButtonStyle.primary,
        emoji: Optional[Union[str, discord.Emoji, discord.PartialEmoji]] = None,
        url: Optional[str] = None,
        disabled: bool = False,
        row: Optional[int] = None,
        callback: Optional[Callable] = None,
    ):
        super().__init__(
            label=label,
            custom_id=custom_id,
            style=style,
            emoji=emoji,
            url=url,
            disabled=disabled,
            row=row,
        )
        self._action_callback = callback

    async def callback(self, interaction: discord.Interaction):
        if is_interaction_handled(interaction):
            return
        mark_interaction_handled(interaction)

        if self._action_callback:
            try:
                if inspect.iscoroutinefunction(self._action_callback):
                    await self._action_callback(interaction)
                else:
                    res = self._action_callback(interaction)
                    if inspect.iscoroutine(res):
                        await res
            except Exception as e:
                log.exception(f"Error in button '{self.label}' callback: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"Error handling button action: {e}", ephemeral=True
                    )
        else:
            if not interaction.response.is_done():
                await interaction.response.defer()


class SimpleSelect(discord.ui.Select):
    """
    A dropdown select menu supporting synchronous or asynchronous callbacks.
    """

    def __init__(
        self,
        placeholder: Optional[str] = None,
        custom_id: Optional[str] = None,
        options: Optional[List[discord.SelectOption]] = None,
        min_values: int = 1,
        max_values: int = 1,
        disabled: bool = False,
        row: Optional[int] = None,
        callback: Optional[Callable] = None,
    ):
        init_kwargs = {
            "placeholder": placeholder,
            "options": options or [],
            "min_values": min_values,
            "max_values": max_values,
            "disabled": disabled,
            "row": row,
        }
        if custom_id is not None:
            init_kwargs["custom_id"] = custom_id
        super().__init__(**init_kwargs)
        self._action_callback = callback

    async def callback(self, interaction: discord.Interaction):
        if is_interaction_handled(interaction):
            return
        mark_interaction_handled(interaction)

        values = getattr(self, "values", [])
        if not values and hasattr(interaction, "data") and isinstance(interaction.data, dict):
            values = interaction.data.get("values", [])

        if self._action_callback:
            try:
                if inspect.iscoroutinefunction(self._action_callback):
                    await self._action_callback(interaction, values)
                else:
                    res = self._action_callback(interaction, values)
                    if inspect.iscoroutine(res):
                        await res
            except Exception as e:
                log.exception(f"Error in select menu callback: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"Error handling selection: {e}", ephemeral=True
                    )
        else:
            if not interaction.response.is_done():
                await interaction.response.defer()


class ActionRowView(discord.ui.View):
    """
    Convenience discord.ui.View container for buttons and dropdowns.
    """

    def __init__(self, *items: discord.ui.Item, timeout: Optional[float] = 180.0):
        super().__init__(timeout=timeout)
        self._ensure_active_future()
        for item in items:
            self.add_item(item)

    def _ensure_active_future(self):
        """
        Ensure view has an active __stopped future on the Discord client loop.
        discord.py's BaseView sets __stopped to None when created outside an active asyncio loop,
        which causes discord.py to silently drop all component interactions.
        """
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                from discordlib.person import DiscordSender

                if DiscordSender.client and getattr(DiscordSender.client, "loop", None):
                    loop = DiscordSender.client.loop
            except Exception:
                pass

        if loop:
            stopped = getattr(self, "_BaseView__stopped", None)
            if stopped is None or stopped.done() or getattr(stopped, "_loop", None) != loop:
                try:
                    self._BaseView__stopped = loop.create_future()
                except Exception:
                    pass

    def add_button(
        self,
        label: str,
        custom_id: Optional[str] = None,
        style: discord.ButtonStyle = discord.ButtonStyle.primary,
        emoji: Optional[Union[str, discord.Emoji, discord.PartialEmoji]] = None,
        url: Optional[str] = None,
        disabled: bool = False,
        row: Optional[int] = None,
        callback: Optional[Callable] = None,
    ) -> "ActionRowView":
        self.add_item(
            SimpleButton(
                label=label,
                custom_id=custom_id,
                style=style,
                emoji=emoji,
                url=url,
                disabled=disabled,
                row=row,
                callback=callback,
            )
        )
        return self

    def add_select(
        self,
        placeholder: Optional[str] = None,
        custom_id: Optional[str] = None,
        options: Optional[List[Union[discord.SelectOption, tuple, str]]] = None,
        min_values: int = 1,
        max_values: int = 1,
        disabled: bool = False,
        row: Optional[int] = None,
        callback: Optional[Callable] = None,
    ) -> "ActionRowView":
        formatted_options = []
        if options:
            for opt in options:
                if isinstance(opt, discord.SelectOption):
                    formatted_options.append(opt)
                elif isinstance(opt, (tuple, list)):
                    formatted_options.append(
                        discord.SelectOption(
                            label=str(opt[0]),
                            value=str(opt[1]) if len(opt) > 1 else str(opt[0]),
                            description=str(opt[2]) if len(opt) > 2 else None,
                        )
                    )
                else:
                    formatted_options.append(discord.SelectOption(label=str(opt), value=str(opt)))

        self.add_item(
            SimpleSelect(
                placeholder=placeholder,
                custom_id=custom_id,
                options=formatted_options,
                min_values=min_values,
                max_values=max_values,
                disabled=disabled,
                row=row,
                callback=callback,
            )
        )
        return self


class SimpleModal(discord.ui.Modal):
    """
    A convenient Modal dialog container for text inputs.
    """

    def __init__(
        self,
        title: str,
        custom_id: Optional[str] = None,
        timeout: Optional[float] = None,
        on_submit: Optional[Callable] = None,
    ):
        modal_kwargs = {"title": title, "timeout": timeout}
        if custom_id is not None:
            modal_kwargs["custom_id"] = custom_id
        super().__init__(**modal_kwargs)
        self._on_submit_callback = on_submit
        self.inputs = {}
        self._ensure_active_future()

    def _ensure_active_future(self):
        """
        Ensure modal has an active __stopped future on the Discord client loop.
        """
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                from discordlib.person import DiscordSender

                if DiscordSender.client and getattr(DiscordSender.client, "loop", None):
                    loop = DiscordSender.client.loop
            except Exception:
                pass

        if loop:
            stopped = getattr(self, "_BaseView__stopped", None)
            if stopped is None or stopped.done() or getattr(stopped, "_loop", None) != loop:
                try:
                    self._BaseView__stopped = loop.create_future()
                except Exception:
                    pass

    def add_text_input(
        self,
        label: str,
        custom_id: str,
        style: discord.TextStyle = discord.TextStyle.short,
        placeholder: Optional[str] = None,
        default: Optional[str] = None,
        required: bool = True,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ) -> "SimpleModal":
        text_input = discord.ui.TextInput(
            label=label,
            custom_id=custom_id,
            style=style,
            placeholder=placeholder,
            default=default,
            required=required,
            min_length=min_length,
            max_length=max_length,
        )
        self.inputs[custom_id] = text_input
        self.add_item(text_input)
        return self

    async def on_submit(self, interaction: discord.Interaction):
        if is_interaction_handled(interaction):
            return
        mark_interaction_handled(interaction)

        if self._on_submit_callback:
            try:
                values = {
                    cid: getattr(item, "value", getattr(item, "_value", ""))
                    for cid, item in self.inputs.items()
                }
                if (
                    not any(values.values())
                    and hasattr(interaction, "data")
                    and isinstance(interaction.data, dict)
                ):
                    for row in interaction.data.get("components", []):
                        for comp in row.get("components", []):
                            cid = comp.get("custom_id")
                            if cid and cid in self.inputs:
                                values[cid] = comp.get("value", "")

                if inspect.iscoroutinefunction(self._on_submit_callback):
                    await self._on_submit_callback(interaction, values)
                else:
                    res = self._on_submit_callback(interaction, values)
                    if inspect.iscoroutine(res):
                        await res
            except Exception as e:
                log.exception(f"Error in modal on_submit callback: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"Error processing modal: {e}", ephemeral=True
                    )
        else:
            if not interaction.response.is_done():
                await interaction.response.defer()

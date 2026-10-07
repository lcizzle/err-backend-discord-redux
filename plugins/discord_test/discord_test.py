import asyncio
import inspect
import logging
from collections import deque
from typing import Deque

from errbot import BotPlugin, botcmd
from errbot.backends.base import Reaction

try:
    import discord
    from discordlib.commands import message_context_menu, slash_command, user_context_menu
    from discordlib.ui import ActionRowView, SimpleButton, SimpleModal, SimpleSelect
except ImportError:
    discord = None
    slash_command = None
    message_context_menu = None
    user_context_menu = None
    ActionRowView = None
    SimpleButton = None
    SimpleModal = None
    SimpleSelect = None


def _dummy_decorator(*args, **kwargs):
    def wrapper(f):
        return f

    return wrapper


if slash_command is None:
    slash_command = _dummy_decorator
if message_context_menu is None:
    message_context_menu = _dummy_decorator
if user_context_menu is None:
    user_context_menu = _dummy_decorator

log = logging.getLogger(__name__)


class DiscordTest(BotPlugin):
    """
    Discord Backend Feature Testing Plugin for administrators.
    Allows testing:
      - Phase 1: Message Extras, LRU Caching, Thread creation, Reactions
      - Phase 2: Message deletion callbacks, Thread lifecycle callbacks, Forum/Stage query_room
      - Phase 3: Interactive UI components (Buttons, Dropdown Selects, Modals, on_interaction)
      - Phase 4: Native Application Commands (Slash Commands & Context Menus)
    """

    def activate(self):
        super().activate()
        # Ring buffer storing recent reactions observed by callback_reaction
        self.reaction_history: Deque[dict] = deque(maxlen=20)
        # Ring buffer storing recent message deletions
        self.deletion_history: Deque[dict] = deque(maxlen=20)
        # Ring buffer storing recent thread lifecycle events
        self.thread_history: Deque[dict] = deque(maxlen=20)
        # Ring buffer storing recent interactions observed by callback_interaction
        self.interaction_history: Deque[dict] = deque(maxlen=20)

        # Register native slash commands & context menus with backend CommandTree
        if hasattr(self._bot, "register_plugin_commands"):
            try:
                self._bot.register_plugin_commands(self)
                log.info("Registered DiscordTest application commands with CommandTree.")
            except Exception as e:
                log.warning(f"Could not register application commands: {e}")

        log.info("DiscordTest plugin activated.")

    def deactivate(self):
        # Unregister application commands when plugin deactivated
        if hasattr(self._bot, "unregister_plugin_commands"):
            try:
                self._bot.unregister_plugin_commands(self)
                log.info("Unregistered DiscordTest application commands from CommandTree.")
            except Exception as e:
                log.warning(f"Could not unregister application commands: {e}")
        super().deactivate()

    def callback_reaction(self, reaction: Reaction):
        """
        Invoked by DiscordBackend when on_raw_reaction_add or on_raw_reaction_remove fires.
        """
        rxn_info = {
            "action": reaction.action,
            "emoji": reaction.reaction_name,
            "reactor": str(reaction.reactor),
            "message_id": reaction.reacted_to.get("message_id") if reaction.reacted_to else None,
            "channel_id": reaction.reacted_to.get("channel_id") if reaction.reacted_to else None,
            "timestamp": reaction.timestamp,
        }
        self.reaction_history.appendleft(rxn_info)
        log.debug(f"[DiscordTest] Captured reaction event: {rxn_info}")

    def callback_message_deleted(self, msg):
        """
        Invoked by DiscordBackend when on_message_delete fires (cached messages).
        """
        del_info = {
            "type": "cached",
            "message_id": msg.extras.get("discord_message_id") if msg.extras else None,
            "channel_id": msg.extras.get("channel_id") if msg.extras else None,
            "author": str(msg.frm),
            "content": msg.body[:50] if msg.body else "",
        }
        self.deletion_history.appendleft(del_info)
        log.debug(f"[DiscordTest] Captured message deleted event: {del_info}")

    def callback_raw_message_deleted(self, msg):
        """
        Invoked by DiscordBackend when on_raw_message_delete fires (cached & uncached).
        """
        del_info = {
            "type": "raw",
            "message_id": msg.extras.get("discord_message_id") if msg.extras else None,
            "channel_id": msg.extras.get("channel_id") if msg.extras else None,
            "cached": msg.extras.get("cached", False) if msg.extras else False,
            "content": msg.body[:50] if msg.body else "(uncached/empty)",
        }
        self.deletion_history.appendleft(del_info)
        log.debug(f"[DiscordTest] Captured raw message deleted event: {del_info}")

    def callback_thread_created(self, room):
        """
        Invoked by DiscordBackend when a thread is created.
        """
        info = {
            "action": "created",
            "name": getattr(room, "name", str(room)),
            "id": str(getattr(room, "id", "")),
        }
        self.thread_history.appendleft(info)
        log.debug(f"[DiscordTest] Captured thread created event: {info}")

    def callback_thread_deleted(self, room):
        """
        Invoked by DiscordBackend when a thread is deleted.
        """
        info = {
            "action": "deleted",
            "name": getattr(room, "name", str(room)),
            "id": str(getattr(room, "id", "")),
        }
        self.thread_history.appendleft(info)
        log.debug(f"[DiscordTest] Captured thread deleted event: {info}")

    def callback_thread_updated(self, room, before, after):
        """
        Invoked by DiscordBackend when a thread is updated.
        """
        info = {
            "action": "updated",
            "name": getattr(after, "name", str(room)),
            "id": str(getattr(after, "id", "")),
            "archived": getattr(after, "archived", False),
            "locked": getattr(after, "locked", False),
        }
        self.thread_history.appendleft(info)
        log.debug(f"[DiscordTest] Captured thread updated event: {info}")

    def callback_interaction(self, interaction):
        """
        Invoked by DiscordBackend when on_interaction fires (buttons, select menus, modals, etc.).
        """
        custom_id = None
        if hasattr(interaction, "data") and isinstance(interaction.data, dict):
            custom_id = interaction.data.get("custom_id")

        info = {
            "type": str(getattr(interaction, "type", "unknown")),
            "user": str(getattr(interaction, "user", "unknown")),
            "user_id": str(getattr(interaction.user, "id", "")),
            "custom_id": custom_id,
        }
        self.interaction_history.appendleft(info)
        log.debug(f"[DiscordTest] Captured interaction event: {info}")

    @botcmd(admin_only=True)
    def test_phase1(self, msg, args):
        """
        Runs an overview checklist and reports current status for Phase 1 features.
        Usage: !test phase1
        """
        discord_msg_id = msg.extras.get("discord_message_id") if msg.extras else None
        channel_id = msg.extras.get("channel_id") if msg.extras else None
        thread_id = msg.extras.get("thread_id") if msg.extras else None

        cached = False
        backend = self._bot
        if hasattr(backend, "_message_cache") and discord_msg_id:
            with backend._message_cache_lock:
                cached = str(discord_msg_id) in backend._message_cache

        report = [
            "**--- Discord Backend Phase 1 Test Report ---**",
            f"• **discord_message_id populated:** {'`' + str(discord_msg_id) + '`' if discord_msg_id else '❌ None'}",
            f"• **channel_id populated:** {'`' + str(channel_id) + '`' if channel_id else '❌ None'}",
            f"• **thread_id (if in thread):** {'`' + str(thread_id) + '`' if thread_id else 'N/A (Standard Channel)'}",
            f"• **Message cached in LRU cache:** {'✅ Yes' if cached else '❌ No'}",
            f"• **Raw reactions captured in session:** {len(self.reaction_history)}",
            "",
            "**Available Phase 1 Test Commands:**",
            "`!test extras` - Inspect full metadata in msg.extras",
            "`!test thread [text]` - Spawn a new thread from this message",
            "`!test react [emoji]` - Have the bot add a reaction to this command message",
            "`!test reactions` - View the most recently captured raw reaction events",
        ]
        return "\n".join(report)

    @botcmd(admin_only=True)
    def test_phase2(self, msg, args):
        """
        Runs an overview checklist and reports current status for Phase 2 features.
        Usage: !test phase2
        """
        report = [
            "**--- Discord Backend Phase 2 Test Report ---**",
            f"• **Deletions captured in session:** {len(self.deletion_history)}",
            f"• **Thread lifecycle events captured:** {len(self.thread_history)}",
            "",
            "**Available Phase 2 Test Commands:**",
            "`!test deletions` - View recently deleted messages captured by the backend",
            "`!test threvents` - View recently captured thread lifecycle events",
            "`!test query <#channel_id>` - Query room to test Forum/Stage/Thread channel lookup",
        ]
        return "\n".join(report)

    @botcmd(admin_only=True)
    def test_phase3(self, msg, args):
        """
        Runs an overview checklist and reports current status for Phase 3 features.
        Usage: !test phase3
        """
        report = [
            "**--- Discord Backend Phase 3 Test Report ---**",
            f"• **discord.ui supported:** {'✅ Yes' if ActionRowView else '❌ No'}",
            f"• **Interactions captured in session:** {len(self.interaction_history)}",
            "",
            "**Available Phase 3 Test Commands:**",
            "`!test buttons` - Render interactive Discord Buttons (Primary, Success, Danger, Link)",
            "`!test dropdown` - Render interactive Select / Dropdown menu",
            "`!test modal` - Test opening a Discord Modal popup form via button interaction",
            "`!test interactions` - View recent interactions captured by the backend hook",
        ]
        return "\n".join(report)

    @botcmd(admin_only=True)
    def test_buttons(self, msg, args):
        """
        Renders interactive buttons to test UI button components.
        Usage: !test buttons
        """
        if not ActionRowView:
            return "❌ UI components are not available (ActionRowView import failed)."

        view = ActionRowView(timeout=120)

        async def btn_primary_callback(interaction: discord.Interaction):
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"🟦 Primary Button clicked by {interaction.user.mention}!", ephemeral=True
                )

        async def btn_success_callback(interaction: discord.Interaction):
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"🟩 Success Button clicked by {interaction.user.mention}!", ephemeral=True
                )

        async def btn_danger_callback(interaction: discord.Interaction):
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"🟥 Danger Button clicked by {interaction.user.mention}!", ephemeral=True
                )

        view.add_button(
            label="Primary",
            custom_id="btn_primary",
            style=discord.ButtonStyle.primary,
            callback=btn_primary_callback,
        )
        view.add_button(
            label="Success",
            custom_id="btn_success",
            style=discord.ButtonStyle.success,
            callback=btn_success_callback,
        )
        view.add_button(
            label="Danger",
            custom_id="btn_danger",
            style=discord.ButtonStyle.danger,
            callback=btn_danger_callback,
        )
        view.add_button(
            label="GitHub Docs",
            url="https://github.com/lcizzle/err-backend-discord-redux",
            style=discord.ButtonStyle.link,
        )

        reply = self._bot.build_reply(
            msg,
            "Interactive UI Test: Click any button below to verify component responses:",
            view=view,
        )
        self._bot.send_message(reply)
        return None

    @botcmd(admin_only=True)
    def test_dropdown(self, msg, args):
        """
        Renders an interactive dropdown selection menu.
        Usage: !test dropdown
        """
        if not ActionRowView:
            return "❌ UI components are not available."

        view = ActionRowView(timeout=120)

        async def select_callback(interaction: discord.Interaction, values: list):
            chosen = ", ".join(values)
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"🔽 You selected: **{chosen}** ({interaction.user.mention})", ephemeral=True
                )

        options = [
            ("Option 1 (Red)", "opt_red", "Select Red theme"),
            ("Option 2 (Green)", "opt_green", "Select Green theme"),
            ("Option 3 (Blue)", "opt_blue", "Select Blue theme"),
        ]
        view.add_select(
            placeholder="Select a color option...",
            custom_id="select_color",
            options=options,
            callback=select_callback,
        )

        reply = self._bot.build_reply(
            msg, "Interactive Dropdown Test: Select an option from the menu below:", view=view
        )
        self._bot.send_message(reply)
        return None

    @botcmd(admin_only=True)
    def test_modal(self, msg, args):
        """
        Renders a button that opens a Discord Modal popup text form when clicked.
        Usage: !test modal
        """
        if not ActionRowView or not SimpleModal:
            return "❌ UI components or SimpleModal are not available."

        view = ActionRowView(timeout=120)

        async def open_modal_btn(interaction: discord.Interaction):
            modal = SimpleModal(title="Discord Test Modal")
            modal.add_text_input(
                label="Your Name",
                custom_id="name_field",
                placeholder="e.g. John Doe",
                required=True,
            )
            modal.add_text_input(
                label="Feedback / Message",
                custom_id="feedback_field",
                style=discord.TextStyle.paragraph,
                placeholder="Enter feedback...",
                required=False,
            )

            async def modal_submit(modal_inter: discord.Interaction, values: dict):
                user_name = values.get("name_field", "Anonymous")
                feedback = values.get("feedback_field", "No feedback entered")
                if not modal_inter.response.is_done():
                    await modal_inter.response.send_message(
                        f"📝 **Modal Submitted!**\n• User: {modal_inter.user.mention}\n• Name: `{user_name}`\n• Feedback: `{feedback}`",
                        ephemeral=False,
                    )

            modal._on_submit_callback = modal_submit
            if hasattr(self._bot, "send_modal"):
                res = self._bot.send_modal(interaction, modal)
                if inspect.iscoroutine(res) or isinstance(res, asyncio.Future):
                    await res
            else:
                await interaction.response.send_modal(modal)

        view.add_button(
            label="Open Survey Modal",
            custom_id="btn_open_modal",
            style=discord.ButtonStyle.primary,
            callback=open_modal_btn,
        )

        reply = self._bot.build_reply(
            msg, "Modal Test: Click the button below to open a Discord Modal popup form:", view=view
        )
        self._bot.send_message(reply)
        return None

    @botcmd(admin_only=True)
    def test_interactions(self, msg, args):
        """
        Displays recent interaction events captured by callback_interaction.
        Usage: !test interactions
        """
        if not self.interaction_history:
            return "No interactions recorded yet in this session. Click a button or select menu and re-run this command."

        lines = ["**Recent Captured Interaction Events:**"]
        for inter in list(self.interaction_history)[:10]:
            lines.append(
                f"• [{inter['type']}] custom_id: `{inter['custom_id']}` by `{inter['user']}` (`{inter['user_id']}`)"
            )
        return "\n".join(lines)

    @botcmd(admin_only=True)
    def test_deletions(self, msg, args):
        """
        Displays recent messages deleted in Discord captured by the backend.
        Usage: !test deletions
        """
        if not self.deletion_history:
            return "No message deletions recorded yet in this session. Delete a message and re-run this command."

        lines = ["**Recent Captured Message Deletions:**"]
        for d in list(self.deletion_history)[:10]:
            lines.append(
                f"• [{d['type'].upper()}] ID `{d['message_id']}` in Channel `{d['channel_id']}` | Content: `{d.get('content', '')}`"
            )
        return "\n".join(lines)

    @botcmd(admin_only=True)
    def test_threvents(self, msg, args):
        """
        Displays recent thread lifecycle events (create, delete, update).
        Usage: !test threvents
        """
        if not self.thread_history:
            return "No thread lifecycle events recorded yet in this session. Create, lock, or archive a thread and re-run."

        lines = ["**Recent Captured Thread Lifecycle Events:**"]
        for t in list(self.thread_history)[:10]:
            lines.append(f"• [{t['action'].upper()}] Thread `{t['name']}` (ID: `{t['id']}`)")
        return "\n".join(lines)

    @botcmd(admin_only=True)
    def test_query(self, msg, args):
        """
        Tests query_room channel lookup (e.g. for forum or thread channels).
        Usage: !test query <#channel_id>
        """
        target = args.strip()
        if not target:
            return "Usage: `!test query <#channel_id>` or `!test query #channel-name`"

        backend = self._bot
        try:
            room = backend.query_room(target)
            if room:
                return f"✅ Resolved room: `{room.name}` (ID: `{room.id}`, Type: `{type(room).__name__}`)"
            else:
                return f"❌ Could not resolve room for `{target}`."
        except Exception as e:
            return f"❌ Error querying room `{target}`: {e}"

    @botcmd(admin_only=True)
    def test_extras(self, msg, args):
        """
        Inspect all values stored in msg.extras for this message.
        Usage: !test extras
        """
        if not msg.extras:
            return "❌ `msg.extras` is empty or None."

        lines = ["**Message Extras Metadata:**"]
        for k, v in msg.extras.items():
            lines.append(f"• `{k}`: `{v}`")
        return "\n".join(lines)

    @botcmd(admin_only=True)
    def test_thread(self, msg, args):
        """
        Creates a new thread from this command message and posts the reply inside it.
        Usage: !test thread <optional message>
        """
        if msg.is_direct:
            return "❌ Cannot create threads in Direct Messages (DMs). Run this in a guild text channel."

        discord_msg_id = msg.extras.get("discord_message_id") if msg.extras else None
        if not discord_msg_id:
            return "❌ `discord_message_id` is missing from `msg.extras`."

        reply_body = (
            args.strip() or "Hello from a new Discord thread! Phase 1 thread creation verified."
        )
        try:
            self._bot.send_simple_reply(msg, reply_body, threaded=True)
            return None
        except Exception as e:
            log.exception(f"Failed to create thread reply: {e}")
            return f"❌ Failed to create thread: {e}"

    @botcmd(admin_only=True)
    def test_react(self, msg, args):
        """
        Tests bot reacting to your message via add_reaction using the message ID in extras.
        Usage: !test react [emoji]
        """
        emoji = args.strip() or "✅"
        backend = self._bot
        if not hasattr(backend, "add_reaction"):
            return "❌ Backend does not implement `add_reaction`."

        try:
            backend.add_reaction(msg, emoji)
            return f"Added reaction `{emoji}` to message."
        except Exception as e:
            return f"❌ Failed to add reaction: {e}"

    @botcmd(admin_only=True)
    def test_reactions(self, msg, args):
        """
        Displays recent reactions caught by raw reaction gateway handlers.
        Usage: !test reactions
        """
        if not self.reaction_history:
            return "No raw reactions recorded yet in this session. React to any message (old or new) and re-run this command."

        lines = ["**Recent Captured Raw Reactions:**"]
        for rxn in list(self.reaction_history)[:10]:
            lines.append(
                f"• [{rxn['action'].upper()}] `{rxn['emoji']}` by `{rxn['reactor']}` on message `{rxn['message_id']}` in channel `{rxn['channel_id']}`"
            )
        return "\n".join(lines)

    # =========================================================================
    # Phase 4: Native Application Commands (Slash Commands & Context Menus)
    # =========================================================================

    @botcmd(admin_only=True)
    def sync(self, msg, args):
        """
        Manually trigger slash command synchronization with Discord.
        Usage:
          !sync             - Sync commands to the current guild immediately
          !sync <guild_id>  - Sync commands to specified guild immediately
          !sync global      - Sync commands globally (may take up to 1 hr to propagate)
        """
        target = args.strip()
        guild_id = None

        if target.lower() == "global":
            guild_id = None
            scope_desc = "globally (Discord may take up to 1 hour to propagate)"
        elif target:
            try:
                guild_id = int(target)
                scope_desc = f"to guild `{guild_id}`"
            except ValueError:
                return f"❌ Invalid guild ID: `{target}`. Must be an integer or 'global'."
        else:
            # Auto-detect current guild
            if msg.is_direct:
                return "❌ In DMs, please specify guild ID or 'global': `!sync <guild_id>` or `!sync global`"

            # Get guild ID from room
            room = msg.frm.room if hasattr(msg.frm, "room") else None
            guild_id = getattr(room, "guild", None) or getattr(room, "_guild_id", None)
            if not guild_id and hasattr(msg, "extras") and msg.extras:
                guild_id = msg.extras.get("guild_id")
            if not guild_id:
                client = getattr(self._bot, "client", None)
                if client and client.guilds:
                    guild_id = client.guilds[0].id

            if not guild_id:
                return "❌ Could not determine current guild ID. Please specify: `!sync <guild_id>`"
            scope_desc = f"to current guild `{guild_id}`"

        backend = self._bot
        if not hasattr(backend, "sync_slash_commands"):
            return "❌ Backend does not implement `sync_slash_commands`."

        try:
            res = backend.sync_slash_commands(guild_id=guild_id)
            count = len(res) if isinstance(res, (list, tuple)) else "all"
            return f"✅ Slash command synchronization completed {scope_desc} ({count} commands)."
        except Exception as e:
            log.exception(f"Failed to sync slash commands: {e}")
            return f"❌ Failed to sync slash commands: {e}"

    @slash_command(name="test_ping", description="Ping the bot via native slash command")
    async def slash_test_ping(self, interaction: discord.Interaction):
        """Slash command: Ping test."""
        client = getattr(self._bot, "client", None)
        latency_ms = (
            round(client.latency * 1000, 2) if client and hasattr(client, "latency") else "N/A"
        )
        await interaction.response.send_message(
            f"🏓 Pong from native Discord slash command! Gateway latency: `{latency_ms}ms`"
        )

    @slash_command(name="test_echo", description="Echo back a message via native slash command")
    async def slash_test_echo(self, interaction: discord.Interaction, message: str):
        """Slash command: Echo test with arguments."""
        await interaction.response.send_message(f"📢 Echo: {message}")

    @slash_command(
        name="test_ui_slash", description="Test interactive UI components in a slash command"
    )
    async def slash_test_ui(self, interaction: discord.Interaction):
        """Slash command: UI button test."""
        if not ActionRowView or not SimpleButton:
            await interaction.response.send_message(
                "❌ discordlib.ui not available.", ephemeral=True
            )
            return

        view = ActionRowView(timeout=60)

        async def btn1_cb(inter: discord.Interaction):
            await inter.response.send_message("🔘 Clicked Slash Primary Button!", ephemeral=True)

        async def btn2_cb(inter: discord.Interaction):
            await inter.response.send_message("🔘 Clicked Slash Success Button!", ephemeral=True)

        btn1 = SimpleButton(
            label="Slash Primary",
            style=discord.ButtonStyle.primary,
            custom_id="slash_btn_1",
            callback=btn1_cb,
        )
        btn2 = SimpleButton(
            label="Slash Success",
            style=discord.ButtonStyle.success,
            custom_id="slash_btn_2",
            callback=btn2_cb,
        )
        view.add_item(btn1)
        view.add_item(btn2)
        await interaction.response.send_message(
            "Here are interactive buttons dispatched from a slash command:", view=view
        )

    @message_context_menu(name="Quote Message")
    async def ctx_quote_message(self, interaction: discord.Interaction, message: discord.Message):
        """Message Context Menu: Quote selected message."""
        content = message.content or "(Empty message or attachment only)"
        author = message.author.display_name if hasattr(message, "author") else "Unknown"
        await interaction.response.send_message(
            f"💬 **Quoted from {author}**:\n> {content}",
            ephemeral=True,
        )

    @user_context_menu(name="Inspect User")
    async def ctx_inspect_user(self, interaction: discord.Interaction, user: discord.Member):
        """User Context Menu: Inspect selected member."""
        created = (
            user.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
            if hasattr(user, "created_at")
            else "Unknown"
        )
        roles = [r.name for r in getattr(user, "roles", []) if r.name != "@everyone"]
        roles_str = ", ".join(roles) if roles else "None"
        discrim = (
            f"#{user.discriminator}"
            if getattr(user, "discriminator", None) and user.discriminator != "0"
            else ""
        )
        await interaction.response.send_message(
            f"👤 **User Info for {user.display_name}** (`{user.name}{discrim}`):\n"
            f"• **ID**: `{user.id}`\n"
            f"• **Bot**: `{user.bot}`\n"
            f"• **Account Created**: {created}\n"
            f"• **Roles**: {roles_str}",
            ephemeral=True,
        )

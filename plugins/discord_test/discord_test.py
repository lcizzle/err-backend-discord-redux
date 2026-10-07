import logging
from collections import deque
from typing import Deque

from errbot import BotPlugin, botcmd
from errbot.backends.base import Reaction

log = logging.getLogger(__name__)


class DiscordTest(BotPlugin):
    """
    Discord Backend Feature Testing Plugin for administrators.
    Allows testing Phase 1 features:
      - Message Extras & Cache verification
      - Thread creation & threaded replies
      - Gateway Raw Reactions capture & history
      - Bot Reaction execution
    """

    def activate(self):
        super().activate()
        # Ring buffer storing recent reactions observed by callback_reaction
        self.reaction_history: Deque[dict] = deque(maxlen=20)
        log.info("DiscordTest plugin activated.")

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
            reply = self.build_reply(msg, reply_body, threaded=True)
            self.send_message(reply)
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

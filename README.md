# Discord Backend for Errbot (Redux)

A modern, high-performance Discord backend for [Errbot](https://errbot.readthedocs.io/) built on **discord.py 2.7+**.

## Key Features

- **Native Application Commands**: Full integration with `discord.app_commands.CommandTree` supporting native Discord Slash Commands (`/command`), Message Context Menus (`Apps -> Action`), and User Context Menus (`Apps -> Action`).
- **Errbot Command Auto-Bridging**: Automatically bridge prefix `!commands` into Discord `/commands`.
- **Interactive UI Components**: First-class support for Discord UI Buttons, Dropdowns (Select Menus), and Modal popups via `discordlib.ui`.
- **Modern Channel & Thread Architecture**: Native support for Discord Threads, Forum Channels, and Stage Channels with automatic forum post creation.
- **Reliable Event Gateway Delivery**: Raw gateway reaction tracking (`on_raw_reaction_add` / `on_raw_reaction_remove`) and message deletion tracking (`on_raw_message_delete`).
- **Instant Guild Syncing**: Fast guild command synchronization on startup via `guild_sync_id` or on demand via `!sync`.

## Quick Start

### Installation

```bash
pip install err-backend-discord-redux
```

### Configuration (`config.py`)

```python
BACKEND = "Discord"

BOT_IDENTITY = {
    "token": "YOUR_DISCORD_BOT_TOKEN",
    "initial_intents": "default",
    "intents": ["message_content", "guild_reactions"],
    "guild_sync_id": 123456789012345678,     # Optional: instant slash command sync to your server
    "auto_bridge_commands": False,           # Optional: True to auto-bridge !commands to /slash_commands
    "sync_commands": True,                   # Optional: True to auto-sync on startup
}
```

## Plugin Development

### Native Slash Commands & Context Menus

```python
from errbot import BotPlugin
from discordlib.commands import slash_command, message_context_menu
import discord

class MyPlugin(BotPlugin):
    @slash_command(name="ping", description="Check bot latency")
    async def ping_cmd(self, interaction: discord.Interaction):
        await interaction.response.send_message("Pong!")

    @message_context_menu(name="Quote Message")
    async def quote_msg(self, interaction: discord.Interaction, message: discord.Message):
        await interaction.response.send_message(f"Quoted: {message.content}", ephemeral=True)
```

### Interactive UI Components

```python
from errbot import BotPlugin, botcmd
from discordlib.ui import ActionRowView, SimpleButton
import discord

class MyPlugin(BotPlugin):
    @botcmd
    def button_demo(self, msg, args):
        view = ActionRowView()
        view.add_button(
            label="Click Me",
            style=discord.ButtonStyle.primary,
            callback=lambda inter: inter.response.send_message("Button clicked!", ephemeral=True)
        )
        self._bot.send_ui(msg, content="Interactive View:", view=view)
```

## Documentation

Visit the [official documentation](https://err-backend-discord.readthedocs.io/) for detailed guides:
- [Installation](docs/installation.rst)
- [Configuration](docs/configuration.rst)
- [User Guide](docs/user_guide.rst)
- [Developer Guide](docs/developer_guide.rst)

## License

GPL-3.0-only

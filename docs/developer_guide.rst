.. _developer_guide:

Developer Guide
========================================================================

Source Code
------------------------------------------------------------------------

The source code can be found on github in the `err-backend-discord repository <https://github.com/errbotio/err-backend-discord>`_

Person Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Discord identity system doesn't map directly to that of errbots.  The below table attempts to align them as best possible for practical use.


   .. csv-table:: Class attributes
        :header: "Discord ``ClientUser``", "Description", "Errbot ``Person``", "Description"
        :widths: 10, 20, 10, 20

        ``name :str:``, "The user's username.", ``person :str:``, "a backend specific unique identifier representing the person you are talking to."
        ``id :int:``, "The user's unique ID.", ``client :str:``, "a backend specific unique identifier representing the device or client the person is using to talk."
        ``discriminator :str:``, "The user's discriminator. This is given when the username has conflicts."
        ``bot :bool:``, "Specifies if the user is a bot account.",         ``nick :str:``, "a backend specific nick returning the nickname of this person if available."
        ``system :bool:``, "Specifies if the user is a system user (i.e. represents Discord officially).", ``aclattr :str:``, "returns the unique identifier that will be used for ACL matches."
        ``??``, "??", ``fullname :str:``, "the fullname of this user if available."
        ``??``, "??", ``email :str:``, "the email of this user if available."


Room Occupant Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

   .. csv-table:: Class attributes
        :header: "Discord ``??``", "Description", "Errbot ``RoomOccupant``", "Description"
        :widths: 10, 20, 10, 20

        ``??``, "??", ``room :any:``, "the fullname of this user if available."


Room Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

   .. csv-table:: Class attributes
        :header: "Discord ``??``", "Description", "Errbot ``Room``", "Description"
        :widths: 10, 20, 10, 20

        ``??``, "??", ``join()``, "If the room does not exist yet, this will automatically call `create` on it first."
        ``??``, "??", ``leave()``, "Leave the room."
        ``??``, "??", ``create()``, "Create the room or do nothing if it already exists."
        ``??``, "??", ``destroy()``, "Destroy the room or do nothing if it doesn't exists."
        ``??``, "??", ``aclattr :str:``, "returns the unique identifier that will be used for ACL matches."
        ``??``, "??", ``exists :bool:``, "Returns ``True`` if the room exists, `False` otherwise."
        ``??``, "??", ``joined :bool:``, "Returns ``True`` if the room has been joined, `False` otherwise."
        ``??``, "??", ``topic :bool:``, "Returns the topic (a string) if one is set, ``None`` if no topic has been set at all."
        ``??``, "??", ``occupants :list:``, "Returns a list of occupant identities."
        ``??``, "??", ``invite()``, "Invite one or more people into the room."


Identification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Full identification requires to following fields:
::

        "guild_id": "012345678901234567"
        "channel_id": "123456789012345678",
        "author": {
            "username": "bumblebee",
            "public_flags": 0,
            "id": "234567890123456789",
            "discriminator": "0413",
            "bot": true,
            "avatar_decoration": null,
            "avatar": "5dba11479834e662c5e6a71807a3b9c3"
        },

The following examples show how errbot receives message events for identifier resolution

user mention text
::

    frm = username#1234@playground
    username = username#1234@playground
    text = .whoami <@123456789012345678>

channel mention text
::

    frm = username#1234@playground
    username = username#1234@playground
    text = .whoami <#123456789012345678>

raw text
::

    frm = username#1234@channel_name
    username = username#1234@channel_name
    text = .whoami username#1234@channel_name

guild mentions don't exists but could be represented with a string like ``<$123456789012345678>`` which would produce the following text identification representation to be resolved.
::

    #channel_name$guild_vanity_url_code
    #channel_name
    @username#1234


Modern Discord Features & Plugin Development
------------------------------------------------------------------------

The Redux backend provides modern Discord features (discord.py 2.7+) directly to Errbot plugins.

Interactive UI Components (Buttons, Selects, Modals)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Import UI components directly from ``discordlib.ui``:

::

    from discordlib.ui import ActionRowView, SimpleButton, SimpleSelect, SimpleModal
    import discord

**1. Sending Interactive Buttons:**

::

    class MyPlugin(BotPlugin):
        @botcmd
        def interactive(self, msg, args):
            view = ActionRowView(timeout=120)

            async def on_confirm(interaction):
                await interaction.response.send_message("Confirmed!", ephemeral=True)

            btn = SimpleButton(
                label="Confirm",
                style=discord.ButtonStyle.success,
                custom_id="btn_confirm",
                callback=on_confirm
            )
            view.add_item(btn)

            # Send via backend helper or as message extras
            self._bot.send_ui(msg, content="Please choose an option:", view=view)

**2. Select Dropdowns:**

::

    options = [
        discord.SelectOption(label="Red", value="red", description="Red color"),
        discord.SelectOption(label="Blue", value="blue", description="Blue color"),
    ]
    select = SimpleSelect(
        placeholder="Choose a color...",
        options=options,
        callback=lambda inter: inter.response.send_message(f"Selected: {inter.data['values'][0]}")
    )
    view = ActionRowView().add_item(select)
    self._bot.send_ui(msg, view=view)

**3. Modal Dialogs:**

Modals must be opened in response to a Discord interaction:

::

    modal = SimpleModal(title="Feedback Form")
    modal.add_short_input(custom_id="name", label="Your Name", required=True)
    modal.add_paragraph_input(custom_id="feedback", label="Feedback", max_length=500)

    async def on_submit(interaction, values):
        await interaction.response.send_message(f"Thanks {values['name']}! Received: {values['feedback']}", ephemeral=True)

    modal.set_on_submit(on_submit)
    self._bot.send_modal(interaction, modal)

**4. Handling Component Interactions in Plugins:**

Plugins can also observe all incoming interactions by implementing ``callback_interaction``:

::

    def callback_interaction(self, interaction: discord.Interaction):
        custom_id = interaction.data.get("custom_id")
        self.log.info(f"Observed interaction {custom_id} by {interaction.user}")


Native Application Commands (Slash Commands & Context Menus)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Declare native application commands using decorators from ``discordlib.commands``:

::

    from discordlib.commands import slash_command, message_context_menu, user_context_menu

**1. Slash Commands (``/command``):**

::

    class MyPlugin(BotPlugin):
        @slash_command(name="ping", description="Check bot latency")
        async def ping_cmd(self, interaction: discord.Interaction):
            await interaction.response.send_message("Pong!")

        @slash_command(name="echo", description="Echo back text")
        async def echo_cmd(self, interaction: discord.Interaction, message: str):
            await interaction.response.send_message(f"Echo: {message}")

**2. Message & User Context Menus (Right-Click -> Apps):**

::

    @message_context_menu(name="Quote Message")
    async def quote_msg(self, interaction: discord.Interaction, message: discord.Message):
        await interaction.response.send_message(f"Quoted: {message.content}", ephemeral=True)

    @user_context_menu(name="Inspect Member")
    async def inspect_user(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.send_message(f"User ID: {user.id}, Joined: {user.joined_at}", ephemeral=True)

**3. Synchronizing Commands:**

- **Automatic Startup Sync:** Set ``guild_sync_id`` in ``BOT_IDENTITY`` to sync immediately on startup.
- **On-Demand Command:** Administrators can run ``!sync`` (or ``!sync <guild_id>``) at any time.
- **Programmatic Sync:** Call ``self._bot.sync_slash_commands(guild_id=...)``.


Channel & Thread Lifecycle Callbacks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Plugins can hook into Discord events:

::

    class LifecyclePlugin(BotPlugin):
        def callback_message_deleted(self, msg):
            """Fired when a cached message is deleted."""
            self.log.info(f"Message deleted: {msg.body}")

        def callback_raw_message_deleted(self, msg):
            """Fired when any message is deleted (cached or uncached)."""
            self.log.info(f"Raw message deleted ID: {msg.extras.get('discord_message_id')}")

        def callback_thread_created(self, room):
            self.log.info(f"Thread created: {room.name} ({room.id})")

        def callback_thread_deleted(self, room):
            self.log.info(f"Thread deleted: {room.name} ({room.id})")

        def callback_thread_updated(self, room, before, after):
            self.log.info(f"Thread updated: {room.name} (archived={after.archived})")

        def callback_reaction(self, reaction):
            """Fired on raw reaction additions and removals across all messages."""
            self.log.info(f"Reaction {reaction.reaction_name} by {reaction.reactor}")


Contributing
------------------------------------------------------------------------

The process for contributing to the discord backend follows the usual github process as described below:

1. Fork the github project to your github account.
2. Clone the forked repository to your development machine.
3. Create a branch for changes in your locally cloned repository.
4. Develop feature/fix/change in your branch.
5. Push work from your branch to your forked repository
6. Open pull request from your forked repository to the official err-backend-discord repository.

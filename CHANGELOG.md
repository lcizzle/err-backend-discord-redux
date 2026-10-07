# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [4.1.0] - 2026-10-08

### Added
- **Native Application Commands & Context Menus (Phase 4)**:
  - Integrated `discord.app_commands.CommandTree` with `DiscordBackend`.
  - Added decorators `@slash_command`, `@message_context_menu`, and `@user_context_menu` in `discordlib.commands`.
  - Added automatic command registration and unregistration during plugin lifecycle.
  - Added automatic command bridging (`auto_bridge_commands`) mapping active Errbot `@botcmd` methods into Discord `/commands`.
  - Added instant guild synchronization via `guild_sync_id` in `BOT_IDENTITY` and `backend.sync_slash_commands(guild_id)`.
  - Added `!sync [guild_id|'global']` administrative Errbot command.
  - Added interaction response and followup routing in `send_message()`.
- **Interactive UI Components (Phase 3)**:
  - Added `discordlib.ui` module featuring `ActionRowView`, `SimpleButton`, `SimpleSelect`, and `SimpleModal`.
  - Added backend methods `send_ui(recipient, content, embed, view, ephemeral)` and `send_modal(interaction, modal)`.
  - Added `callback_interaction(interaction)` plugin hook for observing component interactions.
  - Added fallback dispatch system for UI views and modal callbacks.
- **Message & Channel Lifecycle Parity (Phase 2)**:
  - Added `on_message_delete` and `on_raw_message_delete` handlers dispatching `callback_message_deleted` and `callback_raw_message_deleted`.
  - Added `on_thread_create`, `on_thread_delete`, and `on_thread_update` handlers dispatching `callback_thread_created`, `callback_thread_deleted`, and `callback_thread_updated`.
  - Added support for `discord.Thread`, `discord.ForumChannel`, and `discord.StageChannel` in `DiscordRoom` and `query_room()`.
  - Added automatic forum thread creation when sending messages to Discord forum channels.
- **Reliable Raw Gateway Reactions & Thread Creation (Phase 1)**:
  - Migrated reaction processing to raw gateway events (`on_raw_reaction_add` and `on_raw_reaction_remove`) to reliably capture reactions on historical and uncached messages.
  - Implemented `_create_thread_from_message` with channel API fetching and LRU message caching.
  - Added LRU cache for recent Discord message objects (`_recent_discord_messages`).

### Changed
- Bumped `discord.py` dependency requirement to `>=2.7.1,<3.0.0`.
- Expanded unit test coverage with dedicated test suites (`tests/test_slash.py`, `tests/test_ui.py`) totaling 70 comprehensive tests.

## [4.0.2]

### Changed
  - Bumped discord.py to version 2.4.0
  - Migrated from depreciated logs_from to history
  - Added some network error handling
  - Added basic rate handling
  - Added basic reaction support
  - Added basic thread support
  - Added basic message editing support
  - Added more room functionality
  - Fixed blocking issue that caused some disconnects
  - Added discord presence support.
  - Reworked some of the added features like reactions, message threads, message editing, presence support, send_card, to have only the minimal functionality in the backend and then a broken out more through implementation as a plugin. Not 100% sure what to do with this yet. Trying to keep the backend light. Maybe start another repo or just leave this part of the implementation up to the plugin developer / end user.

## [4.0.1] Unreleased

## [4.0.1] 2024-03-25

### Added

### Changed
  - Fixed variable name error in Person initilisation when using username and discriminator.
  - Updated String ID length to accept 18 or more digits.

### Removed

## [4.0.0] 2022-11-10

### Added
  - Added upgrade notes section to installation documentation.
  - Added intents management.

### Changed
  - Fixed copy/paste error in documentation.
  - Use the v2.0.1 discord python module.

### Removed
  - Support for python3.7 has been removed to allow the use of the v2.0.1 discord python module.

## [3.0.1] 2022-10-19

### Changed
  - Version bump for pypi release.


## [3.0.0] 2022-10-19

### Added

### Changed
  - Restructured code base to support packaging for pypi.
  - Migrated README documentation to readthedocs format.

### Removed


## [2.1.0] 2021-09-16

### Added
  - Support `#channel@guild_id` representation.

### Changed
  - Use discord client 1.7.3
  - Updated file_upload to support discord client v1.7.3.


## [2.0.0] 2021-01-14

### Changed
  - Use discord client 1.6.0
  - Discord client uses Member intents.

## [1.0.1] 2019-11-23
### Changed
  - Use discord client 1.2.5


## [1.0.0] 2019-10-18

### Added
  - Added changelog file.

### Changed
  - Use discord client 1.2.4

### Removed

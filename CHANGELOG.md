# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

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

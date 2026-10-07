import logging

import pytest
from discordlib.person import DiscordPerson
from mock import MagicMock

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def setup_discord_client():
    mock_client = MagicMock()
    mock_user = MagicMock()
    mock_user.id = 123456789012345678
    mock_user.name = "someone"
    mock_user.discriminator = "0"
    mock_client.get_user.return_value = mock_user
    mock_client.get_all_members.return_value = [mock_user]

    original_client = DiscordPerson.client
    DiscordPerson.client = mock_client
    yield mock_client
    DiscordPerson.client = original_client


def test_wrong_userid():
    with pytest.raises(ValueError, match="Invalid Discord user id"):
        DiscordPerson(user_id="invalid_id")


def test_create_person_without_args():
    with pytest.raises(ValueError, match="Username/discrimator pair or user id not provided."):
        DiscordPerson()


def test_create_person_with_username_only():
    person = DiscordPerson(username="someone")
    assert person.username == "someone"


def test_create_person_with_discriminator_only():
    with pytest.raises(ValueError, match="Username/discrimator pair or user id not provided."):
        DiscordPerson(discriminator="#1234")


def test_create_person_with_id():
    person = DiscordPerson(user_id="0123456789012345678")
    assert person.id == 123456789012345678


def test_create_person_username_and_discriminator(setup_discord_client):
    mock_user = setup_discord_client.get_user.return_value
    mock_user.discriminator = "1234"
    person = DiscordPerson(username="someone", discriminator="1234")
    assert person.id == 123456789012345678
    assert person.fullname == "someone#1234"


def test_username_not_found(setup_discord_client):
    setup_discord_client.get_all_members.return_value = []
    with pytest.raises(LookupError, match="The user nonexistent#1234 can't be found."):
        DiscordPerson(username="nonexistent", discriminator="1234")


def test_user_not_found_by_id(setup_discord_client):
    setup_discord_client.get_user.return_value = None
    with pytest.raises(ValueError, match="Failed to get the user"):
        DiscordPerson(user_id="123456789012345678")


def test_person_properties():
    person = DiscordPerson(user_id="0123456789012345678")
    assert person.email == "Unavailable"
    assert person.aclattr == "someone#0"
    assert str(person) == "someone#0"
    assert person.nick == "someone"


def test_person_equality():
    p1 = DiscordPerson(user_id="0123456789012345678")
    p2 = DiscordPerson(user_id="0123456789012345678")
    assert p1 == p2

"""
Anonymization utilities

To allow displaying of JHE outputs in documentation

Anonymizes patient data and adds noise to Observations
"""

from __future__ import annotations

import base64
import copy
import json
import random
from datetime import date
from typing import Any

from faker import Faker

_faker = Faker()


_jhe_id_cache: dict[str, int] = {}
_reverse_id_cache: dict[int, str] = {}


def deanonymize_id(id: int) -> int:
    """Reverse anonymous id lookup

    So we can lookup by anonymized patient id
    """
    id_key = _reverse_id_cache.get(id, None)
    if id_key is None:
        return id
    kind, _, real_id = id_key.partition("/")
    return int(real_id)


def _random_id(id: int, kind: str, start: int) -> int:
    """Get a random id

    cache for stability and reverse lookup
    """
    id_key = f"{kind}/{id}"
    if id_key not in _jhe_id_cache:
        new_id = random.randint(start, start + 10_000)
        while new_id in _reverse_id_cache:
            new_id = random.randint(start, start + 10_000)
        _jhe_id_cache[id_key] = new_id
        _reverse_id_cache[new_id] = id_key
    return _jhe_id_cache[id_key]


def _random_uuid():
    """return a random uuid"""
    _random_uuid.n += 1
    return f"u-u-i-d-{_random_uuid.n}"


_random_uuid.n = 0


def anonymize_user(user: dict[str, Any]) -> dict[str, Any]:
    """Anonymize a user record"""
    new_user = {}
    new_user["firstName"] = _faker.first_name()
    new_user["lastName"] = _faker.last_name()
    new_user["email"] = (
        f"{new_user['firstName']}.{new_user['lastName']}@example.edu".lower()
    )
    new_user["patient"] = None
    new_user["id"] = _random_id(user["id"], "User", 10_000)
    return new_user


def anonymize_patient(patient: dict) -> dict:
    """Anonymize a JHE Patient record

    Rewrites:

    - id
    - jheUserId
    - nameGiven
    - nameFamily
    - birthDate
    - telecomPhone
    - telecomEmail
    - identifier

    Assumes JHE record id is not considered identifiable
    (i.e. the JHE deployment itself is for demonstration purposes)
    """
    new_patient = {
        "organizationId": patient["organizationId"],
    }
    new_patient["nameGiven"] = _faker.first_name()
    new_patient["nameFamily"] = _faker.last_name()
    if patient["telecomPhone"]:
        new_patient["telecomPhone"] = "(510) 555-1234"
    if patient["telecomEmail"]:
        new_patient["telecomEmail"] = (
            f"{new_patient['nameGiven']}.{new_patient['nameFamily']}@example.edu".lower()
        )
    new_patient["birthDate"] = _faker.date_between(
        date(1950, 1, 1), date(2005, 1, 1)
    ).strftime("%Y-%m-%d")
    if patient["identifier"]:
        new_patient["identifier"] = _random_uuid()

    new_patient["id"] = _random_id(patient["id"], "Patient", start=40_000)
    new_patient["jheUserId"] = _random_id(patient["jheUserId"], "User", start=10_000)
    return new_patient


def _scale_value(value: float | int, noise_scale: float) -> float | int:
    """add noise to a single value"""
    # truncate precision to 10s place
    round_value = round(value, -1)
    # add scaled noise to values
    noise = round_value * (random.random() - 0.5) * noise_scale
    new_value = round_value + noise
    # cast back to int if input is int
    return type(value)(new_value)


def anonymize_observation(observation: dict, noise_scale: float = 0.2) -> dict:
    """Anonymize a single observation"""
    observation = copy.deepcopy(observation)
    # anonymize ids
    observation["id"] = _random_id(observation["id"], "Observation", start=60_000)
    for identifier in observation["identifier"]:
        identifier["value"] = _random_uuid()
    reference = observation["subject"]["reference"]
    kind, _, id = reference.partition("/")
    id = int(id)
    new_id = _random_id(id, kind, start=40_000)
    observation["subject"]["reference"] = f"{kind}/{new_id}"

    # anonymize valueAttachment
    value_data_b64: bytes = observation["valueAttachment"]["data"].encode()
    value_data = json.loads(base64.decodebytes(value_data_b64).decode())
    for field, content in value_data["body"].items():
        if "value" in content:
            original_value = content["value"]
            new_value = _scale_value(original_value, noise_scale)
            if isinstance(original_value, int):
                new_value = int(new_value)
            content["value"] = new_value
    for field in ("uuid", "source_data_point_id"):
        if field in value_data["header"]:
            value_data["header"][field] = _random_uuid()

    observation["valueAttachment"]["data"] = base64.encodebytes(
        json.dumps(value_data).encode()
    ).decode()
    return observation

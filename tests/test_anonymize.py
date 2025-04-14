import copy
from fnmatch import fnmatch

from jupyterhealth_client._anonymize import (
    anonymize_observation,
    anonymize_patient,
    anonymize_user,
    deanonymize_id,
)
from jupyterhealth_client._utils import tidy_observation

from . import testdata


def test_anonymize_user():
    user = testdata.user
    user_unmodified = copy.deepcopy(user)
    anon_user = anonymize_user(user)
    assert user == user_unmodified
    for key in user:
        if key in {}:
            assert anon_user[key] == user[key], key
        else:
            assert anon_user[key] != user[key], key
    assert anon_user["patient"] is None
    anon_2 = anonymize_user(user)
    assert anon_2["id"] == anon_user["id"]


def test_anonymize_patient():
    patient = testdata.patient
    patient_unmodified = copy.deepcopy(patient)
    anon_patient = anonymize_patient(patient)
    assert patient == patient_unmodified
    for key in patient:
        if key in {"organizationId"}:
            assert anon_patient[key] == patient[key], key
        else:
            assert anon_patient[key] != patient[key], key
    assert deanonymize_id(anon_patient["id"]) == patient["id"]
    anon_2 = anonymize_patient(patient)
    assert anon_2["id"] == anon_patient["id"]
    assert anon_2["jheUserId"] == anon_patient["jheUserId"]


def test_anonymize_observation():
    observation = testdata.bp_record
    unmodified = copy.deepcopy(observation)
    anon = anonymize_observation(observation)
    assert observation == unmodified
    tidy_obs = tidy_observation(observation)
    tidy_anon = tidy_observation(anon)
    assert sorted(tidy_obs) == sorted(tidy_anon)
    for key in tidy_obs:
        if any(
            fnmatch(key, pat)
            for pat in [
                "resource_type",
                "code",
                "resourceType",
                "identifier_0_system",
                "meta_lastUpdated",
                "status",
                "code*",
                "code_coding_0_system",
                "modality",
                "schema_*",
                "*date_time*",
                "external_datasheets_*",
                "*_unit",
            ]
        ):
            assert tidy_anon[key] == tidy_obs[key], key
        else:
            assert tidy_anon[key] != tidy_obs[key], key

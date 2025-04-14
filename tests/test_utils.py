import pytest
from pandas import Timestamp

from jupyterhealth_client._utils import flatten_dict, tidy_observation

from .testdata import bp_record, glucose_record

tidy_glucose_record = {
    "resource_type": "omh:blood-glucose:4.0",
    "code": "omh:blood-glucose:4.0",
    "resourceType": "Observation",
    "id": "54321",
    "identifier_0_value": "abc-123",
    "identifier_0_system": "https://commonhealth.org",
    "meta_lastUpdated": Timestamp("2024-10-09 22:10:55.193492+0000", tz="UTC"),
    "status": "final",
    "subject_reference": "Patient/12345",
    "code_coding_0_code": "omh:blood-glucose:4.0",
    "code_coding_0_system": "https://w3id.org/openmhealth",
    "uuid": "0bc92595-04d9-4597-833c-442bdc05ace5",
    "modality": "self-reported",
    "schema_id_name": "blood-glucose",
    "schema_id_version": "3.1",
    "schema_id_namespace": "omh",
    "creation_date_time": Timestamp("2025-03-12 15:40:10.100000+0000", tz="UTC"),
    "external_datasheets_0_datasheet_type": "manufacturer",
    "external_datasheets_0_datasheet_reference": "Health Connect",
    "source_data_point_id": "8f176f11-bd6c-4041-a44c-e224720059b4",
    "source_creation_date_time": Timestamp("2025-02-15 12:20:00+0000", tz="UTC"),
    "blood_glucose_unit": "MGDL",
    "blood_glucose_value": 65,
    "effective_time_frame_date_time": Timestamp("2025-02-10 12:20:00+0000", tz="UTC"),
    "temporal_relationship_to_meal": "fasting",
    "creation_date_time_local": Timestamp("2025-03-12 15:40:10.100000"),
    "source_creation_date_time_local": Timestamp("2025-02-15 12:20:00"),
    "effective_time_frame_date_time_local": Timestamp("2025-02-10 12:20:00"),
}

tidy_bp_record = {
    "resource_type": "omh:blood-pressure:4.0",
    "code": "omh:blood-pressure:4.0",
    "resourceType": "Observation",
    "id": "54322",
    "identifier_0_value": "abc-1234",
    "identifier_0_system": "https://commonhealth.org",
    "meta_lastUpdated": Timestamp("2024-10-09 17:04:36.617988+0000", tz="UTC"),
    "status": "final",
    "subject_reference": "Patient/12345",
    "code_coding_0_code": "omh:blood-pressure:4.0",
    "code_coding_0_system": "https://w3id.org/openmhealth",
    "uuid": "ca29b9bd-a1ba-4157-8770-dad441df26e9",
    "modality": "sensed",
    "schema_id_name": "blood-pressure",
    "schema_id_version": "3.1",
    "schema_id_namespace": "omh",
    "creation_date_time": Timestamp("2024-04-10 09:36:00+0000", tz="UTC"),
    "external_datasheets_0_datasheet_type": "manufacturer",
    "external_datasheets_0_datasheet_reference": "https://ihealthlabs.com/products",
    "source_data_point_id": "0b16ba05-1267-46cf-9d4a-9248c6958758",
    "source_creation_date_time": Timestamp("2024-04-10 09:36:00+0000", tz="UTC"),
    "effective_time_frame_date_time": Timestamp("2024-04-10 09:36:00+0000", tz="UTC"),
    "systolic_blood_pressure_unit": "mmHg",
    "systolic_blood_pressure_value": 120,
    "diastolic_blood_pressure_unit": "mmHg",
    "diastolic_blood_pressure_value": 80,
    "creation_date_time_local": Timestamp("2024-04-10 08:36:00"),
    "source_creation_date_time_local": Timestamp("2024-04-10 08:36:00"),
    "effective_time_frame_date_time_local": Timestamp("2024-04-10 08:36:00"),
}


@pytest.mark.parametrize(
    "in_d, expected",
    [
        pytest.param({}, {}, id="empty"),
        pytest.param({"a": 5}, {"a": 5}, id="simple"),
        pytest.param({"a": ["x", "y"]}, {"a_0": "x", "a_1": "y"}, id="list"),
        pytest.param({"a": {"x": 5}, "b": 10}, {"a_x": 5, "b": 10}, id="nest"),
    ],
)
def test_flatten_dict(in_d, expected):
    assert flatten_dict(in_d) == expected


@pytest.mark.parametrize(
    "observation, tidy",
    [
        pytest.param(bp_record, tidy_bp_record, id="bp"),
        pytest.param(glucose_record, tidy_glucose_record, id="glucose"),
    ],
)
def test_tidy_observation(observation, tidy):
    print(tidy_observation(observation))
    assert tidy_observation(observation) == tidy

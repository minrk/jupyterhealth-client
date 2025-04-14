import base64
import json

# synthetic records matching real record structure

glucose_attachment = {
    "body": {
        "blood_glucose": {"unit": "MGDL", "value": 65},
        "effective_time_frame": {"date_time": "2025-02-10T12:20:00.000Z"},
        "temporal_relationship_to_meal": "fasting",
    },
    "header": {
        "uuid": "0bc92595-04d9-4597-833c-442bdc05ace5",
        "modality": "self-reported",
        "schema_id": {"name": "blood-glucose", "version": "3.1", "namespace": "omh"},
        "creation_date_time": "2025-03-12T15:40:10.100Z",
        "external_datasheets": [
            {"datasheet_type": "manufacturer", "datasheet_reference": "Health Connect"}
        ],
        "source_data_point_id": "8f176f11-bd6c-4041-a44c-e224720059b4",
        "source_creation_date_time": "2025-02-15T12:20:00.000Z",
    },
}

bp_attachment = {
    "body": {
        "effective_time_frame": {"date_time": "2024-04-10T08:36:00-01:00"},
        "systolic_blood_pressure": {"unit": "mmHg", "value": 120},
        "diastolic_blood_pressure": {"unit": "mmHg", "value": 80},
    },
    "header": {
        "uuid": "ca29b9bd-a1ba-4157-8770-dad441df26e9",
        "modality": "sensed",
        "schema_id": {"name": "blood-pressure", "version": "3.1", "namespace": "omh"},
        "creation_date_time": "2024-04-10T08:36:00-01:00",
        "external_datasheets": [
            {
                "datasheet_type": "manufacturer",
                "datasheet_reference": "https://ihealthlabs.com/products",
            }
        ],
        "source_data_point_id": "0b16ba05-1267-46cf-9d4a-9248c6958758",
        "source_creation_date_time": "2024-04-10T08:36:00-01:00",
    },
}

glucose_record = {
    "resourceType": "Observation",
    "id": "54321",
    "identifier": [
        {
            "value": "abc-123",
            "system": "https://commonhealth.org",
        }
    ],
    "meta": {"lastUpdated": "2024-10-09T22:10:55.193492+00:00"},
    "status": "final",
    "subject": {"reference": "Patient/12345"},
    "code": {
        "coding": [
            {"code": "omh:blood-glucose:4.0", "system": "https://w3id.org/openmhealth"}
        ]
    },
    "valueAttachment": {
        "data": base64.encodebytes(json.dumps(glucose_attachment).encode()).decode(),
        "contentType": "application/json",
    },
}

bp_record = {
    "resourceType": "Observation",
    "id": "54322",
    "identifier": [
        {
            "value": "abc-1234",
            "system": "https://commonhealth.org",
        }
    ],
    "meta": {"lastUpdated": "2024-10-09T17:04:36.617988+00:00"},
    "status": "final",
    "subject": {"reference": "Patient/12345"},
    "code": {
        "coding": [
            {"code": "omh:blood-pressure:4.0", "system": "https://w3id.org/openmhealth"}
        ]
    },
    "valueAttachment": {
        "data": base64.encodebytes(json.dumps(bp_attachment).encode()).decode(),
        "contentType": "application/json",
    },
}

user = {
    "id": 10001,
    "email": "user@real.example.org",
    "firstName": "User",
    "lastName": "Name",
    "patient": 40001,
}

patient = {
    "id": 45439,
    "jheUserId": 19259,
    "identifier": "some-external-id",
    "nameFamily": "Williams",
    "nameGiven": "Heather",
    "birthDate": "1967-12-09",
    "telecomPhone": "(801) 555-1233",
    "telecomEmail": "heather.williams@example.edu",
    "organizationId": 20026,
}

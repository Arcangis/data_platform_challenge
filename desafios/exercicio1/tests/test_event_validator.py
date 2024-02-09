import os

import pytest
from unittest.mock import MagicMock

from exercicio1.event_validator import Validation

PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_FILENAME = "schema.json"

@pytest.fixture(scope="class")
def get_schema():
    yield {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "http://example.com/example.json",
        "type": "object",
        "title": "The root schema",
        "description": "The root schema comprises the entire JSON document.",
        "required": [
            "eid",
            "documentNumber",
            "name",
            "age",
            "address"
        ],
        "properties": {
            "eid": {
                "$id": "#/properties/eid",
                "type": "string",
                "title": "The eid schema",
                "description": "An explanation about the purpose of this instance.",
                "examples": [
                    "3e628a05-7a4a-4bf3-8770-084c11601a12"
                ]
            },
            "documentNumber": {
                "$id": "#/properties/documentNumber",
                "type": "string",
                "title": "The documentNumber schema",
                "description": "An explanation about the purpose of this instance.",
                "examples": [
                    "42323235600"
                ]
            },
            "name": {
                "$id": "#/properties/name",
                "type": "string",
                "title": "The name schema",
                "description": "An explanation about the purpose of this instance.",
                "examples": [
                    "Joseph"
                ]
            },
            "age": {
                "$id": "#/properties/age",
                "type": "integer",
                "title": "The age schema",
                "description": "An explanation about the purpose of this instance.",
                "examples": [
                    32
                ]
            },
            "address": {
                "$id": "#/properties/address",
                "type": "object",
                "title": "The address schema",
                "description": "An explanation about the purpose of this instance.",
                "required": [
                    "street",
                    "number",
                    "mailAddress"
                ],
                "properties": {
                    "street": {
                        "$id": "#/properties/address/properties/street",
                        "type": "string",
                        "title": "The street schema",
                        "description": "An explanation about the purpose of this instance.",
                        "examples": [
                            "St. Blue"
                        ]
                    },
                    "number": {
                        "$id": "#/properties/address/properties/number",
                        "type": "integer",
                        "title": "The number schema",
                        "description": "An explanation about the purpose of this instance.",
                        "examples": [
                            3
                        ]
                    },
                    "mailAddress": {
                        "$id": "#/properties/address/properties/mailAddress",
                        "type": "boolean",
                        "title": "The mailAddress schema",
                        "description": "An explanation about the purpose of this instance.",
                        "examples": [
                            True
                        ]
                    }
                }
            }
        }
    }

@pytest.fixture(scope="class")
def create_logger():
    yield MagicMock()

@pytest.fixture
def create_validation(event: dict, create_logger):
    yield Validation(event, create_logger)

@pytest.mark.validation
class TestValidation:

    @pytest.mark.parametrize("event", [{}])
    def test_validate_event_not_empty(self, create_validation):
        assert not create_validation.validate_event_not_empty()
    
    @pytest.mark.parametrize("event", ["24"])
    def test_validate_event_data_structure(self, create_validation):
        assert not create_validation.validate_event_data_structure()
    
    @pytest.mark.parametrize("event", [{
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": 32,
        }
    ])
    def test_compare_event_fields_missing_required_fields(self, create_validation, get_schema):
        assert not create_validation.compare_event_fields(get_schema)
    
    @pytest.mark.parametrize("event", [{
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": 32,
        "address": {
            "mailAddress": True
            }
        }
    ])
    def test_compare_event_fields_missing_required_nested_fields(self, create_validation, get_schema):
        assert not create_validation.validate_event_content(get_schema)
    

    @pytest.mark.parametrize("event", [{
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentType": "CPF",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": 32,
        "address": {
            "street": "St. Blue",
            "number": 3,
            "mailAddress": True
            }
        }
    ])
    def test_compare_event_fields_unregistered_fields(self, create_validation, get_schema):
        assert not create_validation.compare_event_fields(get_schema)
    
    @pytest.mark.parametrize("event", [{
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": 32,
        "address": {
            "street": "St. Blue",
            "number": 3,
            "complement": "Fundos",
            "mailAddress": True
            }
        }
    ])
    def test_compare_event_fields_unregistered_nested_fields(self, create_validation, get_schema):
        assert not create_validation.validate_event_content(get_schema)


    @pytest.mark.parametrize("event", [{
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": "32",
        "address": {
            "street": "St. Blue",
            "number": 3,
            "mailAddress": True
            }
        }
    ])
    def test_validate_event_content_field_type(self, create_validation, get_schema):

        assert not create_validation.validate_event_content(get_schema)
    
    @pytest.mark.parametrize("event", [{
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": 32,
        "address": {
            "street": "St. Blue",
            "number": "3",
            "mailAddress": True
            }
        }
    ])
    def test_validate_event_content_nested_field_type(self, create_validation, get_schema):

        assert not create_validation.validate_event_content(get_schema)

    @pytest.mark.parametrize("event", [{
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": 32,
        "address": {
            "street": "St. Blue",
            "number": 3,
            "mailAddress": True
            }
        }
    ])
    def test_validate_event_content_sucessful(self, create_validation, get_schema):
        assert create_validation.validate_event_content(get_schema)

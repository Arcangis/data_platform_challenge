import os

import pytest
from unittest.mock import MagicMock

from exercicio2.json_schema_to_hive import Query

PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_FILENAME = "schema.json"

@pytest.fixture(scope="class")
def get_schema():
    yield {
        {
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
}

@pytest.fixture(scope="class")
def create_logger():
    yield MagicMock()

@pytest.fixture(scope="class")
def create_query(create_logger):
    yield Query("tb_user", "db_people", create_logger)

@pytest.mark.query
class TestQuery:

    def format_string(self, input_str):
        return ' '.join(input_str.strip().split())

    def test_create_table_empty(self, create_query):
        query = "CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user"
        assert query == create_query.create_table()
    
    @pytest.mark.parametrize("params", [({
        'eid': {'type': 'string', 'description': 'An explanation about the purpose of this instance.'}, 'documentNumber': {'type': 'string', 'description': 'An explanation about the purpose of this instance.'}, 'name': {'type': 'string', 'description': 'An explanation about the purpose of this instance.'}, 'age': {'type': 'integer', 'description': 'An explanation about the purpose of this instance.'}, 'address': {'type': 'object', 'description': 'An explanation about the purpose of this instance.', 'properties': {'street': {'type': 'string', 'description': 'An explanation about the purpose of this instance.'}, 'number': {'type': 'integer', 'description': 'An explanation about the purpose of this instance.'}, 'mailAddress': {'type': 'boolean', 'description': 'An explanation about the purpose of this instance.'}}}}
    )])
    def test_create_table_with_cols(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        (eid varchar COMMENT 'An explanation about the purpose of this instance.',
        documentNumber varchar COMMENT 'An explanation about the purpose of this instance.',
        name varchar COMMENT 'An explanation about the purpose of this instance.',
        age tinyint COMMENT 'An explanation about the purpose of this instance.',
        address struct <street: varchar COMMENT 'An explanation about the purpose of this instance.',
        number: tinyint COMMENT 'An explanation about the purpose of this instance.',
        mailAddress: boolean COMMENT 'An explanation about the purpose of this instance.'> COMMENT 'An explanation about the purpose of this instance.')"""
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(col_params = params)
                )

    @pytest.mark.parametrize("params", [(
        "The root schema comprises the entire JSON document."
    )])
    def test_create_table_with_comments(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        COMMENT 'The root schema comprises the entire JSON document.'"""
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(tb_desc = params)
                )
    
    @pytest.mark.parametrize("params", [(
        {'age': {'type': 'integer', 'description': 'An explanation about the purpose of this instance.'}}
    )])
    def test_create_table_with_partition(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        PARTITIONED BY (age tinyint COMMENT 'An explanation about the purpose of this instance.')"""
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(partition_params = params)
                )
    
    @pytest.mark.parametrize("params", [(
        ["age"], 32
    )])
    def test_create_table_with_clustering(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        CLUSTERED BY (age) INTO 32 BUCKETS"""
        clustering_params, num_buckets = params
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(
                        clustering_params = clustering_params,
                        num_buckets=num_buckets
                    )
                )
    
    @pytest.mark.parametrize("params", [(
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    )])
    def test_create_table_with_row_format(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        ROW FORMAT 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"""
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(row_format = params)
                )
        
    @pytest.mark.parametrize("params", [(
        "PARQUET"
    )])
    def test_create_table_with_file_format(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        STORED AS PARQUET"""
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(file_format = params)
                )
        
    @pytest.mark.parametrize("params", [(
        {'parquet.compress':'SNAPPY', 'serialization.format':'1'}
    )])
    def test_create_table_with_serde_properties(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        WITH SERDEPROPERTIES ('parquet.compress' = 'SNAPPY','serialization.format' = '1')"""
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(serde_properties = params)
                )
        
    @pytest.mark.parametrize("params", [(
        "s3://iti-query-results/"
    )])
    def test_create_table_with_location(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        LOCATION 's3://iti-query-results/'"""
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(location = params)
                )
        
    @pytest.mark.parametrize("params", [(
        {"has_encrypted_data": True}
    )])
    def test_create_table_with_tbl_properties(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        TBLPROPERTIES ('has_encrypted_data' = 'True')"""
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(tbl_properties = params)
                )
        
    @pytest.mark.parametrize("params", [({
        'eid': {'type': 'string', 'description': 'An explanation about the purpose of this instance.'}, 'documentNumber': {'type': 'string', 'description': 'An explanation about the purpose of this instance.'}, 'name': {'type': 'string', 'description': 'An explanation about the purpose of this instance.'}, 'age': {'type': 'integer', 'description': 'An explanation about the purpose of this instance.'}, 'address': {'type': 'object', 'description': 'An explanation about the purpose of this instance.', 'properties': {'street': {'type': 'string', 'description': 'An explanation about the purpose of this instance.'}, 'number': {'type': 'integer', 'description': 'An explanation about the purpose of this instance.'}, 'mailAddress': {'type': 'boolean', 'description': 'An explanation about the purpose of this instance.'}}}},
        "The root schema comprises the entire JSON document.",
        {'age': {'type': 'integer', 'description': 'An explanation about the purpose of this instance.'}},
        ["age"], 32,
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        "PARQUET",
        {'parquet.compress':'SNAPPY', 'serialization.format':'1'},
        "s3://iti-query-results/",
        {"has_encrypted_data": True}
    )])
    def test_create_table_full(self, create_query, params):
        query = """CREATE EXTERNAL TABLE IF NOT EXISTS db_people.tb_user
        (eid varchar COMMENT 'An explanation about the purpose of this instance.',
        documentNumber varchar COMMENT 'An explanation about the purpose of this instance.',
        name varchar COMMENT 'An explanation about the purpose of this instance.',
        age tinyint COMMENT 'An explanation about the purpose of this instance.',
        address struct <street: varchar COMMENT 'An explanation about the purpose of this instance.',
        number: tinyint COMMENT 'An explanation about the purpose of this instance.',
        mailAddress: boolean COMMENT 'An explanation about the purpose of this instance.'> COMMENT 'An explanation about the purpose of this instance.')
        COMMENT 'The root schema comprises the entire JSON document.'
        PARTITIONED BY (age tinyint COMMENT 'An explanation about the purpose of this instance.')
        CLUSTERED BY (age) INTO 32 BUCKETS
        ROW FORMAT 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS PARQUET
        WITH SERDEPROPERTIES ('parquet.compress' = 'SNAPPY','serialization.format' = '1')
        LOCATION 's3://iti-query-results/'
        TBLPROPERTIES ('has_encrypted_data' = 'True')"""
        col_params, tb_desc, partition_params, clustering_params, num_buckets, row_format, file_format, serde_properties, location, tbl_properties = params
        assert  self.format_string(query) == self.format_string(
                    create_query.create_table(
                        col_params = col_params,
                        tb_desc = tb_desc,
                        partition_params = partition_params,
                        clustering_params = clustering_params,
                        num_buckets = num_buckets,
                        row_format = row_format,
                        file_format = file_format,
                        serde_properties = serde_properties,
                        location = location,
                        tbl_properties = tbl_properties,
                    )
                )
    
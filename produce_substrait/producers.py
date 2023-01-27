import json
import jpype.imports
import duckdb

import produce_substrait.java_definitions as java
from com.google.protobuf.util import JsonFormat as json_formatter


class DuckDBProducer:
    def __init__(self, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()
            self.db_connection.execute("INSTALL substrait")
            self.db_connection.execute("LOAD substrait")

    def set_db_connection(self, db_connection):
        self.db_connection = db_connection

    def produce_substrait(self, schema_list, query):
        print(f"hello this is duckdb")

        for schema in schema_list:
            print(f"Executing: {schema}")
            self.db_connection.execute(f"{schema}")

        json_plan = self.db_connection.get_substrait_json(query).fetchone()[0]
        print(json_plan)
        python_json = json.loads(json_plan)
        with open(f"DuckDBProducer_substrait.json", "w") as outfile:
            outfile.write(json.dumps(python_json, indent=4))


class IsthmusProducer:
    def __init__(self, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()

        self.file_names = None

    def set_db_connection(self, db_connection):
        self.db_connection = db_connection

    def produce_substrait(self, schema_list, query):
        print(f"hello this is isthmus")

        java_schema_list = get_java_schema(schema_list)
        print(java_schema_list)
        json_plan = produce_isthmus_substrait(query, java_schema_list)
        print(json_plan)
        python_json = json.loads(json_plan)
        with open(f"Isthmus_substrait.json", "w") as outfile:
            outfile.write(json.dumps(python_json, indent=4))


def get_java_schema(schema_list):
    """
    Create the list of schemas based on the given file names.  If there are no files
    give, a custom schema for the data is used.

    Parameters:
        file_names: List of file names.

    Returns:
        List of all schemas as a java list.
    """
    arr = java.ArrayListClass()

    for create_table in schema_list:
        java_obj = jpype.JObject @ jpype.JString(create_table)
        arr.add(java_obj)

    return java.ListClass @ arr


def produce_isthmus_substrait(sql_string, schema_list):
    """
    Produce the substrait plan using Isthmus.

    Parameters:
        sql_string:
            SQL query.
        schema_list:
            List of schemas.

    Returns:
        Substrait plan in json format.
    """
    print("java sql string:")
    sql_to_substrait = java.SqlToSubstraitClass()
    java_sql_string = jpype.java.lang.String(sql_string)
    print(java_sql_string)
    plan = sql_to_substrait.execute(java_sql_string, schema_list)
    json_plan = json_formatter.printer().print_(plan)
    return json_plan
import json

import duckdb
import jpype.imports
import produce_substrait.java_definitions as java

from com.google.protobuf.util import JsonFormat as json_formatter
from google.protobuf import json_format
from ibis_substrait.compiler.core import SubstraitCompiler


class DuckDBProducer:
    def __init__(self, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()
            self.db_connection.execute("INSTALL substrait")
            self.db_connection.execute("LOAD substrait")

    def produce_substrait(self, schema_list, query):
        for schema in schema_list:
            self.db_connection.execute(f"{schema}")

        json_plan = self.db_connection.get_substrait_json(query).fetchone()[0]
        python_json = json.loads(json_plan)
        file_name = "DuckDB_substrait.json"
        with open(file_name, "w") as outfile:
            outfile.write(json.dumps(python_json, indent=4))
            print(f"substrait plan written to: {file_name}")

        return python_json


class IbisProducer:
    def __init__(self):
        self.compiler = SubstraitCompiler()

    def produce_substrait(self, schema_list, ibis_expr):
        mydict = {'func': ibis_expr, 'args': schema_list}
        ibis_expr = mydict['func'](*mydict['args'])
        tpch_proto_bytes = self.compiler.compile(ibis_expr)
        python_json = json_format.MessageToJson(tpch_proto_bytes)
        file_name = "Ibis_substrait.json"
        with open(file_name, "w") as outfile:
            outfile.write(python_json)
            print(f"substrait plan written to: {file_name}")

        return python_json


class IsthmusProducer:
    def produce_substrait(self, schema_list, query):
        java_schema_list = get_java_schema(schema_list)
        json_plan = produce_isthmus_substrait(query, java_schema_list)
        python_json = json.loads(json_plan)
        file_name = "Isthmus_substrait.json"
        with open(file_name, "w") as outfile:
            outfile.write(json.dumps(python_json, indent=4))
            print(f"substrait plan written to: {file_name}")

        return python_json


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
    sql_to_substrait = java.SqlToSubstraitClass()
    java_sql_string = jpype.java.lang.String(sql_string)
    plan = sql_to_substrait.execute(java_sql_string, schema_list)
    json_plan = json_formatter.printer().print_(plan)
    return json_plan

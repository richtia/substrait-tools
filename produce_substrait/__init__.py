import argparse
import ibis
import ibis.expr.datatypes as dt

import re
import sys
import substrait_validator as sv
from pathlib import Path

from produce_substrait.producers import IbisProducer, IsthmusProducer, DuckDBProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--producer",
        nargs="+",
        required=True,
        choices=["IbisProducer", "IsthmusProducer", "DuckDBProducer"],
        help="Substrait Producer",
    )
    parser.add_argument("--schema", required=True, type=Path,
                        help="File containing SQL schema")
    parser.add_argument(
        "--query_type", default="sql", choices=["sql", "ibis"], required=False
    )
    parser.add_argument("--query", required=False, help="SQL query")
    parser.add_argument(
        "--ibis_expr",
        nargs="*",
        required=False,
        action=KeyValue,
        help="Ibis expression.",
    )
    parser.add_argument("--validate", default=False, action="store_true")
    parser.add_argument("--validator_overrides", nargs='+', type=int)
    args = parser.parse_args()
    query = args.query
    validate_plan = args.validate
    override_levels = args.validator_overrides
    func_file_pair = args.ibis_expr
    print(f"Validating plan: {validate_plan}")
    print(f"Validator override levels: {override_levels}")
    if validate_plan:
        config = sv.Config()
        if override_levels:
            for override_level in override_levels:
                config.override_diagnostic_level(override_level, "warning", "info")

    schema_list = []
    if args.query_type == "sql":
        with open(args.schema) as file:
            for line in file:
                if line != "":
                    schema_list.append(line.rstrip())
    else:
        exec(open(Path(args.schema).resolve().as_posix()).read())
        schema_list = []
        with open(args.schema) as file:
            for line in file:
                if "name" in line:
                    table_name = extract_table_name(line)
                    schema_list.append(eval(table_name))

        func_name = ""
        for function_name, file_name in func_file_pair.items():
            func_name = function_name
            exec(open(Path(file_name).resolve().as_posix()).read())
        query = eval(func_name)

    for producer_name in args.producer:
        producer = str_to_class(producer_name)()
        substrait_plan = producer.produce_substrait(schema_list, query)

        if validate_plan:
            sv.check_plan_valid(substrait_plan, config)


def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


if __name__ == " __main__":
    main()


def extract_table_name(s):
    string = ""
    regexp = re.compile(r'(name=)"(.*?)"')
    for m in regexp.finditer(s):
        string += m.group(2)
    return string


class KeyValue(argparse.Action):
    # Constructor calling
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())

        for value in values:
            # split it into key and value
            key, value = value.split("=")
            # assign into dictionary
            getattr(namespace, self.dest)[key] = value

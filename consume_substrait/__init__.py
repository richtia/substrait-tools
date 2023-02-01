import argparse
import sys
from pathlib import Path

import duckdb
from filelock import FileLock

from consume_substrait.consumers import AceroConsumer, DuckDBConsumer


def prepare_data():
    tpch_data_path = Path.cwd() / "tpch_data"
    tpch_data_path.mkdir(parents=True, exist_ok=True)
    lock_file = tpch_data_path / "data.json"
    with FileLock(str(lock_file) + ".lock"):
        con = duckdb.connect()
        con.execute(f"CALL dbgen(sf=0.1)")
        con.execute(f"EXPORT DATABASE '{tpch_data_path}' (FORMAT PARQUET);")
    print(f"Parquet data written to {tpch_data_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--substrait_plan",
        required=True,
        type=Path,
        help="A substrait plan in json format",
    )
    parser.add_argument(
        "--table_file_pair",
        nargs="*",
        required=True,
        action=KeyValue,
        help="Pairing of table names and their corresponding data files.",
    )
    parser.add_argument(
        "--consumer",
        nargs="+",
        required=True,
        choices=["AceroConsumer", "DuckDBConsumer"],
        help="Substrait Consumer",
    )
    args = parser.parse_args()
    substrait_plan = args.substrait_plan.read_text()
    table_file_pair = args.table_file_pair

    for consumer_name in args.consumer:
        consumer = str_to_class(consumer_name)()

        for table_name, file_name in table_file_pair.items():
            file_path = Path(file_name).resolve()
            consumer.load_data(file_path, table_name)
        res = consumer.run_substrait_query(substrait_plan)
        print(f"result: {res}")


def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


if __name__ == " __main__":
    main()


# create a keyvalue class
class KeyValue(argparse.Action):
    # Constructor calling
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())

        for value in values:
            # split it into key and value
            key, value = value.split("=")
            # assign into dictionary
            getattr(namespace, self.dest)[key] = value

import argparse
import sys
from pathlib import Path

import duckdb
from consume_substrait.consumers import AceroConsumer, DuckDBConsumer
from filelock import FileLock


def prepare_data():
    tpch_data_path = Path(__file__).parent / "adhoc_data"
    tpch_data_path.mkdir(parents=True, exist_ok=True)
    lock_file = tpch_data_path / "data.json"
    with FileLock(str(lock_file) + ".lock"):
        con = duckdb.connect()
        con.execute(f"CALL dbgen(sf=0.1)")
        con.execute(f"EXPORT DATABASE '{tpch_data_path}' (FORMAT PARQUET);")
    print(f"Parquet data written to {tpch_data_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_file", required=True, type=Path, help="A parquet file")
    parser.add_argument("--substrait_plan", required=True, type=Path, help="A substrait plan in json format")
    parser.add_argument("--table_name", required=True)
    parser.add_argument(
        "--consumer",
        nargs="+",
        required=True,
        choices=["AceroConsumer", "DuckDBConsumer"],
        help="Substrait Consumer",
    )
    args = parser.parse_args()
    table_name = args.table_name
    substrait_plan = args.substrait_plan.read_text()

    for consumer_name in args.consumer:
        consumer = str_to_class(consumer_name)()

        consumer.load_data(args.data_file, table_name)
        res = consumer.run_substrait_query(substrait_plan)
        print(f"result: {res}")


def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


if __name__ == " __main__":
    main()

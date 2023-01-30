import argparse
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.substrait as substrait
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
    parser.add_argument("--data_file", required=True, type=Path)
    parser.add_argument("--substrait_plan", required=True, type=Path)
    parser.add_argument("--table_name", required=True)
    parser.add_argument(
        "--consumer",
        nargs="+",
        required=True,
        choices=["AceroConsumer"],
        help="Substrait Consumer",
    )
    args = parser.parse_args()
    table_name = args.table_name
    substrait_plan = args.substrait_plan.read_text()

    for consumer_name in args.consumer:
        consumer = str_to_class(consumer_name)()
        print(f"consumer type: {type(consumer)}")

        consumer.load_data(args.data_file, table_name)
        res = consumer.run_substrait_query(substrait_plan)
        print(f"result: {res}")


def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


if __name__ == " __main__":
    main()


class AceroConsumer:
    """
    Adapts the Acero Substrait consumer to the test framework.
    """

    def __init__(self):
        self.tables = {}
        self.table_provider = lambda names: self.tables[names[0].lower()]

    def load_data(self, file_path, table_name):
        self.tables[table_name] = pq.read_table(file_path)

    def run_substrait_query(self, substrait_query: bytes) -> pa.Table:
        """
        Run the substrait plan against Acero.

        Parameters:
            substrait_query:
                A json formatted byte representation of the substrait query plan.

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """
        if isinstance(substrait_query, str):
            buf = pa._substrait._parse_json_plan(substrait_query.encode())
        else:
            buf = pa._substrait._parse_json_plan(substrait_query)

        reader = substrait.run_query(buf, table_provider=self.table_provider)
        result = reader.read_all()

        return result

from pathlib import Path

from filelock import FileLock
from produce_substrait.producers import DuckDBProducer, IsthmusProducer
import argparse
import duckdb
import sys


def prepare_data():
    data_path = Path(__file__).parent / "adhoc_data"
    data_path.mkdir(parents=True, exist_ok=True)
    lock_file = data_path / "data.json"
    with FileLock(str(lock_file) + ".lock"):
        con = duckdb.connect()
        con.execute(f"CALL dbgen(sf=0.1)")
        con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT PARQUET);")
    print(f"Parquet data written to {data_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prepare_data', default=False,
                        action=argparse.BooleanOptionalAction)
    parser.add_argument('--producer', nargs='+', required=True,
                        choices=['IsthmusProducer', 'DuckDBProducer'],
                        help='Substrait Producer')
    parser.add_argument('--schema', required=True)
    parser.add_argument('--query_type', default=['sql'],
                        choices=['sql'], required=False)
    parser.add_argument('--query', required=True)
    args = parser.parse_args()

    if args.prepare_data is True:
        print(f"prepare data is: {args.prepare_data}")
        prepare_data()
    else:
        print(f"prepare data is: {args.prepare_data}")
        print("Not preparing data.")

    query = args.query
    print(f"query is: {query}")

    print(f"type: {type(args.producer)}")
    print(f"producers: {args.producer}")
    schema_list = []
    if args.schema:
        with open(args.schema) as file:
            for line in file:
                if line != "":
                    schema_list.append(line.rstrip())

    for producer_name in args.producer:
        producer = str_to_class(producer_name)()
        print(type(producer))
        print(dir(producer))

        producer.produce_substrait(schema_list, query)


def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


if __name__ == ' __main__':
          main()

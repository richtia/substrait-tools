import argparse
import sys

from produce_substrait.producers import DuckDBProducer, IsthmusProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--producer",
        nargs="+",
        required=True,
        choices=["IsthmusProducer", "DuckDBProducer"],
        help="Substrait Producer",
    )
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--query_type", default=["sql"], choices=["sql"], required=False
    )
    parser.add_argument("--query", required=True)
    args = parser.parse_args()
    query = args.query

    schema_list = []
    if args.schema:
        with open(args.schema) as file:
            for line in file:
                if line != "":
                    schema_list.append(line.rstrip())

    for producer_name in args.producer:
        producer = str_to_class(producer_name)()
        producer.produce_substrait(schema_list, query)


def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


if __name__ == " __main__":
    main()

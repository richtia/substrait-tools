import argparse
import sys
import substrait_validator as sv

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
    parser.add_argument("--schema", required=True, help="File containing SQL schema")
    parser.add_argument(
        "--query_type", default=["sql"], choices=["sql"], required=False
    )
    parser.add_argument("--query", required=True, help="SQL query")
    parser.add_argument("--validate", default=False, action="store_true")
    parser.add_argument("--validator_overrides", nargs='+', type=int)
    args = parser.parse_args()
    query = args.query
    validate_plan = args.validate
    override_levels = args.validator_overrides
    print(f"Validating plan: {validate_plan}")
    print(f"Validator override levels: {override_levels}")
    if validate_plan:
        config = sv.Config()
        if override_levels:
            for override_level in override_levels:
                config.override_diagnostic_level(override_level, "warning", "info")

    schema_list = []
    if args.schema:
        with open(args.schema) as file:
            for line in file:
                if line != "":
                    schema_list.append(line.rstrip())

    for producer_name in args.producer:
        producer = str_to_class(producer_name)()
        substrait_plan = producer.produce_substrait(schema_list, query)

        if validate_plan:
            sv.check_plan_valid(substrait_plan, config)


def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


if __name__ == " __main__":
    main()

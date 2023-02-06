Substrait Tools
====================================

Table of Contents
=================
* [Overview](#Overview)
* [Setup](#Setup)
* [Command Line Tools](#Command-Line-Tools)
  * [Create TPC-H Data](#Create-TPC-H-Data)
  * [Generate Substrait Plans](#Generate-Substrait-Plans)
  * [Consume Substrait Plans](#Consume-Substrait-Plans)

# Overview
This repository provides tools for substrait


# Setup
Create and activate your conda environment with python3.9:
```commandline
conda create -y -n substrait_tools -c conda-forge python=3.9 openjdk
conda activate substrait_tools

python -m pip install --index-url https://test.pypi.org/simple/ --upgrade --no-cache-dir --extra-index-url=https://pypi.org/simple/substrait-tools-rtia83
```

# Command Line Tools

## Create TPC-H Data

```commandline
prepare_tpch_data                                                                                 
Parquet data written to /Users/richardtia/Voltron/substrait-tools/tpch_data
```

## Generate Substrait Plans

Example usage with SQL:
```commandline
produce_substrait --producer DuckDBProducer --schema ./schema.sql --query "select * from lineitem" --validate --validator_overrides 1002
```

Example usage with Ibis:
```commandline
produce_substrait --producer IbisProducer --schema ./ibis_schema.py --query_type ibis --ibis_expr test_expr=./expression.py
```

Arguments:<br>
--producer: Which substrait producer to generate the plan with.<br>
--schema: SQL Schema.  Each create table command should be on its own line.<br>
--query: SQL query.<br>
--ibis_expr: Ibis expression.  Argument should passed as a key value pair with the ibis expression function name and the python file with it's definition (`--ibis_expr test_expr=./expression.py`)
--validate: Default is false.  If set, the plan will run against the substrait validator.<br>
--validator_overrides: Bypass error codes found by the substrait validator.  One or more integers separates by spaces.<br>

Sample schemas:

SQL: schema.sql
```text
CREATE TABLE lineitem(l_orderkey INTEGER NOT NULL, l_partkey INTEGER NOT NULL, l_suppkey INTEGER NOT NULL, l_linenumber INTEGER NOT NULL, l_quantity INTEGER NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR NOT NULL, l_linestatus VARCHAR NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR NOT NULL, l_shipmode VARCHAR NOT NULL, l_comment VARCHAR NOT NULL);
```
**Note: Each `CREATE TABLE` command should be on its own line.

Ibis: ibis_schema.py
```python
lineitem = ibis.table(
        [
            ("l_orderkey", dt.int64),
            ("l_partkey", dt.int64),
            ("l_suppkey", dt.int64),
            ("l_linenumber", dt.int64),
            ("l_quantity", dt.Decimal(15, 2)),
            ("l_extendedprice", dt.Decimal(15, 2)),
            ("l_discount", dt.Decimal(15, 2)),
            ("l_tax", dt.Decimal(15, 2)),
            ("l_returnflag", dt.string),
            ("l_linestatus", dt.string),
            ("l_shipdate", dt.date),
            ("l_commitdate", dt.date),
            ("l_receiptdate", dt.date),
            ("l_shipinstruct", dt.string),
            ("l_shipmode", dt.string),
            ("l_comment", dt.string),
        ],
        name="lineitem",
    )
```
`--schema ./ibis_schema.py`

Sample ibis expression file:<br>
expression.py
```python
def expr(lineitem):
    new_col = lineitem.l_tax.acos().name("ACOS_TAX")
    return lineitem[new_col]
```
`--query_type ibis --ibis_expr expr=./expression.py`<br>
**Note: The key name (`expr`) should be the same as the function.

## Consume Substrait Plans

Example usage
```commandline
consume_substrait --consumer DuckDBConsumer --substrait_plan ./Isthmus_substrait.json --table_file_pair lineitem=./lineitem.parquet region=./region.parquet
```
Arguments:<br>
--consumer: Which substrait consumer to consume the plan with.<br>
--substrait_plan: Json formatted substrait plan.<br>
--table_file_pair: One or more table name and file pairings. Table name and file should be separated by an equal (=) sign.<br>
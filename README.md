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

python -m pip install --index-url https://test.pypi.org/simple/ --upgrade --no-cache-dir --extra-index-url=https://pypi.org/simple/substrait-tools-rtia49
```

# Command Line Tools

## Create TPC-H Data

```commandline
prepare_tpch_data                                                                                 
Parquet data written to /Users/richardtia/Voltron/substrait-tools/tpch_data
```

## Generate Substrait Plans

Example usage
```commandline
produce_substrait --producer DuckDBProducer --schema ./schema.sql --query "select * from lineitem" --validate --validator_overrides 1002
```
Arguments:<br>
--producer: Which substrait producer to generate the plan with.<br>
--schema: SQL Schema.  Each create table command should be on its own line.<br>
--query: SQL query.<br>
--validate: Default is false.  If set, the plan will run against the substrait validator.<br>
--validator_overrides: Bypass error codes found by the substrait validator.  One or more integers separates by spaces.<br>

Sample schema.sql
```text
CREATE TABLE lineitem(l_orderkey INTEGER NOT NULL, l_partkey INTEGER NOT NULL, l_suppkey INTEGER NOT NULL, l_linenumber INTEGER NOT NULL, l_quantity INTEGER NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR NOT NULL, l_linestatus VARCHAR NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR NOT NULL, l_shipmode VARCHAR NOT NULL, l_comment VARCHAR NOT NULL);
```
Each create table command should be on its own line.

## Consume Substrait Plans

Example usage
```commandline
consume_substrait --consumer DuckDBConsumer --substrait_plan ./Isthmus_substrait.json --table_file_pair lineitem=./lineitem.parquet region=./region.parquet
```
Arguments:<br>
--consumer: Which substrait consumer to consume the plan with.<br>
--substrait_plan: Json formatted substrait plan.<br>
--table_file_pair: One or more table name and file pairings. Table name and file should be separated by an equal (=) sign.<br>
Substrait Tools
====================================

Table of Contents
=================
* [Overview](#Overview)
* [Setup](#Setup)
* [Command Line Tools](#Command-Line-Tools)
  * [Generate Substrait Plans](#Generate-Substrait-Plans)

# Overview
This repository provides tools for substrait


# Setup
Create and activate your conda environment with python3.9:
```commandline
conda create -y -n substrait_tools -c conda-forge python=3.9 openjdk
conda activate substrait_tools

python -m pip install --index-url https://test.pypi.org/simple/ --upgrade --no-cache-dir --extra-index-url=https://pypi.org/simple/ substrait-tools-rtia13
```

# Command Line Tools

## Generate Substrait Plans

Example usage
```commandline
produce_substrait --producer DuckDBProducer --schema ./schema.sql --query "select * from lineitem"
```
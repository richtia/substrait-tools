from setuptools import find_packages, setup

setup(
    name="substrait-tools-rtia83",
    version="0.0.1",
    author="richtia",
    description="A Substrait command line tool",
    long_description="Command line tools for producing, validating, "
                     "and consuming substrait plans",
    python_requires=">=3.9, <4",
    packages=find_packages(
        include=[
            "produce_substrait*",
            "produce_substrait.*",
            "consume_substrait*",
            "consume_substrait.*",
        ]
    ),
    install_requires=[
        "duckdb>=0.6.2.dev1766",
        "filelock",
        "ibis-framework",
        "ibis-substrait",
        "JPype1",
        "protobuf",
        "pyarrow",
        "substrait-validator",
    ],
    package_data={
        "produce_substrait": ["jars/*.jar"],
    },
    entry_points={
        "console_scripts": [
            "produce_substrait = produce_substrait:main",
            "prepare_tpch_data = consume_substrait:prepare_data",
            "consume_substrait = consume_substrait:main",
        ]
    },
)

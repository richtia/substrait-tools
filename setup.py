from setuptools import find_packages, setup

setup(
    name="substrait-tools-rtia17",
    version="0.0.1",
    author="richtia",
    description="A Substrait command line tool",
    long_description="Tool for generation substrait plans",
    # url='https://github.com/substrait-io/consumer-testing',
    # keywords='substrait, consumer',
    python_requires=">=3.9, <4",
    packages=find_packages(include=["produce_substrait*", "produce_substrait.*",
                                    "consume_substrait*", "consume_substrait.*"]),
    install_requires=[
        "duckdb",
        "filelock",
        "JPype1",
        "protobuf",
    ],
    package_data={
        "produce_substrait": ["jars/*.jar"],
    },
    entry_points={
        "console_scripts": [
            "produce_substrait = produce_substrait:main",
            "prepare_tpch_data = consume_substrait:prepare_data",
        ]
    },
)

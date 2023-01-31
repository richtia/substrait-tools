import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.substrait as substrait


class AceroConsumer:
    """
    Adapts the Acero Substrait consumer to the test framework.
    """

    def __init__(self):
        self.tables = {}
        self.table_provider = lambda names: self.tables[names[0].lower()]

    def load_data(self, file_path, table_name):
        self.tables[table_name] = pq.read_table(file_path)

    def run_substrait_query(self, substrait_query: str) -> pa.Table:
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


class DuckDBConsumer:
    def __init__(self, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()
            self.db_connection.execute("INSTALL substrait")
            self.db_connection.execute("LOAD substrait")

    def load_data(self, file_path, table_name):
        create_table_sql = (
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}');"
        )
        self.db_connection.execute(create_table_sql)

    def run_substrait_query(self, substrait_query: str) -> pa.Table:
        """
        Run the substrait plan against DuckDB.

        Parameters:
            substrait_query:
                A substrait plan in byte format

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """
        return self.db_connection.from_substrait_json(substrait_query).arrow()

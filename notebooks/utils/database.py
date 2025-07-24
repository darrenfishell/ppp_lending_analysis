import duckdb
from pathlib import Path

root_dir = Path(__file__).parents[2]
db_dir = root_dir / 'data/ppp_loan_analysis.duckdb'

def query_df(query):
    with duckdb.connect(db_dir) as conn:
        df = conn.execute(query).df()
        return df
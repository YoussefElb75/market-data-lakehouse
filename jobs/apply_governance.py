"""Job task: apply Unity Catalog tags to every lakehouse table (idempotent).

Parameters: --catalog <catalog>
"""

import argparse
import sys

from pyspark.sql import SparkSession

from mdl import governance

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args(sys.argv[1:])
spark = SparkSession.builder.getOrCreate()

existing = {
    f"{args.catalog}.{r.table_schema}.{r.table_name}"
    for r in spark.sql(
        f"SELECT table_schema, table_name FROM {args.catalog}.information_schema.tables"
    ).collect()
}
applied = skipped = 0
for stmt in governance.statements(args.catalog):
    table = stmt.split()[2]
    if table not in existing:  # e.g. ops.contract_results before the first post_checks run
        skipped += 1
        continue
    spark.sql(stmt)
    applied += 1
print(f"governance: {applied} statements applied, {skipped} skipped (table not created yet)")

# Pain Point 2 — Custom merge patterns via ForEachBatch sink (PuPr Dec 2025).
#
# Demonstrates the OVERWRITE_BY_KEY pattern using MERGE INTO inside a
# foreach_batch_sink. Arbitrary Python/SQL per micro-batch — the escape hatch
# for the 12 strategies APPLY CHANGES INTO doesn't cover.
#
# The flow reads from the toy source as a stream, then the sink upserts into
# an external Delta table that the pipeline does NOT manage (this is the whole
# point of ForEachBatch — write anywhere SDP doesn't natively support).

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from delta.tables import DeltaTable


# The target Delta table the sink writes to. The pipeline does NOT create or
# manage it; ForEachBatch can write to ANY external destination. The setup
# notebook creates this table as a side effect via the first pipeline update.
TARGET_TABLE_PATTERN = "{target_catalog}.qa_demos.consultoras_merged_external"


@dp.foreach_batch_sink(name="consultoras_overwrite_by_key_sink")
def overwrite_by_key_sink(batch_df, batch_id):
    """OVERWRITE_BY_KEY semantic via MERGE INTO.

    For each micro-batch:
      - match on consultora_id
      - matched      -> update all columns
      - not matched  -> insert
      - missing in source -> delete

    This is the escape hatch for custom merge strategies (OVERWRITE_BY_KEY,
    DELETE_INSERT, time-windowed incremental, etc.) — any merge logic
    expressible in Spark SQL or the DeltaTable Python API runs inside
    this function.
    """
    spark = batch_df.sparkSession
    target_catalog = spark.conf.get("target_catalog")
    target_name = TARGET_TABLE_PATTERN.format(target_catalog=target_catalog)

    # Lazy-create the external target so the sink can run before the first
    # manual setup. Schema matches the source.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_name} (
          consultora_id INT,
          nome STRING,
          regiao STRING,
          tier STRING,
          valor_total DOUBLE,
          data_pedido TIMESTAMP
        ) USING DELTA
    """)

    target = DeltaTable.forName(spark, target_name)
    (target.alias("t")
        .merge(batch_df.alias("s"), "s.consultora_id = t.consultora_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute())


@dp.append_flow(target="consultoras_overwrite_by_key_sink")
def consultoras_source():
    target_catalog = spark.conf.get("target_catalog")
    return spark.readStream.table(f"{target_catalog}.qa_demos.consultoras_toy_source")

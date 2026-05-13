# Silver Consultoras — Python SDP table reading bronze's Change Data Feed.
#
# bronze.consultoras_raw is written by Lakeflow Connect via MERGE. A
# plain `STREAM(bronze.consultoras_raw)` read fails on non-append
# commits, so we read CDF explicitly via readChangeFeed=true and
# filter to insert + update_postimage events. Each event lands in
# silver as an append row — the input shape AutoCDC SCD2 in
# gold.dim_consultora expects.

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="silver.consultoras",
    comment=(
        "Curated Consultoras — 1:1 from bronze.consultoras_raw via Delta CDF "
        "(insert + update_postimage events as append rows). Three expectations: "
        "pk_consultora (FAIL UPDATE), cpf_format (DROP ROW), email_present "
        "(warn-only). Feeds AutoCDC SCD2 in gold.dim_consultora."
    ),
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("pk_consultora", "consultora_id IS NOT NULL")
@dp.expect_or_drop("cpf_format", "cpf RLIKE '^[0-9]{11}$'")
@dp.expect("email_present", "email IS NOT NULL OR ativo = FALSE")
def silver_consultoras() -> DataFrame:
    target_catalog = spark.conf.get("target_catalog")
    source = f"{target_catalog}.bronze.consultoras_raw"

    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(source)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .select(
            F.col("consultora_id").cast("int").alias("consultora_id"),
            F.col("cpf"),
            F.col("nome"),
            F.col("email"),
            F.col("regiao"),
            F.col("tier"),
            F.col("data_cadastro").cast("timestamp").alias("data_cadastro"),
            F.col("ativo").cast("boolean").alias("ativo"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
        )
    )

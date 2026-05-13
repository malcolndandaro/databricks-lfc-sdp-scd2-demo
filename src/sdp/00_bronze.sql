-- Bronze: Pedidos raw ingest via Auto Loader.
--
-- Source: parquet drops in ${source_volume_path}. Schema evolution mode
-- addNewColumns lets new columns flow through bronze without a manual
-- migration. Bronze is RAW — no curation, no dedup, no type coercion
-- beyond Auto Loader's parquet inference.

CREATE OR REFRESH STREAMING TABLE bronze.pedidos_raw
COMMENT 'Raw Pedidos via Auto Loader from UC Volume parquet drops. Schema evolves additively.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'delta.feature.timestampNtz' = 'supported'
)
AS SELECT
  *,
  _metadata.file_path AS _ingest_file_path,
  _metadata.file_modification_time AS _ingest_file_mtime,
  current_timestamp() AS _ingest_ts
FROM STREAM read_files(
  '${source_volume_path}',
  format => 'parquet',
  schemaEvolutionMode => 'addNewColumns'
);

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "16f689a0-34fb-4282-b579-ab6ff6334b9f",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "811a9706-b5e2-455a-9104-dea791f7c8ff",
# META       "known_lakehouses": [
# META         {
# META           "id": "16f689a0-34fb-4282-b579-ab6ff6334b9f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_bronze_inventory_purchasing
# 
# | Source | Files | Load type | Table |
# |---|---|---|---|
# | Inventory snapshots | one per month | incremental (watermark) | `bronze_inventory_snapshot` |
# | Store transfers | single file | full | `bronze_store_transfers` |
# | Goods receipts | single file | full | `bronze_goods_receipts` |
# | Promotions | single file | full | `bronze_promotions` |
# 
# Purchase orders are - JSON 


# CELL ********************

LANDING = "Files/raw"
LOAD_UP_TO = None      # e.g. "2024_12" to test with 3 months first

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Incremental helper functions
# The same functions as in `nb_bronze_sales`.

# CELL ********************

from pyspark.sql import functions as F

spark.sql("""
    CREATE TABLE IF NOT EXISTS etl_watermark (
        source_name STRING,
        last_file   STRING,
        updated_at  TIMESTAMP
    ) USING DELTA
""")


def get_watermark(source_name):
    """Name of the last file already loaded for this source ('' if none)."""
    rows = spark.table("etl_watermark").filter(F.col("source_name") == source_name).collect()
    return rows[0]["last_file"] if rows else ""


def set_watermark(source_name, last_file):
    spark.sql(f"""
        MERGE INTO etl_watermark t
        USING (SELECT '{source_name}' AS source_name,
                      '{last_file}'   AS last_file,
                      current_timestamp() AS updated_at) s
        ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET t.last_file = s.last_file, t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (source_name, last_file, updated_at)
                              VALUES (s.source_name, s.last_file, s.updated_at)
    """)


def select_new_files(file_names, last_file, prefix, load_up_to=None):
    """Pick the files that still need loading, oldest first.

    File names look like  <prefix>YYYY_MM.csv , so sorting them as text also
    sorts them by date. A file is new if its name is greater than the watermark.
    """
    names = sorted(n for n in file_names if n.startswith(prefix) and n.endswith(".csv"))
    names = [n for n in names if n > last_file]
    if load_up_to:
        names = [n for n in names if n <= f"{prefix}{load_up_to}.csv"]
    return names


def load_incremental(source_name, folder, prefix, table, load_up_to=None):
    """Append only the monthly files that have not been loaded yet."""
    files = {f.name: f.path for f in notebookutils.fs.ls(folder) if not f.isDir}
    last_file = get_watermark(source_name)
    new_names = select_new_files(files.keys(), last_file, prefix, load_up_to)

    # Safety net: never load a file that is already in the table,
    # even if the watermark was not updated after an earlier run.
    if spark.catalog.tableExists(table):
        already = {r["_source_file"] for r in
                   spark.table(table).select("_source_file").distinct().collect()}
        new_names = [n for n in new_names if n not in already]

    print(f"[{source_name}] watermark = '{last_file or 'none'}', files to load = {len(new_names)}")
    if not new_names:
        print("Nothing new. Table is up to date.")
        return

    df = (spark.read
          .option("header", True)          # first line holds column names
          .option("inferSchema", False)    # Bronze keeps everything as string
          .csv([files[n] for n in new_names])
          .withColumn("_source_file", F.col("_metadata.file_name"))
          .withColumn("_ingested_at", F.current_timestamp()))

    df.write.format("delta").mode("append").saveAsTable(table)
    set_watermark(source_name, new_names[-1])
    print(f"Loaded {new_names[0]} -> {new_names[-1]} into {table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Inventory snapshots (incremental)

# CELL ********************

load_incremental(
    source_name="inventory_snapshot",
    folder=f"{LANDING}/inventory",
    prefix="inventory_snapshot_",
    table="bronze_inventory_snapshot",
    load_up_to=LOAD_UP_TO,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Single-file sources (full load)

# CELL ********************

FULL_LOAD_FILES = {
    "purchasing/store_transfers.csv": "bronze_store_transfers",
    "purchasing/goods_receipts.csv":  "bronze_goods_receipts",
    "promotions/promotions.csv":      "bronze_promotions",
}

for path, table in FULL_LOAD_FILES.items():
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)
          .csv(f"{LANDING}/{path}")
          .withColumn("_source_file", F.col("_metadata.file_name"))
          .withColumn("_ingested_at", F.current_timestamp()))

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table))

    print(f"{table}: {spark.table(table).count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check
# 
# 
# | Table | Rows |
# |---|---|
# | `bronze_inventory_snapshot` | 157,080 (24 files) |
# | `bronze_store_transfers` | 134,441 |
# | `bronze_goods_receipts` | 10,318 |
# | `bronze_promotions` | 256 |

# CELL ********************

for t in ["bronze_inventory_snapshot", "bronze_store_transfers",
          "bronze_goods_receipts", "bronze_promotions"]:
    print(f"{t}: {spark.table(t).count()} rows")

display(spark.table("etl_watermark"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

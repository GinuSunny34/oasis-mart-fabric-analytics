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

# # nb_bronze_sales
# 
# Loads the monthly sales files **incrementally**: each run picks up only the files that have not been loaded before.
# 
# How it works:
# 1. A small control table, `etl_watermark`, remembers the last file loaded for each source.
# 2. The notebook lists the files in the landing folder and keeps those with a name later than the watermark.
# 3. It appends them to `bronze_sales`, then moves the watermark forward.
# 
# Running it twice in a row loads nothing the second time. That property is called *idempotent*.
# 
# **Before running:** attach `lh_bronze` as the default lakehouse.

# CELL ********************

LANDING = "Files/raw"

# For testing. "2024_12" loads only files up to December 2024 (the first 3 months).
# Set to None to load everything that is new.
# LOAD_UP_TO = "2024_12"
LOAD_UP_TO = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

load_incremental(
    source_name="sales",
    folder=f"{LANDING}/sales",
    prefix="sales_",
    table="bronze_sales",
    load_up_to=LOAD_UP_TO,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check
# 
# **Test 1:** with `LOAD_UP_TO = "2024_12"` you should see 3 files and **116,924** rows.
# 
# **Test 2:** run the load cell again without changing anything. It should say *Nothing new*.
# 
# **Test 3:** set `LOAD_UP_TO = None`, rerun the config cell and the load cell. It should load the other 21 files, giving 24 files and **923,345** rows in total.

# CELL ********************

display(spark.table("etl_watermark"))

display(spark.table("bronze_sales")
        .groupBy("_source_file").count()
        .orderBy("_source_file"))

print("Total rows:", spark.table("bronze_sales").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Starting again (only if you need to)
# 
# To repeat the test from zero, run the cell below once, then rerun the notebook. It is commented out so it cannot run by accident.

# CELL ********************

# spark.sql("DROP TABLE IF EXISTS bronze_sales")
# spark.sql("DELETE FROM etl_watermark WHERE source_name = 'sales'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

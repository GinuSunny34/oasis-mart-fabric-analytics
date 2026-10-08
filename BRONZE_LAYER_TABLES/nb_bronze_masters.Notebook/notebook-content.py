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

# ### nb_bronze_masters
# Loads the five master files into Bronze Delta tables - as raw data 
# - Every column is kept as text. Cleaning and data types belong to Silver.
# - Two audit columns are added: `_source_file` and `_ingested_at`.


# CELL ********************

from pyspark.sql import functions as F

# Folder in the lakehouse where you uploaded the raw files.
# Check it in the left panel: Files > landing > master > stores.csv
LANDING = "Files/raw"

# file name (without .csv)  ->  Bronze table name
MASTER_FILES = {
    "stores":               "bronze_stores",
    "distribution_centres": "bronze_distribution_centres",
    "products":             "bronze_products",
    "suppliers":            "bronze_suppliers",
    "uae_retail_calendar":  "bronze_calendar",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for file_name, table in MASTER_FILES.items():
    df = (spark.read
          .option("header", True)         
          .option("inferSchema", False)    # keep every column as string
          .csv(f"{LANDING}/master/{file_name}.csv")
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

# CELL ********************

display(spark.table("bronze_products").limit(5))
spark.table("bronze_products").printSchema()   # evry table plus the two audit columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

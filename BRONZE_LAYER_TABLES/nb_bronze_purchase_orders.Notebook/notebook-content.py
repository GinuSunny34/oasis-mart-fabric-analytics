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

# # nb_bronze_purchase_orders
# 
# Loads `purchase_orders.json` into two Bronze tables.
# 
# flattening it:
# 1. `explode(purchase_orders)` turns the list of orders into one row per order → `bronze_po_header`
# 2. `explode(lines)` turns each order's lines into one row per line → `bronze_po_lines`
# 
# 
# 
# **Why PySpark here:** reading a single JSON document needs the `multiLine` option, and nested lists are simplest with `explode`.


# CELL ********************

from pyspark.sql import functions as F

LANDING = "Files/raw"
PO_FILE = f"{LANDING}/purchasing/purchase_orders.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1. Read the file

# CELL ********************

raw = (spark.read
       .option("multiLine", True)        # the whole file is one JSON document
       .json(PO_FILE)
       .withColumn("_source_file", F.col("_metadata.file_name"))
       .withColumn("_ingested_at", F.current_timestamp()))

raw.printSchema()   # shows the nested structure: purchase_orders -> lines

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2. One row per purchase order

# CELL ********************

orders = raw.select(
    F.col("extracted_at").alias("_extracted_at"),
    "_source_file",
    "_ingested_at",
    F.explode("purchase_orders").alias("po"),          # list of orders -> rows
)

header = orders.select(
    F.col("po.po_number").cast("string").alias("po_number"),
    F.col("po.po_date").cast("string").alias("po_date"),
    F.col("po.supplier_code").cast("string").alias("supplier_code"),
    F.col("po.ship_to").cast("string").alias("ship_to"),
    F.col("po.promised_delivery_date").cast("string").alias("promised_delivery_date"),
    F.col("po.status").cast("string").alias("status"),
    "_extracted_at", "_source_file", "_ingested_at",
)

(header.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable("bronze_po_header"))

print("bronze_po_header:", spark.table("bronze_po_header").count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3. One row per purchase order line

# CELL ********************

lines = (orders
         .select(F.col("po.po_number").alias("po_number"),
                 "_source_file", "_ingested_at",
                 F.explode("po.lines").alias("line"))     # list of lines -> rows
         .select(
             F.col("po_number").cast("string").alias("po_number"),
             F.col("line.line_no").cast("string").alias("line_no"),
             F.col("line.product_code").cast("string").alias("product_code"),
             F.col("line.ordered_qty").cast("string").alias("ordered_qty"),
             F.col("line.unit_cost_aed").cast("string").alias("unit_cost_aed"),
             "_source_file", "_ingested_at",
         ))

(lines.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("bronze_po_lines"))

print("bronze_po_lines:", spark.table("bronze_po_lines").count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check
# 
# - `bronze_po_header`: **10,449** rows
# - `bronze_po_lines`: **10,449** rows. Every order in this dataset has exactly one line, so the counts match. The flattening pattern is the same for orders with many lines.
# - **25** orders carry a supplier *name* in upper case instead of a supplier code. That is deliberate; Silver fixes it in step 2.5.

# CELL ********************

display(spark.table("bronze_po_header").limit(5))
display(spark.table("bronze_po_lines").limit(5))

# The 25 orders with a supplier name instead of a code
display(spark.table("bronze_po_header")
        .filter(~F.col("supplier_code").startswith("SUP"))
        .groupBy("supplier_code").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

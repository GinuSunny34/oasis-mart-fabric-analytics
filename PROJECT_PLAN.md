# Project Plan: Oasis Mart Supply Chain Analytics on Microsoft Fabric

Built for short sessions: each step takes about **30–45 minutes**.
At 2–3 sessions a week, the full plan takes roughly **12–14 weeks**. If a week is busy, skip it; nothing breaks.

**Language choice:** transformation work is in **SQL**: Spark SQL (`%%sql` cells) in notebooks, and T-SQL in the warehouse.
PySpark is used only where SQL can't do the job (listing files for incremental loads), and that part is already done.

**Legend:** ✅ done · 🆕 added Fabric feature · Language shown in brackets

---

## Phase 0: Set up ✅

- [x] **0.1** GitHub repository `oasis-mart-fabric-analytics`
- [x] **0.2** Workspace `Oasis-Mart-Dev`, connected to GitHub (**Git integration**)
- [x] **0.3** 🆕 Domain `Supply Chain`, workspace assigned (**Domains**)
- [ ] **0.4** Architecture sketch (draw.io or Excalidraw) → `docs/architecture.png`. Refine at the end.

## Phase 1: Bronze, land the raw data

- [x] **1.1** Lakehouse `lh_bronze`; raw files uploaded to `Files/raw/`
- [x] **1.2** `nb_bronze_masters`: five master files as-is, with audit columns [PySpark]
- [x] **1.3** `nb_bronze_sales`: incremental load with the `etl_watermark` table [PySpark]
- [x] **1.4** `nb_bronze_inventory_purchasing`: inventory (incremental), transfers, receipts, promotions [PySpark]
- [ ] **1.5** `nb_bronze_purchase_orders`: read `purchase_orders.json` and flatten the nested `lines` into `bronze_po_header` and `bronze_po_lines` [**Spark SQL**: query the file directly, `explode`]
- [ ] **1.6** Data pipeline `pl_ingest_bronze`: runs the Bronze notebooks in order, with a failure path. ➜ *Screenshot.*

## Phase 2: Silver, clean and conform

- [ ] **2.1** 🆕 **Dataflow Gen2** `df_silver_masters`: clean stores, products, suppliers, distribution centres (trim, upper-case codes, fix category case, set data types) → `lh_silver` [Power Query]
- [ ] **2.2** 🆕 **OneLake shortcut**: in `lh_silver`, add a shortcut to `bronze_calendar` instead of copying it
- [ ] **2.3** `nb_silver_sales`: read both date formats, standardise store codes, remove exact duplicates, split SALE and RETURN, fill missing `unit_price` from the product price [**Spark SQL**]
- [ ] **2.4** Send orphan product codes to `silver_sales_rejected` with a reason column, instead of dropping them [Spark SQL]
- [ ] **2.5** `nb_silver_inventory_purchasing`: clean inventory; map the 25 POs that carry supplier names back to supplier codes [Spark SQL]
- [ ] **2.6** Data quality log `dq_results`: rule name, rows checked, rows failed, run date ➜ *strong interview talking point* [Spark SQL `INSERT`]
- [ ] **2.7** Make Silver writes **MERGE (upsert)** so reruns are safe [Spark SQL `MERGE INTO`]

## Phase 3: Gold, the star schema in a Warehouse

Create **Warehouse** `wh_gold`. Build everything with **T-SQL**, reading from `lh_silver` with cross-database queries.

| Dimensions | Facts |
|---|---|
| `dim_date` (from the UAE retail calendar) | `fact_sales` (day × store × product) |
| `dim_store` | `fact_inventory_snapshot` (week × location × product) |
| `dim_product` (**SCD Type 2** on price / status) | `fact_purchase_order_line` |
| `dim_supplier` | `fact_goods_receipt` |
| `dim_location` (stores + DCs) | `fact_store_transfer` |

- [ ] **3.1** Dimensions with surrogate keys [T-SQL]
- [ ] **3.2** `dim_product` as SCD Type 2 [T-SQL]
- [ ] **3.3** `fact_sales` and `fact_inventory_snapshot` [T-SQL]
- [ ] **3.4** Supplier facts: join PO lines to receipts → **actual lead time, days late, fill rate** per PO line [T-SQL]
- [ ] **3.5** **Estimate lost sales**: days a store-product had zero stock × its normal daily sales. Compare with `answer_key/` and write down how close you got [T-SQL]
- [ ] **3.6** Wrap the Gold load in a **stored procedure** `usp_load_gold` [T-SQL]
- [ ] **3.7** Data pipeline `pl_daily_load`: Bronze notebooks → Dataflow → Silver notebooks → stored procedure

## Phase 4: Semantic model + DAX  *(= PL-300 / DP-600 practice)*

- [ ] **4.1** **Direct Lake** semantic model on `wh_gold`: relationships, hide keys, mark `dim_date` as date table, display folders. *(If it asks for a Power BI Pro licence, start the free 60-day Power BI trial.)*
- [ ] **4.2** Core measures: Sales AED, Units, Gross Margin %, Sales LY, YoY %, Sales YTD
- [ ] **4.3** Supply chain measures:
  - **Days of Cover** = on-hand ÷ average daily units (last 28 days)
  - **Stock-out Rate** = store-product-days with zero stock ÷ total
  - **Estimated Lost Sales AED**
  - **Supplier OTIF %** (on time *and* in full)
  - **Average Lead Time** vs promised; **Fill Rate %**
  - **Inventory Value at Cost**; **Slow-Moving Stock** (cover > 60 days)
- [ ] **4.4** **Row-level security**: one role per emirate. Test with "View as role".
- [ ] **4.5** A **calculation group** for time intelligence (Current / LY / YoY %)

## Phase 5: Power BI report

Four pages, each answering one question:
- [ ] **5.1 Executive overview**: sales trend with Ramadan / DSF shaded, YoY, margin, KPI cards
- [ ] **5.2 Availability**: stock-out heatmap (store × category × week), estimated lost sales, worst 10 products
- [ ] **5.3 Supplier performance**: OTIF by supplier, lead-time variance, the 2025 shipping disruption made visible
- [ ] **5.4 Inventory health**: days of cover by category, slow movers, inventory value by location
- [ ] **5.5** Drill-through from a product to its full story (sales, stock, POs); tooltips; a clean theme

## Phase 6: Production and governance

- [ ] **6.1** Deployment pipeline: Dev → Prod workspaces
- [ ] **6.2** Schedule `pl_daily_load`; add a failure email (Outlook activity) or Teams message
- [ ] **6.3** 🆕 **Activator** alert on the report: email when the weekly stock-out rate goes above 5%
- [ ] **6.4** 🆕 **Endorsement**: mark the Gold warehouse and the semantic model as Promoted
- [ ] **6.5** 🆕 **Lineage view**: screenshot of the path from raw files to the report
- [ ] **6.6** Final Git commit; final architecture diagram

## Phase 7: 🆕 Optional extras (if time allows)

- [ ] **7.1** **Real-Time Intelligence**: an Eventstream with Fabric's sample data into an Eventhouse, plus one KQL query
- [ ] **7.2** **Copilot** in a notebook or the report, if your capacity supports it

## Phase 8: Present it

- [ ] **8.1** GitHub README: problem → architecture diagram → screenshots → **3 key findings** → what you'd do next
- [ ] **8.2** A **2-minute screen recording** walking through one finding
- [ ] **8.3** Portfolio website page: a grid of screenshots, one per Fabric feature, with one line on why it was the right tool
- [ ] **8.4** CV, LinkedIn Featured section, and one modest LinkedIn post

---

### Fabric features this project covers

Workspaces · Domains · Git integration · Lakehouse · Notebooks (PySpark + Spark SQL) · Dataflow Gen2 · OneLake shortcuts · Warehouse (T-SQL, stored procedures, cross-database queries) · Data pipelines · Direct Lake semantic model · RLS · Calculation groups · Power BI report · Activator · Deployment pipelines · Endorsement · Lineage · *(optional)* Real-Time Intelligence, Copilot

### Interview talking points

- Incremental loading with a watermark; idempotent MERGE
- Handling a source format change (the date-format switch)
- A data quality framework with a quarantine table
- Choosing the tool: Dataflow Gen2 for simple reference data, Spark SQL for volume, T-SQL warehouse for the star schema
- SCD Type 2 in T-SQL
- Estimating something the source never records (lost sales) and checking the estimate
- Business insight: Ramadan planning, supplier risk, working capital in slow stock

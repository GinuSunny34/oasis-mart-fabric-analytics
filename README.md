# Oasis Mart — Supply Chain Analytics on Microsoft Fabric

End-to-end analytics platform for a **fictional** UAE grocery retailer, built on Microsoft Fabric:
raw ERP/POS files → Lakehouse (Bronze / Silver / Gold) → Direct Lake semantic model → Power BI.

> All data is synthetic, produced by `generate_data.py`. Oasis Mart is not a real company.

## The business

Oasis Mart runs **12 stores** across Dubai, Sharjah, Ajman, Abu Dhabi and Al Ain, supplied by
**2 distribution centres** (Jebel Ali, KEZAD) and **18 suppliers** from the UAE, India, Pakistan,
Vietnam, Egypt, Turkey and others. It sells **150 products** in 14 categories.
Data covers **24 months: Oct 2024 – Sep 2026**.

### Questions the platform should answer
1. How do sales move with Ramadan, Eid, Dubai Shopping Festival, summer and back-to-school?
2. Where and when do we run out of stock, and what does it cost us?
3. Which suppliers deliver late or short, and how does that hurt store availability?
4. How many days of stock do we hold, and where is money tied up in slow-moving inventory?
5. Do promotions really lift sales, or do they just move them around?

### Stories hidden in the data (find them!)
- A strong **Ramadan peak** (Mar 2025, Feb–Mar 2026), led by dates, rice, oil and canned goods.
- A **sea-freight disruption** (mid-Apr to Jun 2025) delaying overseas suppliers → stock-outs.
- **Summer slowdown** in Jul–Aug, **DSF lift** in Dec–Jan, weekend (Fri/Sat) peaks.
- Two **price increases** (Apr 2025, Jan 2026) and 14 promotional campaigns.
- New products launched mid-period and some discontinued.

## Raw files (`raw/`)

| Folder | File | Grain | Notes |
|---|---|---|---|
| master | `stores.csv` | 1 row per store | emirate, format, serving DC |
| master | `distribution_centres.csv` | 1 row per DC | |
| master | `products.csv` | 1 row per product | cost, price, supplier, case pack, launch / discontinued dates |
| master | `suppliers.csv` | 1 row per supplier | country, standard lead time |
| master | `uae_retail_calendar.csv` | 1 row per day | Ramadan, Eid, DSF, back-to-school flags |
| sales | `sales_YYYY_MM.csv` | 1 row per day × store × product × type | **one file per month** for incremental loading; SALE and RETURN rows |
| inventory | `inventory_snapshot_YYYY_MM.csv` | weekly (Sunday) on-hand per location × product | stores and DCs |
| purchasing | `purchase_orders.json` | nested PO header → lines | API-style export |
| purchasing | `goods_receipts.csv` | 1 row per receipt | received vs rejected qty |
| purchasing | `store_transfers.csv` | 1 row per DC → store shipment | |
| promotions | `promotions.csv` | 1 row per product per campaign | discount % |

### Deliberate data quality issues (your Silver layer must fix these)
- ~0.3% **exact duplicate** sales rows
- ~1% store codes in **lower case with trailing spaces** (`st003 `)
- ~0.2% sales rows with **missing `unit_price`**
- 40 sales rows with **product codes that don't exist** in the master (orphans)
- From **Apr 2026** the POS vendor changed: dates arrive as **DD/MM/YYYY** instead of YYYY-MM-DD
- **Returns** stored as negative quantities
- 25 POs carry the **supplier name in upper case** instead of the supplier code
- Supplier names with **stray whitespace**; 3 product categories in **UPPER CASE**

### Answer key (`answer_key/` — do NOT ingest)
`lost_sales_and_waste_truth.csv` holds the true lost demand (stock-outs) and perishable waste
from the simulation. A real POS never records lost sales, so you must **estimate** them in Gold
(e.g. zero-stock days × average daily sales). Use the answer key afterwards to check how good
your estimate is — a great talking point in interviews.

## Regenerating the data
```bash
pip install numpy pandas
python generate_data.py   # ~1 minute, fixed random seed so results are reproducible
```

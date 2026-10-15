# Olist E-Commerce Medallion Pipeline

An end-to-end data engineering project built on the real-world [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), implementing a full **Bronze → Silver → Gold** medallion architecture on Databricks using **Lakeflow Spark Declarative Pipelines (SDP)**, with a **Power BI** dashboard on top.

The goal of this project was not just to move data through layers, but to make and defend real data engineering decisions: incremental ingestion, data quality enforcement, star-schema modeling, and business-ready reporting — all on messy, real-world data rather than a clean synthetic sample.

---

## Architecture

```
Raw CSVs (Volume)
      │
      ▼
┌─────────────┐     Auto Loader (cloudFiles), schema evolution,
│   BRONZE    │     rescued-data handling, full audit trail
└─────────────┘
      │
      ▼
┌─────────────┐     Type casting, deduplication, data quality
│   SILVER    │     expectations (fail / drop / warn), joins
└─────────────┘
      │
      ▼
┌─────────────┐     Star schema: dimensions + fact tables
│    GOLD     │     (materialized views)
└─────────────┘
      │
      ▼
┌─────────────┐     Aggregated, business-ready views
│    MARTS    │     (revenue, sellers, delivery, RFM)
└─────────────┘
      │
      ▼
┌─────────────┐
│  POWER BI   │     Live dashboard via Databricks SQL
└─────────────┘
```

**Why Spark Declarative Pipelines instead of notebooks:** SDP lets each table be declared as the *definition* of its desired state rather than a sequence of imperative steps. The engine resolves the dependency graph, manages checkpoints and incremental file tracking for streaming tables, and enforces data quality rules natively — closer to how production pipelines are actually built than a linear notebook.

---

## Tech Stack

- **Databricks** (Lakeflow / Spark Declarative Pipelines)
- **Delta Lake** — storage format for every layer
- **Auto Loader (`cloudFiles`)** — incremental, schema-aware ingestion
- **PySpark** — all transformation logic
- **Unity Catalog** — catalog/schema governance (`bronze`, `silver`, `gold`)
- **Power BI Desktop** — BI layer, connected live via Databricks SQL

---

## Bronze Layer

Nine streaming tables, one per raw source file (`orders`, `order_items`, `payments`, `reviews`, `customers`, `sellers`, `products`, `geolocation`, `category_translation`), each built with:

- `cloudFiles` (Auto Loader) format, explicitly pointed at a persisted schema location
- `cloudFiles.schemaEvolutionMode = "addNewColumns"` — new columns in future files are added automatically rather than breaking the pipeline
- Two audit columns on every table: `ingestion_timestamp`, `source_file`
- No data validation — Bronze's only job is to faithfully capture the raw source, including malformed rows (captured via `_rescued_data`)

## Silver Layer

Each Bronze table is cleaned into a corresponding Silver table:

- String columns cast to their real types (timestamps, doubles)
- Deduplication on the correct grain — **not always a single ID column**. `payments_silver` and `order_items_silver` are deduplicated on composite keys (`order_id` + `payment_sequential` / `order_item_id`), since a single order can have multiple payments or items; naively deduplicating on `order_id` alone would have silently discarded real data.
- **Three-tier data quality enforcement**, chosen deliberately per rule rather than uniformly:
  - `expect_or_fail` — for missing primary/foreign keys that make a row unusable (e.g., null `order_id`)
  - `expect_or_drop` — for rows that are genuinely invalid but shouldn't halt the whole pipeline (e.g., `review_score` outside 1–5, `payment_value <= 0`)
  - `expect` (warn-only) — for values that are unusual but may be legitimate (e.g., an unrecognized `order_status`)
- `products_silver` performs a **stream-static join** against `category_translation_bronze` (a small, static lookup table) to translate Portuguese category names to English — read with `.read` rather than `.readStream` on the lookup side, since joining two streams would require windowed stream-stream join semantics that don't apply to a static reference table.

### A real debugging example

While building `reviews_silver`, a single malformed row (caused by an unescaped character in a free-text review field shifting the CSV column alignment) caused an `expect_or_fail` rule to halt the entire pipeline. The fix wasn't to loosen the rule blindly — it was to reconsider the *severity*: a corrupted row from a parsing artifact is a different category of problem than a row with a genuinely missing key, so the rule was changed to `expect_or_drop`. This let ~99% of valid reviews load while quarantining the ~5% that were either corrupted or had out-of-range scores.

## Gold Layer — Star Schema

**Dimensions** (descriptive, one row per entity):
- `dim_customer`, `dim_seller`, `dim_product`
- `dim_date` — a generated calendar table (not derived from Silver), with a `date_key` (`yyyyMMdd` integer) used as the standard join key across all fact tables
- `dim_payment_type` — a small lookup of distinct payment methods, with a generated surrogate key (`row_number()` over a `Window`)

**Facts** (events, with measures):
- `fact_order_item` — item-grain bridge table joining `order_items`, `product`, `seller`, and `date`
- `fact_payment` — payment-grain table referencing `dim_payment_type` by surrogate key rather than carrying the raw text value
- `fact_order` — order-grain table, built by aggregating `fact_payment` (summed by `order_id`) before joining, since payments are multi-row-per-order and orders are not

All dimension joins use `left` joins, not `inner` — an inner join would silently drop any fact row whose dimension lookup didn't match, losing real data with no error.

### Modeling decisions worth calling out

- An early version of this schema included a table named `dim_payment`, built by aggregating `payments_silver` directly to the order grain. On review, this was reclassified: it held only aggregated numeric measures (`total_payment_value`, `payment_count`) with no descriptive attributes — the defining trait of a **fact** table, not a dimension. It was split into a proper `dim_payment_type` (the genuinely dimensional attribute, payment method) and `fact_payment` (the event-grain data), with `fact_order` aggregating from `fact_payment` directly rather than depending on a mislabeled intermediate table.
- `fact_order_item` and a seller performance mart were initially joined naively against `fact_order`; this was caught before implementation as a **grain mismatch / fan-out risk** — joining item-grain data to order-grain data would have duplicated each order's payment total once per item in that order, overstating revenue. Seller performance is computed directly from `fact_order_item`, which already holds all needed measures at the correct grain.

## Business Marts

Four marts answering specific business questions, built on top of the star schema:

| Mart | Question it answers |
|---|---|
| `mart_daily_revenue` / `mart_monthly_revenue` | Revenue, order count, and average order value over time |
| `mart_seller_performance` | Revenue, items sold, and distinct products per seller |
| `mart_delivery_performance` | On-time / late / early delivery rate and average delay, by customer state |
| `mart_customer_rfm` | Recency, Frequency, Monetary scoring and customer segmentation |

### RFM notes

RFM scores are computed per **`customer_unique_id`** (the person-level identifier), not `customer_id` (which is unique per order) — grouping by the wrong key would have made every customer appear to have a frequency of 1. Recency, frequency, and monetary are each scored into quintiles (1–5) using `ntile()` over a `Window`, then combined into a 3-digit `rfm_score`.

**Known limitation, stated honestly rather than hidden:** the vast majority of customers in this dataset placed only one order, which means the *Frequency* dimension has almost no variance. RFM is most informative for businesses with meaningful repeat-purchase behavior; seeing limited frequency variation here is itself a legitimate finding about this dataset, not a flaw in the implementation.

### A second debugging example

Several customers showed a `null` monetary value in the RFM mart. Tracing it back: `payments_silver` had zero null `payment_value` rows, so the null wasn't coming from the source data directly — it was introduced by `fact_order`'s left join to the payment aggregation, for orders with **no matching payment rows at all**. All four affected orders turned out to be `canceled` — three correctly so (a canceled order was never paid), but one was `delivered` with no payment record, a genuine data anomaly worth flagging rather than silently smoothing over. The fix was `coalesce(monetary, 0)`, with the anomaly noted rather than ignored.

## Dashboard

Built in Power BI Desktop, connected live to Databricks via the native Databricks connector (Import mode):

- **Daily revenue trend** — line chart, with a visible Black Friday spike
- **Top sellers by revenue** — bar chart
- **Late delivery % by state** — bar chart, correctly normalized by percentage rather than raw count (raw late-order counts would simply reflect order volume, not delivery reliability)
- **Customer segment distribution** — bar chart over `rfm_score` / `customer_segment`

![Dashboard overview](dashboard/screenshots/overview.png)

---

## Project Structure

```
olist-medallion-pipeline/
├── README.md
├── pipeline/
│   └── transformations/
│       ├── 01_bronze/      # 9 streaming tables, Auto Loader ingestion
│       ├── 02_silver/      # cleaned, typed, deduplicated, quality-checked
│       └── 03_gold/        # dimensions, facts, and business marts
└── dashboard/
    └── screenshots/
```

## Reproducing This Project

1. A Databricks workspace with Unity Catalog and the ability to create Lakeflow Spark Declarative Pipelines
2. Download the [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and upload the CSVs into a Volume, one subfolder per source table
3. Create a catalog with `bronze`, `silver`, and `gold` schemas
4. Create a Pipeline pointed at the `pipeline/transformations/` folder and run it — Bronze, Silver, and Gold tables are created automatically in dependency order
5. Connect Power BI Desktop to the Databricks SQL warehouse and build visuals on top of the `gold` schema marts

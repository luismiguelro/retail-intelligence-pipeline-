# Retail Intelligence Pipeline

> **Merchandising analytics for a retail chain** — from raw CSV to an interactive dashboard, fully automated with dbt and Streamlit.

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt&logoColor=white)](https://getdbt.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.56-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io)
[![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL_17-3ECF8E?logo=supabase&logoColor=white)](https://supabase.com)
[![dbt tests](https://img.shields.io/badge/dbt_tests-52%2F52_passing-27AE60)](retaildbt/)

**[🔗 Live Demo →](https://retail-intelligence-blinkit.streamlit.app)**

---

## The Business Question

> *"Which products are occupying shelf space in our stores — and is that space being used efficiently?"*

Most retail datasets come with sales figures and the answer feels obvious: show revenue by SKU.
This dataset didn't have sales data. Instead of forcing metrics that weren't there, the analysis pivots to a question **equally important to a category manager**:

- Are the right products getting shelf visibility?
- Do premium-priced items get proportionally more shelf space?
- Which store format has the highest revenue potential, and why?

The metric that makes this possible: `weighted_revenue_potential = price × shelf_fraction`.
Not real sales — a **prioritization signal** derived from what we actually have.

---

## Architecture

```
  RAW DATA            INGEST             CLEAN              MODEL                DASHBOARD
  ──────────────────────────────────────────────────────────────────────────────────────────
  Blinkit Dataset  →  Python          →  Validation     →  dbt Core          →  Streamlit
  (Kaggle CSV)        psycopg2            & correction       Staging layer         + Plotly
  5,681 rows          01_ingest.py        of source data     stg_sales (view)
                      ↓                   02_clean.py        ↓
                  raw_sales                              Analysis layer
                  (Supabase)          clean_sales         dim_product  (table)
                                      (Supabase)          dim_outlet   (table)
                                                          fact_sales   (table)
                                                          ↓
                                                     52 / 52 tests ✅
```

**Data flow:**
1. CSV loaded into Supabase `raw_sales` via bulk insert (`execute_values` — 5,681 rows in ~5 sec)
2. `02_clean.py` handles nulls, normalizes fat content labels, fixes types → `clean_sales`
3. dbt staging layer renames columns and casts types
4. dbt marts build a star schema: two dimension tables + one fact table with the key derived metric
5. Streamlit reads directly from `public_marts.fact_sales` via SQLAlchemy

---

## Star Schema

```
                    ┌─────────────────┐
                    │  dim_product    │
                    │─────────────────│
                    │ product_key  PK │
                    │ item_id         │
                    │ item_name       │
                    │ item_category   │
                    │ item_fat_content│
                    │ price_tier      │  ← Budget / Mid-Range / Premium
                    └────────┬────────┘
                             │
  ┌──────────────┐   ┌───────▼──────────────────────────────┐
  │  dim_outlet  │   │              fact_sales               │
  │──────────────│   │──────────────────────────────────────│
  │ outlet_key PK│◄──│ fact_key            PK (md5 hash)    │
  │ outlet_id    │   │ product_key         FK                │
  │ outlet_type  │   │ outlet_key          FK                │
  │ outlet_size  │   │ item_mrp                              │
  │ outlet_tier  │   │ item_shelf_fraction                   │
  │ outlet_age   │   │ item_weight_kg                        │
  │ type_rank    │   │ price_tier                            │
  └──────────────┘   │ weighted_revenue_potential  ← KEY    │
                     └──────────────────────────────────────┘
                          5,681 rows · 52/52 dbt tests ✅
```

---

## Dashboard — 4 Analysis Views

| Tab | Question answered |
|---|---|
| **Overview** | How is the catalog split across Budget / Mid-Range / Premium? Which segment holds the most revenue potential? |
| **Store Types** | Which outlet format (Grocery vs Supermarket Type 1/2/3) has the highest catalog breadth and revenue potential? |
| **Shelf Space** | Which categories dominate shelf visibility — and does that align with price tier? |
| **Where's the money?** | Top 10 categories by revenue potential, efficiency ratio (potential per SKU), and segment breakdown |

Each view ends with an auto-generated insight calculated from live data, not hardcoded text.

---

## Key Findings

- **Mid-Range products** hold the largest revenue potential despite Budget SKUs outnumbering them 2:1 — the catalog is optimized for volume, not margin.
- **Shelf space and price are not correlated** — several Budget products occupy more shelf fraction than Premium ones, signaling a misaligned planogram.
- **Supermarket Type 1** stores show the highest individual potential with the broadest catalog, making them the ideal channel for premium product launches.
- The most *efficient* category (highest potential per SKU) is not the same as the one with the highest total potential — a distinction that matters for catalog expansion decisions with limited shelf space.

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python + psycopg2 | `execute_values` bulk insert — 4 round-trips vs 8,523 with `executemany` |
| Storage | Supabase (PostgreSQL 17) | Managed Postgres with connection pooler, free tier sufficient for the dataset |
| Transformation | dbt Core | Version-controlled SQL, layered models, built-in test suite |
| Dashboard | Streamlit + Plotly | Fastest path from a Postgres query to an interactive chart |
| Secrets | st.secrets (Cloud) / .env (local) | Same codebase runs in both environments without changes |

---

## Project Structure

```
retail-intelligence-pipeline/
├── src/
│   ├── 01_ingest.py        # CSV → raw_sales (Supabase bulk load)
│   ├── 02_clean.py         # raw_sales → clean_sales (nulls, types, labels)
│   ├── db_connection.py    # shared psycopg2 connection helper
│   └── app.py              # Streamlit dashboard (5 tabs, Plotly charts)
│
├── retaildbt/
│   └── models/
│       ├── staging/
│       │   ├── sources.yml         # Supabase source declaration
│       │   ├── schema.yml          # column-level docs + tests
│       │   └── stg_sales.sql       # rename + cast, materialized as view
│       └── marts/
│           ├── schema.yml          # not_null / unique / accepted_values tests
│           ├── dim_product.sql     # 1 row per unique product
│           ├── dim_outlet.sql      # 1 row per store
│           └── fact_sales.sql      # grain: product × outlet, weighted_revenue_potential
│
├── notebooks/
│   └── 01_eda.ipynb        # exploratory analysis — distributions, nulls, correlations
│
├── .streamlit/
│   └── config.toml         # theme + headless config for Cloud deploy
│
└── requirements.txt        # 6 dependencies (streamlit, pandas, plotly, sqlalchemy, psycopg2-binary, python-dotenv)
```

---

## Run Locally

**Prerequisites:** Python 3.12+, a Supabase project with the data loaded.

```bash
# 1. Clone and set up environment
git clone https://github.com/luismiguelro/retail-intelligence-pipeline-.git
cd retail-intelligence-pipeline-
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Configure credentials
cp .env.example .env   # then add your DATABASE_URL

# 3. Load data into Supabase
python src/01_ingest.py
python src/02_clean.py

# 4. Run dbt transformations
cd retaildbt
dbt run
dbt test    # should show 52/52 passing

# 5. Launch dashboard
cd ..
streamlit run src/app.py
```

**`.env` format:**
```
DATABASE_URL=postgresql://postgres.<ref>:<password>@aws-1-us-east-1.pooler.supabase.com:5432/postgres
```

---

## dbt Test Coverage

```
52 tests · 0 warnings · 0 errors

staging/
  stg_sales          → not_null (item_id, outlet_id, item_mrp, item_shelf_fraction)
                     → unique (item_id + outlet_id composite)

marts/
  dim_product        → not_null (product_key, item_id, item_category, price_tier)
                     → unique (product_key, item_id)
                     → accepted_values: price_tier in [Budget, Mid-Range, Premium]

  dim_outlet         → not_null (outlet_key, outlet_id, outlet_type, outlet_tier)
                     → unique (outlet_key, outlet_id)
                     → accepted_values: outlet_tier in [Tier 1, Tier 2, Tier 3]

  fact_sales         → not_null (fact_key, product_key, outlet_key, weighted_revenue_potential)
                     → unique (fact_key)
                     → relationships: product_key → dim_product, outlet_key → dim_outlet
```

---

## About the Dataset

**Source:** [Blinkit Grocery Dataset — Kaggle](https://www.kaggle.com/datasets/mukeshgpta/blinkit-grocery-data)

Blinkit is a rapid-delivery grocery chain in India. The dataset contains catalog information for 1,559 products distributed across 10 store locations (5,681 product × store combinations).

**Important note:** The downloaded file is the Kaggle competition test set — it does not include `item_outlet_sales`. Rather than sourcing a different dataset, the analysis reframes around a question this data *can* answer well: catalog positioning and shelf strategy. This is a real decision a category manager faces even with complete sales data.

---

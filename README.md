# 🛒 Retail Real-Time Data Warehouse & Analytics

An end-to-end **Retail Data Engineering and Analytics project** that simulates real-world retail events such as sales, returns, inventory updates, customer activity, and store operations.

The project demonstrates how raw and streaming retail data can be collected, stored, transformed, modeled, and visualized using modern data engineering tools.

---

## 📌 Project Overview

### Business Objective

Build a scalable retail analytics platform that helps businesses understand:

* Sales and revenue performance
* Product performance
* Inventory availability
* Store performance
* Customer behavior
* Product returns

### End-to-End Flow

```text
Python
   ↓
Redpanda / Kafka
   ↓
AWS S3
   ↓
Snowflake
   ↓
dbt
   ↓
Power BI
```

---

# 🏗️ Architecture

```text
┌──────────────────────┐
│   Python Generators  │
│                      │
│ Products             │
│ Customers            │
│ Stores               │
│ Retail Events        │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│       Redpanda       │
│ Kafka-Compatible     │
│ Event Streaming      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│       AWS S3         │
│ Historical Events    │
│ Retail Events        │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│      Snowflake       │
│                      │
│ Staging              │
│ Facts                │
│ Dimensions           │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│         dbt          │
│ Cleaning & Modeling  │
│ Business Logic       │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│       Power BI       │
│ Business Dashboards  │
└──────────────────────┘
```

---

# 🛠️ Technologies

| Technology | Purpose                            |
| ---------- | ---------------------------------- |
| Python     | Data generation & event production |
| Redpanda   | Kafka-compatible event streaming   |
| Docker     | Local streaming environment        |
| AWS S3     | Cloud storage                      |
| Snowflake  | Data warehouse                     |
| dbt        | Data transformation & modeling     |
| Power BI   | Analytics & visualization          |
| DAX        | Power BI business metrics          |
| Git/GitHub | Version control                    |

---

# 📂 Project Structure

```text
Retail-data-project/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── generator/
│   ├── products_generator.py
│   ├── stores_generator.py
│   ├── retail_live_producer.py
│   └── ...
│
├── dbt/
│   └── retail_dbt/
│       ├── models/
│       │   ├── staging/
│       │   ├── dimensions/
│       │   └── facts/
│       ├── schema.yml
│       └── dbt_project.yml
│
├── sql/
│   └── ...
│
├── powerbi/
│   └── ...
│
├── docker-compose.yml
└── README.md
```

---

# 📊 Data

The project contains simulated retail data for:

* **10,000+ customers**
* **500+ products**
* **100 stores**
* Historical retail events
* Continuously generated retail events

### Main Event Types

```text
SALE
RETURN
INVENTORY_RECEIVED
INVENTORY_ADJUSTMENT
```

---

# ☁️ AWS S3

AWS S3 is used as the cloud storage layer for retail event data.

Canonical retail event path:

```text
historical/retail_events/
```

The S3 layer acts as the landing/storage layer before data is processed in Snowflake.

---

# ❄️ Snowflake Data Warehouse

Snowflake is used as the central analytical warehouse.

### Main Layers

```text
S3
 ↓
Staging
 ↓
Fact & Dimensions
 ↓
Analytics
```

### Main Models

#### Staging

`STG_RETAIL_EVENTS`

Handles:

* Data type conversion
* NULL handling
* Data cleaning
* Standardization

#### Dimensions

```text
DIM_CUSTOMERS
DIM_PRODUCTS
```

#### Fact

```text
FACT_RETAIL_EVENTS
```

Contains sales, returns, and inventory events.

---

# 🔧 dbt

dbt is used for data transformation and dimensional modeling.

Key concepts demonstrated:

* Staging models
* `ref()`
* Incremental models
* Merge strategy
* Data cleaning
* Business transformations
* Fact and dimension modeling
* Data quality handling

Example:

```sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
) }}
```

---

# 🧹 Data Quality

The project intentionally includes real-world data quality problems such as:

* NULL values
* Missing product IDs
* Missing customer information
* Missing city information
* Negative values
* Duplicate events
* Inconsistent data

Missing product information is handled using an `UNKNOWN_PRODUCT` approach instead of simply dropping the event.

---

# 📈 Power BI Business Reports

The final analytics layer contains four major dashboards.

## 1. Executive Sales Dashboard

Answers:

> How is the retail business performing?

Key metrics:

* Total Revenue
* Total Units Sold
* Total Sales
* Total Customers
* Average Selling Price
* Return Rate

---

## 2. Product Performance Dashboard

Answers:

> Which products are performing well and where are inventory problems occurring?

Analysis includes:

* Top products by revenue
* Units sold
* Revenue by category
* Stock availability
* Low-stock products
* Out-of-stock products
* Inventory turnover

---

## 3. Inventory & Store Performance Dashboard

Answers:

> Which stores are performing best and how efficiently are they managing inventory?

Analysis includes:

* Store revenue
* Units sold
* Sales events
* Available inventory
* Inventory turnover
* Store comparison

---

## 4. Customer & Returns Dashboard

Answers:

> Who are our customers, which segments generate revenue, and where are returns affecting the business?

### Customer KPIs

* Total Customers
* VIP Customers
* Premium Customers
* Standard Customers
* Revenue per Customer

### Customer Analysis

* Revenue by Customer Segment
* Customers by Segment
* Customer City Revenue

### Returns Analysis

* Returned Units
* Returned Value
* Return Events
* Return Rate
* Returns by Product Category
* Returns by Product
* Returns by Customer Segment

---

# 📐 Key Business Metrics

### Revenue

```text
Quantity × Unit Price
```

### Revenue Per Customer

```text
Total Revenue ÷ Distinct Customers
```

### Return Rate

```text
Returned Units ÷ Sold Units × 100
```

### Inventory Turnover

```text
Units Sold ÷ Average Inventory
```

---

# 🔄 Data Pipeline

```text
Generate
   ↓
Stream
   ↓
Store
   ↓
Load
   ↓
Transform
   ↓
Model
   ↓
Analyze
   ↓
Visualize
```

This project demonstrates an end-to-end modern data engineering workflow from **raw retail events to business intelligence**.

---

# 🎯 Key Skills Demonstrated

* SQL
* Snowflake
* dbt
* Python
* AWS S3
* Redpanda / Kafka
* Docker
* Data Warehousing
* Dimensional Modeling
* ETL / ELT
* Incremental Data Processing
* Data Quality
* DAX
* Power BI
* Git & GitHub

---

# 🚀 Future Enhancements

* Airbyte Cloud integration
* Automated data ingestion
* Pipeline scheduling
* Data quality monitoring
* Real-time dashboard updates
* Production-style orchestration

---

# 👨‍💻 Author

**Harish Kumar**

Retail Data Engineering & Analytics Portfolio Project

**Tech Stack:**
`Redpanda` `AWS S3` `Snowflake` `dbt` `Power BI` `DAX` `Docker` `Git`

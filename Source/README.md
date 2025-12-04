# 🛒 Retail ETL with PySpark 

This project implements a **real-world style ETL pipeline** for a retail dataset (customers, products, and orders).  
It follows the **medallion architecture** (Bronze → Silver → Gold) with industry-standard optimizations and logging practices.

---

## 📂 Project Structure

├── data/
│ ├── input/ # Raw CSVs (ingestion)
│ ├── bronze/ # Cleaned parquet
│ ├── silver/ # Enriched & partitioned data
│ └── gold/ # Presentation KPIs
├── logs/ # ETL log files
├── notebooks/ # Jupyter notebooks (ingestion, transformation, gold KPIs)
├── docker-compose.yml
├── Dockerfile
└── README.md


## ⚙️ Pipeline Overview

### 🔹 Bronze Layer
- Ingest raw `orders`, `customers`, and `products`
- Store as **parquet**
- Add ingestion metadata (timestamps, source)

### 🔹 Silver Layer
- Join orders with customers & products  
- Compute `order_value`  
- Partition by `region`, `Year`, `Month`  
- Optimize small file writes (`coalesce`, `repartition`)  

### 🔹 Gold Layer
- Customer KPIs → Lifetime Value, Avg Order Value, Active Months  
- Product KPIs → Revenue, Units Sold, Avg Transaction Value  
- Regional KPIs → Revenue & Units by Month  

---


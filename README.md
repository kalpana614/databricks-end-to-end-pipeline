# Databricks End-to-End Pipeline (Medallion Architecture)

This project demonstrates a complete end-to-end data pipeline using **Databricks** and the **Medallion Architecture** (Bronze → Silver → Gold).

Key features of the pipeline:
- 🔁 **Streaming ingestion** using **Databricks Autoloader**
- 🧪 **Schema evolution** and rescue mode handling
- 🧱 **Bronze layer** for raw data ingestion
- 🧼 **Silver layer** built using **LakeFlow (Delta Live Tables)** for cleansing, CDC, and enrichment
- 🧠 **Data quality rules** and expectations with `@dlt.expect_all`
- 🔁 **CDC handling** with `create_auto_cdc_flow()` (SCD Type 1)
- 📊 **Business-ready joins** and transformations in the Silver layer
- 📦 (Optional) **Gold layer** for aggregations and reporting

This pipeline processes streaming datasets like **bookings, flights, passengers, and airports**, and transforms them into reliable, structured data ready for analytics.

📌 Technologies used:
- Databricks Lakehouse Platform
- PySpark
- Delta Lake
- Autoloader
- LakeFlow / Delta Live Tables (DLT)
- Structured Streaming

---

Feel free to fork, explore, or suggest improvements!

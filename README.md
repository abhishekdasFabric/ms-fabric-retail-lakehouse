# ms-fabric-retail-lakehouse
End-to-End Retail Sales &amp; Inventory Optimization Lakehouse built on Microsoft Fabric, PySpark, Medallion Architecture, and Power BI.
# Retail Sales & Inventory Optimization Lakehouse (Microsoft Fabric)
[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-Certified-0078D4?logo=microsoft)](https://learn.microsoft.com) [![PySpark](https://img.shields.io/badge/PySpark-3.4+-E25A1C?logo=apachespark)](https://spark.apache.org) [![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion-00ADF0)](https://delta.io)

This repository contains the end-to-end implementation of an enterprise-grade retail analytics lakehouse using **Microsoft Fabric**, **Delta Lake**, **PySpark**, and **Power BI Direct Lake mode**.

## 🏗️ Architecture Overview

The project follows the **Medallion Architecture** pattern:
- **Bronze Layer:** Raw data ingestion from POS systems, MongoDB, and SFTP into OneLake using Azure Data Factory pipelines.
- **Silver Layer:** Data scrubbing, null handling, deduplication, and schema validation via Fabric PySpark Notebooks.
- **Gold Layer:** Dimensional Star Schema modeling (Fact_Sales, Dim_Product, Dim_Store, Dim_Customer) stored in Delta format.
- **Reporting:** Real-time Power BI reporting using Direct Lake mode with Copilot AI narrative integration.

## 🚀 Key Features & Innovations

- **Direct Lake Mode:** Achieved sub-second DAX query execution without data duplicate refreshes. - **Automated Data Quality:** PySpark validation rules enforce constraints before promotion to Silver. - **Power BI Copilot Integration:** Embedded natural language query engines for rapid narrative generation.

## 🛠️ Tech Stack

Microsoft Fabric, PySpark, Azure Data Factory, Delta Lake, Power BI Desktop/Service, T-SQL, DAX, Git, Azure DevOps.

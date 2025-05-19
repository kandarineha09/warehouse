# 🏗️ MySQL Data Warehouse ETL Pipeline

This project demonstrates the design and implementation of a *Data Warehouse ETL pipeline* using *MySQL*.
It covers a full pipeline from raw data ingestion (bronze layer) to cleaned and transformed data (silver layer), using SQL scripts and stored procedures.

This repository contains a MySQL-based ETL (Extract, Transform, Load) pipeline designed with Bronze and Silver staging layers to enable clean, structured, and analytics-ready data storage.

🚀 Project Overview
This ETL pipeline simulates a real-world data warehousing process using raw CSV data files, with the following components:

Bronze Layer: Raw data ingestion from various CSV sources.
Silver Layer: Cleaned, transformed, and standardized data ready for analytics.
Stored Procedures: Modular transformation logic wrapped in a MySQL stored procedure for automation.

📂 Pipeline Structure
1. Bronze Layer
Initial raw data is loaded from CSVs into the following tables:

bronze_crm_cust_info

bronze_crm_prd_info

bronze_crm_sales_details

bronze_erp_cust_az12

bronze_erp_loc_a101

bronze_erp_px_cat_g1v2

2. Silver Layer
Data is cleaned, transformed, and loaded into these tables:

silver_crm_cust_info

silver_crm_prd_info

silver_crm_sales_details

silver_erp_cust_az12

silver_erp_loc_a101

silver_erp_px_cat_g1v2

3. Stored Procedure
A single procedure named load_silver():

Applies conditional logic to fix inconsistent gender, marital status, and date values.

Fixes nulls in date fields.

Ensures price, sales, and quantity consistency.



Author
Neha
Roll No: 24103
Dronacharya College of Engineering, MDU Rohtak

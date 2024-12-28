![image](https://github.com/user-attachments/assets/471c9080-c4e4-4490-b161-29ab783ded26)# Data Pipeline for Taxi Trip Data Analysis

**Overview**
This repository contains the code and resources for a data pipeline that analyzes taxi trip data using Azure Databricks and Delta Lake. The pipeline ingests raw data, transforms it, and stores it in Delta Lake for subsequent analysis and reporting.

**Architecture**
![image](https://github.com/user-attachments/assets/0028a3b3-aa54-4916-9385-b0d4b52f191a)

**The pipeline follows a three-stage architecture:**

**Ingestion:**
Raw data is ingested from a source (e.g., CSV files) and NYC TAXI Open source API into a raw data store in Data Lake Gen2.
Databricks Data Factory is used to orchestrate the ingestion process.

**Transformation:**
Data is transformed using Databricks notebooks with Spark SQL and Python.
Transformations include cleaning, filtering, and aggregating data.
Transformed data is stored in Parquet format in Data Lake Gen2.

**Serving:**
Transformed data is optimized for serving using Delta Lake's features like ACID transactions and schema evolution.
Data is stored in Delta Lake format in Data Lake Gen2.

**Key Technologies**
1. Azure Databricks
2. Delta Lake
3. Data Lake Gen2
4. Databricks Data Factory
5. Spark SQL
6. Python

**Project Snaps:**
![image](https://github.com/user-attachments/assets/32f10839-2792-4338-a5fc-95b42f5ef63d)
![image](https://github.com/user-attachments/assets/d5324f03-5f19-477f-b296-3e5562fe3959)
![image](https://github.com/user-attachments/assets/a8c81fdb-2a1d-48bb-a72d-c23ba7aab9cd)
![image](https://github.com/user-attachments/assets/e4ab3c05-858e-4b11-aabc-e907d7df3a48)
![image](https://github.com/user-attachments/assets/d36e2cd0-1a82-466d-9b48-46f9ceed6ad4)
![image](https://github.com/user-attachments/assets/710b5610-c510-434f-b7ce-79b69e443652)


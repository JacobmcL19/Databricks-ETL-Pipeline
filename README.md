# Sales Data ETL Pipeline in Azure Databricks

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline in Databricks for processing sales data from a CSV file. The pipeline extracts data, applies necessary transformations, and loads the cleaned dataset into a Delta table for further analysis.

## Data Source
- **File:** `supermarket_sales - Sheet1.csv`
- **Format:** CSV
- **Contents:** Sales transaction data including branch, city, customer type, product details, and purchase amounts.

## ETL Pipeline Structure
The pipeline consists of three main steps:

### 1. Extraction (`extract.py`)
- Reads the sales data from the provided CSV file.
- Loads raw data into a temporary Delta table for further processing.
- Uses PySpark to efficiently handle large datasets.

### 2. Transformation (`transform.ipynnb`)
- Cleans the dataset by handling missing values and renaming columns.
- Converts data types where necessary.
- Removes duplicate entries to ensure data integrity.

### 3. Loading (`load.ipynb`)
- Writes the transformed data into a final Delta table.
- Optimizes the table for efficient querying and analysis in Databricks.

## Technologies Used
- **Databricks** for cloud-based data processing.
- **Apache Spark** for scalable data transformations.
- **Delta Lake** for efficient and reliable data storage.
- **Python (PySpark)** for implementing the ETL logic.

## How to Use
1. Upload the `supermarket_sales - Sheet1.csv` file to Databricks.
2. Run `extract.py` to load raw data into a temporary table.
3. Execute `transform.py` to process and clean the data.
4. Finally, run `load.py` to store the cleaned dataset into a Delta table.

## Future Improvements
- Implement real-time data processing using Structured Streaming.
- Integrate with external databases or cloud storage for seamless data ingestion.
- Add data validation and quality checks to enhance reliability.

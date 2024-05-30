# Anomalies Detection in API-logs using Spark

## Project Analysis:

### Objective:
The project aims to develop a robust Spark application for analyzing Apache Access logs, specifically focusing on detecting anomalies in API logs. Leveraging the power of PySpark, it delves into various dimensions such as response codes, traffic patterns, content size statistics, top endpoints, frequent visitors, and bad requests.

### Methodology:

#### Data Preparation:
1. **Creation of necessary directories and upload of sample log data (sample_logs.csv)**.

#### Data Processing and Analysis:
- Load the log data into a Spark DataFrame.
- Clean and filter the data to ensure data quality.
- Conduct basic data exploration to gain insights into the dataset.
- Analyze trends based on response codes and traffic patterns.
- Calculate content size statistics and identify top endpoints by content size.
- Perform in-depth analysis of response codes and anomalies detection.

#### Output Generation:
- Save the analysis results into CSV files for further reference and reporting.

## Project Summary/Explanation:

### Overview:
This project harnesses the capabilities of PySpark within a Databricks notebook to conduct thorough analysis of Apache Access logs. By employing advanced data processing techniques, it extracts actionable insights to detect anomalies in API logs.

### Key Highlights:
- Comprehensive data exploration, cleaning, and filtering processes.
- Detailed trend analysis encompassing response codes, traffic patterns, and content size statistics.
- Identification of frequent visitors, top endpoints, and anomalies such as bad requests.
- Generation of insightful output in the form of CSV files for further analysis and reporting.

## Reference:

### Framework:
- PySpark: A powerful distributed data processing engine for large-scale data analytics.
- Databricks: An integrated platform for data engineering and collaborative data science.

### Documentation:
- Apache Spark: Official documentation for Apache Spark framework.

## Final Output:

### Analysis Results:
- **response_code_stats.csv**: Contains detailed statistics on response codes.
- **frequent_ips.csv**: Provides insights into frequent IP addresses accessing the server.
- **latest_404_requests.csv**: Lists the latest 404 requests along with their endpoints and timestamps.


.

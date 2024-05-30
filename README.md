# Anomalies Detection in API Logs using Spark

## Project Overview
This project aims to develop a Spark application for analyzing Apache Access logs to detect anomalies in API logs. By analyzing various aspects of the log data, such as response codes, traffic patterns, and content size statistics, the application aims to identify potential anomalies and irregularities in the server's behavior.

## Features
- **Data Extraction and Cleaning**: Extract information from semi-structured Apache Access logs and clean the data to remove inconsistencies and missing values.
- **Traffic Analysis**: Analyze traffic patterns to identify unusual spikes or drops in request volume, as well as unusual distribution of response codes.
- **Content Size Statistics**: Calculate statistics related to content size, including minimum, maximum, and average content size, as well as identifying top endpoints transferring maximum content.
- **Frequent Visitors**: Identify IP addresses accessing the server more than a specified threshold to detect potential security threats or excessive usage.
- **Bad Requests Analysis**: Analyze bad requests, including the top latest 404 requests with their endpoints and timestamps, to identify broken links or potential security vulnerabilities.

## How to Run
1. **Data Preparation**:
    - Upload the Apache Access log data to Databricks in CSV format.
    - Ensure that the log data is structured and contains columns for IP address, timestamp, endpoint, response code, and content size.

2. **Notebook Execution**:
    - Import the provided `AnomalyDetection.dbc` notebook into your Databricks workspace.
    - Open the notebook and execute each cell sequentially to load, clean, and analyze the log data.
    - Review the output of each cell to understand the results of the analysis.

3. **View Results**:
    - After executing the notebook, review the generated visualizations and summary statistics to gain insights into the log data.
    - Explore the CSV files containing detailed statistics, such as response code counts, frequent IP addresses, and latest 404 requests.

4. **Further Analysis**:
    - Modify the notebook as needed to perform additional analysis or customize the existing analysis based on specific requirements or use cases.
    - Experiment with different Spark transformations and actions to gain deeper insights into the log data.

## Dependencies
- Apache Spark: The project requires Apache Spark for distributed data processing and analysis.
- Databricks: The notebook is designed to run on the Databricks platform for scalable data analysis in the cloud.

## Contributor
- [Sudarshan Poudel](https://github.com/darsan23): Project Developer

.

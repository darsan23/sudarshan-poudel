# Databricks notebook source
# Create the directory if it doesn't exist
dbutils.fs.mkdirs("/FileStore/log_data/")

# Upload the file content
dbutils.fs.put("/FileStore/log_data/sample_logs.csv", """
ip,timestamp,endpoint,response_code,content_size
192.168.1.1,2023-05-01T00:00:01Z,/api/v1/resource,200,512
192.168.1.2,2023-05-01T00:00:02Z,/api/v1/resource,404,0
192.168.1.1,2023-05-01T00:00:03Z,/api/v1/resource,200,256
192.168.1.3,2023-05-01T00:00:04Z,/api/v1/resource,500,128
192.168.1.1,2023-05-01T00:00:05Z,/api/v1/resource,200,1024
""", True)

# List files in the /FileStore/log_data directory to verify upload
display(dbutils.fs.ls("dbfs:/FileStore/log_data/"))


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, avg, sum, desc

# Load the log data from the uploaded CSV file
log_df = spark.read.csv("dbfs:/FileStore/log_data/sample_logs.csv", header=True, inferSchema=True)
log_df.show(5)

# Clean and filter the logs
filtered_logs = log_df.select("ip", "timestamp", "endpoint", "response_code", "content_size") \
    .filter(col("response_code").isNotNull())
filtered_logs.show(5)

# Basic Data Exploration
filtered_logs.printSchema()
filtered_logs.describe().show()
log_count = filtered_logs.count()
print(f"Total number of log entries: {log_count}")

# Analyze Trends Based on Response Codes and Traffic
response_code_counts = filtered_logs.groupBy("response_code").agg(count("response_code").alias("count")).orderBy("count", ascending=False)
response_code_counts.show()

# Top 10 most frequent IP addresses
top_ips = filtered_logs.groupBy("ip").agg(count("ip").alias("count")).orderBy("count", ascending=False).limit(10)
top_ips.show()

# Calculate Content Size Statistics
content_size_stats = filtered_logs.select(
    min("content_size").alias("min_size"),
    max("content_size").alias("max_size"),
    avg("content_size").alias("avg_size")
)
content_size_stats.show()

# Top Endpoints by Content Size
top_endpoints_by_size = filtered_logs.groupBy("endpoint").agg(sum("content_size").alias("total_size")).orderBy("total_size", ascending=False).limit(10)
top_endpoints_by_size.show()

# Analyze Response Codes
response_code_stats = filtered_logs.groupBy("response_code").count().orderBy("count", ascending=False)
response_code_stats.show()

# Identify IP Addresses Accessing Server More Than 10 Times
frequent_ips = filtered_logs.groupBy("ip").count().filter(col("count") > 10).orderBy("count", ascending=False)
frequent_ips.show()

# Analyze Bad Requests (404 Errors)
bad_requests = filtered_logs.filter(col("response_code") == 404)
latest_404_requests = bad_requests.orderBy(col("timestamp").desc()).select("timestamp", "endpoint").limit(10)
latest_404_requests.show()

# Save Results to CSV Files
response_code_stats.write.csv("dbfs:/FileStore/log_data/response_code_stats.csv", header=True)
frequent_ips.write.csv("dbfs:/FileStore/log_data/frequent_ips.csv", header=True)
latest_404_requests.write.csv("dbfs:/FileStore/log_data/latest_404_requests.csv", header=True)


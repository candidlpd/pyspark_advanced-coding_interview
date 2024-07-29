# Databricks notebook source
sc

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

pip install pillow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Access the Spark context
sc = spark.sparkContext

# Display Spark context information
spark_context_info = {
    "Application Name": sc.appName,
    "Master URL": sc.master,
    "Version": sc.version,
    "Is Active": sc._jsc is not None
}

display(spark_context_info)

# COMMAND ----------

!java -version

# COMMAND ----------

import sys
print(sys.executable)

# COMMAND ----------

!pip list

# COMMAND ----------

# Define the data and columns
data = [
    ('Alice', 'Badminton, Tennis'),
    ('Bob', 'Tennis, Cricket'),
    ('Julie', 'Cricket, Carroms')
]
columns = ["Name", "Hobbies"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Display the DataFrame
df.show()

# COMMAND ----------



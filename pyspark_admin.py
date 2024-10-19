# Databricks notebook source
sc

# COMMAND ----------

spark

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

import sys
print(sys.executable)

# COMMAND ----------

!pip --version

# COMMAND ----------

# MAGIC %fs ls /
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/lpd")

# COMMAND ----------

dbutils.fs.ls("/mnt/lpd")

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %fs ls

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

!pip list

# COMMAND ----------

import sys
print(sys.executable)

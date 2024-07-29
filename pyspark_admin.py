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



# Databricks notebook source
sc

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

import sys
print(sys.executable)

# COMMAND ----------

!pip3 --version

# COMMAND ----------

configurations = spark.sparkContext.getConf().getAll()
display(configurations)

# COMMAND ----------



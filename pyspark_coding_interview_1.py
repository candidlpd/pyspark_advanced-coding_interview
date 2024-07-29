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

from IPython.display import Image, display

# Specify the path to the uploaded image
image_path = '/Workspace/Users/100707-anv-28@deccansoftstudents.onmicrosoft.com/pyspark-coding-interview/data/name_hobbies.png.png
'

# Display the image
display(Image(filename=image_path))

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

from pyspark.sql.functions import split
df_array = df.withColumn("Hobbies_array", split(df.Hobbies, ','))
display(df_array.show())

# COMMAND ----------

from pyspark.sql.functions import explode

df_flatten = df_array.withColumn("Hobbies_flatten", explode(df_array["Hobbies_array"])).drop("Hobbies_array", "Hobbies")
display(df_flatten.show())

# COMMAND ----------



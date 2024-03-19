# Databricks notebook source
# MAGIC %md #### Workshop for ETL 

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)
df_driver.count()

# COMMAND ----------

display(df_driver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform Data

# COMMAND ----------

# Adding new column to df_driver dataframe, which takes a string and values for the column
# Before we can get it to work, we need to import pyspark.sql
df_driver = df_driver.withColumn("age", datediff(current_date(), df_driver.dob)/365)

# COMMAND ----------

display(df_driver)

# COMMAND ----------

#We need to cast age as an integer
#Note if you see a red squiggly line, hover over it 

df_driver = df_driver.withColumn('age', df_driver['age'].cast(IntegerType()))

# COMMAND ----------

display(df_driver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining Dataframes

# COMMAND ----------

#Like SQL, we need to have the attribute you are joining on in your select statement
df_lap_drivers = df_driver.select('driverId', 'nationality', 'age', 'surname', 'forename', 'dob', 'url').join(df_laptimes, on=['driverId'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate By Age

# COMMAND ----------

df_lap_drivers = df_lap_drivers.groupBy('nationality', 'age').agg(avg('milliseconds'))

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Storing Data in S3

# COMMAND ----------

df_lap_drivers.write.csv('s3://sg4283-gr5069/processed/in_class_workshop/laptimes_by_drivers.csv')

# COMMAND ----------



# Databricks notebook source
type(spark)

# COMMAND ----------

data = [(1,"Lokesh"),(2,"Tadashi"),(3,"Virat"),(4,"Hitman")]
schema = ['id', "name"]
df = spark.createDataFrame(data = data, schema=schema)
display(df)

# COMMAND ----------

data = [(1,"Shark"),(5,"BlueLock")]
schema = ['id', "name"]
df1 = spark.createDataFrame(data = data, schema=schema)
display(df1)

# COMMAND ----------

df.write.csv(path='dbfs:/tmp/emps', header=True, mode="overwrite")

# COMMAND ----------

df1.write.csv(path='dbfs:/tmp/emps', header=True, mode="append")

# COMMAND ----------

df_read = spark.read.csv(path='dbfs:/tmp/emps', header=True, inferSchema=True)
display(df_read.orderBy("id"))

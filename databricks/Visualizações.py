# Databricks notebook source
df = spark.read.format("parquet").load("/covidPartByYearMonth.parquet")

# COMMAND ----------

display(df.filter(df.CountryCode == 'US'))

# COMMAND ----------

df.createOrReplaceTempView("covid")

# COMMAND ----------

df.select(df.date)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select  countryCode
# MAGIC FROM covid 
# MAGIC where date= "2021-06-09"
# MAGIC order by Deaths DESC  
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Name as Paises,
# MAGIC   date schema,
# MAGIC   Deaths AS Mortes
# MAGIC FROM covid
# MAGIC where CountryCode in(
# MAGIC     Select  countryCode
# MAGIC     FROM covid 
# MAGIC     where date= "2021-06-09"
# MAGIC     order by Deaths DESC  
# MAGIC     limit 3
# MAGIC )
# MAGIC and date like "%-01"
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Name as Paises,
# MAGIC   date schema,
# MAGIC   Deaths AS Mortes
# MAGIC FROM covid
# MAGIC where CountryCode in(
# MAGIC     Select  countryCode
# MAGIC     FROM covid 
# MAGIC     where date= "2021-06-09"
# MAGIC     order by Deaths DESC  
# MAGIC     limit 10
# MAGIC )
# MAGIC and date like "%-01"
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Name as Paises,
# MAGIC   Deaths AS Mortes
# MAGIC FROM covid
# MAGIC where 
# MAGIC date = "2021-06-09"
# MAGIC ORDER BY Mortes DESC
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Name as Paises,
# MAGIC   Confirmed AS Confirmados
# MAGIC FROM covid
# MAGIC where 
# MAGIC date = "2021-06-09"
# MAGIC ORDER BY Confirmados DESC
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Name as Paises,
# MAGIC   date schema,
# MAGIC   Confirmed AS Confirmados
# MAGIC FROM covid
# MAGIC where CountryCode in(
# MAGIC     Select  countryCode
# MAGIC     FROM covid 
# MAGIC     where date= "2021-06-09"
# MAGIC     order by Confirmed DESC  
# MAGIC     limit 10
# MAGIC )
# MAGIC and date like "%-01"
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Name as Paises,
# MAGIC   date schema,
# MAGIC   Confirmed AS Confirmados
# MAGIC FROM covid
# MAGIC where Name = 'India'
# MAGIC 
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Name as India,
# MAGIC   date schema,
# MAGIC   Active AS Ativos
# MAGIC FROM covid
# MAGIC where Name = 'India'
# MAGIC 
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Name, date schema,
# MAGIC   Active AS Ativos
# MAGIC FROM covid
# MAGIC WHERE Name in ('India', 'China', 'Brazil')  
# MAGIC ORDER BY date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Name, date schema,
# MAGIC   Active AS Ativos
# MAGIC FROM covid
# MAGIC WHERE Name in ('India', 'China', 'Brazil') 
# MAGIC AND date > '2020-12-19' 
# MAGIC ORDER BY date DESC

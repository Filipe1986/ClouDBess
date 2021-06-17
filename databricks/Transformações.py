# Databricks notebook source
countryDf = spark.read.format("json").load("/raw/country.json")
casesDf = spark.read.format("json").load("/raw/cases.json")
usaCasesDf = spark.read.format("json").load("/raw/usaCases.json")

display(countryDf)

# COMMAND ----------

# help(countryDf.join)

joinDF = countryDf.join(casesDf, 'countryCode', how='inner')
display(joinDF)

# COMMAND ----------

display(joinDF.filter(joinDF.date.isNull()))

# COMMAND ----------

joinDF = joinDF.drop("Lat").drop("Lon")


# COMMAND ----------

usaCasesDf.printSchema

# COMMAND ----------

from pyspark.sql.functions import countDistinct

display(usaCasesDf.select(countDistinct("date" )))

# COMMAND ----------

from pyspark.sql.functions import sum as sum_spark, lit

usaCasesDf = usaCasesDf.groupBy('date').agg(

  sum_spark("Confirmed").alias("Confirmed"), 
  sum_spark("Deaths").alias("Deaths"),
  sum_spark("Recovered").alias("Recovered"),
  sum_spark("Active").alias("Active"),
)
usaCasesDf = usaCasesDf.withColumn("CountryCode", lit("US"))

usaCasesDf.createOrReplaceTempView("usa_cases_table")


# COMMAND ----------

joinUsaCases = countryDf.join(usaCasesDf, 'countryCode', how='inner')

joinUsaCases = joinUsaCases.drop("Lat").drop("Lon")

display(joinUsaCases)
joinUsaCases.filter(joinUsaCases.CountryCode == 'US').count()

# COMMAND ----------

joinUsaCases = joinUsaCases.select(joinDF.columns)

# COMMAND ----------

display(joinUsaCases)

# COMMAND ----------

joinDF.printSchema



# COMMAND ----------

joinUsaCases.printSchema

# COMMAND ----------

joinUsaCases.select().count()

# COMMAND ----------

unionDf = joinUsaCases.union(joinDF)

# COMMAND ----------

from pyspark.sql.functions import date_trunc, year, month, concat_ws

unionDf = unionDf.withColumn("yearMonth",  concat_ws("-",    year(unionDf.date) , month( unionDf.date)))

display(unionDf)

# COMMAND ----------

unionDf.write.partitionBy("yearMonth").mode("Overwrite").format("parquet").save("covidPartByYearMonth.parquet")

# COMMAND ----------

unionDf.filter(unionDf.CountryCode == 'US').count()

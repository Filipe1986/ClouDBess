// Databricks notebook source
// MAGIC %scala
// MAGIC val jdbcHostname = "gama.cn27cseohzrm.us-east-2.rds.amazonaws.com"
// MAGIC val jdbcPort = 3306
// MAGIC val jdbcDatabase = "cloudbees"
// MAGIC 
// MAGIC val jdbcUsername = "admin"
// MAGIC val jdbcPassword = "XXX"
// MAGIC 
// MAGIC // Create the JDBC URL without passing in the user and password parameters.
// MAGIC val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
// MAGIC 
// MAGIC // Create a Properties() object to hold the parameters.
// MAGIC import java.util.Properties
// MAGIC val connectionProperties = new Properties()
// MAGIC 
// MAGIC connectionProperties.put("user", s"${jdbcUsername}")
// MAGIC connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

// MAGIC %scala
// MAGIC import java.sql.DriverManager
// MAGIC val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
// MAGIC connection.isClosed()

// COMMAND ----------

// MAGIC %scala
// MAGIC val country_table = spark.read.jdbc(jdbcUrl, "Country", connectionProperties)
// MAGIC val cases_table = spark.read.jdbc(jdbcUrl, "Cases", connectionProperties)
// MAGIC val usa_table = spark.read.jdbc(jdbcUrl, "UsaCases", connectionProperties)

// COMMAND ----------

// MAGIC %scala
// MAGIC country_table.printSchema
// MAGIC cases_table.printSchema
// MAGIC usa_table.printSchema

// COMMAND ----------

// MAGIC %scala
// MAGIC val country_query = """
// MAGIC (SELECT *
// MAGIC FROM Country) country_raw
// MAGIC """
// MAGIC 
// MAGIC val countryRawDf = spark.read.jdbc(url=jdbcUrl, table=country_query, properties=connectionProperties)
// MAGIC 
// MAGIC countryRawDf.write.format("json").mode("overwrite").save("raw/country.json")
// MAGIC 
// MAGIC //countryRawDf.show()
// MAGIC display(countryRawDf)

// COMMAND ----------

// MAGIC %python
// MAGIC jdbcHostname = "gama.cn27cseohzrm.us-east-2.rds.amazonaws.com"
// MAGIC jdbcPort = 3306
// MAGIC jdbcDatabase = "cloudbees"
// MAGIC jdbcUsername = "admin"
// MAGIC jdbcPassword = "XXX"
// MAGIC 
// MAGIC jdbcUrl = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"
// MAGIC 
// MAGIC 
// MAGIC cases_query = """
// MAGIC (SELECT *
// MAGIC FROM Cases) cases_raw
// MAGIC """
// MAGIC connectionProperties = {
// MAGIC   'user' : 'admin',
// MAGIC   'password': 'XXX'
// MAGIC }
// MAGIC casesRawDf = spark.read.jdbc(url=jdbcUrl, table=cases_query, properties=connectionProperties)
// MAGIC casesRawDf.write.format("json").mode("overwrite").save("raw/cases.json")
// MAGIC 
// MAGIC 
// MAGIC display(casesRawDf)

// COMMAND ----------

// MAGIC %python
// MAGIC usaCases_query = """
// MAGIC (SELECT *
// MAGIC FROM UsaCases) usaCases_raw
// MAGIC """
// MAGIC 
// MAGIC usaCasesRawDf = spark.read.jdbc(url=jdbcUrl, table=usaCases_query, properties=connectionProperties)
// MAGIC usaCasesRawDf.write.format("json").mode("overwrite").save("raw/usaCases.json")
// MAGIC 
// MAGIC display(casesRawDf)

// COMMAND ----------

// MAGIC %python
// MAGIC #Transformação

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %python
// MAGIC display(joinDF.filter(joinDF.date.isNull()))

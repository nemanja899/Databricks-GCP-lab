# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks Notebooks
# MAGIC - Support Scala, Python, SQL, R, and Markdown
# MAGIC - Version control 
# MAGIC - Connect to remote Git Repository via Repos
# MAGIC - Permissions for sharing notebooks
# MAGIC - Schedule as production jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Notebooks
# MAGIC - Create new notebook and set default language
# MAGIC 
# MAGIC <img src = "https://storage.googleapis.com/databricks-public-images/bmathew/create_notebook.png" height=600 width=600>
# MAGIC 
# MAGIC <br>
# MAGIC - Import a notebook
# MAGIC 
# MAGIC <img src = "https://storage.googleapis.com/databricks-public-images/bmathew/import_notebook.png" height=300 width=300>
# MAGIC 
# MAGIC <br>
# MAGIC - Notebooks in Repos
# MAGIC 
# MAGIC <img src = "https://storage.googleapis.com/databricks-public-images/bmathew/repos_img1.png" height=400 width=400>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Attach the notebook to a cluster 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Scala, Python, SQL, R, and Markdown into the notebook cells
# MAGIC - Streaming + Batch ETL code, SQL, Machine Learning, Deep Learning, and Graph Analytics
# MAGIC - Java is not directly supported in the notebooks and you have to submit Java JAR files via the RESTP API
# MAGIC <br><br>
# MAGIC - Example below uses Python to read a parquet file from Google Cloud storage into a Spark DataFrame 
# MAGIC - Learn more about DataFrames: https://docs.gcp.databricks.com/getting-started/spark/dataframes.html
# MAGIC - When you first create a notebook, you will set the default language
# MAGIC    - To use another language specify the interpreter in the first line - e.g. %python, %scala, %sql, %r, or %md
# MAGIC - Up to 1000 rows will be displayed in tabular format
# MAGIC    - Can download up to 1 million rows, or you can disable downloading of results for users

# COMMAND ----------

# MAGIC %python
# MAGIC df = sqlContext.read.parquet("gs://databricksgcplabs/sales_data")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - <b>Spark development tip:</b> Cache this DataFrame to memory for faster access when reused in subsequent operations
# MAGIC - Notice the Python language interpreter is not set. That's because the default language for this notebook is already set Python.

# COMMAND ----------

df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC - Create temporary table from the cached DataFrame that we can easily perform SQL commands on
# MAGIC - We will learn to create a materialized table in the last lab exercise

# COMMAND ----------

df.createOrReplaceTempView("sales_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL 
# MAGIC - ANSI SQL Syntax you are already familiar with
# MAGIC - Aggregations, Filtering, Grouping, Sorting, Analytical Functions, Joins, etc....
# MAGIC - Since we are switching from the default language, Python, we must set the sql language interpreter in the first line
# MAGIC - Can perform SQL on materialized tables, views, or in-memory objects

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT productline AS `Product Line`,
# MAGIC concat('$',format_number(sum(sales),2)) AS `Total Sales Revenue`
# MAGIC FROM sales_data WHERE lower(status) = 'shipped' GROUP BY 1 ORDER BY sum(sales) ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize the data
# MAGIC - Databricks supports many different types of visualizations to create rich dashboards.
# MAGIC - Connect your BI tools to Databricks via JDBC/ODBC to analyze the data.
# MAGIC - Create your own custom visualizations: HTML, JavaScript, D3, SVG, Matplotlib, ggplot2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT productline AS `Product Line`,
# MAGIC sum(sales) AS `Total Sales Revenue`
# MAGIC FROM sales_data WHERE lower(status) = 'shipped' GROUP BY 1 ORDER BY sum(sales) ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Defined Functions

# COMMAND ----------

# MAGIC %md
# MAGIC - Create function and register as User Defined Function (UDF) to call from SparkSQL or DataFrames
# MAGIC - This example takes 2 input parameters, price and tax%, to calculate total price with tax

# COMMAND ----------

import decimal
def price_with_tax_local (price, tax):
  price = decimal.Decimal(price)
  tax = tax / 100
  price_with_tax = price + (price * tax)
  return price_with_tax
sqlContext.udf.register("price_with_tax_local", price_with_tax_local)

# COMMAND ----------

# MAGIC %md
# MAGIC - Call function from SQL query to use against the data in a table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ordernumber, orderdate, sales, price_with_tax_local(sales, 6.70) AS sales_amount_plus_tax FROM sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC - Create custom user defined functions available to everyone as Java, Scala, or Python libraries
# MAGIC    - In this example, I created a Python Egg file with the package my_custom_functions that contains various functions
# MAGIC    - I created the Egg file locally on my laptop and uploaded to Databricks. You can upload using the GUI, API, or CLI
# MAGIC    - I will import the libary into Databricks and attach the libary to the cluster and then run the cell
# MAGIC    - Similar example to above UDF, but now source code for function is written using Python
# MAGIC    
# MAGIC - UDFs are executed row by row and if working with very large datasets, use vectorized UDFs with Python Pandas: https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html

# COMMAND ----------

from my_custom_functions import price_with_tax

df4 = spark.sql("SELECT ordernumber, orderdate, sales FROM sales_data")
output = df4.rdd.map(lambda line: (line[0], line[1], line[2], price_with_tax(line[2],6.70))).toDF(["ordernumber", "orderdate", "sales", "sales_amount_plus_tax"])
display(output)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now that you understand how to develop code using notebooks, let's learn how to connect to data in Google Cloud Storage! 

# COMMAND ----------

# MAGIC %md
# MAGIC #### [Click here to return to agenda]($./Agenda)

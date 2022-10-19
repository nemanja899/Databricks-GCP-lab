# Databricks notebook source
# MAGIC %md 
# MAGIC ### Challenge
# MAGIC * A challenge faced by many organizations today is that data is spread across the enterprise between many different systems. This includes data for reporting, dashboards, KPIs, and analytics.
# MAGIC * As data volumes grow it becomes more difficult to collect and analyze this data across all the different sources using existing tools.
# MAGIC * Data Lakes built using cloud storage provide scalable and cost-effective storage but accessing that data becomes challenging due to issues with performance,  reliability, and consistency. 
# MAGIC 
# MAGIC ### Solution: Build a Lakehouse on Google Cloud Storage using Delta Lake
# MAGIC * Store all data in one location for all personas in your organization - Data Engineering, Data Science, Analysts. 
# MAGIC * Brings Scale, Performance, and Reliability to cloud data lakes. 
# MAGIC * Open-source project and no lock-in
# MAGIC * Learn more about Delta Lake: https://docs.gcp.databricks.com/delta/index.html
# MAGIC 
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<img src = "https://storage.googleapis.com/databricks-public-images/bmathew/delta_lake_lab_2.png" height=700 width=700>

# COMMAND ----------

# MAGIC %md
# MAGIC - Please read the text in each of the notebook cells. 
# MAGIC - There are instructions in almost all of the cells that require input from you so that the code will run as expected. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Database for you to work in
# MAGIC - The database needs to be unique 
# MAGIC - Choose the username assigned to you for this class
# MAGIC - In the cell below, so that everyone has their own unique database, replace the - Replace the parameter "your-username" with your unique username
# MAGIC    - For example: odl_instructor_490106@databrickslabs.com --> odl_instructor_490106

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS odl_user_768559

# COMMAND ----------

# MAGIC %md
# MAGIC - Create a directory where we will write data files to
# MAGIC - Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /tmp/odl_user_768559/delta/clickstream

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Delta table
# MAGIC - Make this an external/unmanaged table by specifying the location. By doing this, if we drop the table, then the data still remains.
# MAGIC - If we don't specify the location, then this becomes a managed table with the default location at /user/hive/warehouse/. If we drop the table, then the directory and all data will  be removed. 
# MAGIC - Delta will automatically compress the Parquet files for this table using Snappy compression.
# MAGIC - Delta also supports table partitions just like Hive; however, in this example we are not partitioning the table. In practice, it's recommended to partition on a few select columns to improve performance when there are a large number of rows. Partition on columns that have low cardinality. 
# MAGIC - In the table creation syntax (DDL), notice the format 'USING DELTA'
# MAGIC - Optionally add constraints
# MAGIC    - Insertion will fail if condition check fails
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS odl_user_768559.clickstream_data;
# MAGIC CREATE TABLE odl_user_768559.clickstream_data (
# MAGIC   domain_name STRING,
# MAGIC   event_time STRING,
# MAGIC   user_id STRING,
# MAGIC   country STRING, 
# MAGIC   division STRING,
# MAGIC   page_url STRING,
# MAGIC   page_app STRING,
# MAGIC   page_id STRING,
# MAGIC   browser STRING,
# MAGIC   os STRING,
# MAGIC   platform STRING)
# MAGIC USING DELTA
# MAGIC LOCATION '/tmp/<your-username>/delta/clickstream';
# MAGIC 
# MAGIC ALTER TABLE odl_user_768559.clickstream_data ADD CONSTRAINT dateWithinRange CHECK (event_time > '2015-01-01');
# MAGIC ALTER TABLE odl_user_768559.clickstream_data ADD CONSTRAINT validCountryCode CHECK (country in ('MX','TW','PE','CO','UY','PR','CL','ID','CA','GB','JP','US','PA'));
# MAGIC ALTER TABLE odl_user_768559.clickstream_data DROP CONSTRAINT dateWithinRange;
# MAGIC ALTER TABLE odl_user_768559.clickstream_data DROP CONSTRAINT validCountryCode;

# COMMAND ----------

# MAGIC %md
# MAGIC ### We will parse a few fields from JSON data we have inside Google Cloud storage and save to the table we created above
# MAGIC - We will read the source data into a Spark DataFrame and then save to the location for the table we created above
# MAGIC - Write the format of the DataFrame as 'delta'
# MAGIC - The write we can be an OVERWRITE or APPEND
# MAGIC    - OVERWRITE will replace all the existing data in the directory with new files
# MAGIC    - APPEND will add new files to the existing set of files
# MAGIC - The schema of our DataFrame must match the schema of the table, otherwise the job will fail. This is Schema Enforcement.
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col

df = sqlContext.read.json("gs://databricksgcplabs/clickstream-json")

parsed_fields = df.select( \
  col('http_vhost_name').alias('domain_name') \
 ,regexp_replace(regexp_replace("event_time","T"," "),"Z","").alias("event_time") \
 ,col('user.browserId').alias('user_id') \
 ,col('page.country').alias('country') \
 ,col('page.division').alias('division') \
 ,col('page.url').alias('page_url') \
 ,col('page.app').alias('page_app') \
 ,col('page.id').alias('page_id') \
 ,col('user.browser').alias('browser') \
 ,col('user.os').alias('os') \
 ,col('user.platform').alias('platform'))

output = (
           parsed_fields.write.format("delta")  # Specify the format as Delta
             .mode("overwrite")  # Specify the mode, OVERWRITE or APPEND. Overwrite will rewrite whereas append will add new records
             .saveAsTable("<your-username>.clickstream_data") ## Specify table name or specify a file path if no table is created and you want to write to any directory
         )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexes to improve read performance
# MAGIC - By default, Delta will automatically create Data Skipping indexes for the first 32 columns
# MAGIC - With Data Skipping Indexes, Delta keeps metadata for each Parquet file with the MIN and MAX values for each column in that Parquet file
# MAGIC - This improves query read performance as Delta knows exactly which Parquet files contain the data to satisfy the query and doesn't have to scan all the Parquet files
# MAGIC - Learn more about Data Skipping Indexes: https://docs.gcp.databricks.com/spark/latest/spark-sql/dataskipping-index.html
# MAGIC - You can also optionally create bloom filters: https://docs.gcp.databricks.com/delta/optimizations/bloom-filters.html
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM <your-username>.clickstream_data where country = 'US'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect table statistics
# MAGIC - It's important to regularly collect table statistics as new data is loaded into the table so the optimizer can choose the most optimal query plan when users submit queries
# MAGIC - Collect statistics for all the individual columns of the table
# MAGIC - Learn more about collecting table statistics: https://docs.gcp.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-aux-analyze-table.html
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE <your-username>.clickstream_data COMPUTE STATISTICS FOR COLUMNS domain_name, event_time, user_id, country, division, page_url, page_app, page_id, browser, os, platform

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's examine this Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED <your-username>.clickstream_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's do a file listing of the directory for this table
# MAGIC - The files are open source Parquet with Snappy compression 
# MAGIC - Notice the Delta log directory _delta_log
# MAGIC    - Maintains transaction history of all the data changes for the table
# MAGIC - This Delta table has quite a few small sized parquet files
# MAGIC - Delta can optimize the table by compacting smaller files into fewer larger ones
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/<your-username>/delta/clickstream

# COMMAND ----------

# MAGIC %md
# MAGIC ### Too many small files will negatively impact performance
# MAGIC - Delta mitigates this by performing a file compaction to create fewer larger sized parquet files.
# MAGIC - Delta also caches data on the local SSD Drives on the VM's after it has first been read from Cloud Storage.
# MAGIC - Thus, subsequent reads of the data will be fetched locally from the SSD drives on the VMs instead of reading again from Cloud Storage.
# MAGIC - Optimize command will by default try to create a 1 GB file and this setting is configurable
# MAGIC    - There is an optional auto-optimize feauture you can enable
# MAGIC - Learn more about Optimize: https://docs.gcp.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE <your-username>.clickstream_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### After performing the file compaction we now have 1 large parquet file
# MAGIC - Delta created the new parquet file
# MAGIC - Delta will read from this new version of the data
# MAGIC - The old files will still be kept unless you explicitly run a VACUUM command to remove them
# MAGIC - We will use the VACUUM command later
# MAGIC 
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/<your-username>/delta/clickstream

# COMMAND ----------

# MAGIC %md
# MAGIC ### ACID Transactions
# MAGIC - Multiple processes and users can access and modify a dataset/table and lets end users see consistent views of the data.
# MAGIC - <b>DELETES/UPDATES/UPSERTS</b>
# MAGIC     - Writers can modify a dataset without interfering with current jobs reading the dataset.
# MAGIC 
# MAGIC ### Delta helps you comply with data privacy and data retention</b>
# MAGIC - Use Delta Update and Merge functionality to modify data in base tables
# MAGIC - Use Delta Delete functionality to delete data in base table
# MAGIC - Use Databricks Table Access Control Lists to control access to Databases, Tables, and Views
# MAGIC    - Restrict access to authorized Users and/or Groups 
# MAGIC    - Learn more about Table Access Control Lists: https://docs.gcp.databricks.com/security/access-control/table-acls/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update data
# MAGIC - UPDATE: Update rows in single table
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from <your-username>.clickstream_data

# COMMAND ----------

# MAGIC %md
# MAGIC - Let's update event_time to a static value and set domain_name to a static value
# MAGIC - Learn more about the UPDATE command: https://docs.gcp.databricks.com/spark/latest/spark-sql/language-manual/delta-update.html
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE <your-username>.clickstream_data SET event_time = '2022-01-01 14:11:32', domain_name= "TEST UPDATE DOMAIN NAME";

# COMMAND ----------

# MAGIC %md
# MAGIC - The data has been updated
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM <your-username>.clickstream_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge data
# MAGIC - MERGE: 
# MAGIC    - Insert or Update data in a table with values from a second table by joining them on 1 or more common keys. 
# MAGIC    - Use cases include for updating Slowly Changing Dimensions (SCD) and Change Data Capture (CDC).
# MAGIC - Learn more about the MERGE command: https://docs.gcp.databricks.com/spark/latest/spark-sql/language-manual/delta-merge-into.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic CDC Example 
# MAGIC    - ETL process will get new data from source systems
# MAGIC    - This data then needs to be loaded into our target table
# MAGIC    - Load can be INSERT or UPDATE: INSERT new records or UPDATE existing records
# MAGIC    - MERGE command can perform this

# COMMAND ----------

# MAGIC %md
# MAGIC - Let's load the new data into a DataFrame which contains new records to INSERT and existing records to UPDATE (Upsert)
# MAGIC - Create a temporary table for this data

# COMMAND ----------

df = spark.read.parquet("gs://databricksgcplabs/merge-data/merge.snappy.parquet")
df.createOrReplaceTempView("clickstream_data_stage")

# COMMAND ----------

# MAGIC %md
# MAGIC - Let's view the data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM clickstream_data_stage

# COMMAND ----------

# MAGIC %md
# MAGIC - Let's get a count of records from our original table before we run the MERGE operation
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM <your-username>.clickstream_data;

# COMMAND ----------

# MAGIC %md
# MAGIC - Run the MERGE operation to INSERT new data and UPDATE existing data
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO <your-username>.clickstream_data a /** This is the target table **/
# MAGIC USING clickstream_data_stage b /** This is the source table **/
# MAGIC ON a.user_id = b.user_id AND a.event_time = b.event_time /** This is the join condition **/
# MAGIC WHEN MATCHED THEN /** Update the following columns when matched **/
# MAGIC   UPDATE SET
# MAGIC   a.domain_name = b.domain_name
# MAGIC   ,a.event_time = b.event_time
# MAGIC   ,a.country = b.country
# MAGIC   ,a.division = b.division
# MAGIC   ,a.page_url = b.page_url
# MAGIC   ,a.page_app = b.page_app
# MAGIC   ,a.page_id = b.page_id
# MAGIC   ,a.browser = b.browser
# MAGIC   ,a.os = b.os
# MAGIC   ,a.platform = b.platform
# MAGIC WHEN NOT MATCHED THEN /** Insert a new row when not matched **/
# MAGIC   INSERT (domain_name,event_time,user_id,country,division,page_url,page_app,page_id,browser,os,platform)
# MAGIC   VALUES (b.domain_name,b.event_time,b.user_id,b.country,b.division,b.page_url,b.page_app,b.page_id,b.browser,b.os,b.platform)

# COMMAND ----------

# MAGIC %md
# MAGIC - Let's get a count of records
# MAGIC - Notice before we had 100,000 records and now we have 100,018
# MAGIC - Some existing records were updated and new records were inserted
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM <your-username>.clickstream_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete 
# MAGIC - DELETE: Delete rows in single table
# MAGIC - Can also specify conditions in the WHERE clause
# MAGIC - The command below will run 2 SQL statements: First the delete command and then a select command
# MAGIC - Notice that the data has been removed
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM <your-username>.clickstream_data;
# MAGIC SELECT * FROM <your-username>.clickstream_data 

# COMMAND ----------

# MAGIC %md
# MAGIC ### We just deleted all of our data! Not to worry, Delta allows us to restore the data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Provenance: History of changes to data
# MAGIC - We can see all the changes made to the table: when change was made, by who, the operation performed and other information
# MAGIC - These are all the different versions of the table over time
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY <your-username>.clickstream_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can rollback to an earlier version if we have not removed (VACUUM) the old files
# MAGIC - We made several changes to the data: UPDATE, MERGE, AND DELETE
# MAGIC - Delta versions the Parquet files and does not delete them
# MAGIC - Old file are kept unless you remove them using the VACUUM command
# MAGIC - Since the old files are still there we can perform Time Travel
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/<your-username>/delta/clickstream

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel to recover previous version of the data
# MAGIC - Let's restore to a previous version of the table
# MAGIC - You restore by version number or the timestamp
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO <your-username>.clickstream_data
# MAGIC SELECT * FROM <your-username>.clickstream_data VERSION AS OF 5 /** Can also specify the timestamp instead of the version **/

# COMMAND ----------

# MAGIC %md
# MAGIC - The table has been restored
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM <your-username>.clickstream_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM command to remove old files
# MAGIC - Use the VACUUM commamnd to remove old files
# MAGIC - We do not recommend that you set a retention interval shorter than 7 days, because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table. 
# MAGIC - If VACUUM cleans up active files, concurrent readers can fail or, worse, tables can be corrupted when VACUUM deletes files that have not yet been committed.
# MAGIC - By default, VACUUM is set to 7 days. You can override this setting by setting spark.databricks.delta.retentionDurationCheck.enabled to false. 
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %md
# MAGIC - Let's first Optimize the table

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE <your-username>.clickstream_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled=false;
# MAGIC VACUUM <your-username>.clickstream_data RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC - Now notice all the old files have been deleted
# MAGIC 
# MAGIC -- Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/<your-username>/delta/clickstream

# COMMAND ----------

# MAGIC %md
# MAGIC #### [Click here to return to agenda]($./Agenda)

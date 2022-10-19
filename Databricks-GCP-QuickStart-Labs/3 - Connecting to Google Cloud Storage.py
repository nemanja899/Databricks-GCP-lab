# Databricks notebook source
# MAGIC %md
# MAGIC #### Please read the text in each of the notebook cells. <BR>There are instructions in some of them that require input from you so that the code will run as expected. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Sources
# MAGIC - Databricks can connect to source data from a variety of differnt storage mediums and formats
# MAGIC    - RDBMS
# MAGIC    - Message Bus (e.g. PubSub, Kafka, and more)
# MAGIC    - NoSQL 
# MAGIC    - Files (Delimited text, JSON, Avro, Parquet, ORC, Sequence Files, Images, Documents, and more)
# MAGIC    - Google Cloud Services: BigQuery, Bigtable, CloudSQL, PubSub, and other GCP services
# MAGIC    - In the cloud or on-premise, but Databricks is optimized for cloud object storage - Google Cloud Storage
# MAGIC - Connecting to Data Sources: https://docs.gcp.databricks.com/data/data-sources/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### Authenticating to Google Cloud Storage in Databricks
# MAGIC Use a Service Account 
# MAGIC - Secure and does not expose any sensitive information
# MAGIC - Create a Service Account in Google Cloud, give that Service Account the proper permissions, associate to Google Cloud Storage bucket, <BR>and specify the Service Account used when launching the cluster. 
# MAGIC - There are other ways of using a Service Account instead of asscociating a Service Account to a cluster and you can add the Service Account key values into spark.conf. 
# MAGIC - Visit the online documentation to learn more about how to use Service Accounts with Databricks: https://docs.gcp.databricks.com/data/data-sources/google/gcs.html
# MAGIC   
# MAGIC <img src = "https://storage.googleapis.com/databricks-public-images/bmathew/service_account.png" height=600 width=600>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Accessing Google Cloud Storage in Databricks
# MAGIC There are two common ways to access data in Google Cloud Storage from Databricks.
# MAGIC 1. <b>Mount points</b>
# MAGIC    - Mounting your storage container to the Databricks workspace to be shared by all users and clusters.
# MAGIC    - This works well in a development environment where all users might need access to the data.
# MAGIC    - The mount is only a pointer to a GCS location and the data is never synced locally.
# MAGIC 2. <b>No mount points</b>
# MAGIC    - Not using a mount point and instead using a specific service account that you have access to
# MAGIC 
# MAGIC Both methods require the use of a Service Account

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Mount Points
# MAGIC - Create a service account for the Databricks cluster.
# MAGIC    - The service account must be in the Google Cloud project that you used to set up the Databricks workspace.
# MAGIC - Configure the GCS bucket so that the service account has access to it.
# MAGIC - Launch a Databricks cluster with a service account attached to it.
# MAGIC - Note: In the example below, for demonstration purposes, we are using our user_id in the mount name; however, we would not typically do this in a live setting.
# MAGIC    - Make certain to replace the parameter "your-username" with your unique username
# MAGIC    - For example: odl_instructor_490106@databrickslabs.com --> odl_instructor_490106

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.mount("gs://databricksgcplabs","/mnt/odl_user_768559/databricksgcplabs")

# COMMAND ----------

# MAGIC %md
# MAGIC - Once mounted, we can view and navigate the contents of our GCS bucket using Databricks %fs file system commands.
# MAGIC - Databricks file system commands has similar Unix style syntax (e.g. ls, head, cp, rm, and mkdirs - Unix is actually mkdir)
# MAGIC - Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/odl_user_768559/databricksgcplabs

# COMMAND ----------

# MAGIC %md
# MAGIC - Unmount the mount point
# MAGIC - Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.unmount("/mnt/odl_user_768559/databricksgcplabs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### No Mount Points
# MAGIC - Create a service account for the Databricks cluster.
# MAGIC    - The service account must be in the Google Cloud project that you used to set up the Databricks workspace.
# MAGIC - Configure the GCS bucket so that the service account has access to it
# MAGIC - Launch a Databricks cluster with a service account attached to it

# COMMAND ----------

# MAGIC %md
# MAGIC - Use Databricks %fs file system commands directly against the GCS bucket

# COMMAND ----------

# MAGIC %fs
# MAGIC ls gs://databricksgcplabs

# COMMAND ----------

# MAGIC %fs
# MAGIC head gs://databricksgcplabs/members/members.csv

# COMMAND ----------

# MAGIC %md
# MAGIC - To run multiple commands in the same cell, use Databricks Utilities, or DBUtils, which executs File System commands
# MAGIC - Here is an example that will create a new directory, copy files to it, and them remove the directory and its contents
# MAGIC - The example below is doing a recursive copy so that any subdirectories will also get copied
# MAGIC - Replace the parameter "your-username" with your unique username

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.mkdirs("/tmp/odl_user_768559/data")
# MAGIC dbutils.fs.cp("gs://databricksgcplabs/products","/tmp/odl_user_768559/data", recurse=True)
# MAGIC dbutils.fs.rm("/tmp/odl_user_768559/data", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now that you learned how to authenticate with data in GCS, let's complete the final lab and build a Data Lakehouse!

# COMMAND ----------

# MAGIC %md
# MAGIC #### [Click here to return to agenda]($./Agenda)

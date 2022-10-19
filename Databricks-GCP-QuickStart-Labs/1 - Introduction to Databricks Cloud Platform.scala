// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ### Databricks Lakehouse Platform
// MAGIC Databricks is a managed cloud analytics platform. You do not need to learn complex cluster management concepts nor perform cluster maintenance tasks. As a Data Engineer, Data Scientist, or Analyst, you just focus on writing code and we provision the cloud infrastructure to run your code and orchestrate job processing.  
// MAGIC - It's a point and click platform for those that prefer a user interface. 
// MAGIC - The UI is also accompanied by a rich set of REST API's for those that want to automate aspects of administration, development, and deployment. 
// MAGIC - To meet the needs of enterprises, Databricks also includes features such as role-based access control, integration with Google Identity, and other optimizations that not only improve usability for users but also reduce costs and complexity for administrators.
// MAGIC - Databricks provides dozens of code examples, or Solutions Accelerators, to help you get started with common and high-impact use cases that our customers are facing. These Solution Accelerators are designed to help you go from idea to proof of concept (PoC) in less than 2 weeks: https://databricks.com/solutions/accelerators

// COMMAND ----------

// MAGIC %md
// MAGIC ### Different flavors of the UI for different user personas
// MAGIC 1.&nbsp;Data Science & Engineering: Use for Data Ingestion, ETL, Data Prep, Feature Engineering and SQL Analytics<br>
// MAGIC 2.&nbsp;Machine Learning: Everything in Data Science & Engineering plus Experiment Tracking, Feature Store, Model Registry
// MAGIC 
// MAGIC For this course we will be focusing on the Data Science & Engineering version of the Databricks platform.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Databricks Terminology
// MAGIC 
// MAGIC Databricks has key common concepts that you will need to understand. You'll notice that many of these line up with the icons on the left navigation menu. They define the fundamental tools that Databricks provides to you as an end user to develop and deploy applications. They are available both in the web application UI as well as the REST API.
// MAGIC 1.   ****Workspace****
// MAGIC 2.   ****Repos****
// MAGIC 2.   ****Data****
// MAGIC 3.   ****Compute****
// MAGIC 4.   ****Jobs****
// MAGIC 5.   ****Libraries****
// MAGIC 6.   ****Notebooks****
// MAGIC 
// MAGIC 
// MAGIC -   ****Workspace****
// MAGIC     -   Workspaces allow you to organize all the work that you are doing on Databricks. 
// MAGIC     -   Like a folder structure in your computer, it allows you to save ****notebooks**** and ****libraries**** and share them with other users. 
// MAGIC     -   Workspaces are not connected to data and should not be used to store data. They're simply for you to store the ****notebooks**** and ****libraries**** that you use to operate on and manipulate your data with.
// MAGIC     -   Every user who has a Databricks account will have their own Workspace.
// MAGIC     -   <a href = "https://docs.gcp.databricks.com/workspace/index.html"> Click here to learn more about Workspace </a><br><br>
// MAGIC -   ****Repos****
// MAGIC     -   Provides repository-level integration with Git providers: GitHub, Bitbucket, GitLab, and Azure DevOps
// MAGIC     -   You can develop code in a Databricks notebook and sync it with a remote Git repository. 
// MAGIC     -   Databricks Repos lets you use Git functionality such as cloning a remote repo, managing branches, pushing and pulling changes, and visually comparing differences upon commit.
// MAGIC     -   Databricks Repos also provides an API that you can integrate with your CI/CD pipeline. For example, you can programmatically update a Databricks repo so that it always has the most recent code version.
// MAGIC     -   <a href = "https://docs.gcp.databricks.com/repos.html"> Click here to learn more about Repos </a><br><br>
// MAGIC -   ****Data****
// MAGIC     -   Structure your data in Google Cloud Storage into Databaes with Tables and Views having schemas with named columns and data types.
// MAGIC     -   Data for tables is stored in your cloud storage account.
// MAGIC     -   Table ACLs to control group and user access to Databases, Tables, and Views for fine grained access including row and column level security.
// MAGIC     -   <a href = "https://docs.gcp.databricks.com/spark/latest/spark-sql/index.html"> Click here to learn more about Spark SQL and Tables in Databricks </a><br><br>
// MAGIC -   ****Compute****
// MAGIC     -   Compute are the clusters which are groups of VM's that run using Google Kubernetes Engine.  
// MAGIC     -   Three types of clusters:
// MAGIC           1. High-Concurrency (Interactive shared cluster)
// MAGIC             - Shared by multiple users and are meant for long running clusters,
// MAGIC           2. Standard (Jobs cluster)
// MAGIC             - Used for single user workloads and/or to run single jobs and are emphemeral or short lived clusters. 
// MAGIC           3. Single Node
// MAGIC             - Single VM use for single-node processing (non-distrubuted computing)
// MAGIC     -    Decoupling of Storage and Compute. Clusters do not store data. Data is stored in your Cloud Storage account, on-premise, and other data sources. 
// MAGIC     -    Clusters are Ephemerial. 
// MAGIC     -    Can have Multiple Clusters
// MAGIC     -    Clusters can be defined as Autoscaling 
// MAGIC     -    Clusters have access controls to control who has access to each cluster
// MAGIC     -   <a href = "https://docs.gcp.databricks.com/clusters/index.html"> Click here to learn more about Clusters in Databricks </a><br><br>
// MAGIC -   ****Jobs****
// MAGIC     -   Jobs are the tool by which you can schedule execution of code to occur either on an already existing ****cluster**** or a new cluster. 
// MAGIC     -   These can be ****notebooks**** as well as Java and Scala jars or python wheel and egg files, and Python scripts. They can be created either manually or via the REST API.
// MAGIC     -   Databricks provides a CRON-style built-in job scheduler to schedule and run jobs on a schedule.
// MAGIC     -   Easily integrate with any 3rd party scheduler for advanced DAG creation to easily define workflows with dependendies - e.g. integrate with Apache Airflow.
// MAGIC     -   <a href = https://docs.gcp.databricks.com/jobs.html>Click here to learn more about jobs</a><br><br>
// MAGIC -   ****Libraries****
// MAGIC     -   Libraries are third-party or custom packages or modules that you can attach to clusters and make available to users.
// MAGIC     -   These may Java or Scala code packaged as JAR files, Python wheel or egg files, Python scripts, and R code.
// MAGIC     -   You can write and upload libraries manually through the UI, automated via an API, or you may install them directly via package management utilities like PyPi, Maven, or CRAN.
// MAGIC     - <a href = https://docs.gcp.databricks.com/libraries/index.html>Click here to learn more about libraries</a><br><br>
// MAGIC -   ****Notebooks****
// MAGIC     -   Notebooks are the visual UI to develop code in `Scala`, `Python`, `R`, `SQL`, or `Markdown`. 
// MAGIC     -   Notebooks are attached to a ****cluster**** to execute code, bu they are not permanently tied to a cluster. This allows notebooks to be shared or downloaded onto your local machine.
// MAGIC     -   Dashboards and rich visualizations can be created from notebooks as a way of displaying the output of cells without the code that generates them. 
// MAGIC     -   Notebooks can also be scheduled as jobs to run a data pipeline, update a machine learning model, or update a dashboard.
// MAGIC     -   We will walk through a lab exercise on using notebooks in a few minutes

// COMMAND ----------

// MAGIC %md
// MAGIC ### Use your own IDE locally from laptop or notebook server
// MAGIC - PyCharm
// MAGIC - IntelliJ (Scala or Java)
// MAGIC - SparkR and RStudio Desktop
// MAGIC - sparklyr and RStudio Desktop
// MAGIC - Eclipse
// MAGIC - Visual Studio Code
// MAGIC - SBT
// MAGIC - Jupyter notebook
// MAGIC 
// MAGIC Visit online documentation for more information: https://docs.gcp.databricks.com/dev-tools/databricks-connect.html

// COMMAND ----------

// MAGIC %md ### Databricks Help Resources
// MAGIC 
// MAGIC - Databricks comes with a variety of tools to help you learn how to use Databricks and Apache Spark effectively. 
// MAGIC - Databricks holds the greatest collection of Apache Spark documentation available anywhere on the web. 
// MAGIC 
// MAGIC To access resources at any time, click the question mark button towards the bottom of the left navigation menu.
// MAGIC 
// MAGIC <img src = "https://storage.googleapis.com/databricks-public-images/bmathew/databricks_help.png" height=300 width=300>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Now that we learned important Databricks terminology, let's get started developing code with notebooks!

// COMMAND ----------

// MAGIC %md
// MAGIC #### [Click here to return to agenda]($./Agenda)

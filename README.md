# pydatasource

## 0) Introduction

````pydatasource```` allows you to smartly manage the tons of SQL queries that you use to transform your data by cleaning, preparing, and aggregating it.

Queries are written using the Jinja templating framework and managed by config files, supporting multiple developing environments  

They are grouped into "layers" of transformation, which are folders, containing multiple queries concerning the same topic, and a config file.    

## 1) Installation

````bash
pip install pydatasource
```` 

Then you need to install one of the following packages, depending on the data warehouse type you use:

- AmazonRedshift : ````pip install pyred````
- Azure SQL Database: ````pip install pyzure````
- Google Cloud BigQuery: ````pip install bigquery````

<u>Tips:</u> If you use Pycharm, you should add "\\{\\{\w+\\}\\}" and "\\{\\{ \w+ \\}\\}" to Tools>Database>User Parameters (in scripts and literals). In this way, Pycharm will recognize parameters as parameters

## 2) First example

1) Create the folder you want to use to organize your ````pydatasource```` project. Here we have called it "datasource". Create a first folders structure as described below. <br>
Here we added 2 groups (=layers) of 2 queries: "sales" and "customer_support"  
    ```
    datasource
    │
    └───layers
        │
        └───sales
        │   │   config.yaml
        │   └───query
        │           query_1.sql
        │           query_2.sql
        └───customer_support
            │   config.yaml
            └───query
                    query_2.sql
                    query_3.sql
    ```

2) Write an SQL query (for instance query_1.sql).<br>
   Parameters are not mandatory but you can use them as below.  
   
    ````sql
    DROP TABLE IF EXISTS {{ table_name }};
    CREATE TABLE {{ table_name }} AS (
       SELECT 
           date, 
           sum(sessions) as total_sessions 
       FROM {{ table_source }}
       GROUP BY date
    );
    ````
3) Set up your config.yaml file. <br> 
table_name parameter is not mandatory, ````pydatasource```` will use the key of the config file as table name by default. 
    
   ````yaml
   queries:
     query_1:
       query_params:
         table_source: ga.traffic_day
   ````

4) Initiate a sandbox.py file <br>
Here we use a Big Query instance so you need a service account accessing to the Big Query instance <br>
Download the service account secret file (JSON) and remove the private key (we'll include it in a var env) <br>
   ````python
   import logging
   from bigquery import BigQueryDBStream
   from googleauthentication import GoogleAuthentication 
   from pydatasource.DataSource import DataSource 
   
   logging.basicConfig(level="INFO")
    
   google_auth = GoogleAuthentication(
        client_secret_file_path="./path_to_client_secret_file.json"
   ) 
   datamart = BigQueryDBStream(
        google_auth=google_auth,     
        instance_name=None,
        client_id=None,
   )
    
   d = DataSource(
        dbstream=datamart,
        path_to_datasource_folder="./datasource/",
    )
   d.compute(layer_name="sales")
   ````

5) Set environnement variables :
   ````bash
   export GOOGLE_PRIVATE_KEY="""""" # Use "\n" to include line breaks
   export BIG_QUERY_PROJECT_ID="id-of-the-project"
   ````

6) Execute the sandbox.py file , table ````datasource_sales.query_1```` should be created!


## 3) Work with a staging environment

Imagine that now query_1.sql is in production, runs every single hour and you would like to change it.
1) Update the query. All your changes are tracked by git. 
2) If you want to use another source table, simply update your config file:
     ````yaml
       queries:
         query_1:
           query_params:
             table_source: 
                production: ga.traffic_day
                staging: ga.traffic_month
       ````
3) In your sandbox.py file, tell to ````pydatasource```` to run your query in another environment:
     ````python
   import logging
   from bigquery import BigQueryDBStream
   from googleauthentication import GoogleAuthentication 
   from pydatasource.DataSource import DataSource 
   
   logging.basicConfig(level="INFO")
    
   google_auth = GoogleAuthentication(
        client_secret_file_path="./path_to_client_secret_file.json"
   ) 
   datamart = BigQueryDBStream(
        google_auth=google_auth,     
        instance_name=None,
        client_id=None,
   )
    
   d = DataSource(
        dbstream=datamart,
        path_to_datasource_folder="./datasource/",
    )
   d.compute(layer_name="sales", environment='staging') # environment params must be the same than in the config file
     ````
4) Execute sandbox.py, a table comparing staging and prod tables should appear!
 
-----------------

Query params are chosen in this order:
1) from config file
2) from sql snippet
3) from function compute params

You can add this kind of args:
- create_clause: ["in_query", "table", "view"] (default "table")








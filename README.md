# pydatasource

## 0) Introduction

````pydatasource```` allows you to smartly manage the tons of SQL queries that you use to transform your data by cleaning, prepare and aggregate it.

Queries are written using the Jinja templating framework and managed by config files, supporting many developing environments  

They are grouped into "layers" of transformation, which are folders containing many queries concerning the same topic and a config file.    

## 1) Installation

````bash
pip install pydatasource
```` 

Then you need to install one of the following packages, depending on the data warehouse type you use:

- AmazonRedshift : ````pip install pyred````
- Azure SQL Database: ````pip pip install pyzure````
- Google Cloud BigQuery: ````pip pip install bigquery````

If you use Pycharm, you should add "\\{\\{\w+\\}\\}" and "\\{\\{ \w+ \\}\\}" to Tools>Database>User Parameters (in scripts and literals)

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
    DROP TABLE IF EXISTS {{ TABLE_NAME }};
    CREATE TABLE {{ TABLE_NAME }} AS (
       SELECT * FROM {{ TABLE_SOURCE }}
    );
    ````
3) Set up your config.yaml file. <br> 
TABLE_NAME parameter is not mandatory, ````pydatasource```` will use the key of the config file as table name by default. 
    
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







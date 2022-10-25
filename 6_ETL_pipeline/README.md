# ETL-pipeline with Airflow

Here's a script in Airflow that runs automatically every day for yesterday. 

The result of the script - a table with needed metrics and audience features - is downloaded into Clickhouse scheme.

("Needed" metrics are: views, likes, messages_received, messages_sent, users_received, users_sent.)  
(Audience features are: gender, age, os.)

*DAG.ipynb* - the script.

*fin_table.xlsx* - final result.
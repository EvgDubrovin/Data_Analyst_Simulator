# ETL-pipeline with Airflow

Here's a script in Airflow that runs automatically every day. 

The result of the script - a table with needed metrics and audience features - is downloaded into Clickhouse scheme.

("Needed" metrics in this case are: views, likes, messages_received, messages_sent, users_received, users_sent.)  
(Audience features are: gender, age, os.)

*DAG_dubrovin.py* - the script.

*[Rendered DAG.ipynb](https://nbviewer.org/github/EvgDubrovin/Data_Analyst_Simulator/blob/main/6_ETL_pipeline/DAG_dubrovin.ipynb#)*

*fin_table.xlsx* - final result.

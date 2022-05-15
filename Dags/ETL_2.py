# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 04:46:54 2021

@author: Dawood Abbas Zaidi
"""

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
def get_From_Source():
    """
    Gets Data from Source based on Configurations provided at Configs.Json
    """
    src_engine = create_engine('postgresql://postgres:source@172.17.0.1:5432/postgres')
        
	#Query
    src_engine.execute("""CREATE SCHEMA IF NOT EXISTS de_source;""")
    src_engine.execute("""CREATE TABLE IF NOT EXISTS de_source.src_data(id int, Creation_DT Date, Amount Float);""")
    src_engine.execute("""INSERT INTO de_source.src_data VALUES(1,'2022-01-01','1200'),(2,'2022-01-02','2400');""")    
    sql_query = pd.read_sql_query('SELECT * FROM de_source.src_data',src_engine)
	
	#Query To Dataframe
    df = pd.DataFrame(sql_query)
    
    dest_engine = create_engine('postgresql://postgres:dest@172.17.0.1:5433/postgres')
    dest_engine.execute("""CREATE SCHEMA IF NOT EXISTS de_destination;""")
    dest_engine.execute("""CREATE TABLE IF NOT EXISTS dest_data(id int, Creation_DT Date, Amount Float);""")
    #Query
    df.to_sql('dest_data', con=dest_engine, schema='de_destination', index=False, if_exists='append')

dag = DAG(
            dag_id='ETL_Pipeline',
            start_date=datetime(2022,5,15),
            schedule_interval='0 8 * * *',
         )
         
task_1 = PythonOperator(
    task_id='ETL',
    provide_context=False,
    python_callable=get_From_Source,
    dag=dag)

task_1 
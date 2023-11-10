'''
=================================================
Name  : Gilang Wiradhyaksa

Objective : The purpose of this project is to create an automation scheduler to pull data from postgreSQL, cleaning it and then push it 
to kibana for visualization and analyzed. The data used is transaction data from beachside restaurant named Ocean Cafe.
=================================================
'''

import pandas as pd
import psycopg2 as db
import numpy as np
from elasticsearch import Elasticsearch

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def getData():
    '''
    This Function used to get data of this project from database

    Call Example:
        getData() 
    '''
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)

    SQL = '''SELECT "Date", "Bill Number", "Item Desc", "Time", 
    "Quantity", "Rate", "Tax", "Discount", "Total", "Category" 
    FROM table_m3_cafe_order'''

    df = pd.read_sql(SQL, conn)

    # Save Raw Data from SQL
    df.to_csv('/opt/airflow/data/P2M3_gilang_data_raw_sql.csv', index=False)
    
def cleaningData():
    '''
    This Function used to clean the raw data from SQL 

    Call Example:
        cleaningData() 
    '''

    # Load File
    df = pd.read_csv('/opt/airflow/data/P2M3_gilang_data_raw_sql.csv', index_col=False)

    # Remove Duplicate
    if df.duplicated().sum() > 0: df.drop_duplicates(inplace=True)

    # Remove Missing Value
    if df.isnull().sum().sum() > 0: df.dropna(inplace=True)

    # Rename Header
    df.rename(columns=lambda x: x.strip().replace(' ', '_').lower(), inplace=True)

    # Data Type Conversion
    floatColumnName = ['rate', 'tax', 'discount', 'total']
    titleColumnName = ['category']
    
    # Format Date and Time
    df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d')
    df['time'] = pd.to_datetime(df['time'],format= '%H:%M:%S %p').dt.time

    # Convert to Float
    for i in floatColumnName:
        df[i] = df[i].str.replace('\,', '.')
        df[i] = df[i].astype(float)
    
    # Format title case
    for i in titleColumnName:
        df[i] = df[i].str.title()
    
    # Remove Whitespace
    df['category'] = df.loc[:, 'category'].str.strip()

    # Replace Typo Value
    df['category'].replace({'Liquor & Tpbacco': 'Liquor & Tobacco'}, inplace=True)

    # Column Creation
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['month_name'] = df['date'].dt.month_name()
    df['day_name'] = df['date'].dt.day_name()
    df['is_weekend'] = np.where(df['day_name'].isin(['Saturday', 'Sunday']), 'Yes', 'No')

    df.index.names = ['id']

    # Save Clean Data
    df.to_csv('/opt/airflow/data/P2M3_gilang_data_clean.csv', index=True)

def postToElasticSearch():
    '''
    This Function used to push cleaned data to elasticsearch

    Call Example:
        postToElasticSearch() 
    '''

    # Load File
    df = pd.read_csv('/opt/airflow/data/P2M3_gilang_data_clean.csv', index_col=False)
    es = Elasticsearch("Elasticsearch")
    es.ping()

    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="p2m3_gilang", body=doc)

default_args = {
    'owner': 'gilang',
    'start_date': dt.datetime(2023, 11, 3, 13, 30, 0) - dt.timedelta(hours=7),
}

with DAG('P2M3_gilang', default_args=default_args, 
            schedule_interval='30 6 * * *',
        ) as dag:

    # Task 1
    fetchFromPostgreSQL = PythonOperator(task_id='fetchFromPostgreSQL', python_callable=getData)

    # Task 2
    cleaningDataTask = PythonOperator(task_id='cleaningDataTask', python_callable=cleaningData)

    # Task 3
    postToElasticTask = PythonOperator(task_id='postToElasticTask', python_callable=postToElasticSearch)

fetchFromPostgreSQL >> cleaningDataTask >> postToElasticTask
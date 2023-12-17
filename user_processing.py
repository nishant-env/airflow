from airflow.models import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
import json
from pandas import json_normalize
from sqlalchemy import create_engine

default_args={
    'start_date': datetime(2023,11,15, 23, 30)
}

def process_user(ti):
    users=ti.xcom_pull(task_ids=['extracting_user'])
    if not len(users):
        raise ValueError('User is empty')
    user_det_all = users[0]['results'][0]
    user_det_filtered = {
        "email": user_det_all["email"],
        "firstname": user_det_all["name"]["first"],
        "lastname": user_det_all["name"]["last"],
        "country": user_det_all["location"]["country"],
        "username": user_det_all["login"]["username"],
        "password": user_det_all["login"]["password"]
    }
    ti.xcom_push("filtered_user", user_det_filtered)


def store_user(ti):
    user=ti.xcom_pull(task_ids=['processing_user'], key="filtered_user")
    if not user:
        raise ValueError("user empty")
    user_df=json_normalize(user)
    engine=create_engine('postgresql://postgres:Root.123@localhost:5432/pipeline_tuts')
    user_df.to_sql('users', engine, if_exists='append', index=False)




with DAG(
    "user_processing",
    schedule_interval='23 23 * * *',
    default_args=default_args,
    catchup=False
    ) as dag:
    # defining operators
    creating_table = PostgresOperator(
        postgres_conn_id="db_postgres",
        task_id="creating_table",
        sql="""
        create table if not exists users(
            email varchar(30) PRIMARY KEY,
            firstname varchar(30),
            lastname varchar(30),
            country varchar(30),
            username varchar(30),
            password varchar(30)
        )
        """
    )
    is_api_available=HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'

    )
    extracting_user=SimpleHttpOperator(
        task_id="extracting_user",
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter= lambda response: json.loads(response.text)
    )
    processing_user=PythonOperator(
        task_id="processing_user",
        python_callable=process_user
    )
    storing_user=PythonOperator(
        task_id="storing_user",
        python_callable=store_user
    )



creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user

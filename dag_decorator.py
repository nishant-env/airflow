from airflow.decorators import dag, task
from datetime import datetime
import requests, json
from typing import Dict, Any
from airflow.operators.email import EmailOperator


@dag(start_date=datetime(2023,12,16), schedule_interval='@daily', catchup=False)
def email_notification():

    @task(task_id='getmyip')
    def getip():
        r = requests.get('http://httpbin.org/get')
        return json.loads(r.text)

    @task(task_id='processip', multiple_outputs=True)
    def processip(ip: Dict[str, Any]):
        extracted_ip = ip['origin']
        return {"extracted_ip" : extracted_ip}   # multiple values will unrap the output value from dict and use key as xcom key.

    # @task(task_id='displayip')
    # def displayip(ext_ip: Dict[str, str]):
    #     print(f'You are watching from {ext_ip["extracted_ip"]}')

    # displayip(processip(getip()))


    ip_info = processip(getip())

    email_task = EmailOperator(
        task_id="email_task",
        to=['nishant.env@gmail.com'],
        subject="Hello from Airflow!",
        html_content=f'Seems like you are using airflow from {ip_info["extracted_ip"]}' 
        ## here if multiple_outputs was set to false, the value should be extracted as: ip_info['return_value']['extracted_ip']
    )
    ip_info >> email_task


# need to call this, else dag is undiscovered
email_notification()

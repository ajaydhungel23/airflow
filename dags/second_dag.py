from importlib import abc


try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd
    from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
    import mysql.connector


    print("All Dag modules are ok ......")
except Exception as e:
  print("Error  {} ".format(e))

mydb = mysql.connector.connect(
host="0ed4699bbd85 ",
user="root",
password="airflow",
database="info"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM infotable where task_id = 'first_function' ")

myresult = mycursor.fetchall()


for x in myresult:
   dagid1 = x[0]
   id1 = x[1]
   context1 =x[2]


def first_function_execute(**context):
    print("first_function_execute   ")
    


def second_function_execute(**context):
    print("second function execute")


with DAG(
        dag_id="second_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1)
               },
        catchup=False) as F :

    first_function_task = PythonOperator(
            task_id= id1,
            python_callable=first_function_execute,
            provide_context= context1
        )

    second_function_task = PythonOperator(
            task_id="second_function_execute",
            python_callable=second_function_execute,
            provide_context=True,
        )

first_function_task >> second_function_task



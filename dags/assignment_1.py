import csv
import json
from tabulate import tabulate
from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def convert_to_csv(**kwargs):
        """
    Converts a list of dictionaries retrieved from an XCom variable into a CSV file.

    Args:
        **kwargs (dict): Keyword arguments passed to the function, including the 'ti' (TaskInstance)
                         used to access XCom data.

    Returns:
        None
        """
        task_instance = kwargs['ti']
        response_data = task_instance.xcom_pull(task_ids="get_posts")  
        
        if response_data:
           
            csv_file_path = 'Assignment_1/posts.csv'  
            
            with open(csv_file_path, 'w', newline='') as csvfile: 
                csv_writer = csv.writer(csvfile)
                header = list(response_data[0].keys())
                csv_writer.writerow(header)
            
                for item in response_data:
                    csv_writer.writerow(list(item.values()))
        else:
            print("No JSON data available in XCom to convert to CSV.")


def log_xcom_data(**kwargs):
    ''' 
    Logs data retrieved from an XCom variable in a human-readable table format.

    Args:
        **kwargs (dict): Keyword arguments passed to the function, including the 'ti' (TaskInstance)
                         used to access XCom data.

    Returns:
        None
    '''
    
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='read_from_postgres')

    # Check if there is any data in XCom
    if not xcom_value:
        print("No data in XCom.")
    else:
        # Log the XCom data as a table
        table = tabulate(xcom_value, headers=["user_id", "title", "body"], tablefmt="pretty")
        print(f"XCom Data as Table:\n{table}")


# Defining DAG for "assignment_dag"
with DAG(
    dag_id="assignmnet_dag",
    schedule_interval= '*/15 * * * *', #runs every 15 min
    start_date= datetime(2023,9,10),
    catchup= False
)as dag:
    

    # Task to check if the API is active by making an HTTP request to an endpoint. 
    task_is_api_active= HttpSensor(
        task_id= 'api_active',
        http_conn_id='api_posts',
        endpoint= 'posts/'
    )

    # Task to retrieve posts data from the API using an HTTP GET request.
    task_get_posts= SimpleHttpOperator(
        task_id= 'get_posts',
        http_conn_id='api_posts',
        endpoint= 'posts/',
        method= 'GET',
        response_filter= lambda response: json.loads(response.text),
        log_response= True
    )
   
    # Task to convert the retrieved JSON data to a CSV file.
    task_convert_to_csv=PythonOperator(
        task_id= 'convert_to_csv',
        python_callable= convert_to_csv,
        provide_context=True,
    )

    # Task to move the generated CSV file to the /tmp directory.
    task_move_to_temp= BashOperator(
        task_id= 'move_to_temp',
        bash_command= 'mv /home/user/airflow/Assignment_1/posts.csv /tmp'
    )

    # Task to check for the existence of a CSV file in the /tmp directory.
    task_check_file=FileSensor(
        task_id='check_for_csv',
        filepath= 'tmp/posts.csv',
        poke_interval=10,
        timeout=300,
        mode='poke'
    )

    # Task to store the CSV data into a PostgreSQL database.
    task_store_to_postgres=PostgresOperator(
        task_id= 'store_to_postgres',
        postgres_conn_id='postgres_local',
        sql= '''CREATE TABLE IF NOT EXISTs airflow(
        id INT,
        user_id INT,
        title VARCHAR,
        body VARCHAR
        );
        TRUNCATE TABLE airflow;
        COPY airflow FROM '/tmp/posts.csv' CSV HEADER;''',

    )

    # Task to submit a Spark job using spark-submit.
    task_spark_submit=BashOperator(
         task_id='spark_submit',
         bash_command='/opt/spark/bin/spark-submit --driver-class-path /lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.6.0.jar /home/user/airflow/python_scripts/sample.py'
    )

    # Task to read data from a PostgreSQL database.
    task_read_from_postgres= PostgresOperator(
         task_id='read_from_postgres',
         postgres_conn_id= 'postgres_local',
         sql='''SELECT * FROM posts_spark''',
    )

    # Task to log data retrieved from XCom as a table.
    task_log=PythonOperator(
        task_id='log_xcom_data',
        python_callable= log_xcom_data,
    )

    # Define the task dependencies in the Airflow DAG.
    task_is_api_active >> task_get_posts >> task_convert_to_csv >> task_move_to_temp>> task_check_file >> task_store_to_postgres >> task_spark_submit >> task_read_from_postgres >> task_log
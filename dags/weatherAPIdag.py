from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import requests
import os
from uuid import uuid4
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.email import send_email
from airflow.models import Variable
from airflow.utils import timezone

def send_failure_email(context):
    """Send email notification on task failure"""
    task_instance = context['task_instance']
    dag = context['dag']
    
    subject = f"Airflow Task Failed: {task_instance.task_id}"
    html_content = f"""
    <h3>Task Failure Notification</h3>
    <p><strong>DAG:</strong> {dag.dag_id}</p>
    <p><strong>Task:</strong> {task_instance.task_id}</p>
    <p><strong>Execution Time:</strong> {context['logical_date']}</p>
    <p><strong>Run ID:</strong> {context['dag_run'].run_id}</p>
    <p><strong>Try Number:</strong> {task_instance.try_number}</p>
    <p><strong>Error:</strong> {context.get('exception', 'No error details available')}</p>
    <p><strong>Task State:</strong> {getattr(task_instance, 'state', 'FAILED')}</p>
    """
    
    send_email(
        to=['riseoflegend2004@gmail.com'],
        subject=subject,
        html_content=html_content
    )

def send_retry_email(context):
    """Send email notification on task retry"""
    task_instance = context['task_instance']
    dag = context['dag']
    
    subject = f"Airflow Task Retry: {task_instance.task_id}"
    html_content = f"""
    <h3>Task Retry Notification</h3>
    <p><strong>DAG:</strong> {dag.dag_id}</p>
    <p><strong>Task:</strong> {task_instance.task_id}</p>
    <p><strong>Execution Time:</strong> {context['logical_date']}</p>
    <p><strong>Run ID:</strong> {context['dag_run'].run_id}</p>
    <p><strong>Retry Number:</strong> {task_instance.try_number}</p>
    <p><strong>Max Retries:</strong> {task_instance.max_tries}</p>
    <p><strong>Reason:</strong> {context.get('reason', 'Retry triggered')}</p>
    """
    
    send_email(
        to=['riseoflegend2004@gmail.com'],
        subject=subject,
        html_content=html_content
    )

def send_success_email(context):
    """Send email notification on DAG success"""
    dag = context['dag']
    dag_run = context['dag_run']
    
    subject = f"Airflow DAG Completed Successfully: {dag.dag_id}"

    # Calculate duration properly with timezone handling
    start = dag_run.start_date
    end = dag_run.end_date
    
    if start and end:
        duration = str(end - start)
    elif start:
        # Use timezone-aware datetime
        from airflow.utils import timezone
        current_time = timezone.datetime.now()
        duration = str(current_time - start) + " (estimated)"
    else:
        duration = "Unknown"

    html_content = f"""
    <h3>DAG Success Notification</h3>
    <p><strong>DAG:</strong> {dag.dag_id}</p>
    <p><strong>Run ID:</strong> {dag_run.run_id}</p>
    <p><strong>Execution Time:</strong> {context['logical_date']}</p>
    <p><strong>Start Time:</strong> {start}</p>
    <p><strong>End Time:</strong> {end}</p>
    <p><strong>Duration:</strong> {duration}</p>
    <p>The weather data ETL pipeline has completed successfully!</p>
    """

    send_email(
        to=['riseoflegend2004@gmail.com'],
        subject=subject,
        html_content=html_content
    )

def send_completion_email_task(**context):
    """Dedicated function for completion email task"""
    dag = context['dag']
    dag_run = context['dag_run']
    
    subject = f"Weather ETL Pipeline Completed: {dag.dag_id}"
    
    # Get DAG run information with proper timezone handling
    start_time = dag_run.start_date
    end_time = dag_run.end_date
    
    # Handle timezone-aware datetime calculations
    if start_time and end_time:
        duration = str(end_time - start_time)
    elif start_time:
        # Use proper timezone-aware datetime
        from airflow.utils import timezone
        current_time = timezone.utcnow()  # This is the correct method
        duration = str(current_time - start_time) + " (estimated)"
    else:
        duration = "Unknown"
    
    html_content = f"""
    <h3>🎉 Weather ETL Pipeline Completion Report</h3>
    <p><strong>DAG ID:</strong> {dag.dag_id}</p>
    <p><strong>Run ID:</strong> {dag_run.run_id}</p>
    <p><strong>Execution Date:</strong> {context['logical_date']}</p>
    <p><strong>Start Time:</strong> {start_time}</p>
    <p><strong>End Time:</strong> {end_time}</p>
    <p><strong>Total Duration:</strong> {duration}</p>
    <p><strong>Status:</strong> ✅ SUCCESS</p>
    
    <h4>Pipeline Summary:</h4>
    <ul>
        <li>✅ Weather data extracted from OpenWeatherMap API</li>
        <li>✅ Data transformed and processed</li>
        <li>✅ Data loaded to S3 bucket</li>
        <li>✅ Data loaded to Snowflake database</li>
        <li>✅ Notifications sent to Slack channels</li>
    </ul>
    
    <p><em>All components of the weather data ETL pipeline have been executed successfully!</em></p>
    """
    
    send_email(
        to=['riseoflegend2004@gmail.com'],
        subject=subject,
        html_content=html_content
    )

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def transform_data(task_instance):
    """Transform the raw weather data"""
    data_list = task_instance.xcom_pull(task_ids="Extract.extract_weather_data", key="weather_data")

    if not data_list:
        raise ValueError("❌ No weather data received from extract_weather_data. Check API or XCom push.")

    transformed_data_list = []
    for data in data_list:
        city = data.get("name")
        weather_description = data.get("weather", [{}])[0].get('description')
        temp_farenheit = kelvin_to_fahrenheit(data.get("main", {}).get("temp", 0))
        feels_like_farenheit = kelvin_to_fahrenheit(data.get("main", {}).get("feels_like", 0))
        min_temp_farenheit = kelvin_to_fahrenheit(data.get("main", {}).get("temp_min", 0))
        max_temp_farenheit = kelvin_to_fahrenheit(data.get("main", {}).get("temp_max", 0))
        pressure = data.get("main", {}).get("pressure")
        humidity = data.get("main", {}).get("humidity")
        wind_speed = data.get("wind", {}).get("speed")
        time_of_record = datetime.utcfromtimestamp(data.get('dt', 0) + data.get('timezone', 0))
        sunrise_time = datetime.utcfromtimestamp(data.get('sys', {}).get('sunrise', 0) + data.get('timezone', 0))
        sunset_time = datetime.utcfromtimestamp(data.get('sys', {}).get('sunset', 0) + data.get('timezone', 0))

        transformed_data = {
            "City": city,
            "Description": weather_description,
            "Temperature (F)": temp_farenheit,
            "Feels Like (F)": feels_like_farenheit,
            "Minimun Temp (F)": min_temp_farenheit,
            "Maximum Temp (F)": max_temp_farenheit,
            "Pressure": pressure,
            "Humidty": humidity,
            "Wind Speed": wind_speed,
            "Time of Record": time_of_record,
            "Sunrise (Local Time)": sunrise_time,
            "Sunset (Local Time)": sunset_time
        }
        transformed_data_list.append(transformed_data)

    task_instance.xcom_push(key='transformed_weather_data', value=transformed_data_list)
    print(f"✅ Successfully transformed {len(transformed_data_list)} weather records")

def load_data_to_s3(task_instance):
    """Load transformed data to S3 bucket"""
    transformed_data = task_instance.xcom_pull(task_ids="transform_weather_data", key="transformed_weather_data")
    
    if not transformed_data:
        raise ValueError("No transformed data found in XCom for S3 upload")
    
    # Create CSV and upload to S3
    df_data = pd.DataFrame(transformed_data)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    filename = f"current_weather_data_{dt_string}_{uuid4().hex}.csv"
    df_data.to_csv(filename, index=False)
    file_path = f"/home/sumit/dev/airflow_project/simple ETL with Airflow/{filename}"
    s3_hook = S3Hook(aws_conn_id="aws_s3_bucket")
    s3_hook.load_file(
        filename=filename,
        key=f"weather-data/{filename}",
        bucket_name="weather-api-etl-airflow-results",
        replace=True
    )
    os.remove(file_path)
    print(f"Successfully uploaded {filename} to S3 with {len(transformed_data)} records")

def insert_data_to_snowflake(task_instance):
    """Insert transformed data into Snowflake table"""
    transformed_data = task_instance.xcom_pull(task_ids="transform_weather_data", key="transformed_weather_data")
    
    if not transformed_data:
        raise ValueError("No transformed data found in XCom for Snowflake insertion")
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    
    # Prepare insert statements
    insert_sql = """
    INSERT INTO weather_data (
        city, description, temperature_f, feels_like_f, minimum_temp_f, 
        maximum_temp_f, pressure, humidity, wind_speed, time_of_record,
        sunrise_local_time, sunset_local_time
    ) VALUES (
        %(city)s, %(description)s, %(temperature_f)s, %(feels_like_f)s, 
        %(minimum_temp_f)s, %(maximum_temp_f)s, %(pressure)s, %(humidity)s, 
        %(wind_speed)s, %(time_of_record)s, %(sunrise_local_time)s, %(sunset_local_time)s
    )
    """
    
    # Transform data format for Snowflake insertion
    snowflake_data = []
    for record in transformed_data:
        snowflake_record = {
            'city': record['City'],
            'description': record['Description'],
            'temperature_f': record['Temperature (F)'],
            'feels_like_f': record['Feels Like (F)'],
            'minimum_temp_f': record['Minimun Temp (F)'],
            'maximum_temp_f': record['Maximum Temp (F)'],
            'pressure': record['Pressure'],
            'humidity': record['Humidty'],
            'wind_speed': record['Wind Speed'],
            'time_of_record': record['Time of Record'],
            'sunrise_local_time': record['Sunrise (Local Time)'],
            'sunset_local_time': record['Sunset (Local Time)']
        }
        snowflake_data.append(snowflake_record)
    
    # Execute batch insert
    for record in snowflake_data:
        snowflake_hook.run(insert_sql, parameters=record)
    
    print(f"Successfully inserted {len(snowflake_data)} records into Snowflake")

def send_s3_success_notification(context):
    """Send notification for successful S3 upload"""
    task_instance = context['task_instance']
    dag = context['dag']
    
    subject = f"S3 Upload Successful: {dag.dag_id}"
    html_content = f"""
    <h3>S3 Upload Success Notification</h3>
    <p><strong>DAG:</strong> {dag.dag_id}</p>
    <p><strong>Task:</strong> {task_instance.task_id}</p>
    <p><strong>Run ID:</strong> {context['dag_run'].run_id}</p>
    <p><strong>Execution Time:</strong> {context['logical_date']}</p>
    <p>Weather data has been successfully uploaded to S3 bucket!</p>
    """
    
    send_email(
        to=['riseoflegend2004@gmail.com'],
        subject=subject,
        html_content=html_content
    )

def send_snowflake_success_notification(context):
    """Send notification for successful Snowflake insertion"""
    task_instance = context['task_instance']
    dag = context['dag']
    
    subject = f"Snowflake Load Successful: {dag.dag_id}"
    html_content = f"""
    <h3>Snowflake Load Success Notification</h3>
    <p><strong>DAG:</strong> {dag.dag_id}</p>
    <p><strong>Task:</strong> {task_instance.task_id}</p>
    <p><strong>Run ID:</strong> {context['dag_run'].run_id}</p>
    <p><strong>Execution Time:</strong> {context['logical_date']}</p>
    <p>Weather data has been successfully loaded into Snowflake database!</p>
    """
    
    send_email(
        to=['riseoflegend2004@gmail.com'],
        subject=subject,
        html_content=html_content
    )

def fetch_weather_data(task_instance, **kwargs):
    cities = ["London", "Delhi","Mumbai","Kolkata","Chennai","Bengaluru"]
    api_key = os.getenv("OPEN_WEATHER_API_KEY")
    all_data = []

    for city in cities:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
        response = requests.get(url)
        data = response.json()
        all_data.append(data)
    print(all_data)
    task_instance.xcom_push(key='weather_data', value=all_data)
    return all_data

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2025,6,12),
    'retries':2,
    'retry_delay':timedelta(minutes=2),
    'on_failure_callback': send_failure_email,
    'on_retry_callback': send_retry_email,
}

with DAG(
    'weatherAPI_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    on_success_callback=send_success_email,
    max_active_runs=1,
    description='Weather API ETL Pipeline with parallel S3 and Snowflake loading'
) as dag:
    
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weather_api',
        endpoint='/data/2.5/weather?q=India,London&appid=af341a72bead1e87df154a73ef0dd69d',
        timeout=60,
        poke_interval=30,
        mode='poke'
    )
    
    with TaskGroup(group_id='Extract', tooltip='Extract API Data') as Extract:
        extract_weather_data = PythonOperator(
            task_id='extract_weather_data',
            python_callable=fetch_weather_data,
        )
    
    # Transform data (single task)
    transform_weather_data = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_data,
    )
    
    # Parallel loading task groups
    with TaskGroup(group_id='load_to_s3', tooltip='Loading data to S3 bucket') as s3_load:
        load_s3_data = PythonOperator(
            task_id='load_data_to_s3',
            python_callable=load_data_to_s3,
            on_success_callback=send_s3_success_notification,
        )
        
        notify_s3_slack = SlackWebhookOperator(
            task_id='notify_s3_slack',
            slack_webhook_conn_id='slack_notification_conn',
            message='✅ Weather data successfully uploaded to S3 bucket!',
            channel='#weather-api-etl-airflow',
        )
        
        load_s3_data >> notify_s3_slack
    
    with TaskGroup(group_id='load_to_snowflake', tooltip='Loading data to Snowflake') as snowflake_load:
        create_table_snowflake = SQLExecuteQueryOperator(
            task_id='creating_table_snowflake',
            conn_id='snowflake_conn_id',
            sql='''
                CREATE TABLE IF NOT EXISTS weather_data (
                    id NUMBER IDENTITY(1,1) PRIMARY KEY,
                    city VARCHAR(100) NOT NULL,
                    description VARCHAR(100) DEFAULT 'No description',
                    temperature_f FLOAT NOT NULL,
                    feels_like_f FLOAT NOT NULL,
                    minimum_temp_f FLOAT NOT NULL,
                    maximum_temp_f FLOAT NOT NULL,
                    pressure INT NOT NULL,
                    humidity INT NOT NULL,
                    wind_speed FLOAT DEFAULT 0.0,
                    time_of_record TIMESTAMP NOT NULL,
                    sunrise_local_time TIMESTAMP,
                    sunset_local_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''
        )
        
        insert_weather_data = PythonOperator(
            task_id='insert_weather_data',
            python_callable=insert_data_to_snowflake,
            on_success_callback=send_snowflake_success_notification,
        )
        
        notify_snowflake_slack = SlackWebhookOperator(
            task_id='notify_snowflake_slack',
            slack_webhook_conn_id='slack_notification_conn',
            message='✅ Weather data successfully loaded into Snowflake database!',
            channel='#weather-api-etl-airflow',
        )
        
        create_table_snowflake >> insert_weather_data >> notify_snowflake_slack
    
    # Final completion notification
    final_notification = SlackWebhookOperator(
        task_id='final_completion_notification',
        slack_webhook_conn_id='slack_notification_conn',
        message='🎉 Weather API ETL Pipeline completed successfully! Data available in both S3 and Snowflake.',
        channel='#weather-api-etl-airflow',
        trigger_rule='all_success'
    )
    
    # Success email notification task - Fixed with proper callable
    send_completion_email = PythonOperator(
        task_id='send_completion_email',
        python_callable=send_completion_email_task,
        trigger_rule='all_success'
    )
    
    # Task dependencies - Fixed the incomplete dependency chain
    is_weather_api_ready >> Extract >> transform_weather_data
    transform_weather_data >> [s3_load, snowflake_load]
    [s3_load, snowflake_load] >> final_notification
    [s3_load, snowflake_load] >> send_completion_email
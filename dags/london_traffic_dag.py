from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.tfl_hook import TfLHook
from datetime import datetime, timedelta
import json
import pandas as pd

def extract_bus_data():
    hook = TfLHook()

    data = hook.get_data("Line/159/Arrivals")   
    
    file_path = f"/usr/local/airflow/include/bus_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    # with open(file_path, 'w') as f:
    #     json.dump(data[:100], f) 
    df = pd.DataFrame(data)
    df_limited = df.head(100)
    df_limited.to_json(file_path, orient='records', lines=True)
    print(f"Đã trích xuất dữ liệu thành công vào {file_path}")

default_args = {
    'owner': 'gemini_user',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='london_bus_extraction_v1',
    default_args=default_args,
    schedule='@hourly', 
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id='extract_from_api',
        python_callable=extract_bus_data
    )
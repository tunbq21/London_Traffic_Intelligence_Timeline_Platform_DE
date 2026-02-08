from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.tfl_hook import TfLHook
from datetime import datetime, timedelta
import json
import logging
import pandas as pd
from pathlib import Path





columns = [
"id",
"category",
"severity",
"locationName",
"corridorName",
"comments",
"startDateTime",
"endDateTime",
"lastModDateTime",
"point"
]

# https://api.tfl.gov.uk/Road/All/Disruption?app_key=daceb72060ae4121a8c4a3b3b76a950a

def extract_road_disruption_data():
    try: 
        hook = TfLHook()
        data = hook.get_data("Road/All/Disruption")
        if not data:
            logging.info("Không có dữ liệu từ API.")
            return None

        # Định nghĩa folder bạn muốn chắc chắn là nó tồn tại
        folder_path = "/usr/local/airflow/include/json/road_disruption"

        # Tạo folder
        Path(folder_path).mkdir(parents=True, exist_ok=True)

        file_path = f"/usr/local/airflow/include/json/road_disruption/road_disruption_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        chunk_size = 200
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            df = pd.json_normalize(chunk)
            existing_cols = df.columns.intersection(columns)
            df_filtered = df[existing_cols]
            df_filtered.to_json(file_path, orient='records', lines=True, mode='a')
            logging.info(f"Đã xử lý chunk {i//chunk_size + 1}, ghi vào {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Lỗi trích xuất: {e}")
        raise

def jsonl_to_csv(**kwargs):
    try:
        ti = kwargs['ti']
        jsonl_file_path = ti.xcom_pull(task_ids='extraction_group.extract_road_disruption_data')
        if not jsonl_file_path:
            logging.info("Không có file JSONL để chuyển đổi.")
            return None
        folder_path = "/usr/local/airflow/include/csv/road_disruption"
        Path(folder_path).mkdir(parents=True, exist_ok=True)
        df = pd.read_json(jsonl_file_path, lines=True)
        csv_file_path = jsonl_file_path.replace('.jsonl', '.csv').replace('/json/road_disruption/', '/csv/road_disruption/')
        df.to_csv(csv_file_path, index=False)
        logging.info(f"Chuyển đổi thành công sang CSV: {csv_file_path}")
        return csv_file_path
    except Exception as e:
        logging.error(f"Lỗi chuyển đổi JSONL sang CSV: {e}")
        raise

default_args = {
    'owner': 'Tuan Quang',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['tbuiquang2002@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


with DAG(
    dag_id='london_road_disruption_dag',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    with TaskGroup('extraction_group') as extraction_group:
        extract_task = PythonOperator(
            task_id='extract_road_disruption_data',
            python_callable=extract_road_disruption_data
        )

        jsonl_to_csv_task = PythonOperator(
            task_id='jsonl_to_csv',
            python_callable=jsonl_to_csv,
        )

        extract_task >> jsonl_to_csv_task

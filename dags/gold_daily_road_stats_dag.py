from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'Tuan Quang',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

@dag(
    dag_id='london_road_gold_transform',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule='@daily',      # Chạy tổng hợp mỗi ngày một lần
    catchup=False,
    tags=['gold', 'analytics', 'azure_postgres']
)
def gold_transformation_pipeline():

    @task
    def create_gold_table():
        """Tạo bảng tổng hợp nếu chưa tồn tại"""
        pg_hook = PostgresHook(postgres_conn_id='azure_postgres_conn')
        sql = """
        CREATE TABLE IF NOT EXISTS gold_daily_road_stats (
            report_date DATE,
            category VARCHAR(100),
            severity VARCHAR(100),
            total_disruptions INT,
            avg_duration_hours FLOAT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (report_date, category, severity)
        );
        """
        pg_hook.run(sql)

    @task
    def build_daily_summary():
        """Logic chính: Tổng hợp dữ liệu từ bảng Silver sang bảng Gold"""
        pg_hook = PostgresHook(postgres_conn_id='azure_postgres_conn')
        
        # SQL xử lý định dạng thời gian TEXT sang TIMESTAMP và tính toán
        sql = """
            INSERT INTO gold_daily_road_stats (report_date, category, severity, total_disruptions, avg_duration_hours)
            SELECT 
                CAST(startDateTime AS DATE) as report_date,
                category,
                severity,
                COUNT(id) as total_disruptions,
                -- Tính khoảng cách giờ giữa Start và End
                AVG(
                    EXTRACT(EPOCH FROM (CAST(endDateTime AS TIMESTAMP) - CAST(startDateTime AS TIMESTAMP))) / 3600
                ) as avg_duration_hours
            FROM london_road_disruptions
            WHERE startDateTime IS NOT NULL 
              AND endDateTime IS NOT NULL
              AND startDateTime != '' -- Tránh các dòng trống
            GROUP BY 1, 2, 3
            ON CONFLICT (report_date, category, severity) 
            DO UPDATE SET 
                total_disruptions = EXCLUDED.total_disruptions,
                avg_duration_hours = EXCLUDED.avg_duration_hours,
                last_updated = CURRENT_TIMESTAMP;
        """
        pg_hook.run(sql)
        logging.info("Data Mart đã được cập nhật thành công.")

    create_gold_table() >> build_daily_summary()

gold_dag = gold_transformation_pipeline()
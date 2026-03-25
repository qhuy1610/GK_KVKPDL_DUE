from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Thêm đường dẫn chứa script của bạn vào hệ thống để Airflow có thể import được
# Giả sử file script của bạn nằm ở: /opt/airflow/scripts/etl_minio_process.py
sys.path.append('/opt/airflow/scripts')

# Import hàm main_etl từ file script của bạn
from etl_minio_process import main_etl

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 21), # Chỉnh lại ngày phù hợp với bạn
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
with DAG(
    'giangvien_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL xử lý dữ liệu giảng viên từ MinIO Bronze sang Silver',
    schedule_interval='0 0 * * 0', # Chạy hàng ngày hoặc để None nếu chạy tay
    catchup=False,
    tags=['etl', 'minio', 'data_engineering'],
) as dag:

    # Task duy nhất gọi hàm main_etl mà bạn đã bọc
    run_etl_task = PythonOperator(
        task_id='run_main_etl_process',
        python_callable=main_etl,
    )

    run_etl_task
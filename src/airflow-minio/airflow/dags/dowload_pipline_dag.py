from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os

LIST_FILES = [
    "df_final_thongtinchung1.csv",
    "df_final_khoahoc1.csv",
    "df_final_baibao1.csv",
    "df_final_giaithuong1.csv",
    "df_final_giangdaysaudaihoc1.csv",
    "df_final_khenthuong1.csv",
    "df_final_mongiangday1.csv",
    "df_final_sach1.csv"
]

def download_file_from_minio(file_name):

    print("Bắt đầu tải:", file_name)

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="JXHpslDC7PokmWYb",
        aws_secret_access_key="iVNGopKzabBfzrEFGfj08kuM6KFve2Xv",
        region_name="us-east-1"
    )

    local_folder = "/opt/airflow/data"
    os.makedirs(local_folder, exist_ok=True)

    local_path = f"{local_folder}/{file_name}"

    try:
        s3.download_file("silver", file_name, local_path)
        print("Tải thành công:", file_name)

    except Exception as e:
        print("Lỗi khi tải:", file_name)
        print(e)
        raise e


with DAG(
    dag_id="download_silver_bucket2",
    start_date=datetime(2026,3,21),
    schedule_interval="15 0 * * 0",
    catchup=False
) as dag:

    tasks = []

    for file_name in LIST_FILES:

        task = PythonOperator(
            task_id=f"download_{file_name.replace('.','_')}",
            python_callable=download_file_from_minio,
            op_args=[file_name]
        )

        tasks.append(task)
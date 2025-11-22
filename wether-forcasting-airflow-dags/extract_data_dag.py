from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from datetime import timedelta
import pandas as pd
import requests


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_data_dag",
    default_args=default_args,
    description="DAG to extract weather data and upload to GCS",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["weather", "data_extraction"],
) as dag:

    # task 1 - extract data in virtualenv (reuse system packages)
    def _extract_openweather_data(api_key: str) -> str:
        endpoint = "https://api.openweathermap.org/data/2.5/forecast"
        params = {"q": "London,uk", "appid": api_key}

        response = requests.get(endpoint, params=params)
        response.raise_for_status()

        df = pd.json_normalize(response.json()["list"])
        return df.to_csv(index=False)

    extract_weather_data = PythonVirtualenvOperator(
        task_id="extract_weather_data",
        python_callable=_extract_openweather_data,
        requirements=["requests", "pandas"],
        system_site_packages=True,
        op_args=[Variable.get("OPENWEATHER_API_KEY")],
    )

    # task 2 - upload to GCS
    def _upload_to_gcs(ds: str, **kwargs):
        ti = kwargs["ti"]
        csv_data = ti.xcom_pull(task_ids="extract_weather_data")
        hook = GCSHook()
        hook.upload(
            bucket_name="wether_forcasting_rawzone",
            object_name=f"weather/{ds}/forecast.csv",
            data=csv_data,
            mime_type="text/csv",
        )

    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=_upload_to_gcs,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # task 3 - trigger next DAG
    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_data_transform_dag",
        trigger_dag_id="transformed_wether_data_to_bq",
        wait_for_completion=False,
    )

    # DAG flow
    extract_weather_data >> upload_to_gcs >> trigger_transform_dag

from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.base import PokeReturnValue
from include.stock_market.tasks import _get_formatted_csv, _stock_prices

SYMBOL = "NVDA"


@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["stock_market"],
)
def stock_market():
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"https://{api.host}{api.extra_dejson['endpoint']}{SYMBOL}"
        response = requests.get(url, headers=api.extra_dejson["headers"])
        condition = response.json()["chart"]["error"] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    def get_stock_prices(symbol: str):
        api = BaseHook.get_connection("stock_api")
        url = f"https://{api.host}{api.extra_dejson['endpoint']}{symbol}"
        response = requests.get(url, headers=api.extra_dejson["headers"])
        data = response.json()
        # Tambahkan logika untuk memproses data di sini
        return data

    get_stock_prices_task = PythonOperator(
        task_id="get_stock_prices",
        python_callable=get_stock_prices,
        op_kwargs={"symbol": SYMBOL},
    )

    stock_prices = PythonOperator(
        task_id="stock_prices",
        python_callable=_stock_prices,
        op_kwargs={
            "stock": '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}'
        },
    )

    format_prices = DockerOperator(
        task_id="format_prices",
        image="airflow/stock-app",
        container_name="format_prices",
        api_version="auto",
        auto_remove=True,
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "SPARK_APPLICATION_ARGS": '{{ task_instance.xcom_pull(task_ids="stock_prices") }}'
        },
    )

    get_formatted_csv = PythonOperator(
        task_id="get_formatted_csv",
        python_callable=_get_formatted_csv,
        op_kwargs={"path": '{{ task_instance.xcom_pull(task_ids="stock_prices") }}'},
    )

    (
        is_api_available()
        >> get_stock_prices_task
        >> stock_prices
        >> format_prices
        >> get_formatted_csv
    )


stock_market()

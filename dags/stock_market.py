from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue

SYMBOL = "AAPL"  # for Apple Inc. stock


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

    is_api_available() >> get_stock_prices_task


stock_market()

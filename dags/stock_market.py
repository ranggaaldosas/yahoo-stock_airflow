from datetime import datetime

from airflow.decorators import dag


@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["stock_market"],
)
def stock_market():
    pass


stock_market()

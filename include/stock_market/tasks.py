import json
from io import BytesIO

import requests
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from minio import Minio

BUCKET_NAME = "stock-market"


def _get_minio_client():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,
    )
    return client


def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    api = BaseHook.get_connection("stock_api")
    response = requests.get(url, headers=api.extra_dejson["headers"])

    if response.status_code != 200:
        raise ValueError(
            f"Error in fetching stock prices: {response.status_code} - {response.text}"
        )

    try:
        data = response.json()
    except json.JSONDecodeError:
        raise ValueError(f"Error decoding JSON response: {response.text}")

    if (
        "chart" not in data
        or "result" not in data["chart"]
        or not data["chart"]["result"]
    ):
        raise ValueError("Unexpected API response format.")

    return json.dumps(data["chart"]["result"][0])


def _stock_prices(stock):
    print(f"Isi dari stock sebelum parsing: {stock}")

    # Check if stock is already a dictionary
    if isinstance(stock, dict):
        parsed_stock = stock
    else:
        try:
            # Try to parse as JSON
            parsed_stock = json.loads(stock)
        except json.JSONDecodeError:
            # If JSON parsing fails, try to evaluate as a Python literal
            import ast

            try:
                parsed_stock = ast.literal_eval(stock)
            except (ValueError, SyntaxError):
                raise ValueError(f"Unable to parse stock data: {stock}")

    # Extract the relevant data from the nested structure
    if (
        "chart" in parsed_stock
        and "result" in parsed_stock["chart"]
        and parsed_stock["chart"]["result"]
    ):
        stock_data = parsed_stock["chart"]["result"][0]
    else:
        raise ValueError(f"Invalid stock data format: {parsed_stock}")

    if "meta" not in stock_data or "symbol" not in stock_data["meta"]:
        raise ValueError(f"Missing required fields in stock data: {stock_data}")

    symbol = stock_data["meta"]["symbol"]
    data = json.dumps(stock_data, ensure_ascii=False).encode("utf8")

    if len(data) == 0:
        raise ValueError("Stock data is empty, cannot save to Minio.")

    try:
        client = _get_minio_client()
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)

        objw = client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f"{symbol}/prices.json",
            data=BytesIO(data),
            length=len(data),
        )
        return f"{objw.bucket_name}/{symbol}"
    except Exception as e:
        raise ValueError(f"Error saving stock data to Minio: {str(e)}")


def _get_formatted_csv(path):
    # path = "stock-market/NVDA"
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            return obj.object_name
    raise AirflowNotFoundException("The csv file does not exist.")

from datetime import datetime
from pathlib import Path
import pendulum
import yfinance as yf
import os

# Importing necessary modules from Airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor

default_args = {
    "start_date": datetime(2025, 3, 5),
}

def load_stocks():
    with open('/tmp/stocks.txt', 'r') as fp:
        stocks = [line.strip() for line in fp.readlines()]
    return stocks

def lookup_prices(stocks):
    print('-'*72)
    print(stocks)
    print('-'*72)
    dat = yf.Tickers(' '.join(stocks))
    print('-'*72)
    print(dat)
    print('-'*72)
    prices = {_:dat.tickers[_].info['currentPrice'] for _ in stocks}
    with open('/tmp/stock_prices.txt', 'w') as fp:
        for stock in stocks:
            fp.write(f"{stock}: {prices[stock]}\n")


def lookup_volumes(stocks):
    dat = yf.Tickers(' '.join(stocks))
    volumes = {_:dat.tickers[_].info['volume'] for _ in stocks}
    with open('/tmp/stock_volumes.txt', 'w') as fp:
        for stock in stocks:
            fp.write(f"{stock}: {volumes[stock]}\n")

def remove_stocks_file():
    os.remove('/tmp/stocks.txt')

with DAG(
    "lookup_stocks",
    default_args=default_args,
    schedule=pendulum.duration(days=1),
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    wait_for_file = FileSensor(
        task_id="wait_for_file",  # Task ID for the FileSensor
        filepath="/tmp/stocks.txt",  # Path to the file to wait for
        poke_interval=10,  # Interval in seconds to check for the file
        timeout=600,  # Timeout in seconds before failing the task
    )

    load_stocks = PythonOperator(
        task_id="loads_stocks_task",
        python_callable=load_stocks,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=remove_stocks_file,
    )

    with TaskGroup('group1') as group1:
        task1 = PythonOperator(
            task_id="lookup_prices_task",
            python_callable=lookup_prices,
            op_args=[load_stocks.output],
        )
        task2 = PythonOperator(
            task_id="lookup_volumes_task",
            python_callable=lookup_volumes,
            op_args=[load_stocks.output],
        )

    start >> load_stocks >> group1 >> end

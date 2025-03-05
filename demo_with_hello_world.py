from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# This is a DAG (Directed Acyclic Graph) file
# A DAG is a collection of tasks with dependencies
# DAG are a description of how to run a workflow
# This file will be placed in the DAGs folder "~/airflow/dags"


# This is a simple Python function that will be called by the DAG
# We call this a task
def hello_world():
    # This function can do anything you want, usually it will call
    # other functions or scripts to do some work

    # For now, it will just print "Hello, world!" to the console
    print("Hello, world!")


# DAGs are defined using the DAG class
# This is a minimal DAG definition
# The with statement is used to create a context for the DAG
with DAG(
    "demo_hello_world",  # This is the ID of the DAG
    schedule_interval="@daily",  # This is the schedule for the DAG
    default_args={
        "start_date": datetime(
            2025, 1, 1
        ),  # This is the start date of the DAG
    },
) as dag:
    # A DAG is a collection of tasks with dependencies
    # A task can be defined using an Operator
    # This operator is for tasks coded in Python
    hello_world_task = PythonOperator(
        task_id="hello_world",  # This is the ID of the task
        python_callable=hello_world,  # This is the function to call
        # this line can be removed
        # dag=dag,  # This is the DAG to which the task belongs
    )

    # This defines the order of execution of the tasks when running the DAG
    hello_world_task


# clean up the dag
# this line can be removed, not really needed
# del dag

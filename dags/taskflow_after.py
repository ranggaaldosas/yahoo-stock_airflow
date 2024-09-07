import random
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="A simple DAG to generate and check random numbers using TaskFlow API",
)
def random_number_checker_taskflow():
    # Define the first task using the @task decorator
    @task
    def generate_random_number():
        number = random.randint(1, 100)
        print(f"Generated random number: {number}")
        return number

    # Define the second task using the @task decorator
    @task
    def check_even_odd(number: int):
        result = "even" if number % 2 == 0 else "odd"
        print(f"The number {number} is {result}.")

    number = generate_random_number()
    check_even_odd(number)


# Instantiate the DAG
random_number_checker_taskflow()

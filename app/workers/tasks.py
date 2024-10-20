import os
import celery
import httpx
from celery import chord, group, subtask
from celery.utils.log import get_task_logger
from typing import List
from shared.app import app

logger = get_task_logger(__name__)


#############################
# API dependant tasks
#############################

API_BASE_URL = os.getenv("API_BASE_URL")

def call_api_sync(url: str) -> float:
    with httpx.Client() as client:
        response = client.get(url)
        response.raise_for_status()
        result = response.json()
        return result["result"]

def call_api_sync_binary_operation(operation: str, x: float, y: float) -> float:
    return call_api_sync(url=f"{API_BASE_URL}/{operation}?x={x}&y={y}")

def call_api_sync_single_operation(operation: str, x: float) -> float:
    return call_api_sync(url=f"{API_BASE_URL}/{operation}?x={x}")

@app.task(name="add_numbers")
def add_numbers(x: float, y: float) -> float:
    logger.info(f"Calling add API for: {x} + {y}")
    result = call_api_sync_binary_operation("add", x, y)
    logger.info(f"Result: {result}")
    return result

@app.task(name="subtract_numbers")
def subtract_numbers(x: float, y: float) -> float:
    logger.info(f"Calling subtract API for: {x} - {y}")
    result = call_api_sync_binary_operation("subtract", x, y)
    logger.info(f"Result: {result}")
    return result

@app.task(name="multiply_numbers")
def multiply_numbers(x: float, y: float) -> float:
    logger.info(f"Calling multiply API for: {x} * {y}")
    result = call_api_sync_binary_operation("multiply", x, y)
    logger.info(f"Result: {result}")
    return result

@app.task(name="divide_numbers")
def divide_numbers(x: float, y: float) -> float:
    logger.info(f"Calling divide API for: {x} / {y}")
    result = call_api_sync_binary_operation("divide", x, y)
    logger.info(f"Result: {result}")
    return result

@app.task(name="double_number")
def double_number(x: float) -> float:
    logger.info(f"Calling double API for: {x}")
    result = call_api_sync_single_operation("double", x)
    logger.info(f"Result: {result}")
    return result

@app.task(name="square_number")
def square_number(x: float) -> float:
    logger.info(f"Calling square API for: {x}")
    result = call_api_sync_single_operation("square", x)
    logger.info(f"Result: {result}")
    return result

#############################
# Local computation tasks
#############################

# @app.task(name="double_number")
# def double_number(x: float) -> float:
#     logger.info(f"Doubling number: {x}")
#     return x * 2

# @app.task(name="square_number")
# def square_number(x: float) -> float:
#     logger.info(f"Squaring number: {x}")
#     return x * x

@app.task(name="sum_numbers")
def sum_numbers(numbers: List[float]) -> float:
    logger.info(f"Summing numbers: {numbers}")
    return sum(numbers)

#############################
# Common tasks
#############################

@app.task(name="generate_list")
def generate_list(amount: float) -> List[float]:
    logger.info(f"Generating list of integers up to {amount}.")
    return list(range(1, amount + 1))

@app.task(name="process_numbers_individually", bind=True)
def process_numbers_individually(self, numbers: List[float], task_chain):
    logger.info(f"Processing all numbers: {numbers}")
    logger.info(f"Old task ID: {self.request.id}")
    
    branches = []
    s = subtask(task_chain)

    for number in numbers:
        sig = s.clone(args=(number,))
        if isinstance(sig, celery.canvas._chain):
            first_task = sig.tasks[0]
            first_task.args = (number,)
        branches.append(sig)

    return self.replace(group(branches))

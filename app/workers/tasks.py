import celery

from celery import chain, chord, group, subtask
from celery.utils.log import get_task_logger
from typing import List

from shared.app import app

logger = get_task_logger(__name__)

@app.task(name="generate_list")
def generate_list(amount: int) -> List[int]:
    logger.info(f"Generating list of integers up to {amount}.")
    return list(range(1, amount + 1))

@app.task(name="double_number")
def double_number(x: int) -> int:
    logger.info(f"Doubling number: {x}")
    return x * 2

@app.task(name="square_number")
def square_number(x: int) -> int:
    logger.info(f"Squaring number: {x}")
    return x * x

@app.task(name="sum_numbers")
def sum_numbers(numbers: List[int]) -> int:
    logger.info(f"Summing numbers: {numbers}")
    return sum(numbers)

# # reference: https://blog.det.life/replacing-celery-tasks-inside-a-chain-b1328923fb02
@app.task(name="process_numbers_individually", bind=True)
def process_numbers_individually(self, numbers: List[int], task_chain):
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

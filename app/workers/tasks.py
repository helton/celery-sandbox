from copy import deepcopy
from typing import List

from celery import group, subtask, chord
from shared.app import app
from time import sleep
from celery.utils.log import get_task_logger
from celery.result import AsyncResult
from pydantic import BaseModel
from functools import wraps
import os

logger = get_task_logger(__name__)
wait_in_s_between_tasks=0.05

def auto_unwrap(func):
  """
  Decorator to allow a celery task function to accept:
  - Individual positional and keyword arguments,
  - A single list argument (unpacked as positional arguments),
  - A single dictionary argument (unpacked as keyword arguments).
  """
  @wraps(func)
  def wrapper(*args, **kwargs):
    # Case 1: Single list argument, no kwargs
    if len(args) == 1 and isinstance(args[0], list) and not kwargs:
      logger.info(f"[{func.__name__}] Received list args: {args[0]}")
      return func(*args[0], **kwargs)
    
    # Case 2: Single dict argument, no kwargs
    elif len(args) == 1 and isinstance(args[0], dict) and not kwargs:
      logger.info(f"[{func.__name__}] Received dict args: {args[0]}")
      return func(**args[0])
    
    # Case 3: Regular args and kwargs
    else:
      logger.info(f"[{func.__name__}] Received args: {args}, kwargs: {kwargs}")
      return func(*args, **kwargs)

  return wrapper

class ArgumentsModel(BaseModel):
  x: int
  y: int

class ReturnModel(BaseModel):
  value: int

@app.task(name="add_using_pydantic", pydantic=True)
def add_using_pydantic(arg: ArgumentsModel) -> ReturnModel:
  print("Received")
  print(arg)

  result = ReturnModel(value=arg.x + arg.y)
  print("Returning")
  print(result)

  return result

@app.task(name="add")
@auto_unwrap
def add(x: int, y: int) -> int:
  logger.info(f"Running add({x}, {y})")
  sleep(wait_in_s_between_tasks)
  return x + y

@app.task(name="subtract")
@auto_unwrap
def subtract(x: int, y: int) -> int:
  logger.info(f"Running subtract({x}, {y})")
  sleep(wait_in_s_between_tasks)
  return x - y

@app.task(name="divide")
@auto_unwrap
def divide(x: int, y: int) -> int:
  logger.info(f"Running divide({x}, {y})")
  sleep(wait_in_s_between_tasks)
  return x / y

@app.task(name="multiply")
@auto_unwrap
def multiply(x: int, y: int) -> int:
  logger.info(f"Running multiply({x}, {y})")
  sleep(wait_in_s_between_tasks)
  return x * y

@app.task(name="download")
@auto_unwrap
def download(uid: str, source: str, source_type: str):
  if source_type in ["url", "s3.object"]:
    file_name = source.split("/")[-1]
  else:
    file_name = source
  base_name, _ = os.path.splitext(file_name)
  return {
    "uid": uid,
    "source": f"s3://mybucket/{uid}/downloads/{base_name}/{file_name}",
    "source_type": "s3.object"
  }

@app.task(name="extract")
@auto_unwrap
def extract(uid: str, source: str, source_type: str):
  file_name = source.split("/")[-1]
  base_name, _ = os.path.splitext(file_name)
  return {
    "uid": uid,
    "source": f"s3://mybucket/{uid}/extractions/{base_name}/{base_name}.txt",
    "source_type": "s3.object"
  }

@app.task(name="chunkenize")
@auto_unwrap
def chunkenize(uid: str, source: str, source_type: str):
  file_name = source.split("/")[-1]
  base_name, _ = os.path.splitext(file_name)
  return [{
    "uid": uid,
    "source": f"s3://mybucket/{uid}/chunks/{base_name}/chunk_{i}.txt",
    "source_type": "s3.folder"
  } for i in range(5)]

@app.task(name="embedding")
@auto_unwrap
def embedding(uid: str, source: str, source_type: str):
  file_name = source.split("/")[-1]
  base_name, _ = os.path.splitext(file_name)
  return [{
    "uid": uid,
    "source": f"s3://mybucket/{uid}/embeddings/{base_name}/embedding_n.json",
    "source_type": "s3.folder"
  }]

@app.task
def split_file(file_path):
    """
    Splits a large file into smaller chunks.

    Args:
        file_path (str): Path to the large input file.

    Returns:
        list: A list of chunk file paths.
    """
    chunks = []
    with open(file_path, 'r') as f:
        chunk_size = 100  # Number of lines per chunk
        chunk = []
        for i, line in enumerate(f):
            chunk.append(line)
            if (i + 1) % chunk_size == 0:
                chunk_file = f"{file_path}_chunk_{len(chunks)}.txt"
                with open(chunk_file, 'w') as chunk_f:
                    chunk_f.writelines(chunk)
                chunks.append(chunk_file)
                chunk = []
        # Write any remaining lines as the last chunk
        if chunk:
            chunk_file = f"{file_path}_chunk_{len(chunks)}.txt"
            with open(chunk_file, 'w') as chunk_f:
                chunk_f.writelines(chunk)
            chunks.append(chunk_file)
    return chunks

@app.task
def process_chunk(chunk_path):
    """
    Processes a single file chunk.

    Args:
        chunk_path (str): Path to the chunk file.

    Returns:
        dict: Processing results for the chunk.
    """
    results = {}
    with open(chunk_path, 'r') as f:
        for line in f:
            words = line.strip().split()
            for word in words:
                results[word] = results.get(word, 0) + 1
    return results

@app.task
def aggregate_results(processed_chunks):
    """
    Aggregates results from all processed chunks.

    Args:
        processed_chunks (list): List of dictionaries with processed data.

    Returns:
        dict: Aggregated results.
    """
    final_results = {}
    for chunk_result in processed_chunks:
        for word, count in chunk_result.items():
            final_results[word] = final_results.get(word, 0) + count
    return final_results

# ------------------

@app.task(name="generate_list")
def generate_list(amount: int) -> List[int]:
    logger.info(f"Generating list of integers up to {amount}.")
    return list(range(1, amount + 1))

@app.task(name="double_number")
def double_number(x: int) -> int:
    logger.info(f"Doubling number: {x}")
    return x * 2

@app.task(name="sum_numbers")
def sum_numbers(numbers: List[int]) -> int:
    logger.info(f"Summing numbers: {numbers}")
    return sum(numbers)

@app.task(name="double_number_list", bind=True)
def double_number_list(self, numbers: List[int]):
    logger.info(f"Creating chord for numbers: {numbers}")
    double_group = group(
        app.signature("double_number", args=(number,)) for number in numbers
    )
    logger.info("Replacing double_number_list for a celery group of double_number")
    return self.replace(double_group)

# @app.task(name="dmap")
# def dmap(it, callback):
#     return group(subtask(callback).clone([arg,]) for arg in it)()
# 
# @app.task(name="dmap")
# def dmap(args_iter, callback):
#     callback = subtask(callback)
#     print(f"ARGS: {args_iter}")
#     run_in_parallel = group(clone_signature(callback, args if type(args) is list else [args]) for args in args_iter)
#     print(f"Finished Loops: {run_in_parallel}")
#     return run_in_parallel.delay()
# 
# def clone_signature(sig, args=(), kwargs=(), **opts):
#     """
#     Turns out that a chain clone() does not copy the arguments properly - this
#     clone does.
#     From: https://stackoverflow.com/a/53442344/3189
#     """
#     if sig.subtask_type and sig.subtask_type != "chain":
#         raise NotImplementedError(
#             "Cloning only supported for Tasks and chains, not {}".format(sig.subtask_type)
#         )
#     clone = sig.clone()
#     if hasattr(clone, "tasks"):
#         task_to_apply_args_to = clone.tasks[0]
#     else:
#         task_to_apply_args_to = clone
#     args, kwargs, opts = task_to_apply_args_to._merge(args=args, kwargs=kwargs, options=opts)
#     task_to_apply_args_to.update(args=args, kwargs=kwargs, options=deepcopy(opts))
#     return clone

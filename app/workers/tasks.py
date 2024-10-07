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
  [{
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

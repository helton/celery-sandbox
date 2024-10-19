import json
import random
from time import sleep

from celery import group

from shared.app import app
from datetime import datetime, timezone


def run_workflow(title: str, workflow, timeout: int = 300):
  def try_pretty_print_json(data):
    try:
      if isinstance(data, str):
        parsed = json.loads(data)
        pretty = json.dumps(parsed, indent=2)
        return pretty
      else:
        pretty = json.dumps(data, indent=2)
        return pretty
    except (TypeError, json.JSONDecodeError):
      return str(data)  
  
  start_time = datetime.now(timezone.utc)

  result = workflow.delay()
  workflow_id = result.id

  print(title)
  print(f"🛠️ Workflow '{workflow_id}' created:")
  print(f"⏳ Waiting for result (timeout {timeout}s)...")

  try:
    elapsed_time = (datetime.now(timezone.utc) - start_time).total_seconds()
    workflow_result = result.get(timeout=timeout)
    pretty_result = try_pretty_print_json(workflow_result)
    print(f"✅ Workflow id='{workflow_id}' completed successfully in {elapsed_time:.2f}s. Result:\n{pretty_result}")
  except TimeoutError:
    elapsed_time = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"⏰ Workflow id='{workflow_id}' did not complete in time. Timeout reached after {elapsed_time:.2f}s.")
  except Exception as e:
    elapsed_time = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"❌ Workflow id='{workflow_id}' failed after {elapsed_time:.2f}s.")
    print(f"📝 Traceback:\n{result.traceback}")
  finally:
    print('=' * 100)


if __name__ == "__main__":
  size = random.randint(1, 10)
  run_workflow(
    title="Static Workflow",
    workflow=group([app.signature("double_number", args=(n,)) | app.signature("square_number") for n in range(1, size)]) | app.signature("sum_numbers")
  )

  # run_workflow(
  #   title="Dynamic Workflow",
  #   workflow=
  #       app.signature("generate_list", kwargs={"amount": random.randint(1, 10)})
  #     | app.signature("process_numbers_individually", args=(app.signature("double_number") | app.signature("square_number"),))
  #     | app.signature("sum_numbers")
  # )

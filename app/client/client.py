import random
from shared.app import app
from datetime import datetime, timezone
import time
from ulid import ulid

def run_workflow(title: str, workflow, timeout: int = 600, poll_interval: int = 1):
  start_time = datetime.now(timezone.utc)
  elapsed_time = 0

  result = workflow.delay()
  print(title)
  print(f"🔄 Workflow '{result}' created")   

  print(f"⏳ Waiting until result is available (timeout {timeout}s)...")
  while not result.ready():
    current_time = datetime.now(timezone.utc)
    elapsed_time = (current_time - start_time).total_seconds()
    print(f"🔍 Result not ready yet. Checking again in {poll_interval}s...")

    if elapsed_time > timeout:
      print("⏰ Timeout reached. Workflow did not complete in time. 🛑")
      return

    time.sleep(poll_interval)

  if result.successful():
    workflow_result = result.get(disable_sync_subtasks=False)
    print(f"✅ Workflow '{result}' completed successfully in {elapsed_time:.2f}s. Result:\n{workflow_result}")
  else:
    print("❌ Workflow '{result}' failed. 😞")
    print(f"📝 Traceback:\n{result.traceback}")
  print('=' * 100)


if __name__ == "__main__":
  run_workflow(
    title="Dynamic Workflow",
    workflow=
        app.signature("generate_list", kwargs={"amount": random.randint(1, 10)})
      | app.signature("process_numbers_individually", args=(app.signature("double_number") | app.signature("square_number"),))
      | app.signature("sum_numbers")
  )

from shared.app import app
from datetime import datetime, timezone
from celery import chain, group, chord
import time
from ulid import ulid

def run_workflow(title: str, workflow, timeout: int = 30, poll_interval: int = 1):
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
    workflow_result = result.get()
    print(f"✅ Workflow '{result}' completed successfully in {elapsed_time:.2f}s. Result:\n{workflow_result}")
  else:
    print("❌ Workflow '{result}' failed. 😞")
    print(f"📝 Traceback:\n{result.traceback}")
  print('=' * 100)


if __name__ == "__main__":
  run_workflow(
    title="Data Pipeline",
    workflow=
      app.signature("download", kwargs={
        "uid": ulid(),
        "source": "https://example.com/my_custom_file.pdf",
        "source_type": "url"
      })
      | app.signature("extract")
      | app.signature("chunkenize")
  )

  # run_workflow(
  #   title="Using Pydantic with kwargs",
  #   workflow=app.signature("add_using_pydantic", kwargs={"arg": {"x": 1, "y": 2}})
  # )  

  # run_workflow(
  #   title="Using Pydantic with args",
  #   workflow=app.signature("add_using_pydantic", args=({"x": 1, "y": 2},))
  # )

  # run_workflow(
  #   title="Math: (((1 + 2) - 5) * 15) / -3",
  #   workflow=
  #       app.signature("add",      args=(1, 2,))
  #     | app.signature("subtract", args=(5,))
  #     | app.signature("multiply", args=(15,))
  #     | app.signature("divide",   args=(-3,))
  # )

  # run_workflow(
  #   title="Math: ((1 + 2) * (10 / 5)) - 10",
  #   workflow=
  #     chord(
  #       group(
  #         app.signature("add",    args=(1, 2,)),
  #         app.signature("divide", args=(10, 5,))
  #       ),
  #       app.signature("multiply"),
  #     )
  #     | app.signature("subtract", args=(10,))
  # )

from shared.app import app
from datetime import datetime, timezone
from celery import chain, group, chord, subtask
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

def process_large_file(file_path):
    """
    Orchestrates the processing of a large file by splitting it into chunks,
    processing each chunk, and aggregating the results.

    Args:
        file_path (str): Path to the large input file.
    """
    # Define the splitter task signature using task name as a string
    splitter_signature = app.signature("split_file", args=(file_path,))
    
    # Define the aggregator task signature using task name as a string
    aggregator_signature = app.signature("aggregate_results")
    
    # Define the processing_chunk task signature template
    # This will be used to create signatures for each chunk
    process_chunk_template = app.signature("process_chunk")
    
    # Create a chord where:
    # - The header is the splitter task
    # - The body is the aggregator task
    # 
    # Note: Since `chord` expects the header to be a group of tasks,
    # and `split_file` returns a list of chunks,
    # we'll use `split_file` to trigger the creation of `process_chunk` tasks.
    #
    # To achieve this, we'll use a callback task that takes the output of `split_file`,
    # creates a group of `process_chunk` tasks, and then chains it to `aggregate_results`.
    
    # Define a callback signature that creates process_chunk group and chains to aggregator
    def split_callback(split_result):
        """
        Callback function to handle the output of split_file,
        create process_chunk tasks, and chain to aggregate_results.
        
        Args:
            split_result (list): List of chunk identifiers or data.
        """
        # Create a group of process_chunk tasks for each chunk
        process_group = [process_chunk_template.clone(args=(chunk,)) for chunk in split_result]
        
        # Create a chord: group(process_chunk) -> aggregate_results
        processing_chord = chord(
            header=process_group,
            body=aggregator_signature
        )
        
        # Execute the processing chord asynchronously
        processing_chord.apply_async()
    
    # Define a wrapper task signature that links split_callback as a callback
    # after split_file completes
    splitter_with_callback = splitter_signature.clone(link=split_callback)
    
    # Execute the splitter task asynchronously with the callback
    return splitter_with_callback

if __name__ == "__main__":
  run_workflow(
    title="Dynamic Workflow",
    workflow=
        app.signature("generate_list", kwargs={"amount": 5})
      | app.signature("double_number_list")
      | app.signature("sum_numbers")
  )

 # | app.signature("dmap", kwargs={"callback": app.signature("double_number")})

  # run_workflow(title="New workflow", workflow=process_large_file(file_path="/home/helton/development/celery-sandbox/README.md"))

  # run_workflow(
  #   title="Data Pipeline",
  #   workflow=
  #     app.signature("download", kwargs={
  #       "uid": ulid(),
  #       "source": "https://example.com/my_custom_file.pdf",
  #       "source_type": "url"
  #     })
  #     | app.signature("extract")
  #     | app.signature("chunkenize")
  # )

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

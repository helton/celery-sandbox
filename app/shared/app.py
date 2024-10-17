from celery import Celery
import datetime
import os

# Broker
redis_host            = os.getenv("REDIS_HOST")
redis_port            = os.getenv("REDIS_PORT")
redis_password        = os.getenv("REDIS_PASSWORD")
redis_db_id           = os.getenv("REDIS_DB_ID")
redis_url             = f'redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db_id}'

# Backend
dynamodb_region       = os.getenv("DYNAMODB_REGION")
dynamodb_table        = os.getenv("DYNAMODB_TABLE")
dynamodb_host         = os.getenv("DYNAMODB_HOST")
dynamodb_port         = os.getenv("DYNAMODB_PORT")
dynamodb_url          = f"dynamodb://@{dynamodb_region}/{dynamodb_table}"
dynamodb_endpoint_url = f"http://{dynamodb_host}:{dynamodb_port}"

print(f"{redis_url=}")
print(f"{dynamodb_url=}")
print(f"{dynamodb_endpoint_url=}")

app = Celery(
    main="app",
    broker_url=redis_url,
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_transport_options={'visibility_timeout': int(datetime.timedelta(minutes=15).total_seconds())},
    result_backend=dynamodb_url,
    dynamodb_endpoint_url=dynamodb_endpoint_url,
    result_extended=True,
    worker_prefetch_multiplier=1,
    worker_send_task_events=True,
    task_send_sent_event=True,
    task_track_started=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_create_missing_queues=True,
    enable_utc=True,
)

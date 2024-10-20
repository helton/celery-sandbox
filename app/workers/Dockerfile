FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN adduser --disabled-password --gecos '' celery

WORKDIR /app

COPY --chown=celery:celery workers/requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=celery:celery workers/*.py .
COPY --chown=celery:celery shared/ shared/

USER celery

CMD ["sh", "-c", "celery --app tasks.app worker --concurrency=$WORKER_CONCURRENCY --pool=$WORKER_POOL --loglevel=$WORKER_LOG_LEVEL"]

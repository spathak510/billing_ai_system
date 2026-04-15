# Billing AI System

## Celery Setup

This project now includes Celery worker and Celery Beat wiring for asynchronous/background execution.

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Configure environment (.env)

Add these values if you want to override defaults:

```env
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/1
CELERY_TIMEZONE=UTC
CELERY_ENABLE_UTC=true
CELERY_BEAT_HEARTBEAT_MINUTES=5
```

### 3) Start Redis

Run Redis locally (default expected at `localhost:6379`).

### 4) Start Celery worker

```bash
celery -A app.celery_app.celery_app worker --loglevel=info
```

### 5) Start Celery Beat

```bash
celery -A app.celery_app.celery_app beat --loglevel=info
```

### Included tasks

- `app.tasks.beat_heartbeat`: periodic heartbeat task scheduled by Beat.
- `app.tasks.echo`: simple async task for smoke testing.


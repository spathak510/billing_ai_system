from __future__ import annotations

from celery import Celery
from kombu import Queue

from app.config.settings import settings


def create_celery_app() -> Celery:
    app = Celery(
        "billing_ai_system",
        broker=settings.celery_broker_url,
        backend=settings.celery_result_backend,
        include=["app.tasks"],
    )

    app.conf.update(
        timezone=settings.celery_timezone,
        enable_utc=settings.celery_enable_utc,
        broker_connection_retry_on_startup=True,
        task_default_queue="billing_ai_system",
        task_queues=(Queue("billing_ai_system"),),
    )

    # Minimal periodic job to verify Celery Beat wiring.
    app.conf.beat_schedule = {
        "billing-heartbeat": {
            "task": "app.tasks.beat_heartbeat",
            "schedule": settings.celery_beat_heartbeat_minutes * 60,
        }
    }

    return app


celery_app = create_celery_app()

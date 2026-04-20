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
        "billing-post-validation-flow": {
            "task": "app.tasks.run_post_validation_flow_task",
            "schedule": 5 * 60,  # every 5 minutes
        },
        "billing-feedback-flow": {
            "task": "app.tasks.feedback_process_task",
            "schedule": 9 * 60,  # every 12 hours
        },
    }


    return app


celery_app = create_celery_app()

# Main Celery app entry point
from app.celery_tasks.celery_app import celery_app

# This is the main Celery app that should be used
__all__ = ['celery_app']
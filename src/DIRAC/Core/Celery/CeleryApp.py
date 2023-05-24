from celery import Celery

celery = Celery(config_source="DIRAC.Core.Celery.celeryconfig")

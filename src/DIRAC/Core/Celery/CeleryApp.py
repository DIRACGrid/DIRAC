from celery import Celery

celeryApp = Celery()
celeryApp.config_from_object("DIRAC.Core.Celery.CeleryConfig")

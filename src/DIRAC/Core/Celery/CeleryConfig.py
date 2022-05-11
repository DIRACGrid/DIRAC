## Broker settings.
broker_url = "amqp://guest:guest@localhost:5672//"

# List of modules to import when the Celery worker starts.
imports = ("DIRAC.Core.Celery.Task.OptmizeTask.py",)

# Timezone setting
enable_utc = False

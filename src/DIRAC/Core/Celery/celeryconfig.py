# Celery configuration
# http://docs.celeryproject.org/en/latest/configuration.html

broker_url = "amqp://admin:password@rabbitmq:5672/"
result_backend = "rpc://admin:password@rabbitmq:5672/"

# json serializer is more secure than the default pickle
task_serializer = "json"
accept_content = ["json"]
result_serializer = "json"

# List of modules to import when celery starts.
imports = ["DIRAC.WorkloadManagementSystem.Tasks"]

# Use UTC instead of localtime
CELERY_enable_utc = True

# Maximum retries per task
# CELERY_TASK_ANNOTATIONS = {'*': {'max_retries': 3}}

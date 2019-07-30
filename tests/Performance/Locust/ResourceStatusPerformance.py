import time

from locust import Locust, TaskSet, events, task

from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient


class WrapperClient(object):
  """
  Wrapper class around ResourceStatusClient that wrapps around DIRAC's ResourceStatusClient
  and fires locust events on result_success or result_failure so that all requests are
  tracked in locust's statistics
  """

  def __init__(self, client):
    self._client = client

  def __getattr__(self, attr):
    func = self._client.__getattribute__(attr)

    if callable(func):
      # a wrapper around the actual function called to log for locust
      def wrapper_func(*args, **kwargs):
        start_time = time.time()
        try:
          result = func(*args, **kwargs)
        except Exception as e:
          total_time = int((time.time() - start_time) * 1000)
          events.request_failure.fire(request_type='rssclient', name=attr, response_time=total_time, exception=e)
        else:
          total_time = int((time.time() - start_time) * 1000)
          if result['OK'] is True:
            events.request_success.fire(request_type="rssclient", name=attr,
                                        response_time=total_time, response_length=0)
          else:
            e = Exception(result['Message'])
            events.request_failure.fire(request_type='rssclient', name=attr, response_time=total_time, exception=e)

      return wrapper_func
    else:
      return func


class RSSLocust(Locust):

  min_wait = 100
  max_wait = 1000

  def __init__(self):
    self.client = WrapperClient(ResourceStatusClient())

  def setup(self):
    print "Hello World"

  def teardown(self):
    print "Good Bye"

  class task_set(TaskSet):
    @task
    def select_task(self):
      self.client.select('ResourceStatus')

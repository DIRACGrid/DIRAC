"""
It is used to test the tornado web framework. This can be used:
-change of an underlying library such as ThreadPool
-change of the Tornado configuration (running more Tornado processes)
-Tornado scalability of a certain machine
"""

from locust import HttpLocust, TaskSet, task

class UserBehavior(TaskSet):

  @task
  def getSelectionData(self):
    url = "/DIRAC/s:DIRAC-Certification/g:dteam_user/ExampleApp/getSelectionData"
    self.client.get(url)

class WebUser(HttpLocust):
  min_wait = 100
  max_wait = 1000
  task_set = UserBehavior

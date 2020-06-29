"""
Performance test created using multi-mechnize to analyze time
for query processing with MySQL.
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import random
import time

from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB


class Transaction(object):

  def __init__(self):

    self.JobDB = JobDB()
    self.custom_timers = {}

  def run(self):

    start_time = time.time()

    for i in xrange(0, random.randint(1000, 3000)):

      jobID = random.randint(1, 1000)
      self.JobDB.getJobParameters(jobID)

    end_time = time.time()

    self.custom_timers['Execution_Time'] = end_time - start_time


if __name__ == '__main__':

  trans = Transaction()
  trans.run()
  print(trans.custom_timers)

"""
Performance test created using multi-mechnize to analyze time
for update processing with MySQL.
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import random
import string
import time

from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB


def random_generator(size=6, chars=string.ascii_letters):
  return ''.join(random.choice(chars) for x in range(size))


class Transaction(object):

  def __init__(self):

    self.JobDB = JobDB()
    self.custom_timers = {}

  def run(self):

    start_time = time.time()

    for i in range(0, random.randint(1000, 3000)):

      key = random_generator()
      value = random_generator(size=12)
      self.JobDB.setJobParameter(2, key, value)

    end_time = time.time()

    self.custom_timers['Execution_Time'] = end_time - start_time


if __name__ == '__main__':

  trans = Transaction()
  trans.run()
  print(trans.custom_timers)

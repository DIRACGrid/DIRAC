"""
Performance test created using multi-mechnize to analyze time
for update processing with ElasticSearch.
"""

from __future__ import print_function
import random
import string
import time

from DIRAC.WorkloadManagementSystem.DB.ElasticJobParametersDB import ElasticJobParametersDB


def random_generator(size=6, chars=string.ascii_letters):
  return ''.join(random.choice(chars) for x in xrange(size))


class Transaction(object):

  def __init__(self):

    self.elasticJobParametersDB = ElasticJobParametersDB()
    self.custom_timers = {}

  def run(self):

    start_time = time.time()

    for i in xrange(0, random.randint(1000, 3000)):

      key = random_generator()
      value = random_generator(size=12)
      self.elasticJobParametersDB.setJobParameter(2, key, value)

    end_time = time.time()

    self.custom_timers['Execution_Time'] = end_time - start_time


if __name__ == '__main__':

  trans = Transaction()
  trans.run()
  print(trans.custom_timers)

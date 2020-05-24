# Creating 3 processes, for tests,
# that consume some CPU and that have children

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
from multiprocessing import Pool, Process, current_process


def f(n):
  time.sleep(n)


def do_sum(li):
  sum(li)
  proc_name = current_process().name
  print(proc_name)
  p = Process(target=f, args=(10, ))
  p.daemon = False
  p.start()
  p.join()


my_list = range(150000000)

pool = Pool(3)
pool.map(do_sum, [my_list[:50000000],
                  my_list[50000000:100000000],
                  my_list[100000000:]])
pool.close()
pool.join()

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
# Just creating some processes, for tests

import time
from multiprocessing import Pool


def f(n):
  time.sleep(n)


p = Pool(3)
p.map(f, [3, 4, 5])

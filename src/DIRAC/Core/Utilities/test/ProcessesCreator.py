# Just creating some processes, for tests

import time
from multiprocessing import Pool


def f(n):
    time.sleep(n)


p = Pool(3)
p.map(f, [3, 4, 5])

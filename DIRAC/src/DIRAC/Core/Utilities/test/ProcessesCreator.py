# Just creating some processes, for tests

import time
from multiprocessing import Pool


def f(n):
    time.sleep(n)


if __name__ == "__main__":
    p = Pool(3)
    p.map(f, [3, 4, 5])

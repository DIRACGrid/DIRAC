# Creating 3 processes, for tests,
# that consume some CPU and that have children
import time
from multiprocessing import Pool, Process, current_process


def f(n):
    time.sleep(n)


def do_sum(args):
    sum(range(*args))
    proc_name = current_process().name
    print(proc_name)


if __name__ == "__main__":
    pool = Pool(3)
    pool.map(do_sum, [(0, 50000000), (50000000, 100000000), (100000000, 150000000)])
    pool.close()
    pool.join()

    # Generate system CPU usage
    for i in range(1000):
        p = Process(target=f, args=(0.001,))
        p.daemon = False
        p.start()
        p.join()

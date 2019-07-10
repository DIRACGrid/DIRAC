#!/usr/bin/env python

from multiprocessing import Process, Queue, current_process


def do_sum(q, l):
  q.put(sum(l))
  proc_name = current_process().name
  print proc_name


def main():
  my_list = range(20000000)

  q = Queue()

  p1 = Process(target=do_sum, args=(q, my_list[:5000000]))
  p2 = Process(target=do_sum, args=(q, my_list[5000000:10000000]))
  p3 = Process(target=do_sum, args=(q, my_list[10000000:15000000]))
  p4 = Process(target=do_sum, args=(q, my_list[15000000:]))
  p1.start()
  p2.start()
  p3.start()
  p4.start()

  r1 = q.get()
  r2 = q.get()
  r3 = q.get()
  r4 = q.get()
  q.close()
  q.join_thread()

  p1.join()
  p2.join()
  p3.join()
  p4.join()

  print r1 + r2 + r3 + r4

if __name__ == '__main__':
  main()

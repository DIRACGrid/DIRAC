#!/usr/bin/python

from __future__ import print_function
from multiprocessing import Process, Queue

def do_sum( q, l ):
  q.put( sum( l ) )

def main():
  my_list = range( 50000000 )

  q = Queue()

  p1 = Process( target = do_sum, args = ( q, my_list[:25000000] ) )
  p2 = Process( target = do_sum, args = ( q, my_list[25000000:] ) )
  p1.start()
  p2.start()
  r1 = q.get()
  r2 = q.get()
  print(r1 + r2)

if __name__ == '__main__':
  main()

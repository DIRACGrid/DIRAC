#!/usr/bin/python

from multiprocessing import Process, Queue

def do_sum( q, l ):
    q.put( sum( l ) )

def main():
    my_list = range( 50000000 )

    q = Queue()

    p1 = Process( target = do_sum, args = ( q, my_list[:25000000] ) )
    p2 = Process( target = do_sum, args = ( q, my_list[25000000:] ) )
    p3 = Process( target = do_sum, args = ( q, my_list[:25000000] ) )
    p4 = Process( target = do_sum, args = ( q, my_list[25000000:] ) )
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    r1 = q.get()
    r2 = q.get()
    r3 = q.get()
    r4 = q.get()
    print r1 + r2
    print r3 + r4

if __name__ == '__main__':
    main()

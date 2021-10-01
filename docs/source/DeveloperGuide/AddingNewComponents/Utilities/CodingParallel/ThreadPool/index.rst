.. _thread_pool:

===============
Thread Pool
===============

ThreadPool creates a pool of worker threads to process a queue of tasks
much like the producers/consumers paradigm. Users just need to fill the queue
with tasks to be executed and worker threads will execute them

To start working with the ThreadPool first it has to be instanced::

    threadPool = ThreadPool( minThreads, maxThreads, maxQueuedRequests )

- minThreads - at all times no less than <minThreads> workers will be alive
- maxThreads - at all times no more than <maxThreads> workers will be alive
- maxQueuedRequests - No more than <maxQueuedRequests> can be waiting to be executed.
  If another request is added to the ThreadPool, the thread will
  lock until another request is taken out of the queue.

The ThreadPool will automatically increase and decrease the pool of workers as needed

To add requests to the queue::

     threadPool.generateJobAndQueueIt( <functionToExecute>,
                                       args = ( arg1, arg2, ... ),
                                       oCallback = <resultCallbackFunction> )

or::

     request = ThreadedJob( <functionToExecute>,
                            args = ( arg1, arg2, ... )
                            oCallback = <resultCallbackFunction> )
     threadPool.queueJob( request )

The result callback and the parameters are optional arguments.
Once the requests have been added to the pool. They will be executed as soon as possible.
Worker threads automatically return the return value of the requests. To run the result callback
functions execute::

     threadPool.processRequests()

This method will process the existing return values of the requests. Even if the requests do not return
anything this method (or any process result method) has to be called to clean the result queues.

To wait until all the requests are finished and process their result call::

     threadPool.processAllRequests()

This function will block until all requests are finished and their result values have been processed.

It is also possible to set the threadPool in auto processing results mode. It'll process the results as
soon as the requests have finished. To enable this mode call::

     threadPool.daemonize()

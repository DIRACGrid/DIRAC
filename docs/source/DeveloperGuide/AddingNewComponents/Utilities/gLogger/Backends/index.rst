.. _gLogger_backends:

Backends
========

This section presents all the existing *Backend* classes that you can use in your program, followed by their parameters.

StdoutBackend
-------------

Description
~~~~~~~~~~~
Used to emit log records to the standard output.

Parameters
~~~~~~~~~~
+-----------+------------------------------------------------------------------+----------------------+
| Option    | Description                                                      | Default value        |
+===========+==================================================================+======================+
| LogLevel  | log level of the backend: will only accept logs above this level | 'DEBUG'              |
+-----------+------------------------------------------------------------------+----------------------+

StderrBackend
-------------

Description
~~~~~~~~~~~
Used to emit log records to the standard error output.

Parameters
~~~~~~~~~~
+-----------+------------------------------------------------------------------+----------------------+
| Option    | Description                                                      | Default value        |
+===========+==================================================================+======================+
| LogLevel  | log level of the backend: will only accept logs above this level | 'DEBUG'              |
+-----------+------------------------------------------------------------------+----------------------+

FileBackend
-----------

Description
~~~~~~~~~~~
Used to emit log records in a specific file.

Parameters
~~~~~~~~~~
+-----------+------------------------------------------------------------------+----------------------+
| Option    | Description                                                      | Default value        |
+===========+==================================================================+======================+
| FileName  | name of the file where the log records must be sent              | Dirac-log\_[pid].log |
+-----------+------------------------------------------------------------------+----------------------+
| LogLevel  | log level of the backend: will only accept logs above this level | 'DEBUG'              |
+-----------+------------------------------------------------------------------+----------------------+

ServerBackend
-------------

Description
~~~~~~~~~~~
Used to emit log records in the *SystemLogging* service of *DIRAC* in order to store them in the *SystemLoggingDB* database.
This *Backend* only allows log records superior or equal to *Error* to be sent to the service.

Parameters
~~~~~~~~~~
+-----------+----------------------------------------------------------+----------------------+
| Option    | Description                                              | Default value        |
+===========+==========================================================+======================+
| SleepTime | sleep time in seconds                                    | 150                  |
+-----------+----------------------------------------------------------+----------------------+

ElasticSearchBackend
--------------------

Description
~~~~~~~~~~~
Used to emit log records in the an ElasticSearch database.
The *Backend* acccepts logs from *Debug* to *Always* level.

Parameters
~~~~~~~~~~
+-----------+------------------------------------------------------------------+----------------------+
| Option    | Description                                                      | Default value        |
+===========+==================================================================+======================+
| Host      | host machine where the ElasticSearch DB is installed             | ''                   |
+-----------+------------------------------------------------------------------+----------------------+
| Port      | port where the ElasticSearch DB listen                           | 9203                 |
+-----------+------------------------------------------------------------------+----------------------+
| User      | username of the ElasticSearch DB   (optional)                    | None                 |
+-----------+------------------------------------------------------------------+----------------------+
| Password  | password of the ElasticSearch DB   (optional)                    | None                 |
+-----------+------------------------------------------------------------------+----------------------+
| Index     | ElasticSearch index                                              | ''                   |
+-----------+------------------------------------------------------------------+----------------------+
| BufferSize| maximum size of the buffer before sending                        | 1000                 |
+-----------+------------------------------------------------------------------+----------------------+
| FlushTime | maximum waiting time in seconds before sending                   | 1                    |
+-----------+------------------------------------------------------------------+----------------------+
| LogLevel  | log level of the backend: will only accept logs above this level | 'DEBUG'              |
+-----------+------------------------------------------------------------------+----------------------+


.. _gLogger_backends_messagequeue:

MessageQueueBackend
-------------------

Description
~~~~~~~~~~~
Used to emit log records in a MessageQueue server using Stomp protocol.
The *Backend* acccepts logs from *Verbose* to *Always* level, and this cannot be changed.

Parameters
~~~~~~~~~~
+-----------+------------------------------------------------------------------+----------------------+
| Option    | Description                                                      | Default value        |
+===========+==================================================================+======================+
| MsgQueue  | MessageQueueRessources from DIRAC                                | ''                   |
+-----------+------------------------------------------------------------------+----------------------+
| LogLevel  | log level of the backend: will only accept logs above this level | 'VERBOSE'            |
+-----------+------------------------------------------------------------------+----------------------+

MsgQueue represents a MessageQueue resources from DIRAC under this form:

::

  mardirac3.in2p3.fr::Queues::TestQueue

You will find more details about these resources in the :ref:`configuration_message_queues` section.

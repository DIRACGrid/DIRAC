.. _gLogger_oldgLogger:

The old version of gLogger
==========================

**This is the old version of gLogger.** Please, go to :ref:`gLogger_gLogger` to find the documentation on the current version. You can also see the different changes in the :ref:`gLogger_changes`.

Logger creation
---------------

Get a sublogger
~~~~~~~~~~~~~~~

gLogger is considered like the root logger. From it, we can create a
child logger with the command:

::

    gLogger.getSubLogger("logger")

This child logger can be used like gLogger and from it we can also get a
sublogger and so on. We recommend you to not create a sublogger from a
sublogger because there is no particular interest. Otherwise, note that
the created sublogger is identified by its name and can be used again
with the *getSubLogger()* method. For instance :

::

    logger = gLogger.getSubLogger("logger")
    newLogger = gLogger.getSubLogger("logger")
    #logger is the same object as newLogger

*child* attribute in sublogger
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

gLogger and its children are owned by a system by default. It means that
the name of the logger is preceded by the system and component name in
the display. To prevent this feature, we can notify the program changing
a boolean value : *child* in parameter of the *getSubLogger()* method
like this :

::

    gLogger.getSubLogger("logger", child=False)

This allows us to remove the system and component name. However, this
feature seems buggy and should be used carefully. We recommend you to
use only with a direct sublogger of gLogger and only if you execute a
service, an agent or a script.

Levels
------

Level names and numbers
~~~~~~~~~~~~~~~~~~~~~~~

There are 9 different levels in DIRAC :

+--------------+----------------+
| Level name   | Level number   |
+==============+================+
| Always       | 40             |
+--------------+----------------+
| Notice       | 30             |
+--------------+----------------+
| Info         | 20             |
+--------------+----------------+
| Verbose      | 10             |
+--------------+----------------+
| Debug        | 0              |
+--------------+----------------+
| Warn         | -20            |
+--------------+----------------+
| Error        | -30            |
+--------------+----------------+
| Exception    | -30            |
+--------------+----------------+
| Fatal        | -40            |
+--------------+----------------+

They are numbered from 40 to -40. We use them according to the context
attaching a certain level to a logger or to a message.

Set a level to a logger
~~~~~~~~~~~~~~~~~~~~~~~

We can set a certain level to a logger to hide some logs. It is a *V*
system, which means that it functions with absolute values. For
instance, if you set the level of gLogger to *Always*, only always and
fatal logs will appear because their absolute values are superiors or
equals to 40. To set a level, we use the *setLevel()* method like this :

::

    logger.setLevel("notice")

Here, we set a notice level to this logger. However, once we have set a
level to *gLogger*, these children will have the same level restriction,
even if we try to change its level. In this way, the example logger will
not send messages inferior to the absolute value of the *gLogger* level.

Get a logger level
~~~~~~~~~~~~~~~~~~

We can obviously get a level associate to a logger via the *getLevel()*
method.

Message
-------

Naturally, it exists some functions to send a message. These methods
take level names. In this way, we have :

+ always(msg, varMsg='')
+ notice(msg, varMsg='')
+ info(msg, varMsg='')
+ verbose(msg, varMsg='')
+ debug(msg, varMsg='')
+ warn(msg, varMsg='')
+ error(msg, varMsg='')
+ exception(msg, varMsg='', lException=False, lExcInfo=False)
+ fatal(msg, varMsg='')

There are a *Msg* and *varMsg* where you can put any string you want in.
There is no real difference between the two parameters.

The *exception* function contains two more parameters. The first has no
effect on the message and should stay at False. Otherwise, the second
parameter is more interesting because it allows or not the display of
the file and the line where the exception occurs in the stack trace. We
warn you that this method works only if an exception occurs.

::

    try:
      1/0
    except Exception:
      gLogger.exception("Division by 0", lExcInfo=True)
      gLogger.exception("Division by 0")
    #will display:
    #Division by 0
    #== EXCEPTION == ZeroDivisionError
    #  File "toto.py", line 132, in test_exception
    #    1/0
    #
    #ZeroDivisionError: integer division or modulo by zero
    #===============
    #
    #Division by 0
    #== EXCEPTION == ZeroDivisionError
    #
    #ZeroDivisionError: integer division or modulo by zero
    #===============

These methods attach a certain level to the message, and as we seen
above, if the absolute value of the *gLogger* level is superior to the
absolute value of the message level, the log is not created.

::

    glogger.setLevel("notice")
    glogger.debug("this message will not be displayed")
    #the last line will return False

Display
-------

Basic display
~~~~~~~~~~~~~

The basic display for log message is:

::
    [Year]-[Month]-[Day] [Hour]:[Minute]:[Second] UTC /[Component]/[Logname] [Levelname] : [Message]

Example:

::

    2017-04-25 15:51:01 UTC Framework/logMultipleLines ALWAYS: this is a message

The date is UTC formatted and the system and the component names come
from the configuration file. This display can vary according to the
component, the backend and different option parameters.

Component
~~~~~~~~~

Client component
^^^^^^^^^^^^^^^^

All messages from a client , wherever located, are displayed like:

::

    [Year]-[Month]-[Day] [Hour]:[Minute]:[Second] UTC Framework/[Logname][Levelname] : [Message]

The component name disappears and the system name becomes *Framework*.
That is because there are no Client component in configuration files and
*Framework* is the default system name.

Script Component
^^^^^^^^^^^^^^^^

All messages from a script are displayed like:

::

    [Message]

That is because the *parseCommandLine()* method modify one option
parameter in *gLogger* : *showHeaders* to False. Let is talk more about
these options.

Optional Parameter
~~~~~~~~~~~~~~~~~~

*showHeader* option
^^^^^^^^^^^^^^^^^^^

*showHeader* is a boolean variable inside *gLogger* which allow us to
hide or not the prefix of the message from the log. It can be changed
via the *showHeader(val)* method and its default value is obviously
True.

*showThreads* option
^^^^^^^^^^^^^^^^^^^^

As the previous option, *showThreads* is a boolean variable inside
*gLogger* which allow us to hide or not the thread ID in the log. This
thread ID is created from the original thread ID of Python and modified
by the backend to become a word. It is positioned between the log name
and the level name like this:

::

    2017-04-25 15:51:01 UTC Framework/logMultipleLines [PokJl] ALWAYS: this is a message

Its default value is False and we can set it via *showThreadIDs(val)*
method. Nevertheless, if the *showHeaders* option is False, this option
will have no effect on the display.

*LogShowLine* option
^^^^^^^^^^^^^^^^^^^^

This option is only available from the *cfg* file and allows us to add
extra information about the logger call between the logger name and the
level of the message, like this:

::

    2017-04-28 14:56:54 UTC TestLogger/SimplestAgent[opt/dirac/DIRAC/FrameworkSystem/private/logging/Logger.py:160] INFO: Result

It is composed by the caller object path and the line in the file. As
the previous option, it has no effect on the display if the
*showHeaders* option is False.

*LogColor* option
^^^^^^^^^^^^^^^^^

This option is only available from the *cfg* file too, and only for
*PrintBackend*. It allows us to add some colors according to the message
level in the standard output like this:

::

    2017-04-28 14:56:54 UTC TestLogger/SimplestAgent DEBUG: Result
    2017-04-28 14:56:54 UTC TestLogger/SimplestAgent WARN: Result
    2017-04-28 14:56:54 UTC TestLogger/SimplestAgent ERROR: Result

*child* attribute from *getSubLogger()* method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Previously, we saw the basic use of the *child* attribute from the
*getSubLogger()* method. Actually, this attribute is considerably more
complex and can modify the display in several ways but it seems to be
illogic and buggy, so be careful using this attribute with a sublogger
of a sublogger. Here is a simple example of its use with an agent
running:

::

    child = True: 2017-05-04 08:37:10 UTC TestLogger/SimplestAgent/log ALWAYS: LoggingChildTrue
    child = False: 2017-05-04 08:37:10 UTC log ALWAYS: LoggingChildFalse

Backends
--------

Currently, there are four different backends inherited from a base which
build the message according to the options seen above and another called
*LogShowLine*. These four backends just write the message at associated
place. There are :

+-----------------+--------------------+
| Backend         | Output             |
+=================+====================+
| PrintBackend    | standard output    |
+-----------------+--------------------+
| StdErrBackend   | error output       |
+-----------------+--------------------+
| RemoteBackend   | logserver output   |
+-----------------+--------------------+
| FileBackend     | file output        |
+-----------------+--------------------+

They need some information according to their nature. The PrintBackend
needs a color option while the FileBackend needs a file name. In
addition, the RemoteBackend needs a sleep time, an interactivity option
and a site name. These information are collected from the *cfg* file.

Configuration
-------------

Configuration via the *cfg* file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Logger configuration
^^^^^^^^^^^^^^^^^^^^

It is possible to configure some options of the logger via the *cfg*
file. These options are :

+---------------+------------------------------------------------+--------------------------------+
| Option        | Description                                    | Excpected value(s)             |
+===============+================================================+================================+
| LogLevel      | Set a level to gLogger                         | All the level names            |
+---------------+------------------------------------------------+--------------------------------+
| LogBackends   | Add backends to *gLogger* backend list         | stdout, stderr, file, ...      |
+---------------+------------------------------------------------+--------------------------------+
| LogShowLine   | Add information about the logger call          | True, False                    |
+---------------+------------------------------------------------+--------------------------------+
| LogColor      | Add color on messages, only for PrintBackend   | True, False                    |
+---------------+------------------------------------------------+--------------------------------+

Backend configuration
^^^^^^^^^^^^^^^^^^^^^

We also have the possibility to configure backend options via this file.
To do a such operation, we just have to create a *BackendsOptions*
section inside the component. Inside, we can add these following options:

+-----------------+---------------------------------------------+----------------------+
| Option          | Description                                 | Excpected value(s)   |
+=================+=============================================+======================+
| FileName        | Set a file name for FileBackend             | String value         |
+-----------------+---------------------------------------------+----------------------+
| SleepTime       | Set a sleep time for RemoteBackend          | Int value            |
+-----------------+---------------------------------------------+----------------------+
| Interactivity   | Flush messages or not, for Remote Backend   | True, False          |
+-----------------+---------------------------------------------+----------------------+

*cfg* file example
^^^^^^^^^^^^^^^^^^

Here is a component section which contains logger and backend
configuration:

::

    Agents
    {
        SimplestAgent
        {
          LogLevel = INFO
          LogBackends = stdout,stderr,file
          LogColor = True
          LogShowLine = True

          PollingTime = 60
          Message = still working...

          BackendsOptions
          {
            FileName = /tmp/logtmp.log
          }
        }
    }

Configuration via command line argument
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Moreover, it is possible to change the display via one program argument
which is picked up by *gLogger* at its initalization. According to the
number of *d* in the argument, the logger active or not different
options and set a certain level. Here is a table explaining the working:

+----------------------------------+----------------+----------------+-----------+
| Argument                         | ShowHeader     | showThread     | Level     |
+==================================+================+================+===========+
| Default(Client/Agent/Services)   | True           | False          | Notice    |
+----------------------------------+----------------+----------------+-----------+
| Default(Script)                  | False          | False          | Notice    |
+----------------------------------+----------------+----------------+-----------+
| -d                               | DefaultValue   | DefaultValue   | Verbose   |
+----------------------------------+----------------+----------------+-----------+
| -dd                              | True           | DefaultValue   | Verbose   |
+----------------------------------+----------------+----------------+-----------+
| -ddd                             | True           | True           | Debug     |
+----------------------------------+----------------+----------------+-----------+

Multiple processes and threads
------------------------------

Multiple processes
~~~~~~~~~~~~~~~~~~

*DIRAC* is composed by many micro services running in multiple processes. *gLogger* object is naturally different for two distinct processes and can not save the application from process conflicts.
Indeed, *gLogger* is not process-safe, that means that two processes can encounter conflicts if they try to write on a same file at the same time. So, be careful to avoid the case.

Multiple threads
~~~~~~~~~~~~~~~~

*gLogger* does not contain any safety against thread conflicts too, so be careful to not write on one file at the same time with two distinct threads.

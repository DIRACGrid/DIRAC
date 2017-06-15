.. _gLogger_gLogger_basics:

Basics
======

Get a child *Logging* object
----------------------------

*Logging* presentation
~~~~~~~~~~~~~~~~~~~~~~

*gLogger* is an instance of a *Logging* object. The purpose of these
objects is to create log records. Moreover, they are part of a tree,
which means that each *Logging* has a parent and can have a list of
children. *gLogger* is considered as the root *Logging*, on the top of
this tree.

Initialize a child *Logging*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since *Logging* objects are part of a tree, it is possible to get
children from each *Logging* object. For a simple use, we will simply
get one child *Logging* from *gLogger*, the root *Logging*, via the
command:

::

    logger = gLogger.getSubLogger("logger")

This child can be used like *gLogger* in the middleware. In this way, we
recommend you to avoid to use directly *gLogger* and to create at least
one child from it for each component in *DIRAC* with a correct name.

Otherwise, note that the created child is identified by its name,
*logger* in our case, and can be retrieve via the *getSubLogger()*
method. For instance :

::

    logger = gLogger.getSubLogger("logger")
    newLogger = gLogger.getSubLogger("logger")
    # Here, logger and newlogger are a same and unique object 

Get its sub name
~~~~~~~~~~~~~~~~

We can obtain the name of a child *Logging* via the *getSubName* method.
Here is an example of use:

::

    logger = gLogger.getSubLogger("logger")
    logger.getSubName()
    # > logger

Get its system and component names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each *Logging* object belongs to one component from one system, the one
which is running. Thus, we can get these names thanks to the *getName*
method. They will appear as a *system/component* path like this:

::

    logger = gLogger.getSubLogger("logger")
    logger.getName()
    # > Framework/Atom

Send a log record
-----------------

Log record presentation
~~~~~~~~~~~~~~~~~~~~~~~

A log record is composed by a date, a system and a component name, a
*Logging* name, a level and a message. This information represents its
identity.

::

    [Date] UTC [System]/[Component]/[Log] [Level]: [Message]
    2017-04-25 15:51:01 UTC Framework/Atom/log ALWAYS: message

Levels and context of use
~~~~~~~~~~~~~~~~~~~~~~~~~

The level of a log record represents a major characteristic in its
identity. Indeed, it constitutes its nature and defines if it will be
displayed or not. *gLogger* puts 10 different levels at our disposal in
DIRAC and here is a table describing them and their context of use.


+------------+----------------------------------------------------------------------------------------------------+
| Level name | Context of use                                                                                     |
+============+====================================================================================================+
| Fatal      | Must be used before an error forcing the program exit and only in this case.                       |
+------------+----------------------------------------------------------------------------------------------------+
| Always     | Used with moderation, only for message that must appears all the time.                             |
+------------+----------------------------------------------------------------------------------------------------+
| Error      | Used when an error occur but do not need to force the program exit.                                |
+------------+----------------------------------------------------------------------------------------------------+
| Exception  | Actually a specification of the Error level which must be used when an exception is trapped.       |
+------------+----------------------------------------------------------------------------------------------------+
| Notice     | Used to provide an important information.                                                          |
+------------+----------------------------------------------------------------------------------------------------+
| Warn       | Used when a potentially undesired behaviour can occur.                                             |
+------------+----------------------------------------------------------------------------------------------------+
| Info       | Used to provide information.                                                                       | 
+------------+----------------------------------------------------------------------------------------------------+
| Verbose    | Used to provide extra information.                                                                 |
+------------+----------------------------------------------------------------------------------------------------+
| Debug      | Must be used with moderation to debug the program.                                                 |
+------------+----------------------------------------------------------------------------------------------------+

These levels have a priority order from *debug* to *fatal*. In this way,
*fatal* and *always* log records appear almost all the time whereas
*debug* log records rarely appears. Actually, their appearance depends
on the level of the *Logging* object which sends the log records.

Log record creation
~~~~~~~~~~~~~~~~~~~

10 methods are at our disposal to create log records from a *Logging*
object. These methods carry the name of the different levels and they
are all the same signature. They take a message which has to be fixed
and a variable message in parameters and return a boolean value
indicating if the log will appear or not. Here is an example of the
*error* method to create error log records:

::

    boolean error(sMsg, sVarMsg='')

For instance, we create *notice* log records via the following commands:

::

    logger = gLogger.getSubLogger("logger")
    logger.notice("message")
    # > 2017-04-25 15:51:01 UTC Framework/logger NOTICE: message
    logger.notice("mes", "sage")
    # > 2017-04-25 15:51:01 UTC Framework/logger NOTICE: mes sage

Another interesting point is the use of the *exception* method which
gives a stack trace with the message. Here is a use of the *exception*
method:

::

    logger = gLogger.getSubLogger("logger")
    try:
        badIdea = 1/0
        print badIdea
    except:
        logger.exception("bad idea")
    # > 2017-04-25 15:51:01 UTC Framework/logger ERROR: message
    #Traceback (most recent call last):
    #File "....py", line 32, in <module>
    #a = 1/0
    #ZeroDivisionError: integer division or modulo by zero

Log records with variable data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*gLogger* use the old *%-style* to include variable data. Thus, you can
include variable data like this:

::

    logger = gLogger.getSubLogger("logger")
    arg = "argument"
    logger.notice("message with %s" % arg)
    #> 2017-04-25 15:51:01 UTC Framework/logger NOTICE: message with argument

Control the *Logging* level
---------------------------

*Logging* level presentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As we said before, each *Logging* has a level which is set at *notice*
by default. According to this level, the log records are displayed or
not. To be displayed, the level of the log record has to be equal or
higher than the *Logging* level. Here is an example:

::

    # logger level: NOTICE 
    logger = gLogger.getSubLogger("logger")
    logger.error("appears")
    logger.notice("appears")
    logger.verbose("not appears")
    # > 2017-04-25 15:51:01 UTC Framework/logger ERROR: appears
    # > 2017-04-25 15:51:01 UTC Framework/logger NOTICE: appears

As we can see, the *verbose* log record is not displayed because its
level is inferior to *notice*. Moreover, we will see in the advanced
part that the level is propagate to the *Logging* children. Thus, for a
basic use, you do not need to set the level of a child *Logging*.

Set a level via the command line
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The more used and recommended method to set the level of *gLogger* is to
use the command line arguments. It works with any *DIRAC* component but
we can not define a specific level. Here is a table of these different
arguments:

+------------+------------------------------------------+
| Argument   | Level associated to the root *Logging*   |
+============+==========================================+
| default    | notice                                   |
+------------+------------------------------------------+
| -d         | verbose                                  |
+------------+------------------------------------------+
| -dd        | verbose                                  |
+------------+------------------------------------------+
| -ddd       | debug                                    |
+------------+------------------------------------------+

We can find a complete table containing all the effects of the command
line arguments in the `Summary of the command line argument configuration`_ part.

Set a level via the *cfg* file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can also set the *gLogger* level in the *cfg* file via the *LogLevel*
line. We can define a specific level with this method, but it does not
work for scripts. Here is an example of an agent with the root
*Logging*\ level set to *always*:

::

    Agents
    {
      SimplestAgent
      {
        LogLevel = ALWAYS
        ...
      }
    }   

Set a level via the *setLevel* method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is a last method to set any *Logging* level. We just have to give
it a string representing a level like this:

::

    logger = gLogger.getSubLogger("logger")
    logger.setLevel("info")

In this example, the level of *logger* is set to *info*. By the way, we
recommend you to not use this method for a basic use.

Get the level attaching to a specific *Logging*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can obviously get a level associate to a *Logging* via the *getLevel*
method. This method returns a string representing a level. Here is an
example of use:

::

    logger = gLogger.getSubLogger("logger")
    logger.getLevel()
    # > "NOTICE"

Get all the existing levels
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the same way, we can get all the existing level names thanks to the
*getAllPossibleLevels* method. This method returns a list of string
representing the different levels. Here is an example of use:

::

    # 'level' comes from a user
    def method(level):
        if level in self.logger.getAllPossibleLevels():
         # ...

Test the *Logging* level superiority
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In some cases, it can be interesting to test the *Logging* level before
creating a log record. For instance, we need to send a *verbose* log
record using an expensive function and we do not need to make it if it
can not be send to an output. To avoid such an operation, we can use the
*shown* method which controls if the *Logging* level is superior to a
specific level. If it is the case, the method returns *True*, else
returns *False*. Here is an example of this use:

::

    # logger level: ERROR
    logger = gLogger.getSubLogger("logger")
    if logger.shown('verbose'):
        logger.verbose("Expensive message: %s" % expensiveFunc())
    # > False

Modify the log record display
-----------------------------

Default display
~~~~~~~~~~~~~~~

| As we saw before, the basic display for a log record is:

::

    [Date] UTC [System]/[Component]/[Log] [Level]: [Message]
    2017-04-25 15:51:01 UTC Framework/Atom/log ALWAYS: message

The date is UTC formatted and the system and the component names come
from the *cfg* file. By default, the system name is *Framework* while
the component name does not exist. This display can vary according to
different option parameters.

Remove the prefix of the log record
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the scripts, we can observe log record without any prefix, only a
message like this:

::

    [Message]
    message

This behaviour is explained by the *parseCommandLine* function, that we
can find in every scripts, which set the boolean *headerIsShown* from
*Logging* to *False*. To do a such operation, it used the *showHeaders*
method from *Logging*. Here is the signature of the method:

::

    showHeaders(yesno=True)

To summarize, the default value of *headerIsShown* is *True*, which
means that the prefix is displayed, and we can set it at False to hide
it.

There are two ways to modify it, the *showHeaders* method as we saw, and
the command line argument *-d*. Here is a table presenting the changes
according to the argument value:

+--------------------------------------+------------------------------------------+
| Argument                             | Level associated to the root *Logging*   |
+======================================+==========================================+
| Default(Executors/Agents/Services)   | True                                     |
+--------------------------------------+------------------------------------------+
| Default(Scripts)                     | False                                    |
+--------------------------------------+------------------------------------------+
| -d                                   | default value                            |
+--------------------------------------+------------------------------------------+
| -dd                                  | True                                     |
+--------------------------------------+------------------------------------------+
| -ddd                                 | True                                     |
+--------------------------------------+------------------------------------------+

We can find a complete table containing all the effects of the command
line arguments in the `Summary of the command line argument configuration`_ part.

Add the thread ID in the log record
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to add a thread ID in our log records thanks to the
*showThreadIDs* method which modify the boolean *threadIDIsShown* value.
As the *showHeaders* method, it takes a boolean in parameter to set
*threadIDIsShown*. This attribute is set at *False* by default. Here is
an example with the boolean at *True*:

::

    [Date] UTC [System]/[Component]/[Log][Thread] [Level]: [Message]
    2017-04-25 15:51:01 UTC Framework/Atom/log[140218144]ALWAYS: message

We can see the thread ID between the *Logging* name and the level:
[140218144]. Nevertheless, set the boolean value is not the only
requirement. Indeed, *headerIsShown* must be set at *True* to effect the
change. In this way, it is impossible to have the thread ID without the
prefix.

A second way to set the boolean is to use the command line argument
*-d*. Here is a table presenting the changes according to the argument:

+--------------------------------------+------------------------------------------+
| Argument                             | Level associated to the root *Logging*   |
+======================================+==========================================+
| Default(Executors/Agents/Services)   | False                                    |
+--------------------------------------+------------------------------------------+
| Default(Scripts)                     | False                                    |
+--------------------------------------+------------------------------------------+
| -d                                   | default value                            |
+--------------------------------------+------------------------------------------+
| -dd                                  | default value                            |
+--------------------------------------+------------------------------------------+
| -ddd                                 | True                                     |
+--------------------------------------+------------------------------------------+

We can find a complete table containing all the effects of the command
line arguments in the `Summary of the command line argument configuration`_ part.

Remove colors on the log records
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*LogColor* option is only available from the *cfg* file, and only
for the *stdout* and the *stderr* with agents, services and executors.
By default, the *LogColor* option is set a *True* and adds colors on the
log records according to their levels. You can remove colors setting the
option at *False* in the *cfg* file:

::

    LogColor = False

We can find a *cfg* file example containing different options in the `cfg file example`_ part.

Get the option values
~~~~~~~~~~~~~~~~~~~~~

It is possible to obtain the names and the values associated of all
these options with the *getDisplayOptions* method. This method returns
the dictionary used by the *Logging* object itself and not a copy, so we
have to be careful with its use. Here is an example:

::

    logger = gLogger.getSubLogger("logger")
    logger.getDisplayOptions()
    # > {'Color': False, 'Path': False, 
    #    'headerIsShown': True, 'threadIsShown': False}

Send a log record in different outputs
--------------------------------------

Backend presentation
~~~~~~~~~~~~~~~~~~~~

*Backend* objects are used to receive the log record created before,
format it according to the choice of the client, and send it in the
right output. Currently, there are four different *Backend* object
inherited from a base. Here is a table presenting them:

+-----------------+-------------------------+
| Backend name    | Output                  |
+=================+=========================+
| stdout          | standard output         |
+-----------------+-------------------------+
| Stderr          | error output            |
+-----------------+-------------------------+
| RemoteBackend   | SystemLogging service   |
+-----------------+-------------------------+
| FileBackend     | file                    |
+-----------------+-------------------------+

As we may notice, *gLogger* has already a *stdout Backend* by default.

Add a *Backend* to your *Logging*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To send a created log record to an output, our root *Logging* has to add
some *Backend* objects in a list. To do such an operation, we have to
write the desired *Backend* objects in the *cfg* file using the
*LogBackends* option, like this:

::

    LogBackends = stdout,stderr,file,server

Here, we add all of the *Backend* object types in the root *Logging*.
Thus, a log record created will be sent to 4 different outputs. We can
find a *cfg* file example containing different options in the `cfg file example`_ part.

We can also notice that, in the future, we expect to have plugable *Backend* objects in order to allow anyone to define new types of *Backend* objects.

Configure the *Backend* objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some *Backend* objects need some parameters according to their nature.
By default, each type of *Backend* has default parameters but it is
possible to change them via the *BackendsOptions*\ section of the *cfg*.
Here is a table presenting the different parameters that we can
configure for each *Backend* and their default values:

+--------+-----------+------------------------------------------------------+----------------------+
| Type   | Option    | Description                                          | Default value        |
+========+===========+======================================================+======================+
| file   | FileName  | name of the file where the log records must be sent  | Dirac-log\_[pid].log |
+--------+-----------+------------------------------------------------------+----------------------+
| server | SleepTime | sleep time in seconds                                | 150                  |
+--------+-----------+------------------------------------------------------+----------------------+

We can also notice that the *server Backend* requires that the
*Framework/SystemLogging* service is running in order to send log
records to a log server.

Some examples and summaries
---------------------------

*cfg* file example
~~~~~~~~~~~~~~~~~~

Here is a component section which contains *Logging* and *Backend*
configuration:

::

    Agents
    {
        SimplestAgent
        {
          LogLevel = INFO
          LogBackends = stdout,stderr,file
          BackendsOptions
          {
            FileName = /tmp/logtmp.log
          }
          LogColor = False
          LogShowLine = True
        }
    }   

To summarize, this file configures an agent named *SimplestAgent*, sets
the level of *gLogger* at *info*, adds 3 *Backend* objects to it, which
are *stdout*, *stderr* and *file*. Thus, each log record superior to
*info* level, created by a *Logging* object in the agent, will be send
to 3 different outputs. We learn also from the *BackendOptions* that the
*file Backend* will send these log records to the */tmp/logtmp.log*
file.

In addition, the log records will be not displayed with color, and the
caller path name will not appear if we do not change the level to
*debug*.

Summary of the command line argument configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is a complete table explaining the changes provided by the command
line argument *-d*: 

+--------------------------------------+----------------+----------------+-----------+
| Argument                             | ShowHeader     | showThread     | Level     |
+======================================+================+================+===========+
| Default(Executors/Agents/Services)   | True           | False          | Notice    |
+--------------------------------------+----------------+----------------+-----------+
| Default(Scripts)                     | False          | False          | Notice    |
+--------------------------------------+----------------+----------------+-----------+
| -d                                   | DefaultValue   | DefaultValue   | Verbose   |
+--------------------------------------+----------------+----------------+-----------+
| -dd                                  | True           | DefaultValue   | Verbose   |
+--------------------------------------+----------------+----------------+-----------+
| -ddd                                 | True           | True           | Debug     |
+--------------------------------------+----------------+----------------+-----------+

About multiple processes and threads
------------------------------------

Multiple processes
~~~~~~~~~~~~~~~~~~

*gLogger* object is naturally different for two distinct
processes and can not save the application from process conflicts.
Indeed, *gLogger* is not process-safe, that means that two processes can
encounter conflicts if they try to write on a same file at the same
time. So, be careful to avoid the case.

Multiple threads
~~~~~~~~~~~~~~~~

*gLogger* is based on the Python *logging* library which is completely
thread-safe. Nevertheless, we can not guarantee that *gLogger* is thread-safe too.

Advanced part
------------------------------------

You can find more information about *gLogger* and its functionalities in the :ref:`gLogger_gLogger_advanced` part.

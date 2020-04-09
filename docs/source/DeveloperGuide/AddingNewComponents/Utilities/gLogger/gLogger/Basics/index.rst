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

Set a level via the configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can also set the *gLogger* level in the configuration via the *LogLevel*
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

Get the level attached to a specific *Logging*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can obviously get a level associated to a *Logging* via the *getLevel*
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
from the configuration. By default, the system name is *Framework* while
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

    [Date] UTC [System]/[Component]/[Log] [Thread] [Level]: [Message]
    2017-04-25 15:51:01 UTC Framework/Atom/log [140218144]ALWAYS: message

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

Hide the timestamp in the log record
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to hide the timestamp from the log records via the
*showTimeStamps* method which modifies the boolean *timeStampIsShown* value.
As the *showHeaders* method, it takes a boolean in parameter to set
*timeStampIsShown*. This attribute is set to *False* by default. Here is
an example with the boolean at *False*:

::

    [System]/[Component]/[Log] [Level]: [Message]
    Framework/Atom/log ALWAYS: message

Hide the context in the log record
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can also hide the context (compoenent name and logger name) from the log records via the
*showContexts* method which modifies the boolean *contextIsShown* value.
As the *showHeaders* method, it takes a boolean in parameter to set
*contextIsShown*. This attribute is set to *False* by default. Here is
an example with the boolean at *False*:

::

    [Date] UTC [Level]: [Message]
    2017-04-25 15:51:01 UTC ALWAYS: message


Remove colors on the log records
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*LogColor* option is only available from the configuration, and only
for the *stdout* and the *stderr* with agents, services and executors.
By default, the *LogColor* option is set a *True* and adds colors on the
log records according to their levels. You can remove colors setting the
option at *False* in the configuration:

::

    LogColor = False

We can find a configuration example containing different options in the `Configuration example`_ part.

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


.. _gLogger_gLogger_basics_backend:

Send a log record in different outputs
--------------------------------------

Backend presentation
~~~~~~~~~~~~~~~~~~~~

*Backend* objects are used to receive the log record created before,
format it according to the choice of the client, and send it in the
right output. We can find an exhaustive list of the existing *Backend* types in the :ref:`gLogger_backends` part.


Backend resources
~~~~~~~~~~~~~~~~~

A *Backend resource* is the representation of a *Backend* object in the configuration. It is represented by one or two elements depending on its nature. The first is an identifier, which can be a default identifier or a custom:

+ Default identifiers take the name of a *Backend* class name, *<backendID>* will refer to the *<BackendID>Backend* class, *stdout* and *StdoutBackend* for instance.
+ Custom identifiers can take any name like *f015* or *Jwr8*, there is no construction rule.

The second element is a set of parameters according to the *Backend* represented. Custom identifiers absolutely need to complete the *Plugin* option to indicate which *Backend* type they represent using a default identifier. This section can also be empty if the *Backend* do not need parameter and if the identifier is a default identifier. Here is a generic example of a *Backend resource*:

::

    <backendDefaultID1>
    {
        <param1> = <value1>
        <param2> = <value2>
    }

    <backendCustomID>
    {
        Plugin = <backendDefaultID2>
        <param1> = <value1>
    }

Declare the *Backend* resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before using them, *Backend resources* have to be declared in the configuration.
They can be configured in a global way or in a local way.
To declare them in the global way, we must put them in the */Resources/LogBackends* section of the configuration, like this:

::

    Resources
    {
        LogBackends
        {
            <backendID1>
            {
                Plugin = <backendClass1>
                <param1> = <value1>
            }
            <backendID2>
            {
                Plugin = <bakendClass2>
                <param2> = <value2>
            }
            <backendID3>
            {
                <param3> = <value3>
            }
        }
    }

Here is an example of a concrete configuration:

::

    Resources
    {
        LogBackends
        {
            f01
            {
                Plugin = file
                FileName = /path/to/file.log
            }
            es2
            {
                Plugin = elasticSearch
                Host = lhcb
                Port = 9540
            }
            file
            {
                FileName = /path/to/anotherfile.log
            }
        }
    }

In this case, we have 3 *Backend* identifiers, namely *f01* and *es2* which are custom identifiers respectively related on *FileBackend* and *ElasticSearchBackend*, and *file* which is a default identifier based on *FileBackend*.

This configuration allows a *Backend resource* use in any component of the configuration, but we can also create some specific *Backend resources* inside a local component. To create local resources, you have to follow the same process in a *LogBackendsConfig* section like this:

::

    <Agent>
    {
        ...
        LogBackendsConfig
        {
            <backendID4>
            {
                Plugin = <backendClass4>
                <param4> = <value4>
            }
            <backendID5>
            {
                Plugin = <bakendClass5>
                <param5> = <value5>
            }
            <backendID6>
            {
                <param6> = <value6>
            }
        }
    }

Moreover, a same *Backend* identifier can be declared in the both sections in order to update it. Indeed, such a declaration triggers a parameters merger. In case of parameters conflicts, the local parameters are always choosen. Here is an example:

::

    <Systems>
    {
        Agents
        {
            <Agent1>
            {
                ...
                LogBackendsConfig
                {
                    <backendID1>
                    {
                        <param1> = <value1>
                        <param2> = <value2>
                    }
                }
            }
        }
    }
    Resources
    {
        LogBackends
        {
            <backendID1>
            {
                Plugin = <backendClass1>
                <param1> = <value4>
                <param3> = <value3>
            }
        }
    }

In this case, *gLogger* in *<Agent1>* will have one *Backend* instance of the *<backendClass1>Backend* class which will have 3 parameters:

+ <param1> = <value1>
+ <param2> = <value2>
+ <param3> = <value3>

Use the *Backend* resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once our *Backend* resources are declared, we have to specify where we want to use them and we have many possibilities. First of all, we can add them for the totality of the components. This can be made in the */Operations/defaults/* section. Here is the way to proceed:

::

    Operations
    {
        Defaults
        {
            Logging
            {
                DefaultBackends = <backendID1>, <backendID2>, <backendID3>
            }
        }
    }

We can also add them for a specific component type, the agents or the services for instance. Such a declaration will overwrite the previous one for the component type choosen:

::

    Operations
    {
        Defaults
        {
            Logging
            {
                Default<componentType>sBackends = <backendID1>, <backendID2>, <backendID3>
            }
        }
    }

Do not forget the *s* between *<componentType>* and *Backends*. In this case, all the *<componentType>* components will have the same resources if we do not overwritten locally. This can be made by the use of the *LogBackends* option used inside any component like this:

::

    <Agent1>
    {
        LogBackends = <backend1>, <backend2>, <backend3>
    }

If none of these options is specified, the *stdout Backend* will be used.

Some examples and summaries
---------------------------

Configuration example
~~~~~~~~~~~~~~~~~~~~~

Here is a configuration which contains *Logging* and *Backend*
configuration:

::

    Systems
    {
        FrameworkSystem
        {
            Agents
            {
                SimplestAgent
                {
                  LogLevel = INFO
                  LogBackends = stdout,stderr,file, file2, es2
                  LogBackendsConfig
                  {
                    file
                    {
                        FileName = /tmp/logtmp.log
                    }
                    file2
                    {
                        Plugin = file
                        FileName = /tmp/logtmp2.log
                    }
                  }
                  LogColor = False
                }
                AnotherAgent
                {
                    LogLevel = NOTICE
                    LogBackends = stdout, es2
                    LogBackendsConfig
                    {
                        es2
                        {
                            UserName = lchb34
                            Password = passw0rd
                        }
                    }
                }
            }
        }
    }
    Operations
    {
        Defaults
        {
            Logging
            {
                DefaultBackends = stdout
                DefaultAgentsBackends = stderr
            }
        }
    }
    Resources
    {
        LogBackends
        {
            es2
            {
                Plugin = elasticSearch
                Host = lhcb
                Port = 9540
                UserName = lhcb
                Password = 123456
            }
        }
    }

To summarize, this file configures two agents respectively named *SimplestAgent* and *AnotherAgent*.
In *SimplestAgent*, it sets the level of *gLogger* at *info*, adds 5 *Backend* objects to it, which
are *stdout*, *stderr*, two *file Backend* objects and an *ElastiSearch* access. Thus, each log record superior to
*info* level, created by a *Logging* object in the agent, will be sent
to 5 different outputs: *stdout*, *stderr*, */tmp/logtmp.log*, */tmp/logtmp2.log* and ElasticSearch. In *AnotherAgent*, the same process is performed, and each log record superior to *notice* level is sent to *stdout* and another ElasticSearch database because of the redifinition. None of the default *Backend* objects of the *Operations* section are used because of the overwriting.
In addition, the log records will be not displayed with color.

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

*gLogger* is completely thread-safe, there is no conflict possible especially in the case when two threads
try to write on a same file at the same time.

About the use of external libraries
-----------------------------------

*DIRAC* uses some external libraries which have their own loggers, mainly based on the standard logging Python library like *gLogger*. Logs providing by these libraries can be useful in debugging, but not in production. The *enableLogsFromExternalLib* and *disableLogsFromExternalLib* methods allow us to enable or disable the display of these logs.
The first method initializes a specific logger for external libraries like this:

+ a level at Debug
+ a display on the standard error output
+ a log format close to the one used in *DIRAC*

We can call these methods each time that we use an external library and we want to see the logs inside or not.

Filter
------

The output given by the different logger can be further controlled through the use of *filters*. Any
configured backend can be given the paramter *Filter*, which takes a comma separated list of filterIDs.

::

    Resources
    {
        LogBackends
        {
            <backendID1>
            {
                Plugin = <backendClass1>
                Filter = MyFilter[,MyOtherFilter]*
                <param1> = <value4>
                <param3> = <value3>
            }
        }
    }

Each filter can be configured with a given plugin type and the parameters used for the given
plugin. See the documentation for the :mod:`~DIRAC.Resources.LogFilters` for the available plugins
and their parameters.

Each filter is queried, and only the the log record passes *all* filters is passed onwards.

::

    Resources
    {
        LogFilters
        {
            MyFilter
            {
                Plugin = FilterPlugin
                Parameter = Value, Value2
            }
        }
    }

Filter implementation
~~~~~~~~~~~~~~~~~~~~~

The filter implementations need to be located in the *Resources/LogFilters* folder
and can be any class that implements a ``filter`` function that takes a log record as an argument.
See the existing implementations in :mod:`~DIRAC.Resources.LogFilters` as examples.




Advanced part
------------------------------------

You can find more information about *gLogger* and its functionalities in the :ref:`gLogger_gLogger_advanced` part.

=================
Developing Agents
=================

What is an agent?
-----------------

Agents are active software components which run as independent processes to fulfil one or several system functions. They are the engine that make DIRAC beat. Agents are processes that perform actions periodically. Each cycle agents typically contact a service or look into a DB to check for pending actions, execute the required ones and report back the results. All agents are built in the same framework which organizes the main execution loop and provides a uniform way for deployment, configuration, control and logging of the agent activity.

Agents run in different environments. Those belonging to a DIRAC system, for example Workload Management or Data Distribution, are usually deployed close to the corresponding services. They watch for changes in the system state and react accordingly by initiating actions like job submission or result retrieval.


Simplest Agent
--------------

An agent essentially loops over and over executing the same function every *X* seconds. It has essentially two methods, *initialize* and *execute*. When the agent is started it will execute the *initialize* method. Typically this *initialize* method will define (amongst other stuff) how frequently the *execute* method will be run. Then the *execute* method is run. Once it finishes the agent will wait until the required seconds have passed and run the *execute* method again. This will loop over and over until the agent is killed or the specified amount of loops have passed.

Creating an Agent is best illustrated by the example below which is presenting a fully
functional although simplest possible agent:

.. code-block:: python

   """ :mod: SimplestAgent

      Simplest Agent send a simple log message
   """

   # # imports
   from DIRAC import S_OK, S_ERROR
   from DIRAC.Core.Base.AgentModule import AgentModule
   from DIRAC.Core.Base.Client import Client


   class SimplestAgent(AgentModule):
       """
       .. class:: SimplestAgent

       Simplest agent
       print a message on log
       """

       def initialize(self):
	   """agent's initalisation

	   :param self: self reference
	   """
	   self.message = self.am_getOption("Message", "SimplestAgent is working...")
	   self.log.info("message = %s" % self.message)
	   return S_OK()

       def execute(self):
	   """execution in one agent's cycle

	   :param self: self reference
	   """
	   self.log.info("message is: %s" % self.message)
	   simpleMessageService = HelloClient()
	   result = simpleMessageService.sayHello(self.message)
	   if not result["OK"]:
	       self.log.error("Error while calling the service: %s" % result["Message"])
	       return result
	   self.log.info("Result of the request is %s" % result["Value"])
	   return S_OK()

First comes the documentation string describing the service purpose and behavior.
Several import statements will be clear from the subsequent code.

The Agent name is SimplestAgent. The ``initialize`` method is called once when the Agent is created. Here one can put creation and initialization of the global variables if necessary. **Please not that the __init__ method cannot be used when developing an Agent. It is used to intialize the module before it can be used**


Now comes the definition of the ``execute`` method. This method is executed every time Agent runs. Place your code inside this method. Other methods can be defined in the same file and used via ``execute`` method. The result must always be returned as an ``S_OK`` or ``S_ERROR`` structure for the ``execute`` method. The previous example will execute the same example code in the Services section from within the agent.


Default Agent Configuration parameters
--------------------------------------

The Agent is written. It should be placed to the Agent directory of one
of the DIRAC System directories in the code repository, for example FrameworkSystem.
The default Service Configuration parameters should be added to the corresponding
System ConfigTemplate.cfg file. In our case the Service section in the ConfigTemplate.cfg
will look like the following::

  Agents
  {
    ##BEGIN SimplestAgent
    SimplestAgent
    {
      LogLevel = INFO
      LogBackends = stdout
      PollingTime = 60
      Message = still working...
    }
    ##END
  }

'PollingTime' defines the time between cycles, 'Message' is this agent specific
option. ##BEGIN SimplestAgent and ##END are used to automagically include the
agent's documentation into the docstring of the agents' module, by placing this
snippet there, see :ref:`codedocumenting_parameters`

Installing the Agent
--------------------

.. set highlighting to python console input/output
.. highlight:: console

Once the Agent is ready it should be installed. As for the service part, we won't do this part unless we want to mimic a full installation. Also, this part won't work if we won't have a ConfigurationServer running, which is often the case of a developer installation. For our development installation we can modify our local *dirac.cfg* in a very similar fashion to what we have done for the service part in the previous section, and run the agent using the dirac-agent command.


The DIRAC Server installation is described in documentation. If you are adding the Agent to an already existing installation it is sufficient to execute the following in this DIRAC instance::

  $ dirac-install-agent Framework SimplestAgent

This command will do several things:

  * It will create the SimpleAgent Agent directory in the standard place and will set
    it up under the ''runit'' control - the standard DIRAC way of running permanent processes.
  * The SimplestAgent Agent section will be added to the Configuration System.

The Agent can be also installed using the SystemAdministrator CLI interface::

  $ install agent Framework SimplestAgent

The SystemAdministrator interface can also be used to remotely control the Agent, start or
stop it, uninstall, get the Agent status, etc.

Checking the Agent output from log messages
-------------------------------------------

In case you are running a SystemAdministrator service, you'll be able to log in to the machine using (as administrator)
`dirac-admin-sysadmin-cli` and show the log of SimplestAgent using::

  $ show log Framework SimplestAgent

An info message will appear in log::

  Framework/SimplestAgent  INFO: message: still working...

Note that the service is always returning the result in the form of S_OK/S_ERROR structure.

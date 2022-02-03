.. _developing_services:

===================
Developing Services
===================

.. set highlighting to shell console input/output
.. highlight:: console

Service Handler
---------------

All the DIRAC Services are built in the same framework.
Developers should provide a service handler (e.g.: ``MyServiceHandler``) by inheriting the base ``TornadoService`` class.

An instance of the Service Handler is created each time the service receives a client query.
Therefore, the handler data members are only valid for one query.
This means that developers should be aware that if the service state should be preserved,
this should be done using global variables or a database back-end.

Creating a Service Handler is best illustrated by the example below which is presenting a fully functional although a simple service:

.. literalinclude:: HelloHandler.py
   :language: python
   :linenos:

Let us walk through this code to see which elements should be provided.

The first lines shows the documentation string describing the service purpose and behavior.
After that come the import statements. Several import statements will be clear from the subsequent code.

Then comes the definition of the *HelloHandler* class. The Service name is *Hello*.
The ``initializeHandler`` method is called once when the Service is created.
Within this method a developer can put creation and initialization of the variables for the service class if necessary.
Note that the ``initializeHandler`` has a ``@classmethod`` decorator.
That's because the code initializes the class instead of the instance of it.

Then comes the ``initializeRequest`` method. This is used to initialize each instance of the requests.
Every request will trigger a creation of one instance of *HelloHandler*.
This method will be called after all the internal initialization is done.

No ``__init__`` method is specified, and, by construction, it should not be.

Regarding service methods accessible to clients:
the name of each method which will be accessible to the clients has ``export_`` prefix.
Note that the clients will call the method without this prefix.
Otherwise, it is an ordinary class method which takes the arguments provided by the client and returns the result to the client.
The result must always be returned as an ``S_OK`` or ``S_ERROR`` structure.

A useful method is ``srv_getCSOption(csPath, defaultValue)``,
which allows to extract options from the Service section in the Configuration Service directly
without having to use the ``gConfig`` object.

For each "exported" method the service CAN define an ``auth_<method_name>`` class variable being a list.
This will restrict which clients can call this method, but please use this possibility only for doing local tests (see later).
Only clients belonging to groups that have the properties defined in the list will be able to call this method.
*all* is a special keyword that allows anyone to call this method.
*authenticated* is also a special keyword that allows anyone with a valid certificate to call this method.
There is also the possibility to define authentication rules in the Configuration Service.


Default Service Configuration parameters
----------------------------------------

The Hello Handler is written. There's not even the need to copy/paste, because you can do::

    $ cp src/DIRAC/docs/source/DeveloperGuide/AddingNewComponents/DevelopingServices/HelloHandler.py src/DIRAC/FrameworkSystem/Service/

Now, we'll need to put the new service in the DIRAC CS in order to see it running.
Since we are running in an isolated installation, the service will need to be added to the local "dirac.cfg" file.

To do this, we should first have a "/Systems" section in it.
The "/Systems" section keeps references to the real code,
e.g. if you are developing for the "WorkloadManagementSystem" you should have a "/Systems/WorkloadManagement" section.
If there are services that have to run in the WMS, you should place them under "/Systems/WorkloadManagement/<instance>/Services".

The default Service Configuration parameters should be added to the corresponding System ConfigTemplate.cfg file.
In our case the Service section in the ConfigTemplate.cfg will look like the following::

  Services
  {
    Hello
    {
      Protocol = https
      DefaultWhom = Universe
    }
  }

Now, you can try to run the service. To do that, simply::

   $ tornado-start-all -ddd

The ``-ddd`` is for running in DEBUG mode.

If everything goes well, you should see something like::

   2014-05-23 13:58:04 UTC Framework/Hello[MppQ] ALWAYS: Listening at https://localhost:8443/Framework/Hello

The URL displayed should be added to the local *dirac.cfg* in the URLs section (for this example, it already is).

Now, going back for a second on the service calls authorizations: in the example above we have used
``auth_<method_name>`` to define the service authorization properties. What we have done above can be achieved using
the following CS structure::

  Services
  {
    Hello
    {
      Protocol = https
      DefaultWhom = Universe
      Authorization
      {
        sayHello = all
      }
    }
  }

and removing the ``auth_<method_name>`` from the code. This is a better "production" level coding.

You can also specify which default authorizations a service call should have at deploy time by editing the "ConfigTemplate.cfg"
file present in every system.
An example can be found in https://github.com/DIRACGrid/DIRAC/blob/integration/WorkloadManagementSystem/ConfigTemplate.cfg


Calling the Service from a Client
-----------------------------------

Once the Service is running it can be accessed from the clients in the way
illustrated by the following code snippet:

.. code-block:: python

   import DIRAC

   DIRAC.initialize()  # Initialize configuration

   from DIRAC.Core.Base.Client import Client

   simpleMessageService = Client()
   simpleMessageService.serverURL = 'Framework/Hello'
   result = simpleMessageService.sayHello('you')
   if not result['OK']:
       print("Error while calling the service:", result['Message'])  # Here, in DIRAC, you better use the gLogger
   else:
       print(result["Value"])  # Here, in DIRAC, you better use the gLogger

Note that the service is always returning the result in the form of S_OK/S_ERROR structure.


When should a service be developed?
-------------------------------------

Write a service every time you need to expose some information, that is usually stored in a database.

There are anyway cases for which it is not strictly needed to write a service, specifically when all the following are true:

* when you never need to expose the data written in the DB (i.e. the DB is, for the DIRAC point of view, Read-Only)
* when the components writing in it have local access.

The advise is anyway to always write the service, because:

* if later on you'll need it, you won't need to change anything but the service itself
* db-independent logic should stay out of the database class itself.

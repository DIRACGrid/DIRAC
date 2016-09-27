======================================
Developing Services
======================================

Service Handler
-------------------

All the DIRAC Services are built in the same framework where developers should provide a Service Handler by inheriting the base RequestHandler class. An instance of the Service Handler is created each time the service receives a client query. Therefore, the handler data members are only valid for one query. If the service state should be preserved, this should be done using global variables or a database back-end. 

Creating a Service Handler is best illustrated by the example below which is presenting a fully functional although a simple service:

.. literalinclude:: HelloHandler.py

Let us walk through this code to see which elements should be provided.

The first lines show the documentation string describing the service purpose and behavior. It is followed by the ''__RCSID__'' global module variable which is assigned the value of the ''$Id: $'' Git keyword.

After that come the import statements. Several import statements will be clear from the subsequent code.

Then comes the definition of the *HelloHandler* class.  The Service name is *Hello*. The ''initializeHandler'' method is called once when the Service is created. Here one can put creation and initialization of the variables for the service class if necessary. Note that the ''initializeHandler'' has a ''@classmethod'' decorator. That's because the code initializes the class instead of the instance of it.

Then comes the ''initialize'' method. This is used to initialize each instance of the requests. Every request will trigger a creation of one instance of *HelloHandler*. This method will be called after all the internal initialization is done.

Regarding service methods acessible to clients: The name of each method which will be accessible to the clients has *export_* prefix. Note that the clients will call the method without this prefix. Otherwise, it is an ordinary class method which takes the arguments provided by the client and returns the result to the client. The result must always be returned as an S_OK or S_ERROR structure.

A useful method is ''srv_getCSOption( csPath, defaultValue )'' which allows to extract options from the Service section in the Configuration Service directly without having to use the ''gConfig'' object.

For each "exported" method the service can define an *auth_<method_name>* class variable being a list. This will restrict which clients can call this method. Only clients belonging to groups that have the properties defined in the list will be able to call this method. *all* is a special keyword that allows anyone to call this method. *authenticated* is also a special keyword that allows anyone with a valid certificate to call this method.

For each service interface method it is necessary to define *types_<method_name>* class variable of the List type. Each element of the List is one or a list of possible types of the method arguments in the same order as defined in the method definition. The types are imported from the ''types'' standard python module.             

Default Service Configuration parameters
------------------------------------------

The Hello Handler is written. Now, we'll need to put the new service in the dirac CS in order to see it running. Since we are running in an isolated installation, the net effect is that the service will have to be added to the local "dirac.cfg" file. 

To do this, we should first have a "/Systems" section in it. The "/Systems" section keeps references to the real code, e.g. if you are developing for the "WorkloadManagementSystem" you should have a "/Systems/WorkloadManagement" section. If there are services that have to run in the WMS, you should place them under "/Systems/WorkloadManagement/Services". 

For what concerns our example, we should place it to the Service directory of one of the DIRAC System directories, for example we can use FrameworkSystem. 
The default Service Configuration parameters should be added to the corresponding System ConfigTemplate.cfg file. In our case the Service section in the ConfigTemplate.cfg will look like the following::

  Services
  {
    Hello
    {
      Port = 3424
      DefaultWhom = Universe
    }
  }  
  
Note that you should choose the port number on which the service will be listening which is not conflicting with other services. This is the default value which can be changed later in the Configuration Service. The Port parameter should be specified for all the services.  The 'DefaultWhom' is this service specific option.

Now, you can try to run the service. To do that, simply::

  dirac-service Framework/Hello -ddd

The ``-ddd`` is for running in DEBUG mode. At first, this will not work. Useful info will be printed out, and you'll have to work on your dirac.cfg to make it run. Once you are done, you are ready to go.

If everything goes well, you should see something like::

  2014-05-23 13:58:04 UTC Framework/Hello[MppQ] ALWAYS: Listening at dips://diracTutorial.cern.ch:3234/Framework/Hello 

The URL displayed should be added to the local *dirac.cfg* in the URLs section.


Installing the Service
------------------------

We are running in isolation. So, unless you run also a ConfigurationServer on your machine, you won't be able to do the following, and you can safely skip this part.

The Service is ready it should be installed. The DIRAC Server installation is described in [[[here]]]. If you are adding the Service to an already existing installation it is sufficient to execute the following in this DIRAC instance::

  > dirac-install-service Framework Hello
  
This command will do several things:

  * It will create the Hello Service directory in the standard place and will set 
    it up under the ''runit'' control - the standard DIRAC way of running permanent processes. 
  * The Hello Service section will be added to the Configuration System. So, its
    address and parameters will be available to clients.
    
The Service can be also installed using the SystemAdministrator CLI interface (provided that you are running Framework/SystemAdministrator service on your machine)::

  > install service Framework Hello      
  
The SystemAdministrator interface can also be used to remotely control the Service, start or stop it, uninstall, get the Service status, etc. and can be invoked in the standard way via a DIRAC client installation::

  > dirac-admin-sysadmin-cli --host=myDIRACServer

As said in the previous section, in any case, if you are developing a service, you might test it without installing it, by simply running::

  > dirac-service Framework/Hello


Calling the Service from a Client
-----------------------------------

Once the Service is running it can be accessed from the clients in the way
illustrated by the following code snippet:

.. code-block:: python
   
   from DIRAC.Core.DISET.RPCClient import RPCClient
   
   simpleMessageService = RPCClient('Framework/Hello')
   result = simpleMessageService.sayHello( 'you' )
   if not result['OK']:
     print "Error while calling the service:", result['Message'] #Here, in DIRAC, you better use the gLogger
   else:
     print result[ 'Value' ] #Here, in DIRAC, you better use the gLogger

     
Note that the service is always returning the result in the form of S_OK/S_ERROR structure. 


When should a service be developed?
-------------------------------------

Write a service every time you need to expose some information, that is usually stored in a database.

There are anyway cases for which it is not strictly needed to write a service, specifically when all the following are true:
- when you never need to expose the data written in the DB (i.e. the DB is, for the DIRAC point of view, Read-Only)
- when the components writing in it have local access. 

The advise is anyway to always write the service, because:
- if later on you'll need it, you won't need to change anything but the service itself
- db-independent logic should stay out of the database class itself.


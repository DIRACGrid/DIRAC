===========================
HTTPS Services with Tornado
===========================

.. contents::


************
Presentation
************
This page summarize changes between DISET and HTTPS. You can all also see these presentations:

- `Presentation of HTTPS in DIRAC  <https://docs.google.com/presentation/d/1t0hVpceXgV8W8R0ef5raMK3sUgXWnKdCmJUrG_5LsT4/edit?usp=sharing>`_.
- `Presentation of HTTPS migration <https://docs.google.com/presentation/d/1NZ8iKRv3c0OL1_RTXL21hP6YsAUXcKSCqDL2uhkf8Oc/edit?usp=sharing>`_.


*******
Service
*******

.. graphviz::

   digraph {
   TornadoServer -> YourServiceHandler [label=use];
   YourServiceHandler ->  TornadoService[label=inherit];
   

   TornadoServer  [shape=polygon,sides=4, label = "DIRAC.Core.Tornado.Server.TornadoServer"];
   TornadoService  [shape=polygon,sides=4, label = "DIRAC.Core.Tornado.Server.TornadoService"];
   YourServiceHandler  [shape=polygon,sides=4];

   }

Service returns to Client S_OK/S_ERROR encoded in JSON

*********************************************************
Important changes between DISET server and Tornado Server
*********************************************************

Internal structure
******************

- :py:class:`~DIRAC.Core.DISET.ServiceReactor` is now :py:class:`~DIRAC.Core.Tornado.Server.TornadoServer`
- :py:class:`~DIRAC.Core.DISET.private.Service` and :py:class:`~DIRAC.Core.DISET.RequestHandler` are now merge into :py:class:`~DIRAC.Core.Tornado.Server.TornadoService`
- CallStack from S_ERROR are deleted when they are returned to client.
- Common config for all services, there is no more specific config/service. But you can still give extra config files in the command line when you start a HTTPS server.
- Server returns HTTP status codes like ``200 OK`` or ``401 Forbidden``. Not used by client for now but open possibility for usage with external services (like a REST API)

How to write service
********************
Nothing better than example::

  from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
  class yourServiceHandler(TornadoService):

    @classmethod
    def initializeHandler(cls, infosDict):
      ## Called 1 time, at first request

    def initializeRequest(self):
      ## Called at each request

    auth_someMethod = ['all']
    def export_someMethod(self):
      ## Insert your method here, don't forget the return should be serializable
      ## Returned value may be an S_OK/S_ERROR
      ## You don't need to serialize in JSON, Tornado will do it

Write a service is similar in tornado and diset. You have to define your method starting with ``export_``, your initialization method is a class method called ``initializeHandler``
Main changes in tornado are:

- Service are initialized at first request
- You **should not** write method called ``initialize`` because Tornado already use it, so the ``initialize`` from diset handlers became ``initializeRequest``
- infosDict, arguments of initializedHandler is not really the same as one from diset, all things relative to transport are removed, to write on the transport you can use self.write() but I recommend to avoid his usage, Tornado will encode and write what you return.
- Variables likes ``types_yourMethod`` are ignored, but you can still define ``auth_yourMethod`` if you want.

Based on DISET request handler, you still have access to some getters in your handler, getters have the same names as DISET, which includes: 
``getCSOption``, ``getRemoteAddress``, ``getRemoteCredentials``, ``srv_getCSOption``, ``srv_getRemoteAddress``, ``srv_getRemoteCredentials``, ``srv_getFormattedRemoteCredentials``, ``srv_getServiceName`` and ``srv_getURL``.


How to start server
*******************
The easy way, use ``DIRAC/Core/Tornado/script/tornado-start-all.py`` it will start all services registered in configuration ! To register a service you just have to add the service in the CS and ``Protocol = https``. It may look like this::
  
  DIRAC
  {
    Setups
    {
      Tornado = DevInstance
    }
  }

  Systems {
    Tornado
    {
      DevInstance
      {
        Port = 443
      }
    }
    Framework
    {
      DevInstance
      {
        Services
        {
          DummyTornado
          {
            Protocol = https
          }
        }
      }
    }
  }


But you can also control more settings by launching tornado yourself::

  from DIRAC.Core.Tornado.Server.TornadoServer import TornadoServer
  serverToLaunch = TornadoServer(youroptions)
  serverToLaunch.startTornado()

Options availlable are:

- services, should be a list, to start only these services
- debugSSL, True or False, activate debug mode of Tornado (includes autoreload) and SSL, for extra logs use -ddd in the command line
- port, int, if you want to override value from config. If it's also not defined in config, it use 443.

This start method can bu usefull for developing new service or create starting script for a specific service, like the Configuration System (as master).

******
Client
******

.. graphviz::

   digraph {
   TornadoClient -> TornadoBaseClient [label=inherit]
   TornadoBaseClient -> Requests [label=use]

   TornadoClient  [shape=polygon,sides=4, label="DIRAC.Core.Tornado.Client.TornadoClient"];
   TornadoBaseClient  [shape=polygon,sides=4, label="DIRAC.Core.Tornado.Client.private.TornadoBaseClient"];
   Requests [shape=polygon,sides=4]
   }

This diagram present what is behind TornadoClient, but you should use :py:class:`DIRAC.Core.Base.Client` ! The new client integrate a selection system which select for you between HTTPS and DISET client. 

In your client module when you inherit from :py:class:`DIRAC.Core.Base.Client` you can define `httpsClient` with another client, it can be usefull when you can't serialize some data in JSON. Here the step to create and use a JSON patch:

- Create a class which inherit from :py:class:`~DIRAC.Core.Tornado.Client.TornadoClient`
- For every method who need a JSON patch create a method with the same name as the service
- Use self.executeRPC to send / receive datas

You can also see this example::

  class ConfigurationServerJSON(TornadoClient):
    """
      The specific client for configuration system.
      To avoid JSON limitation the HTTPS handler encode data in base64
      before sending them, this class only decode the base64
      An exception is made with CommitNewData wich ENCODE in base64
    """
    def getCompressedData(self):
      """
        Transmit request to service and get data in base64,
        it decode base64 before returning

        :returns str:Configuration data, compressed
      """
      retVal = self.executeRPC('getCompressedData')
      if retVal['OK']:
        retVal['Value'] = b64decode(retVal['Value'])
      return retVal




Behind :py:class:`~DIRAC.Core.Tornado.Client.TornadoClient` the `requests <http://docs.python-requests.org/>`_ library sends a HTTP POST request with:

- procedure: str with procedure name
- args: your arguments encoded in JSON
- clientVO: The VO of client
- extraCredentials: (if apply) Extra informations to authenticate client

Service is determined by server thanks to URL rooting, not with port like in DISET.

By default server listen on port 443, default port for HTTPS.


*****************************
Client / Service interactions
*****************************

.. image:: clientservice.png
    :align: center
    :alt: Client/Service interactions

*****************************************************
Important changes between TornadoClient and RPCClient
*****************************************************

Internal structure
******************

- :py:class:`~DIRAC.Core.DISET.private.innerRPCClient` and :py:class:`~DIRAC.Core.DISET.RPCClient` are now a single class: :py:class:`~DIRAC.Core.Tornado.Client.TornadoClient`. Interface and usage stay the same.
- :py:class:`~DIRAC.Core.Tornado.Client.private.TornadoBaseClient` is the new :py:class:`~DIRAC.Core.DISET.private.BaseClient`. Most of code is copied from :py:class:`~DIRAC.Core.DISET.private.BaseClient` but some method have been rewrited to use `Requests <http://docs.python-requests.org/>`_ instead of Transports. Code duplication is done to fully separate DISET and HTTPS but later, some parts can be merged by using a new common class between DISET and HTTPS (these parts are explicitly given in the docstrings).
- :py:class:`~DIRAC.Core.DISET.private.Transports.BaseTransport`, :py:class:`~DIRAC.Core.DISET.private.Transports.PlainTransport` and :py:class:`~DIRAC.Core.DISET.private.Transports.SSLTransport` are replaced by `Requests <http://docs.python-requests.org/>`_ 
- keepAliveLapse is removed from rpcStub returned by Client because `Requests <http://docs.python-requests.org/>`_  manage it himself.
- Due to JSON limitation you can write some specifics clients who inherit from :py:class:`~DIRAC.Core.Tornado.Client.TornadoClient`, there is a simple example with :py:class:`~DIRAC.Core.Tornado.Client.SpecificClient.ConfigurationClient` who transfer data in base64 to overcome JSON limitations


Connections and certificates
****************************
`Requests <http://docs.python-requests.org/>`_ library check more than DISET when reading certificates and do some stuff for us:

- Server certificate **must** have subject alternative names. Requests also check the hostname and you can have connection errors when using "localhost" for example. To avoid them add subject alternative name in certificate. (You can also see https://github.com/shazow/urllib3/issues/497 ).
- If server certificates are used by clients, you must add clientAuth in the extendedKeyUsage (requests also check that).
- In server side M2Crypto is used instead of GSI and conflict are possible between GSI and M2Crypto, to avoid them you can comment 4 lasts lines at ``DIRAC/Core/Security/__init__.py``
- ``_connect()``, ``_disconnect()`` and ``_purposeAction()`` are removed, ``_connect``/``_disconnect`` are now managed by `requests <http://docs.python-requests.org/>`_ and ``_purposeAction`` is no longer used is in HTTPS protocol. 



************
Launch tests
************

pytest
******
Because for now Tornado does not have "Real" services, you must use some fakes services to compare and test with DISET.
You need tornadoCredDict, diracCredDict, User, UserDirac to run tests. Each test explain how to configure in its docstring.

The only service available is the Configuration/Server, it will work with HTTPS and DISET services who needs to load configuration with a Configuration/Server.






**********************
How to install Tornado
**********************


To install and run service on Tornado you should install DIRAC first. You can install DIRAC in the standard way. But for now you don't need to configure it and generate certificates. You just have to install DIRAC::

  mkdir -p /opt/dirac
  useradd dirac
  chown dirac:dirac /opt/dirac
  su - dirac
  cd /opt/dirac
  curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py
  chmod +x dirac-install.py
  ./dirac-install.py -r v6r20p4 -t server

Once dirac installed you can add Tornado with the following steps:


Installing requirements
***********************
To install and compile some elements used by Tornado you may install some packages with ``yum``: ``python-devel``, ``m2crypto``,  ``gcc``. If you want to do some performance tests please check if ``nscd`` is running on your machine to avoid too many DNS query (on openstack, it is not enabled with SLC6).

Then you need to install Tornado and M2Crypto (for python), but not from official repo::

  pip --trusted-host=files.pythonhosted.org --trusted-host=pypi.org --trusted-host=pypi.python.org install  git+https://gitlab.com/chaen/m2crypto.git@tmpUntilSwigUpdated
  pip --trusted-host=files.pythonhosted.org --trusted-host=pypi.org --trusted-host=pypi.python.org install  git+https://github.com/chaen/tornado.git@iostreamConfigurable
  pip --trusted-host=files.pythonhosted.org --trusted-host=pypi.org --trusted-host=pypi.python.org install  git+https://github.com/chaen/tornado_m2crypto.git
  pip --trusted-host=files.pythonhosted.org --trusted-host=pypi.org --trusted-host=pypi.python.org install  -r /opt/dirac/DIRAC/requirements.txt


Adding Tornado to DIRAC
***********************

Save the DIRAC folder somewhere then clone my GithHub repo, then switch to branch "stage_toDIRAC_clean". You can run the setup.py if ``DIRAC.Core.Tornado`` is not detected by python::

  mv DIRAC DIRAC.old
  git clone https://github.com/louisjdmartin/DIRAC.git
  cd DIRAC
  git checkout stage_toDIRAC_clean
  python setup.py install




Generate Certificates
*********************
To use HTTPS your certificates must be generated using TLS standard, you can use following lines to generate them yourself::

  bash
  cd /tmp
  git clone https://github.com/chaen/DIRAC.git
  cd DIRAC
  git checkout rel-v6r20_FEAT_correctCA

  export DEVROOT=/tmp
  export SERVERINSTALLDIR=/opt/dirac/
  export CI_CONFIG=/tmp/DIRAC/tests/Jenkins/config/ci/

  source /tmp/DIRAC/tests/Jenkins/utilities.sh
  generateCA # automatic
  generateCertificates 365 # Certificates copied to /opt/dirac/etc/grid-security 
  generateUserCredentials 365 # Certificates generated at /opt/dirac/user -> copy to .globus and rename them userkey.pem and usercert.pem
  exit



Configuration (server)
**********************
Like in DISET, check your iptable to open some ports if needed !

Configuration is mostly the same as before, you just have to define ``Protocol`` to ``HTTPS`` inside the Services and add a new Section for tornado. You can use this example::

  LocalSite
  {
    Site = localhost
  }
  DIRAC
  {
    
    Setup = DeveloperSetup
    Setups
    {
      DeveloperSetup
      {
        Tornado = DevInstance
        Framework = DevInstance
      }
    }
    Security
    {
      UseServerCertificate=True
      CertFile = /opt/dirac/etc/grid-security/hostcert.pem
      KeyFile = /opt/dirac/etc/grid-security/hostkey.pem
    }
  }


  LocalInstallation
  {
    Setup = DeveloperSetup
  }


  Systems 
  {
    
    Tornado
    {
      DevInstance
      {
        
        Port = 4444 
      }
    }
    
    Framework
    {
      DevInstance
      {
        Databases
        {
          UserDB
          {
            Host = 127.0.0.1 #localhost
            User = root
            Password =
            DBName = dirac
          }
        }
        Services
        {
          User
          {
            # Use this handler to have a dummyService, can be used for testing without load a database
            #HandlerPath = DIRAC/FrameworkSystem/Service/DummyTornadoHandler.py
            Protocol = https
          }  
        }
      }
    }
  }
  Registry
  {
    # [Add your registry entry, like in DISET]
  }




Configuration (client)
**********************
Nothing change !
Define your URL as DIRAC service, but use https instead of dips::

  DIRAC
  { 
    Setup = DeveloperSetup
    Setups
    {
      DeveloperSetup
      {
        Framework = DevInstance
      }
    }
  }
  Systems
  {
    Framework
    {
      DevInstance
      {
        URLs
        {
          # DISET
          #User = dips://server:9135/Framework/User

          #TORNADO
          User = https://server:4444/Framework/User
        }
      }
    }
  }


Start the server
****************

To start the server you must define ``OPENSSL_ALLOW_PROXY_CERTS`` and run ``DIRAC/TornadoServices/Scripts/tornado-start-all.py`` (or ``tornado-start-CS.py`` if you try to run a configuration server)::

  OPENSSL_ALLOW_PROXY_CERTS=1 python /opt/dirac/DIRAC/TornadoServices/scripts/tornado-start-all.py


You can now run DIRAC services. You can check the docstring of tests file (``DIRAC/test/Integration/TornadoServices`` and ``DIRAC/TornadoServices/test``) to know how to run tests.



Run performance tests
*********************
For performance test unset ``PYTHONOPTIMIZE`` if it is set in your environement::

  unset PYTHONOPTIMIZE


Then you have to start some clients (adapt the port)::

  cd /opt/dirac/DIRAC/test/Integration/TornadoServices
  multimech-run perf-test-ping -p 9000 -b 0.0.0.0

Modify first lines of ``DIRAC/TornadoServices/test/multi-mechanize/distributed-test.py`` and ``DIRAC/TornadoServices/test/multi-mechanize/plot-distributed-test.py`` (follow instruction of each files)

On the server start ``DIRAC/test/Integration/TornadoServices/getCPUInfos`` (redirect output to a file)

Run ``distributed-test.py [NameOfYourTest]`` at the end of execution, the command to plot is given. Before executing command, copy output of ``getCPUInfos`` on ``/tmp/results.txt`` (on your local machine).
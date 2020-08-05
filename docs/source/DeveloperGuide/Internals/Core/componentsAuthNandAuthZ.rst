.. _compAuthNAndAutZ:

===========================================
Components authentication and authorization
===========================================

DIRAC components (services, agents and executors) by default use the certificate of the host onto which they run
for authentication and authorization purposes.

Components can be instructed to use a "shifter proxy" for authN and authZ of their service calls.
A shifter proxy is proxy certificate, which should be:

- specified in the "Operations/<setup>/Shifter" section of the CS
- uploaded to the ProxyManager (i.e. using "--upload" option of dirac-proxy-init)

Within an agent, in the "initialize" method, we can specify::

   self.am_setOption('shifterProxy', 'DataManager')

when used, the requested shifter's proxy will be added in the environment of the agent with simply::

   os.environ['X509_USER_PROXY'] = proxyDict['proxyFile']

and nothing else.

Which means that, still, each and every agent or service or executors by default will use the server certificate because,
e.g. in dirac-agent.py script we have::

   localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")

Which means that, if no further options are specified,
all the calls to services OUTSIDE of DIRAC will use the proxy in os.environ['X509_USER_PROXY'],
while for all internal communications the server certificate will be used.

If you want to use proxy certificate inside an agent for ALL service calls (inside AND outside of DIRAC) add::

    gConfigurationData.setOptionInCFG('/DIRAC/Security/UseServerCertificate', 'false')

in the initialize or in the execute (or use a CS option in the local .cfg file)

Two decorators are available for safely doing all that:

  * :py:func:`~DIRAC.Core.Utilities.Proxy.executeWithoutServerCertificate`
  * :py:func:`~DIRAC.Core.Utilities.Proxy.executeWithUserProxy`


================================
Authentication and authorization
================================
When a client calls a service, he needs to be identified. If a client opens a connection a :py:class:`~DIRAC.Core.DISET.private.Transports.BaseTransport` object is created then the service use the handshake to read certificates, extract informations and store them in a dictionary so you can use these informations easily. Here an example of possible dictionary::

   {
     'DN': '/C=ch/O=DIRAC/[...]',
     'group': 'devGroup',
     'CN': u'ciuser',
     'x509Chain': <X509Chain 2 certs [...][...]>,
     'isLimitedProxy': False,
     'isProxy': True
   }


When connection is opened and handshake is done, the service calls the :py:class:`~DIRAC.Core.DISET.AuthManager` and gave him this dictionary in argument to check the authorizations. More generally you can get this dictionary with :py:meth:`BaseTransport.getConnectingCredentials <DIRAC.Core.DISET.private.Transports.BaseTransport.BaseTransport.getConnectingCredentials>`.

***********
AuthManager
***********
AuthManager.authQuery() returns boolean so it is easy to use, you just have to provide a method you want to call, and credDic. It's easy to use but you have to instantiate correctly the AuthManager. For initialization you need the complete path of your service, to get it you may use the PathFinder::

   from DIRAC.ConfigurationSystem.Client import PathFinder
   from DIRAC.Core.DISET.AuthManager import AuthManager
   authManager = AuthManager( "%s/Authorization" % PathFinder.getServiceSection("Framework/someService") )
   authManager.authQuery( csAuthPath, credDict, hardcodedMethodAuth ) #return boolean
   # csAuthPath is the name of method for RPC or 'typeOfCall/method'
   # credDict came from BaseTransport.getConnectingCredentials()
   # hardcodedMethodAuth is optional

To determine if a query can be authorized or not the AuthManager extract valid properties for a given method.
First AuthManager try to get it from gConfig, then try to get it from hardcoded list (hardcodedMethodAuth) in your service and if nothing was found get default properties from gConfig.

AuthManager also extract properties from user with credential dictionary and configuration system to check if properties matches. So you don't have to extract properties by yourself, but if needed you can use :py:class:`DIRAC.Core.Security.CS.getPropertiesForGroup()`


.. _about_proxies:

=============
About proxies
=============


DIRAC uses X509 for authentication. Proxies are an extension to the traditional X509 certificate PKI infrastructure. For a detailed explanation, please see the RFC 3820.

Handling the proxies and certificates within DIRAC is done with the classes in :py:mod:`DIRAC.Core.Security`. Please look inside the various classes documentation for details.
These classes are used only for manipulating the objects and the information they contains. The use of the X509 entity for establishing connections is done directly with the underlying libraries (openssl)

One important mechanism is the delegation mechanism. This allows to give credentials to a remote entity, without every having a private key going through the network. This principle is used everywhere: when uploading a proxy to the proxyDB, when retrieving it, when submitting a transfer to FTS, when getting VOMS attributes, etc. The principle goes as follow:


1. The client tells the server that it wants to delegate.
2. The server generates a certificate *request* containing the public key, and the corresponding private key.
3. The server sends to the client the request (containing the public key)
4. The client signs the request using its own private key, and sets the subject of this new certificate as its own, appending some CN field (here the clients also decides the lifetime of this certificate!)
5. The client sends the signed certificate, appending its own certificate chain, back to the server
6. The server stores the new certificate together with the private key it has from before. This is now a full proxy

The "magic" happens when the storage (or any other endpoint needing a certificate) gets the proxy certificate. It then start following up as this

1. /DC=cern/CN=user/CN=proxy/CN=proxy signed by /DC=cern/CN=user/CN=proxy, do I have the signer's certificate?
2. Yes, it is part of the proxy chain. OK, /DC=cern/CN=user/CN=proxy is signed by /DC=cern/CN=user, do I have the signer's certificate?
3. Yes, it is part of the proxy chain. OK, /DC=cern/CN=user is signed by /DC=cern, do I have the signer's certificate?
4. Yes, it is a ROOT CA (/DC=cern) I know and trust, so the full chain can be trusted


Some proxy might be `limited`. a limited proxy has an extra flag set that, by convention, is checked by job submission services that, by convention, shall refuse limited proxies for further job submissions.

Such services shall accept regular proxies _and_ create limited delegations of those proxies that in turn will be used to equip the jobs.  A limited proxy cannot lose its limitation in further delegations.  All this machinery is needed to prevent that jobs can submit other jobs and thus create a job storm.  That is particularly important
to prevent such an abuse of stolen proxies.

Data management services shall simply ignore the flag.
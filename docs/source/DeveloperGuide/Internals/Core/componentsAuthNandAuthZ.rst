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

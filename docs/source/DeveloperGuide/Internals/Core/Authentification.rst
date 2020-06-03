================================
Authentication and authorization
================================
When a client calls a service, he needs to be identified. If a client opens a connection a :py:class:`~DIRAC.Core.DISET.private.Transports.BaseTransport` object is created then the service use the handshake to read certificates, extract informations and store them in a dictionnary so you can use these informations easily. Here an example of possible dictionnary::

	{
		'DN': '/C=ch/O=DIRAC/[...]',
		'group': 'devGroup',
		'CN': u'ciuser', 
		'x509Chain': <X509Chain 2 certs [...][...]>, 
		'isLimitedProxy': False, 
		'isProxy': True
	}


When connection is opened and handshake is done, the service calls the :py:class:`~DIRAC.Core.DISET.AuthManager` and gave him this dictionnary in argument to check the authorizations. More generally you can get this dictionnary with :py:meth:`BaseTransport.getConnectingCredentials <DIRAC.Core.DISET.private.Transports.BaseTransport.BaseTransport.getConnectingCredentials>`.


All procedure have a list of required properties and user may have at least one propertie to execute the procedure. Be careful, properties are associated with groups, not directly with users!


There is two main way to define required properties:

- "Hardcoded" way: Directly in the code, in your request handler you can write ```auth_yourMethodName = listOfProperties```. It can be useful for development or to provide default values.
- Via the configuration system at ```/DIRAC/Systems/(SystemName)/(InstanceName)/Services/(ServiceName)/Authorization/(methodName)```, if you have also define hardcoded properties, hardcoded properties will be ignored. (you can see the administrator guide for more informations)

A complete list of properties is available in the administrator guide.
If you don't want to define specific properties you can use "authenticated", "any" and "all".

- "authenticated" allow all users registered in the configuration system to use the procedure (```/DIRAC/Registry/Users```).
- "any" and "all" have the same effect, everyone can call the procedure. It can be dangerous if you allow non-secured connections.

You also have to define properties for groups of users in the configuration system at ```/DIRAC/Registry/Groups/(groupName)/Properties```.


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

AuthManager also extract properties from user with credential dictionnary and configuration system to check if properties matches. So you don't have to extract properties by yourself, but if needed you can use :py:class:`DIRAC.Core.Security.CS.getPropertiesForGroup()`


You can also read `Components authentication and authorization <./componentsAuthNandAuthZ.html>`_ for informations about client-side authentication.
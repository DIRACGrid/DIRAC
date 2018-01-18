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

   os.environ[ 'X509_USER_PROXY' ] = proxyDict[ 'proxyFile' ]

and nothing else.

Which means that, still, each and every agent or service or executors by default will use the server certificate because,
e.g. in dirac-agent.py script we have::

   localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )

Which means that, if no further options are specified,
all the calls to services OUTSIDE of DIRAC will use the proxy in os.environ[ 'X509_USER_PROXY' ],
while for all internal communications the server certificate will be used.

If you want to use proxy certificate inside an agent for ALL service calls (inside AND outside of DIRAC) add::

    gConfigurationData.setOptionInCFG('/DIRAC/Security/UseServerCertificate', 'false')

in the initialize or in the execute (or use a CS option in the local .cfg file)
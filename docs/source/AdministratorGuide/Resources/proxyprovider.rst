.. _resourcesProxyProvider:

==============
ProxyProvider
==============

This resource type provides an interface to obtain proxy certificates using a user identifier. The following proxy providers are presented here: ``DIRACCA``. When all users upload their proxies to proxy manager manually, you do not need to deploy these resources. The **/Registry/Users** section describes how to specify a proxy provifer for a user's DN.

----------------------
DIRACCA proxy provider
----------------------

ProxyProvider implementation for the proxy generation using local Certification Authority (CA) credentials. DIRACCA type of the proxy provider is a simple CA, its main purpose is to generate a proxy on the fly for DIRAC users who do not have a certificate registered in DIRAC registry. To use it needs to have CA certificate and key locally. Here are two ways to set up this type of proxy provider in the DIRAC configuration:

.. literalinclude:: /dirac.cfg
  :start-after: ## DIRACCA type:
  :end-before: ##
  :dedent: 2
  :caption: /Resources/ProxyProviders section


The Proxy provider supports the following distinguished names, `more details here <https://www.cryptosys.net/pki/manpki/pki_distnames.html>`_:

* SN(surname)
* GN(givenName)
* C(countryName)
* CN(commonName)
* L(localityName)
* Email(emailAddress)
* O(organizationName)
* OU(organizationUnitName)
* SP,ST(stateOrProvinceName)
* SERIALNUMBER(serialNumber)

Usage
^^^^^

The ProxyProvider is typically used by the ProxyManager to provide a proxy for a DIRAC user/group in the case the proxy in the proxyDB is expired or is absent.

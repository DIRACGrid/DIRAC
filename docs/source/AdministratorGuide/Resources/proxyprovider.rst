.. _resourcesProxyProvider:

==============
ProxyProvider
==============

This resource type provides an interface to obtain proxy certificates using a user identifier. The following proxy providers are presented here: ``DIRACCA``, ``PUSP``.


DIRACCA proxy provider
----------------------

ProxyProvider implementation for the proxy generation using local Certification Authority (CA) credentials. DIRACCA type of the proxy provider is a simple CA, its main purpose is to generate a proxy on the fly for DIRAC users who do not have a certificate registered in DIRAC registry. To use it need to have CA certificate and key locally. Here are two ways to set up this type of proxy provider in the DIRAC configuration:

Recommended way to set all required parameters in the DIRAC configuration::

    Resources
    {
      ProxyProviders
      {
        MY_DIRAC_CA
        {
          ProviderType = DIRACCA
          CertFile = /opt/dirac/pro/etc/grid-security/MY_DIRAC_CA.pem
          KeyFile = /opt/dirac/pro/etc/grid-security/MY_DIRAC_CA.key.pem
          Match = O, OU
          Supplied = CN
          Optional = emailAddress
          DNOrder = O, OU, CN, emailAddress
        }
      }
    }
  
Configuration options are:

* ``ProviderType``: main option, to show which proxy provider type you want to register. In the case of the DIRACCA proxy provider you must type ``DIRACCA``.
* ``CertFile``: the path to the CA certificate. This option is required.
* ``KeyFile``: the path to the CA key. This option is required.
* ``Match``: the distinguished name fields that must contain the exact same contents as that field in the CA's DN. If this parameter is not specified, the default value will be a empty list.
* ``Supplied``: the distinguished name fields list that must be present. If this parameter is not specified, the default value will be a ``CN``.
* ``Optional``: the distinguished name fields list that are allowed, but not required. If this parameter is not specified, the default value will be a ``C, O, OU, emailAddress``.
* ``DNOrder``: order of the distinguished name fields in a created user certificate. If this parameter is not specified, the default value will be a ``C, O, OU, CN, emailAddress``.
* ``<One of the supported distinguished names>``: to set default value for distinguished name field, for example.: ``OU = MY, DIRAC``.

Also, as an additional feature, this class can read properties from a simple openssl CA configuration file. To do this, just set the path to an existing openssl configuration file as a CAConfigFile parameter. Required parameters in the DIRAC configuration for its implementation::

    Resources
    {
      ProxyProviders
      {
        CFG_DIRAC_CA
        {
          ProviderType = DIRACCA
          CAConfigFile = /opt/dirac/pro/etc/openssl_config_ca.cnf
          DNOrder = O, OU, CN, emailAddress
      }
    }

Configuration options are:

* ``ProviderType``: main option, to show which proxy provider type you want to register. In the case of the DIRACCA proxy provider you must type ``DIRACCA``.
* ``CAConfigFile``: the path to the openssl configuration file. This is optional and not recomended to use. But if you choose to use this option, it is recommended to use a relatively simple configuration.
* ``DNOrder``: order of the distinguished name fields in a created user certificate. If this parameter is not specified, the distinguished names order in the created proxy will be the same as in the configuration file policy block.


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


PUSP proxy provider
-------------------

ProxyProvider implementation for a Per-User Sub-Proxy(PUSP) proxy generation using PUSP proxy server. `More details about PUSP here <https://wiki.egi.eu/wiki/Usage_of_the_per_user_sub_proxy_in_EGI>`_. Required parameters in the DIRAC configuration for its implementation::

    Resources
    {
      ProxyProviders
      {
        PUSP_Prov
        {
          ProviderType = PUSP
          ServiceURL = https://mypuspservice.url/
        }
      }
    }

Configuration options are:

* ``ProviderType``: main option, to show which proxy provider type you want to register. In the case of the PUSP proxy provider you must type ``PUSP``.
* ``ServiceURL``: PUSP service URL.


Usage
^^^^^

The ProxyProvider is typically used by the ProxyManager to provide a proxy for a DIRAC user/group in the case the proxy in the proxyDB is expired or is absent.

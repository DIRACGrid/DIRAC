.. _resourcesProxyProvider:

==============
ProxyProvider
==============

This resource type provide interface to obtain proxy certificate on the fly. The following proxy providers are represented here: ``DIRACCA``, ``PUSP``.


DIRACCA proxy provider
----------------------

ProxyProvider implementation for the proxy generation using local CA credentials. This class is a simple CA, it's main purpose is to generate a simple proxy on the fly for DIRAC users who do not have any certificate register. To use it need to have CA certificate and key localy. Required parameters in the DIRAC configuration for its implementation::

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
* ``CertFile``: the path to the CA certificate.
* ``KeyFile``: the path to the CA key.
* ``CAConfigFile``: the path to the openssl configuration file. This is optional and not recomended to use. But if you choose to use this option, it is recommended to use a relatively simple configuration.
* ``Match``: the distinguished name fields that must contain the exact same contents as that field in the CA's DN.
* ``Supplied``: the distinguished name fields list that must be present.
* ``Optional``: the distinguished name fields list that are allowed, but not required.
* ``DNOrder``: order of the distinguished name fields in a created user certificate.
* ``<One of the supported distinguished names>``: to set default value for distinguished name field, for example.: ``OU = MY, DIRAC``.

Also, as an additional feature, this class can read properties from a simple openssl CA configuration file. To do this, just set the path to an existing openssl configuration file as a CAConfigFile parameter. In this case, the distinguished names order in the created proxy will be the same as in the configuration file policy block.

The Proxy provider supports the following distinguished names (more details about distinguished names here: https://www.cryptosys.net/pki/manpki/pki_distnames.html):
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
----------------------

ProxyProvider implementation for a Per-User Sub-Proxy(PUSP) proxy generation using PUSP proxy server. More details about PUSP here: https://wiki.egi.eu/wiki/Usage_of_the_per_user_sub_proxy_in_EGI. Required parameters in the DIRAC configuration for its implementation::

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

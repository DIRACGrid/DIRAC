.. _resourcesProxyProviders:

Resources / ProxyProviders
==========================

In this section options for ProxyProviders can be set


Location for Parameters
-----------------------

Subsections are instead used to describe proxy providers:

  /Resources/ProxyProviders/<PROXY_PROVIDER_NAME>

Where `PROXY_PROVIDER_NAME` is the name of the proxy provider, like as "CheckInProxy" or "DIRAC_EOSC_CA".

General Parameters
------------------

These parameters are valid for all types of proxy providers

+---------------------------------+------------------------------------------------+-----------------------------------+
| **Name**                        | **Description**                                | **Example**                       |
+---------------------------------+------------------------------------------------+-----------------------------------+
| ProviderType                    | Type of proxy provider                         | OAuth2, DIRACCA, PUSP             |
+---------------------------------+------------------------------------------------+-----------------------------------+


OAuth2 Parameters
-----------------

+-------------------------+---------------------------------------------------+-------------------------------------------------------------------------------+
| **Name**                | **Description**                                   | **Example**                                                                   |
+-------------------------+---------------------------------------------------+-------------------------------------------------------------------------------+
| issuer                  | Authorization server's issuer identifier URL      | https://masterportal-pilot.aai.egi.eu/mp-oa2-server                           |
+-------------------------+---------------------------------------------------+-------------------------------------------------------------------------------+
| client_id               | Identifier of OAuth client                        | myproxy:oa4mp,2012:/client_id/aca7c8dfh439fewjb298fdb                         |
+-------------------------+---------------------------------------------------+-------------------------------------------------------------------------------+
| client_secret           | Secret key of OAuth client                        | ISh-Q32bkXRf-HD2hdh93d(#hd20DH2-wqedwiU@S22                                   |
+-------------------------+---------------------------------------------------+-------------------------------------------------------------------------------+
| <OAuth2 parameter>      | Some parameter specified in OAuth2 authorization  | prompt = consent                                                              |
|                         | framework(https://tools.ietf.org/html/rfc6749)    |                                                                               |
+-------------------------+---------------------------------------------------+-------------------------------------------------------------------------------+
| <specific parameter>    | Some specific parameter for specific proxy        | max_proxylifetime = 864000                                                    |
|                         | provider installation, e.g. CheckIns MasterPortal | proxy_endpoint = https://masterportal-pilot.aai.egi.eu/mp-oa2-server/getproxy |
+-------------------------+---------------------------------------------------+-------------------------------------------------------------------------------+


DIRACCA Parameters
------------------

+------------------------+----------------------------------------------------+--------------------------------------------------------+
| **Name**               | **Description**                                    |  **Example**                                           |
+------------------------+----------------------------------------------------+--------------------------------------------------------+
| CertFile               | Path to certificate file of CA                     | /opt/dirac/etc/grid-security/DIRACCA-EOSH/cert.pem     |
+------------------------+----------------------------------------------------+--------------------------------------------------------+
| KeyFile                | Path to certificate key file of CA                 | /opt/dirac/etc/grid-security/DIRACCA-EOSH/key.pem      |
+------------------------+----------------------------------------------------+--------------------------------------------------------+
| Match                  | the distinguished name fields that must contain    | O, OU                                                  |
|                        | the exact same contents as that field in the CA's  |                                                        |
|                        | DN. If this parameter is not specified, the default|                                                        |
|                        | value will be a empty list.                        |                                                        |
+------------------------+----------------------------------------------------+--------------------------------------------------------+
| Supplied               | the distinguished name fields list that must be    | C, CN                                                  |
|                        | present. If this parameter is not specified, the   |                                                        |
|                        | default value will be a "CN".                      |                                                        |
+------------------------+----------------------------------------------------+--------------------------------------------------------+
| Optional               | the distinguished name fields list that are        | emailAddress                                           |
|                        | allowed, but not required. If this parameter is not|                                                        |
|                        | specified, the default value will be a             |                                                        |
|                        | "C, O, OU, emailAddress".                          |                                                        |
+------------------------+----------------------------------------------------+--------------------------------------------------------+
| DNOrder                | order of the distinguished name fields in a created| C, O, OU, emailAddress, CN                             |
|                        | user certificate. If this parameter is not         |                                                        |
|                        | specified, the default value will be a             |                                                        |
|                        | "C, O, OU, CN, emailAddress".                      |                                                        |
+------------------------+----------------------------------------------------+--------------------------------------------------------+
| <DN attributes>        | DN attributes that will be use to create user proxy| C = FR, O = DIRAC, OU = DIRAC TEST                     |
+------------------------+----------------------------------------------------+--------------------------------------------------------+
| CAConfigFile           | the path to the openssl configuration file.        | /opt/dirac/pro/etc/openssl_config_ca.cnf               |
|                        | This is optional and not recomended to use. But if |                                                        |
|                        | you choose to use this option, it is recommended   |                                                        |
|                        | to use a relatively simple configuration.          |                                                        |
+------------------------+----------------------------------------------------+--------------------------------------------------------+


PUSP Parameters
------------------

+------------------------+----------------------------------------------------+--------------------------------------------------------+
| **Name**               | **Description**                                    |  **Example**                                           |
+------------------------+----------------------------------------------------+--------------------------------------------------------+
| ServiceURL             | PUSP service URL.                                  | https://mypuspserver.com/                              |
+------------------------+----------------------------------------------------+--------------------------------------------------------+

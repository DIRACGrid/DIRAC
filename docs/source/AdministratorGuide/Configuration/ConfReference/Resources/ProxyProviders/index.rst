.. _resourcesProxyProviders:

Resources / ProxyProviders
==========================

In this section options for ProxyProviders can be set


Location for Parameters
-----------------------

Subsections are instead used to describe proxy providers:

  /Resources/ProxyProviders/<ProxyProviderName>


General Parameters
------------------

These parameters are valid for all types of proxy providers

+---------------------------------+------------------------------------------------+-----------------------------------+
| **Name**                        | **Description**                                | **Example**                       |
+---------------------------------+------------------------------------------------+-----------------------------------+
| ProxyProviderName               | Name of proxy provider. It`s name of section   | CheckInProxy, DIRAC_EOSC_CA       |
+---------------------------------+------------------------------------------------+-----------------------------------+
| ProxyProviderType               | Type of proxy provider                         | OAuth2, DIRACCA, PUSP             |
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
| <DN attributes>        | DN attributes that will be use to create user proxy| C = FR, O = DIRAC, OU = DIRAC TEST                     |
+------------------------+----------------------------------------------------+--------------------------------------------------------+

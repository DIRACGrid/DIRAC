.. _manageAuthNAndAuthZ:

Manage Authentication and Authorizations
========================================

**************
Authentication
**************

For technical details see :ref:`about_proxies`.

DIRAC uses X509 certificates to identify clients and hosts, by conception X509 certificates are a very strong way to identify hosts and client thanks to asymetric cryptography. DIRAC is based on the openSSL library.

To identify users DIRAC use RBAC model (Role Based Access Control)

- A role (called property in DIRAC) carries some authorization
- A hostname has a DN and some properties
- A username has a DN, and the groups in which it is included
- A user group has a number of properties

Before authorize or not some tasks you have to define these properties, hostnames, usernames and groups. For that you may register informations at ``/DIRAC/Registry``. After registering users create a proxy with a group and this guarantees certain properties.

Bellow a simple example with only one user, one group and one host::

   Registry
   {
     Users
     {
       userName
       {
         DN = /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
         Email = youremail@yourprovider.com
       }
     }

     Groups
     {
       groupName
       {
         Users = userName
         Properties = CSAdministrator, JobAdministrator, ServiceAdministrator, ProxyDelegation, FullDelegation
       }
     }

     Hosts
     {
       hostName
       {
         DN = /C=ch/O=DIRAC/OU=DIRAC CI/CN=dirac.cern.ch/emailAddress=lhcb-dirac-ci@cern.ch
         Properties = CSAdministrator, JobAdministrator, ServiceAdministrator, ProxyDelegation, FullDelegation
       }
     }
   }


Users and their roles registered in a VOMS server can be synchronized to the DIRAC configuration using the
:mod:`~DIRAC.ConfigurationSystem.Agent.VOMS2CSAgent`.


**************
Authorizations
**************


All procedure have a list of required Properties and user may have at least one property to execute the procedure. Be careful, properties are associated with groups, not directly with users!



There are two main ways to define required properties:

- "Hardcoded" way: Directly in the code, in your request handler you can write ```auth_yourMethodName = listOfProperties```. It can be useful for development or to provide default values.
- Via the configuration system at ```/DIRAC/Systems/(SystemName)/(InstanceName)/Services/(ServiceName)/Authorization/(methodName)```, if you have also define hardcoded properties, hardcoded properties will be ignored.

A complete list of properties is available in :ref:`systemAuthorization`.
If you don't want to define specific properties you can use "authenticated", "any" and "all".

- "authenticated" allow all users registered in the configuration system to use the procedure (``/DIRAC/Registry/Users``).
- "any" and "all" have the same effect, everyone can call the procedure. It can be dangerous if you allow non-secured connections.

You also have to define properties for groups of users in the configuration system at ```/DIRAC/Registry/Groups/(groupName)/Properties```.

For a comprehensive list of Properties, see :py:mod:`~DIRAC.Core.Security.Properties`
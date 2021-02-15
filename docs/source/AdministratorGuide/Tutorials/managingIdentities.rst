.. _tuto_managing_identities:

===================
Managing identities
===================

.. set highlighting to console input/output
.. highlight:: console

Pre-requisite
=============

You should:

* have a machine setup as described in :ref:`tuto_basic_setup`
* be able to install dirac components


Tutorial goal
=============

Very quickly when using DIRAC, you will need to manage identities of people and their proxies. This is done with the ``ProxyManager`` service and with several configuration options.
In this tutorial, we will install the ``ProxyManager``, create a new group, and define some ``Shifter``.


Further reading
===============

* :ref:`compAuthNAndAutZ`
* :ref:`manageAuthNAndAuthZ`

Installing the ``ProxyManager``
===============================

This section is to be performed as ``diracuser`` with ``dirac_admin`` group proxy::

  [diracuser@dirac-tuto ~]$ source ~/DiracInstallation/bashrc
  [diracuser@dirac-tuto ~]$ dirac-proxy-init -g dirac_admin


The ``ProxyManager`` will host delegated proxies of the users. As any other service, it is very easy to install with the ``dirac-admin-sysadmin-cli``::

  [diracuser@dirac-tuto ~]$ dirac-admin-sysadmin-cli -H dirac-tuto

And then in the CLI::

  [dirac-tuto]$ install db ProxyDB
  MySQL root password:
  Adding to CS Framework/ProxyDB
  Database ProxyDB from DIRAC/FrameworkSystem installed successfully
  [dirac-tuto]$ install service Framework ProxyManager
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/FrameworkSystem/ConfigTemplate.cfg
  Adding to CS service Framework/ProxyManager
  service Framework_ProxyManager is installed, runit status: Run



.. note:: The ProxyDB contains sensitive information. For production environment, it is recommended that you keep this in a separate database with different credentials and strict access control.


Testing the ``ProxyManager``
============================

The simplest way to test it is to upload your user proxy::

  [diracuser@dirac-tuto ~]$ dirac-proxy-init
  Generating proxy...
  Uploading proxy for dirac_user...
  Proxy generated:
  subject      : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch/CN=6045995638
  issuer       : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
  identity     : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
  timeleft     : 23:59:59
  DIRAC group  : dirac_user
  rfc          : True
  path         : /tmp/x509up_u501
  username     : ciuser
  properties   : NormalUser

  Proxies uploaded:
  DN                                                                     | Group      | Until (GMT)
  /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch | dirac_user | 2020/04/09 14:43

As you can see, the ProxyDB now contains a delegated proxy for the ``ciuser`` with the group ``dirac_user``.

If you use a proxy with the ``ProxyManagement`` permission, like the ``dirac_admin`` group has, you can retrieve proxies stored in the DB::

  [diracuser@dirac-tuto ~]$ dirac-proxy-init -g dirac_admin
  Generating proxy...
  Proxy generated:
  subject      : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch/CN=5472309786
  issuer       : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
  identity     : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
  timeleft     : 23:59:59
  DIRAC group  : dirac_admin
  rfc          : True
  path         : /tmp/x509up_u501
  username     : ciuser
  properties   : AlarmsManagement, ServiceAdministrator, CSAdministrator, JobAdministrator, FullDelegation, ProxyManagement, Operator
  [diracuser@dirac-tuto ~]$ dirac-admin-get-proxy ciuser dirac_user
  Proxy downloaded to /home/diracuser/proxy.ciuser.dirac_user


Adding a new group
==================

Groups are useful to manage permissions and separate activities. For example, we will create a new group ``dirac_data``, and decide to use that group for all the data centrally managed.

Using the ``Configuration Manager`` application in the WebApp using the ``dirac_admin`` group, create a new section ``dirac_data`` in ``/Registry/Groups``::

  Users = ciuser
  Properties = NormalUser
  AutoUploadProxy = True

You should now be able to get a proxy belonging to the `dirac_data` group that will be automatically uploaded::

  [diracuser@dirac-tuto ~]$ dirac-proxy-init -g dirac_data
  Generating proxy...
  Uploading proxy for dirac_data...
  Proxy generated:
  subject      : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch/CN=6009266000
  issuer       : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
  identity     : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
  timeleft     : 23:59:59
  DIRAC group  : dirac_data
  rfc          : True
  path         : /tmp/x509up_u501
  username     : ciuser
  properties   : NormalUser

  Proxies uploaded:
  DN                                                                     | Group      | Until (GMT)
  /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch | dirac_data | 2020/04/09 14:43
  /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch | dirac_user | 2020/04/09 14:43


.. note:: if you get ``Unauthorized query ( 1111 : Unauthorized query)``, it means the ProxyManager has not yet updated its internal configuration. Just restart it to save time, or wait.


Adding a Shifter
================

``Shifter`` is basically a role, to which you associate a given proxy, for example ``DataManager`` (it could be anything). You can then tell your Components to use the ``DataManager`` identity to perform certain operations (at random: data management operations ? :-) ).

Using the ``Configuration Manager`` application in the WebApp, create a new section ``Shifter`` in ``/Operations/Defaults``::

  DataManager
  {
    User = ciuser
    Group = dirac_data
  }

You can now force any agent (don't, unless you know what you are doing) to use a proxy instead of the host certificate by specifying the ``shifterProxy`` option.

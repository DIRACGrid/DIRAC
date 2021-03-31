.. _tuto_install_rms:

=======================================
Installing the RequestManagement System
=======================================

.. set highlighting to console input/output
.. highlight:: console

Pre-requisite
=============

You should:

* have a machine setup as described in :ref:`tuto_basic_setup`
* be able to install dirac components
* have installed two DIRAC SE using the tutorial (:ref:`tuto_install_dirac_se`).
* have installed the DFC (:ref:`tuto_install_dfc`)
* have followed the tutorial on identity management (:ref:`tuto_managing_identities`)

Tutorial goal
=============

The aim of the tutorial is to install the RequestManagement system components and to use it to perform a simple replication of file.

More links
==========

More information can be found at the following places:

* :ref:`data-management-system`
* :ref:`requestManagementSystem`

Installing the RMS
==================

This section is to be executed as ``diracuser`` with a proxy with ``dirac_admin`` group.

The RMS needs the ``ReqManager`` service and the ``RequestExecutingAgent`` to work (you may want to add the ``CleanReqDBAgent`` if you scale...).

The RMS is no different than any other DIRAC system. The installation step are thus very simple::

  [diracuser@dirac-tuto ~]$ dirac-admin-sysadmin-cli --host dirac-tuto
  Pinging dirac-tuto...
  [dirac-tuto]$ add instance RequestManagement Production
  Adding RequestManagement system as Production self.instance for MyDIRAC-Production self.setup to dirac.cfg and CS
  RequestManagement system instance Production added successfully
  [dirac-tuto]$ restart *
  All systems are restarted, connection to SystemAdministrator is lost
  [dirac-tuto]$ install db ReqDB
  MySQL root password:
  Adding to CS RequestManagement/ReqDB
  Database ReqDB from DIRAC/RequestManagementSystem installed successfully
  [dirac-tuto]$ install service RequestManagement ReqManager
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/RequestManagementSystem/ConfigTemplate.cfg
  Adding to CS service RequestManagement/ReqManager
  service RequestManagement_ReqManager is installed, runit status: Run
  [dirac-tuto]$ install agent RequestManagement RequestExecutingAgent
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/RequestManagementSystem/ConfigTemplate.cfg
  Adding to CS agent RequestManagement/RequestExecutingAgent
  agent RequestManagement_RequestExecutingAgent is installed, runit status: Run
  [dirac-tuto]$ quit


By default, the installation of the ``RequestExecutingAgent`` will configure it with a whole bunch of default Operations possible. You can see that in the Agent configuration in ``/Systems/RequestManagement/Production/Agents/RequestExecutingAgent/OperationHandlers``


Testing the RMS
===============

This section is to be executed with a proxy with `dirac_data` group.

The test we are going to do consists in transferring a file from one storage element to another, using the RequestExecutingAgent.

First, let's add a file::

  [diracuser@dirac-tuto ~]$ echo "My Test File" > /tmp/myTestFile.txt
  [diracuser@dirac-tuto ~]$ dirac-dms-add-file /tutoVO/user/c/ciuser/myTestFile.txt /tmp/myTestFile.txt StorageElementOne

  Uploading /tutoVO/user/c/ciuser/myTestFile.txt
  Successfully uploaded file to StorageElementOne


We can see that our file is indeed in the ``StorageElementOne``::

  [diracuser@dirac-tuto ~]$ dirac-dms-lfn-replicas /tutoVO/user/c/ciuser/myTestFile.txt
  LFN                                  StorageElement    URL
  ==========================================================
  /tutoVO/user/c/ciuser/myTestFile.txt StorageElementOne dips://dirac-tuto:9148/DataManagement/StorageElement/tutoVO/user/c/ciuser/myTestFile.txt

Let's replicate it to ``StorageElementTwo`` using the RMS::

  [diracuser@dirac-tuto ~]$ dirac-dms-replicate-and-register-request myFirstRequest /tutoVO/user/c/ciuser/myTestFile.txt StorageElementTwo
  Request 'myFirstRequest' has been put to ReqDB for execution.
  RequestID(s): 8
  You can monitor requests' status using command: 'dirac-rms-request <requestName/ID>'


The Request has a name (``myFirstRequest``) that we chose, but also an ID, returned by the system (here ``8``). The ID is guaranteed to be unique, while the name is not, so it is recommended to use the ID when you interact with the RMS. You can see the status of your Request, using its name or ID::

  [diracuser@dirac-tuto ~]$ dirac-rms-request myFirstRequest
  Request name='myFirstRequest' ID=8 Status='Waiting'
  Created 2019-04-23 14:37:05, Updated 2019-04-23 14:37:05, NotBefore 2019-04-23 14:37:05
  Owner: '/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser', Group: dirac_data
    [0] Operation Type='ReplicateAndRegister' ID=8 Order=0 Status='Waiting'
        TargetSE: StorageElementTwo - Created 2019-04-23 14:37:05, Updated 2019-04-23 14:37:05
      [01] ID=2 LFN='/tutoVO/user/c/ciuser/myTestFile.txt' Status='Waiting' Checksum='1e750431'

  [diracuser@dirac-tuto ~]$ dirac-rms-request 8
  Request name='myFirstRequest' ID=8 Status='Waiting'
  Created 2019-04-23 14:37:05, Updated 2019-04-23 14:37:05, NotBefore 2019-04-23 14:37:05
  Owner: '/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser', Group: dirac_data
    [0] Operation Type='ReplicateAndRegister' ID=8 Order=0 Status='Waiting'
        TargetSE: StorageElementTwo - Created 2019-04-23 14:37:05, Updated 2019-04-23 14:37:05
      [01] ID=2 LFN='/tutoVO/user/c/ciuser/myTestFile.txt' Status='Waiting' Checksum='1e750431'


You can here clearly see that the Request consists of one ``ReplicateAndRegister`` operation (which does what it says) targeting the LFN ``/tutoVO/user/c/ciuser/myTestFile.txt``. The RequestExecutingAgent will pick up the request and execute it. And shortly you should be able to see it done::

  [diracuser@dirac-tuto ~]$ dirac-rms-request 8
  Request name='myFirstRequest' ID=8 Status='Done'
  Created 2019-04-23 14:37:05, Updated 2019-04-23 14:37:29, NotBefore 2019-04-23 14:37:05
  Owner: '/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser', Group: dirac_data
    [0] Operation Type='ReplicateAndRegister' ID=8 Order=0 Status='Done'
        TargetSE: StorageElementTwo - Created 2019-04-23 14:37:05, Updated 2019-04-23 14:37:29
      [01] ID=2 LFN='/tutoVO/user/c/ciuser/myTestFile.txt' Status='Done' Checksum='1e750431'

  [diracuser@dirac-tuto ~]$ dirac-dms-lfn-replicas /tutoVO/user/c/ciuser/myTestFile.txt
  LFN                                  StorageElement    URL
  ==========================================================
  /tutoVO/user/c/ciuser/myTestFile.txt StorageElementTwo dips://dirac-tuto:9147/DataManagement/StorageElementTwo/tutoVO/user/c/ciuser/myTestFile.txt
                                      StorageElementOne dips://dirac-tuto:9148/DataManagement/StorageElement/tutoVO/user/c/ciuser/myTestFile.txt


Conclusion
==========

You now have an RMS in place, which is the base for all the asynchronous operations in DIRAC. This is used for big scale operations, failover, or even more !

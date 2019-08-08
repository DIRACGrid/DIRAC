.. _tuto_install_dirac_se:

===============================
Install a DIRAC Storage Element
===============================

.. set highlighting to console input/output
.. highlight:: console

Pre-requisite
=============

You should have a machine setup as described in :ref:`tuto_basic_setup`, and be able to install dirac components. For simple interaction with the StorageElement using ``dirac-dms-*`` commands, you should also have a working FileCatalog.


Tutorial goal
=============

The aim of the tutorial is to do a step by step guide to install a DIRAC StorageElement. By the end of the tutorial, you will be able to have a fully functional storage element that can be addressed like any other storage.


More links
==========

* :ref:`resourcesStorageElement`
* StorageElement service documentation: :py:mod:`DIRAC.DataManagementSystem.Service.StorageElementHandler`


Machine setup
=============

This section is to be executed as ``dirac`` user.

We will simply create a folder where the files will be stored::

  [dirac@dirac-tuto ~]$ mkdir /opt/dirac/storageElementOne/


Installing the service
======================

This section is to be executed as ``diracuser`` user, with ``dirac_admin`` proxy (reminder: ``dirac-proxy-init -g dirac_admin``).

Install the StorageElement service using ``dirac-admin-sysadmin-cli``. First we add the *DataManagement* system to the
*Production* instance on the configuration, then we *restart* all services so they pick up the new configuration, and finally we install the
``StorageElement`` pointing to the ``BasePath`` we created earlier::

  [diracuser@dirac-tuto ~]$ dirac-admin-sysadmin-cli --host dirac-tuto
  Pinging dirac-tuto...
  [dirac-tuto]$ add instance DataManagement Production
  Adding DataManagement system as Production self.instance for MyDIRAC-Production self.setup to dirac.cfg and CS
  DataManagement system instance Production added successfully
  [dirac-tuto]$ restart *
  All systems are restarted, connection to SystemAdministrator is lost
  [dirac-tuto]$ install service DataManagement StorageElement -p BasePath=/opt/dirac/storageElementOne/
  Loading configuration template /home/diracuser/DIRAC/DIRAC/DataManagementSystem/ConfigTemplate.cfg
  Adding to CS service DataManagement/StorageElement
  service DataManagement_StorageElement is installed, runit status: Run
  [dirac-tuto]$ quit

You now have a *Service* offering grid like storage. However, you still need to declare a *StorageElement* for it to be usable within DIRAC.


Adding the StorageElement
=========================

You need to add a StorageElement in the ``Resources/StorageElements`` section (create the sections if necessary).  Using the WebApp, just add the following::

  StorageElementOne
  {
    BackendType = DISET
    DIP
    {
      Host = dirac-tuto
      Port = 9148
      Protocol = dips
      Path = /DataManagement/StorageElement
      Access = remote
    }
  }


You now have a storage element that you can address as ``StorageElementOne`` in all the dirac commands or in your code.


Test it
=======

Create a dummy file::

  [diracuser@dirac-tuto ~]$ echo "dummyFile" > /tmp/dummy.txt

Now create a file called ``/tmp/testSE.py``, with the following content

.. code-block:: python

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  localFile = '/tmp/dummy.txt'
  lfn = '/tutoVO/myFirstFile.txt'

  from DIRAC.Resources.Storage.StorageElement import StorageElement


  se = StorageElement('StorageElementOne')

  print "Putting file"
  print se.putFile({lfn: localFile})

  print "Listing directory"
  print se.listDirectory('/tutoVO')

  print "Getting file"
  print se.getFile(lfn, '/tmp/')

  print "Removing file"
  print se.removeFile(lfn)

  print "Listing directory"
  print se.listDirectory('/tutoVO')




This script uploads ``/tmp/dummy.txt`` on the StorageElement as ``myFirstFile.txt``, list the directory, downloads the uploaded file and removes it from the StorageElement. The output should be something like that::

  [diracuser@dirac-tuto ~]$ python /tmp/testSE.py
  Putting file
  {'OK': True, 'Value': {'Successful': {'/tutoVO/myFirstFile.txt': 10}, 'Failed': {}}}
  Listing directory
  {'OK': True, 'Value': {'Successful': {'/tutoVO': {'Files': {'myFirstFile.txt': {'Accessible': True, 'Migrated': 0, 'Unavailable': 0, 'Lost': 0, 'Exists': True, 'Cached': 1, 'Checksum': '166203b7', 'Mode': 420, 'File': True, 'Directory': True, 'TimeStamps': (1555342476, 1555342476, 1555342476), 'Type': 'File', 'Size': 10}}, 'SubDirs': {}}}, 'Failed': {}}}
  Getting file
  {'OK': True, 'Value': {'Successful': {'/tutoVO/myFirstFile.txt': 10}, 'Failed': {}}}
  Removing file
  {'OK': True, 'Value': {'Successful': {'/tutoVO/myFirstFile.txt': True}, 'Failed': {}}}
  Listing directory
  {'OK': True, 'Value': {'Successful': {'/tutoVO': {'Files': {}, 'SubDirs': {}}}, 'Failed': {}}}

The list of files within ``tmp`` should also contain ``dummy.txt`` as well as ``myFirstFile.txt``.

Adding a second DIRAC SE
========================

It is often interesting to have a second SE.

As ``dirac`` user, create a new directory::

 [dirac@dirac-tuto ~]$  mkdir /opt/dirac/storageElementTwo/

Now the rest is to be installed with ``diracuser`` and a proxy with ``dirac_admin`` group.

We need another StorageElement service. However, it has to have a different *name*, *Port* and *BasePath* than the first one, so we will just call this service ``StorageElementTwo``::

  [diracuser@dirac-tuto ~]$ dirac-admin-sysadmin-cli --host dirac-tuto
  Pinging dirac-tuto...
  [dirac-tuto]$ install service DataManagement StorageElementTwo -m StorageElement -p Port=9147 -p BasePath=/opt/dirac/storageElementTwo/
  Loading configuration template /home/diracuser/DIRAC/DIRAC/DataManagementSystem/ConfigTemplate.cfg
  Adding to CS service DataManagement/StorageElementTwo
  service DataManagement_StorageElementTwo is installed, runit status: Run


Using the WebApp, add the new StorageElement definition in the ``/Resources/StorageElements`` section::

  StorageElementTwo
  {
    BackendType = DISET
    DIP
    {
      Host = dirac-tuto
      Port = 9147
      Protocol = dips
      Path = /DataManagement/StorageElementTwo
      Access = remote
    }
  }


In order to test it, just re-use ``/tmp/testSE.py``, replacing ``StorageElementOne`` with ``StorageElementTwo``

.. _tuto_install_ts:

===================================
Installing the TransformationSystem
===================================

.. set highlighting to console input/output
.. highlight:: console

Pre-Requisite
=============

You should:

* have a machine setup as described in :ref:`tuto_basic_setup`
* have installed two DIRAC SE using the tutorial (:ref:`tuto_install_dirac_se`).
* have installed the DFC using the tutorial (:ref:`tuto_install_dfc`).
* have followed the tutorial on identity management (:ref:`tuto_managing_identities`)
* have installed the RMS using the tutorial (:ref:`tuto_install_rms`)


Tutorial Goal
=============

The aim of the tutorial is to install the Transformation system components and to use it to perform an automatic replication.


More Links
==========

* :ref:`adminTS`
* See the options for services and agents in code documentation of the :mod:`~DIRAC.TransformationSystem`

Installing the TransformationSystem
===================================

.. highlight:: console

This section is to be executed as ``diracuser`` with a proxy int the ``dirac_admin`` group.

The Transformation System(TS) needs the ``TransformationManager`` service and the ``TransformationAgent``, ``InputDataAgent``,
``RequestTaskAgent`` to work for data management purposes. The ``WorkflowTaskAgent`` is needed to submit jobs.
Finally the ``TransformationCleaning`` cleans up if transformations are finished. As the agents need to run one after
the other we set the PollingTime to 30 seconds to reduce the waiting time once we create transformations.

The TS is no different than any other DIRAC system. The installation steps are thus very simple::

  [diracuser@dirac-tuto ~]$ dirac-admin-sysadmin-cli --host dirac-tuto
  Pinging dirac-tuto...
  [dirac-tuto]$ add instance Transformation Production
  Adding Transformation system as Production self.instance for MyDIRAC-Production self.setup to dirac.cfg and CS
  Transformation system instance Production added successfully
  [dirac-tuto]$ restart *
  All systems are restarted, connection to SystemAdministrator is lost
  [dirac-tuto]$ install db TransformationDB
  MySQL root password:
  Adding to CS Transformation/TransformationDB
  Database TransformationDB from DIRAC/TransformationSystem installed successfully
  [dirac-tuto]$ install service Transformation TransformationManager
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/TransformationSystem/ConfigTemplate.cfg
  Adding to CS service Transformation/TransformationManager
  service Transformation_TransformationManager is installed, runit status: Run
  [dirac-tuto]$ install agent Transformation TransformationAgent -p PollingTime=30
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/TransformationSystem/ConfigTemplate.cfg
  Adding to CS agent Transformation/TransformationAgent
  agent Transformation_TransformationAgent is installed, runit status: Run
  [dirac-tuto]$ install agent Transformation InputDataAgent -p PollingTime=30
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/TransformationSystem/ConfigTemplate.cfg
  Adding to CS agent Transformation/InputDataAgent
  agent Transformation_InputDataAgent is installed, runit status: Run
  [dirac-tuto]$ install agent Transformation WorkflowTaskAgent -p PollingTime=30 -p MonitorTasks=True -p MonitorFiles=True
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/TransformationSystem/ConfigTemplate.cfg
  Adding to CS agent Transformation/WorkflowTaskAgent
  agent Transformation_WorkflowTaskAgent is installed, runit status: Run
  [dirac-tuto]$ install agent Transformation RequestTaskAgent -p PollingTime=30 -p MonitorTasks=True -p MonitorFiles=True
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/TransformationSystem/ConfigTemplate.cfg
  Adding to CS agent Transformation/RequestTaskAgent
  agent Transformation_RequestTaskAgent is installed, runit status: Run

Add a ProductionManagement Group
================================

We create a new group ``dirac_prod``, which will be used to manage transformations

Using the ``Configuration Manager`` application in the WebApp, create a new section ``dirac_prod`` in ``/Registry/Groups``::

    Users = ciuser
    Properties = ProductionManagement, NormalUser
    AutoUploadProxy = True


After restarting the ``ProxyManager``, you should now be able to get a proxy belonging to the ``dirac_prod`` group that
will be automatically uploaded.

The ``ProductionManagement`` property allows users in the group to access and change all transformations. There is also
a ``ProductionSharing`` property to only allow access to transformations in the same group and ``ProductionUser`` to
only allow users to access their own transformations.

Add a ProdManager Shifter
=========================

Using the ``Configuration Manager`` application in the WebApp, create a new shifter ``ProdManager`` in the
``/Operations/Defaults/Shifter`` section::

  ProdManager
  {
    User = ciuser
    Group = dirac_prod
  }



Add a Sites which the StorageElements belong to
===============================================

Using the ``Configuration Manager`` application in the WebApp, create a new section ``Sites`` in ``/Resources``, which
contains a *Grid* with two *Sites*, to which the two SEs are associated::

  Sites
  {
    MyGrid
    {
      MyGrid.Site1.uk
      {
        SE = StorageElementOne
      }
      MyGrid.Site2.de
      {
        SE = StorageElementTwo
      }
    }
  }


Conclusion
==========

You now have a Transformation System in place, which is the base for all automatic operations in DIRAC.

.. _fts3:

---------------------
FTS3 support in DIRAC
---------------------

.. versionadded:: v6r20

.. contents:: Table of contents
   :depth: 2


DIRAC DMS can be configured to make use of FTS3 servers in order to schedule and monitor efficient transfer of large amounts of data between SEs. Please see :ref:`multiProtocol` for more details about the protocol used for transfers.

The transfers using FTS come from the RequestManagementSystem ( see :ref:`requestManagementSystem`). It will receive the files to transfer, as well as the list of destinations. If no source is defined, it will choose one. The files will then be grouped together and submited as jobs to the fts servers. These jobs will be monitored, retried if needed, the new replicas will be registered, and the status of the files will be reported back to the RMS.

There are no direct submission possible to the FTS system, it has to go through the RMS.

This system is independent from the previous FTS system, and is **totally incompatible with it. Both systems cannot run at the same time**.

To go from the old one, you must wait until there are no more Scheduled requests in the RequestManagementSystem (RMS). For that, either you do not submit any transfer for a while (probably not possible), or you switch to transfers using the DataManager. Once you have processed all the Schedule request, you can enable the new FTS3 system.



FTS3 Installation
-----------------

One needs to install an FTS3DB, the FTS3Manager, and the FTS3Agent. Install the
FTS3DB with `dirac-install-db` or directly on your mysql server and add the
Database in the Configuration System::

  dirac-admin-sysadmin-cli -H diracserver034.institute.tld
  > install service DataManagement FTS3Manager
  > install agent DataManagement FTS3Agent


===============================
Enable FTS transfers in the RMS
===============================

In order for the transfers to be submitted to the FTS system, the following options in the configuration section ``Systems/RequestManagement/Agents/RequestExecutingAgent/OperationHandlers/ReplicateAndRegister/`` need to be set:

   * ``FTSMode`` must be True
   * ``FTSBannedGroups`` should contain the list of groups for which you'd rather do direct transfers.
   * ``UseNewFTS3`` should be True in order to use this new FTS system (soon to be deprecated)

========================
Operations configuration
========================

  * DataManagement/FTSVersion: FTS2/FTS3. Set it to FTS3...
  * DataManagement/FTSPlacement/FTS3/ServerPolicy: Policy to choose the FTS server see `FTSServer policy`_.
  * DataManagement/FTSPlacement/FTS3/FTS3Plugin: Plugin to alter the behavior of the FTS3Agent


======================
FTS servers definition
======================

The servers to be used are defined in the ``Resources/FTSEndpoints/FTS3`` section. Example:

.. code-block:: python

    CERN-FTS3 = https://fts3.cern.ch:8446
    RAL-FTS3 = https://lcgfts3.gridpp.rl.ac.uk:8446

The option name is just the server name as used internaly. Note that the port number has to be specified, and should correspond to the REST interface


FTS3Agent
---------

This agent is in charge of performing and monitoring all the transfers. Note that this agent can be duplicated as many time as you wish.

See: :py:mod:`~DIRAC.DataManagementSystem.Agent.FTS3Agent` for configuration details.

FTS3 system overview
--------------------

There are two possible tasks that can be done with the FTS3 system: transferring and staging.

Each of these task is performed by a dedicated ``FTS3Operation``: ``FTS3TransferOperation`` and ``FTS3StagingOperation``. These ``FTS3Operation`` contain a list of ``FTS3File``. An ``FTS3File`` is for a specific targetSE. The ``FTS3Agent`` will take an ``FTS3Operation``, group the files following some criteria (see later) into ``FTS3Jobs``. These ``FTS3Jobs`` will then be submitted to the FTS3 servers to become real FTS3 jobs. These Jobs are regularly monitored by the ``FTS3Agent``. When all the ``FTS3Files`` have reached a final status, the ``FTS3Operation`` callback method is called. This callback method depends on the type of ``FTS3Operation``.

Note that by default, the ``FTS3Agent`` is meant to run without shifter proxy. It will however download the proxy of the user submitting the job in order to delegate it to FTS. This also means that it is not able to perform registration in the DFC, and relies on Operation callback for that.


=====================
FTS3TransferOperation
=====================

The RMS will create one FTS3TransferOperation per RMS Operation, and one FTS3File per RMS File. This means that there can be several destination SEs, and potentially source SEs specified.

The grouping into jobs is done following this logic:
    * Group by target SE
    * Group by source SE. If not specified, we take the active replicas as returned by the DataManager
    * Since there might be several possible source SEs, we need to pick one only. By default, the choice is random, but this can be changed (see FTS3Plugins)
    * Divide all that according to the maximum number of files we want per job

Once the FTS jobs have been executed, and all the operation is completed, the callback takes place. The callback consists in fetching the RMS request which submitted the FTS3Operation, update the status of the RMS files, and insert a Registration Operation.
Note that since the multiple targets are grouped in a single RMS operation, failing to transfer one file to one destination will result in the failure of the Operation. However, there is one Registration operation per target, and hence correctly transferred files will be registered.

====================
FTS3StagingOperation
====================

.. warning ::

   Still in development, not meant to be used

This operation is meant to perform BringOnline. The idea behind that is to replace, if deemed working, the whole StorageSystem of DIRAC.

FTSServer policy
----------------

The FTS server to which the job is sent is chose based on the policy. There are 3 possible policy:

  * Random: the default. makes a random choice
  * Failover: pick one, and stay on that one until it fails
  * Sequence: take them in turn, always change


FTS3 state machines
-------------------

The states for the ``FTS`` objects are as follow:

.. literalinclude:: /../../src/DIRAC/DataManagementSystem/Client/FTS3Operation.py
   :language: python
   :start-after: # START states
   :end-before: # END states
   :caption: FTS3Operation states

.. literalinclude:: /../../src/DIRAC/DataManagementSystem/Client/FTS3Job.py
   :language: python
   :start-after: # START states
   :end-before: # END states
   :caption: FTS3Job states

.. literalinclude:: /../../src/DIRAC/DataManagementSystem/Client/FTS3File.py
   :language: python
   :start-after: # START states
   :end-before: # END states
   :caption: FTS3File states

The status of the FTS3Jobs and FTSFiles are updated every time we monitor the matching job.

The FTS3Operation goes to ``Processed`` when all the files are in a final state, and to ``Finished`` when the callback has been called successfully


FTS3 Plugins
------------

.. versionadded:: v7r1p37
    The ``FTS3Plugin`` option


The ``FTS3Plugin`` option allows one to specify a plugin to alter some default choices made by the FTS3 system. These choices concern:

   * the list of third party protocols used
   * the selection of a source storage element
   * the FTS activity used
   * The multihop strategy

This can be useful if you want to implement a matrix-like selection of protocols, or if some links require specific protocols, etc. The plugins must be placed in :py:mod:`DIRAC.DataManagementSystem.private.FTS3Plugins`. The default behaviors, as well as the documentation on how to implement your own plugin can be found in :py:mod:`DIRAC.DataManagementSystem.private.FTS3Plugins.DefaultFTS3Plugin`


MultiHop support
----------------

.. versionadded:: v7r3p21

.. |trade|   unicode:: U+2122

.. warning::
   Experimental feature with drawbacks, see below

In the 2020s, the grid world made a leap forward as hardly ever before. In order to make the best use of all the years of experience in manipulating common protocols and fully using the dedicated network to interconnect all sites and storages, WLCG blessed the following ThirdPartyCopy strategy for the years to come:

* incompatible protocols between storages are okay
* storages not reachable from other site storages are okay

They named this strategy *MultiHop* |trade|. It consists of turning a transfer ``A -> B`` into ``A -> I1 -> I2 -> ... -> In -> B``.

DIRAC supports one intermediate hop (note: not that it would be very hard to implement any number, but having to implement that feature to transfer between two WLCG sites is already afflicting enough, so let's not overdo it). The choice of the intermediate SE is done in the FTS3Plugin, so feel free to customize it (see above).

The configuration of multihop is done in the form of a ``[source][destination]`` matrix in ``Operations/<vo/setup>/DataManagement/MultiHopMatrixOfShame``. You can:

   * Use specific SE Names
   * Use ``BaseSE`` names
   * Use ``Default`` as a *any other* catch all
   * Use ``disabled`` as a value to disable multihop for a given route

More details on how the intermediate SE selection is done and how the matrix is defined are available in :py:meth:`DIRAC.DataManagementSystem.private.FTS3Plugins.DefaultFTS3Plugin.DefaultFTS3Plugin.findMultiHopSEToCoverUpForWLCGFailure`

.. warning::
   If you initiate transfer from A to C that has to go via B but the file is already at B (and registered in the FileCatalog), the multihop will not know about it (with all the danger that comes with this). To avoid that, we recommend defining dedicated SE for intermediate hop

.. warning::
   This creates dark data !! As of today, the intermediate file is not cleanup, neither by DIRAC nor by FTS.
   Work in FTS has a `task <https://its.cern.ch/jira/projects/FTS/issues/FTS-1755>`_ to try and bring that feature in.
   A future solution may come from DIRAC.
   In the meantime, the best solution is to ask the site to either cleanup themselves (some storages like EOS have that built in) or to give you a dump of the namespace, and then do the cleaning yourself.


Token support
----------------

.. versionadded:: v8.0.51

.. warning::
   Very experimental feature


The current state is the one in which LHCb ran the DC24 challenge. It only worked for dCache site, as there is still not a uniform way for storages to understand permissions...
A transfer will happen with token if:

   * ``UseTokens`` is true in the FTSAgent configuration
   * ``WLCGTokenBasePath`` is set for both the source and the destination

The tokens use specific file path, and not generic wildcard permissions.

.. warning::
   Token support is as experimental as can be in any layer of the stack (DIRAC, storage, FTS... even the model is experimental)

.. warning::
   The FTS3Agent got occasionaly stuck when tokens were used

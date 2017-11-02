---------------------
FTS3 support in DIRAC
---------------------

.. versionadded:: v6r20

.. contents:: Table of contents
   :depth: 2

DIRAC comes with a system optimized to interact with the FTS3 servers.

This system is independent from the previous FTS system, and is **totally incompatible with it. Both systems cannot run at the same time**.

To go from the old one, you must wait until there are no more Scheduled requests in the RequestManagementSystem (RMS). For that, either you do not submit any transfer for a while (probably not possible), or you switch to transfers using the DataManager. Once you have processed all the Schedule request, you can enable the new FTS3 system.


FTS3 system overview
--------------------

There are two possible tasks that can be done with the FTS3 system: transferring and staging.

Each of these task is performed by a dedicated FTS3Operation: *FTS3TransferOperation* and *FTS3StagingOperation*.
These FTS3Operation contain a list of FTS3File. An FTS3File is for a specific targetSE. The FTS3Agent will take an FTS3Operation, group the files following some criteria (see later) into FTS3Jobs. These FTS3Jobs will then be submitted to the FTS3 servers to become real FTS3 jobs. These Jobs are regularly monitored by the FTS3Agent. When all the FTS3Files have reached a final status, the FTS3Operation callback method is called. This callback method depends on the type of FTS3Operation.

Note that by default, the FTS3Agent is meant to run without shifter proxy. It will however download the proxy of the user submitting the job in order to delegate it to FTS. This also means that it is not able to perform registration in the DFC, and relies on Operation callback for that.


FTS3TransferOperation
=====================

When enabled by the flag *UseNewFTS3* in the ReplicateAndRegister operation definition, the RMS will create one FTS3TransferOperation per RMS Operation, and one FTS3File per RMS File. This means that there can be several destination SEs, and potentially source SEs specified.

The grouping into jobs is done following this logic:
    * Group by target SE
    * Group by source SE. If not specified, we take the active replicas as returned by the DataManager
    * Since their might be several possible source SE, we need to pick one only. The choice is to select the SE where there is the most files of the operation present. This increases the likely hood to pick a good old Tier1
    * Divide all that according to the maximum number of files we want per job

Once the FTS jobs have been executed, and all the operation is completed, the callback takes place. The callback consists in fetching the RMS request which submitted the FTS3Operation, update the status of the RMS files, and insert a Registration Operation.
Note that since the multiple targets are grouped in a single RMS operation, failing to transfer one file t one destination will result in the failure of the Operation. However, there is one Registration operation per target, and hence correctly transferred files will be registered.

FTS3StagingOperation
====================

.. warning ::

   Still in development, not meant to be used

This operation is meant to perform BringOnline. The idea behind that is to replace, if deemed working, the whole StorageSystem of DIRAC.

FTS3 state machines
-------------------

These are the states for FTS3File::

  ALL_STATES = [ 'New',  # Nothing was attempted yet on this file
                 'Submitted', # From FTS: Initial state of a file as soon it's dropped into the database
                 'Ready', # From FTS: File is ready to become active
                 'Active', # From FTS: File went active
                 'Finished', # From FTS: File finished gracefully
                 'Canceled', # From FTS: Canceled by the user
                 'Staging', # From FTS: When staging of a file is requested
                 'Failed', # From FTS: File failure
                 'Defunct', # Totally fail, no more attempt will be made
                 ]

  FINAL_STATES = ['Canceled', 'Finished', 'Defunct']
  FTS_FINAL_STATES = ['Canceled', 'Finished', 'Done']
  INIT_STATE = 'New'

These are the states for FTS3Operation::

  ALL_STATES = ['Active',  # Default state until FTS has done everything
                'Processed',  # Interactions with FTS done, but callback not done
                'Finished',  # Everything was done
                'Canceled',  # Canceled by the user
                'Failed',  # I don't know yet
               ]
  FINAL_STATES = ['Finished', 'Canceled', 'Failed' ]
  INIT_STATE = 'Active'

States from the FTS3Job::

  # States from FTS doc
  ALL_STATES = ['Submitted',  # Initial state of a job as soon it's dropped into the database
                'Ready', # One of the files within a job went to Ready state
                'Active', # One of the files within a job went to Active state
                'Finished', # All files Finished gracefully
                'Canceled', # Job canceled
                'Failed', # All files Failed
                'Finisheddirty',  # Some files Failed
               ]

  FINAL_STATES = ['Canceled', 'Failed', 'Finished', 'Finisheddirty']
  INIT_STATE = 'Submitted'


The status of the FTS3Jobs and FTSFiles are updated every time we monitor the matching job.

The FTS3Operation goes to Processed when all the files are in a final state, and to Finished when the callback has been called successfully

FTS3 Installation
-----------------

One needs to install an FTS3DB, the FTS3Manager, and the FTS3Agent. Install the
FTS3DB with `dirac-install-db` or directly on your mysql server and add the
Databse in the Configuration System.

  dirac-admin-sysadmin-cli -H diracserver034.institute.tld
  > install service DataManagement FTS3Manager
  > install agent DataManagement FTS3Agent

Then enable the *UseNewFTS3* flag for the ReplicateAndRegister operation as
described in `FTS3TransferOperation`_.

FTS3 System Configuration
-------------------------

There are various configuration options for this system::


  FTS3Agent
  {
    PollingTime = 120
    MaxThreads = 10
    # How many Operation we will treat in one loop
    OperationBulkSize = 20
    # How many Job we will monitor in one loop
    JobBulkSize = 20
    # Max number of files to go in a single job
    MaxFilesPerJob = 100
    # Max number of attempt per file
    maxAttemptsPerFile = 256
  }

DataManagement/FTSPlacement/FTS3/ServerPolicy see :ref:`dirac-operations-dms`.

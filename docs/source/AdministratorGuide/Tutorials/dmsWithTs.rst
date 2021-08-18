=========================================================
Large Scale DataManagement with the Transformation System
=========================================================

Pre-Requisite
=============

You should:

* have a machine setup as described in :ref:`tuto_basic_setup`
* have installed two DIRAC SE using the tutorial (:ref:`tuto_install_dirac_se`).
* have installed the DFC using the tutorial (:ref:`tuto_install_dfc`).
* have followed the tutorial on identity management (:ref:`tuto_managing_identities`)
* have installed the RMS using the tutorial (:ref:`tuto_install_rms`)
* have installed the TS using the tutorial (:ref:`tuto_install_ts`)


Tutorial Goal
=============

The aim of the tutorial is to demonstrate how large scale data management operations (removals, replications, etc.) can
be achieved using the Transformation System.  By the end of the tutorial, you will be able to:

* Submit simple transformation for manipulating a given list of files
* Have transformations automatically fed thanks to metadata
* Write your own plugin for the TransformationSystem

The transformations can be monitored and controlled with the ``Transformation Monitor`` in the ``WebApp`` when you use
the ``dirac_prod`` group.


More Links
==========

* :ref:`adminTS`


Creating a Transformation with a DIRAC Command
==============================================

.. highlight:: console

This section is to be performed as ``diracuser`` with a proxy in ``dirac_prod`` group.

First we need to create some files and upload them to ``StorageElementOne``::

  [diracuser@dirac-tuto ~]$ for ID in {1..10}; do echo "MyContent $ID" > File_${ID} ; dirac-dms-add-file /tutoVO/data/Trans_01/File_${ID} File_${ID} StorageElementOne ; done

Then we create the list of LFNs we just uploaded::

  [diracuser@dirac-tuto ~]$ dirac-dms-find-lfns Path=/tutoVO/data/Trans_01 > trans01.lfns

The easiest way to create a transformation to replicate files is by using the
:doc:`/AdministratorGuide/CommandReference/dirac-transformation-replication` command::

  [diracuser@dirac-tuto ~]$ dirac-transformation-replication 0 StorageElementTwo --Plugin Broadcast --Enable
  Created transformation NNN
  Successfully created replication transformation

This created transformation with the unique transformation ID *NNN* (e.g., 1).

By default this transformation uses *Metadata* information to obtain the input files using the
``InputDataAgent``. Instead we can also just add files manually with the :doc:`dirac-transformation-add-files
</AdministratorGuide/CommandReference/dirac-transformation-add-files>` command and using the list we created previously,
replace NNN by the ID of the transformation that was just created::

  [diracuser@dirac-tuto ~]$ dirac-transformation-add-files NNN trans01.lfns
  Successfully added 10 files


Now we have to wait until the ``TransformationAgent`` runs again and creates a *Task* for each of the files. Once the
tasks are created, the ``RequestTaskAgent`` creates a request out of each task, which is then processed in the
``RequestExecutingAgent`` of the RMS.


Creating a Transformation with a Script
=======================================


In this step we want to remove the replicas of our files from ``StorageElementOne``, for this purpose we have to write a
script that creates a removal transformation:

.. code-block:: python
   :caption: createRemoval.py
   :linenos:

    #!/bin/env python

    # set up the DIRAC configuration, parse command line arguments
    from DIRAC import gLogger, S_OK, S_ERROR
    from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
    Script.parseCommandLine()

    from DIRAC.TransformationSystem.Client.Transformation import Transformation

    # create a Transformation instance
    myTrans = Transformation()

    # transformation names need to be unique
    uniqueIdentifier = "Trans1"
    transformationName = "RemoveReplicas_%s" % uniqueIdentifier
    myTrans.setTransformationName(transformationName)

    # describe what the transformation will do
    description = "Remove replicas from StorageElementOne"
    myTrans.setDescription(description)
    myTrans.setLongDescription(description)

    # 'Replication' type means we do data management
    myTrans.setType('Removal')

    # group transformations that belong together, these can be selected in the WebApp
    transGroup = "myRemovals"
    myTrans.setTransformationGroup(transGroup)

    # groupSize defines the number of files each request will treat
    groupSize = 1
    myTrans.setGroupSize(groupSize)

    # the transformation plugin defines which input files are treated, and how they are grouped, for example
    plugin = 'Broadcast'
    myTrans.setPlugin(plugin)

    # the 'body' of the transformation, defines a list of Request Operations
    # that are executed in order for each file added to the transformation
    targetSE = 'StorageElementOne'
    transBody = [("RemoveReplica", {"TargetSE": targetSE})]

    myTrans.setBody(transBody)

    res = myTrans.setTargetSE(targetSE)
    if not res['OK']:
      gLogger.error("TargetSE not valid: %s" % res['Message'])
      exit(1)

    res = myTrans.addTransformation()
    if not res['OK']:
      gLogger.error("Failed to add the transformation: %s" % res['Message'])
      exit(1)

    # now activate the transformation
    myTrans.setStatus('Active')
    myTrans.setAgentType('Automatic')
    transID = myTrans.getTransformationID()['Value']
    gLogger.notice('Created RemoveReplica transformation: %r' % transID)
    exit(0)

When we execute the script, the transformation is created with the ID MMM (e.g. 2)::

    [diracuser@dirac-tuto ~]$ python createRemoval.py
    Created transformation MMM
    Created RemoveReplica transformation: MMML

To remove a replica from StorageElementOne, we just have to add files to this transformation::

    [diracuser@dirac-tuto ~]$ dirac-transformation-add-files MMM /tutoVO/data/Trans_01/File_10
    Successfully added 1 files

And then wait again for the ``TransformationAgent``, ``RequestTaskAgent``, ``RequestExecutingAgent`` chain to complete.

After a short while, you should see that the folder ``/opt/dirac/storageElementOne/tutoVO/data/Trans_01/``, no longer
contains ``File_10``.


Using Metadata Queries to Add Files to Transformations
======================================================

Adding files manually to transformations can be useful, but if we want to automatically add files to transformations we
can make use of metadata queries in combination with the ``InputDataAgent``, which executes the queries and adds new
files to the corresponding transformation.

To benefit from metadata query, we first have to create a metadata key, and add the key to a directory. These
operations can be done with the ``dirac-dms-filecatalog-cli``::

  [diracuser@dirac-tuto ~]$ dirac-dms-filecatalog-cli
  Starting FileCatalog client

  File Catalog Client $Revision: 1.17 $Date:

  FC:/$ ls -l
  drwxrwxr-x 0 ciuser dirac_user 0 2019-05-06 14:30:36 tutoVO

In the ``dirac-dms-filecatalog-cli``, like in the other DIRAC CLIs you can use ``help`` and ``help <command>`` to see
information about the available commands.

Initially there are no metadata keys defined::

  FC:/$ meta show
        FileMetaFields : {}
   DirectoryMetaFields : {}

We now create in integer directory metadata called ``TransformationID``::

  FC:/$ meta index -d TransformationID int
  Added metadata field TransformationID of type int
  FC:/$ meta show
        FileMetaFields : {}
   DirectoryMetaFields : {'TransformationID': 'INT'}

Let's add the ``TransformationID=1`` to the files we uploaded earlier::

  FC:/$ meta set /tutoVO/data/Trans_01/ TransformationID 1
  /tutoVO/data/Trans_01 {'TransformationID': '1'}

You can see the metadata set for a given diretory with the ``meta get`` command, and you can use the ``find`` command
inside the ``dirac-dms-filecatalog-cli`` to search for files with metadata::

  FC:/$ meta get /tutoVO/data/Trans_01/
    !TransformationID : 1
  FC:/$ find / TransformationID=1
  Query: {'TransformationID': 1}
  /tutoVO/data/Trans_01/File_1
  [..snip..]
  /tutoVO/data/Trans_01/File_9
  QueryTime 0.00 sec

Now let us create another directory, and set a different metadata value, before we create another transformation
including an inputdata query::

  FC:/$ mkdir /tutoVO/data/Trans_02/
  Successfully created directory: /tutoVO/data/Trans_02
  FC:/$ meta set /tutoVO/data/Trans_02/ TransformationID 2
  /tutoVO/data/Trans_02 {'TransformationID': '2'}
  FC:/$ meta get /tutoVO/data/Trans_02/
     !TransformationID : 2

Now upload some files to this folder::

  [diracuser@dirac-tuto ~]$ for ID in {1..10}; do echo "MyContent $ID" > File_${ID} ; dirac-dms-add-file /tutoVO/data/Trans_02/File_${ID} File_${ID} StorageElementOne ; done

We can also use the command ``dirac-dms-find-lfns`` to search for files with given metadata::

  [diracuser@dirac-tuto ~]$ dirac-dms-find-lfns Path=/ TransformationID=2


Now we create a transformation, which uses the metadata to pick up the files::

 [diracuser@dirac-tuto ~]$  dirac-transformation-replication 2 StorageElementTwo --Plugin=Broadcast --Enable
 Created transformation LLL
 Successfully created replication transformation

In fact the command ``dirac-transformation-replication`` already uses metadata, the first argument is the value for the
``TransformationID`` metadata. Now we have to wait for the ``InputDataAgent``, ``TransformationAgent``,
``RequestTaskAgent``, ``RequestExecutingAgent`` chain to run its course.

In the log file of the ``InputDataAgent`` in ``/opt/dirac/pro/runit/Transformation/InputDataAgent/log/current``
eventually this line should appear::

  <SomeDate> Transformation/InputDataAgent INFO: 10 files returned for transformation LLL from the metadata catalog


You may add some more files to ``/tutoVO/data/Trans_02/`` and see them appearing in your transformation::

  [diracuser@dirac-tuto ~]$ for ID in {11..20}; do echo "MyContent $ID" > File_${ID} ; dirac-dms-add-file /tutoVO/data/Trans_02/File_${ID} File_${ID} StorageElementOne ; done


InputDataQuery in the Script
----------------------------

To add the metadata query functionality to our ``createRemoval.py`` script from above, we just need to insert a couple
of lines

.. code-block:: python
   :lineno-start: 44

   metaQuery = {'TransformationID': 2}
   myTrans.setInputMetaQuery(metaQuery)

   ...

Adapt the script by inserting the lines and changing the ``uniqueIdentifier`` and execute it::

  [diracuser@dirac-tuto ~]$ python createRemoval.py
  Created transformation JJJ
  Created RemoveReplica transformation: JJJL

Conclusion
==========

You now have all the knowledge to perform DataManagement in DIRAC with the TransformationSystem.

To learn how to extend the system by creating new transformation plugins, please see how to
:ref:`dev-ts-transformationagent-plugins`.

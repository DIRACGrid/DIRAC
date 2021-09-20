.. _dev_ts_architecture:

Architecture
============

The TS is a standard DIRAC system, and therefore it is composed by components in the following categories: Services, DBs, Agents.
A technical drawing explaining the interactions between the various components follow.

.. image:: ../../../_static/Systems/TS/TS-technical.png
   :alt: Transformation System schema.
   :align: center

* **Services**

  * TransformationManagerHandler:
    DISET request handler base class for the TransformationDB

* **DB**

  * TransformationDB:
    it's used to collect and serve the necessary information in order to automate the task of job preparation for high level transformations.
    This class is typically used as a base class for more specific data processing databases. Here below the DB tables:

  ::

      mysql> use TransformationDB;
      Database changed
      mysql> show tables;
      +------------------------------+
      | Tables_in_TransformationDB   |
      +------------------------------+
      | AdditionalParameters         |
      | DataFiles                    |
      | TaskInputs                   |
      | TransformationFileTasks      |
      | TransformationFiles          |
      | TransformationInputDataQuery |
      | TransformationLog            |
      | TransformationTasks          |
      | Transformations              |
      +------------------------------+


  **Note** that since version v6r10, there are important changes in the TransformatioDB, as explained in the `release notes <https://github.com/DIRACGrid/DIRAC/wiki/DIRAC-v6r10#transformationdb>`_ (for example the Replicas table can be removed). Also, it is highly suggested to move to InnoDB. For new installations, all these improvements will be installed automatically.

* **Agents**

  * TransformationAgent: it processes transformations found in the TransformationDB and creates the associated tasks,
    by connecting input files with tasks given a plugin. It's not useful for MCSimulation type

  * WorkflowTaskAgent: it takes workflow tasks created in the TransformationDB and it submits to the WMS.
    There are some capabilities in the form of TaskManager plugins,
    please refer to <https://github.com/DIRACGrid/DIRAC/wiki/DIRAC-v6r13#changes-for-transformation-system>`_. 
    These plugins determine how the destination site is chosen.

  * RequestTaskAgent: it takes request tasks created in the TransformationDB and submits to the RMS.
    Both RequestTaskAgent and WorkflowTaskAgent inherits from the same agent, "TaskManagerAgentBase", whose code contains large part of the logic that will be executed. But, TaskManagerAgentBase should not be run standalone.

  * MCExtensionAgent: it extends the number of tasks given the Transformation definition. To work it needs to know how many events each production will need, and how many events each job will produce. It is only used for 'MCSimulation' type

  * TransformationCleaningAgent: it cleans up the finalised Transformations

  * InputDataAgent: it updates the transformation files of active Transformations given an InputDataQuery fetched from the Transformation Service

  * ValidateOutputDataAgent: it runs few integrity checks prior to finalise a Production.

The complete list can be found in the `DIRAC project GitHub repository <https://github.com/DIRACGrid/DIRAC/tree/integration/TransformationSystem/Agent>`_.

* **Clients**

  * TaskManager: it contains TaskBase, inherited by WorkflowsTasks and RequestTasks modules, for managing jobs and requests tasks, i.e. it contains classes wrapping the logic of how to 'transform' a Task in a job/request. WorkflowTaskAgent uses WorkflowTasks, RequestTaskAgent uses RequestTasks.

  * TransformationClient: class that contains client access to the transformation DB handler (main client to the service/DB). It exposes the functionalities available in the DIRAC/TransformationHandler. This inherits the DIRAC base Client for direct execution of server functionality

  * Transformation: it wraps some functionalities mostly to use the 'TransformationClient' client



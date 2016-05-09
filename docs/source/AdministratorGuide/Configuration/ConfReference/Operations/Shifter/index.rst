Operations / Shifter - Subsection
=================================

In this subsection managers are described for some systems. User credentials for the agents than will be used during execution of some tasks.


+--------------------------+--------------------------+---------------------+
| **Name**                 | **Description**          | **Example**         |
+--------------------------+--------------------------+---------------------+
| *<Service_Manager>*      | Name of service managers | Admin               |
|                          |                          | ProductionManager   |
|                          |                          | SAMManager          |
|                          |                          | DataManager         |
+--------------------------+--------------------------+---------------------+
| *<Service_Manager>/User* | DIRAC user name          | User = vhamar       |
+--------------------------+--------------------------+---------------------+
| *<Service_Manager/Group* | DIRAC user group         | Group = dirac_admin |
+--------------------------+--------------------------+---------------------+

Agents requiring to act with a credential have always the option **shifterProxy** with a certain default: DataManager, ... At each installation this default identity can be changed for each of them provided the corresponding section is created here.

The default identities currently used by DIRAC Agents are:

- **SAMManager**: Configuration/CE2CSAgent
- **DataManager**: DataManagement/FTSCleaningAgent, DataManagement/FTSMonitorAgent, DataManagement/FTSSubmitAgent, DataManagement/LFCvsSEAgent, DataManagement/RegistrationAgent, DataManagement/RemovalAgent, DataManagement/ReplicationScheduler, DataManagement/SEvsLFCAgent,DataManagement/TransferAgent, StorageManagement/MigrationMonitoringAgent, StorageManagement/PinRequestAgent, StorageManagement/RequestFinalizationAgent, StorageManagement/RequestPreparationAgent, StorageManagement/SENamespaceCatalogCheckAgent, StorageManagement/StageMonitorAgent, StorageManagement/StageRequestAgent, *Transformation/MCExtensionAgent*, Transformation/TransformationCleaningAgent, Transformation/ValidateOutputDataAgent
- **ProductionManager**: Transformation/RequestTaskAgent, Transformation/TransformationAgent, Transformation/WorkflowTaskAgent, WorkloadManagement/InputDataAgent

In general, to force any Agent to execute using a "manager" credential, instead of the certificate of the server it is only necessary to add a valid **shifterProxy** option in its configuration.

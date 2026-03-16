.. contents:: Table of contents
   :depth: 3

=================
Accounting System
=================

The DIRAC Accounting system is designed to collect, store, and generate reports on various system activities including job execution and data transfers. It uses MySQL for storing historical data organized in time buckets for efficient querying.


--------
Overview
--------

The Accounting system provides:

- **Data collection**: Gathering accounting records from various DIRAC components
- **Storage**: Persisting records in MySQL databases with automatic bucketing
- **Reporting**: Generating plots and reports from the stored data
- **Multi-DB support**: Distributing different accounting types across multiple database instances

The system consists of several key components:

- **Services**: DataStore, ReportGenerator
- **Databases**: AccountingDB, with optional MultiAccountingDB for multiple database backends
- **Clients**: DataStoreClient, ReportsClient
- **Accounting Types**: BaseAccountingType and various type implementations (Job, Pilot, DataOperation, etc.)
- **Agents**: NetworkAgent for consuming perfSONAR metrics
- **Private modules**: Plotters, Policies, DBUtils, MainReporter for report generation


---------------------
Architecture and Flow
---------------------

Data Flow
~~~~~~~~~~

The data flow in the Accounting system follows this pattern:

1. **Record Creation**: Components create accounting records using type classes (e.g., Job, Pilot, DataOperation)
2. **Client Buffering**: Raw records are collected by DataStoreClient and buffered locally
3. **Submission**: Raw records are submitted in bundles to the DataStore service
4. **Queuing**: Raw records are inserted into "in" tables in the AccountingDB
5. **Administering**: Raw records are moved into the "type" tables, and its descriptive attributes organized in "key" tables
6. **Bucketing**: A periodic process aggregates records from "type" tables into time buckets
7. **Report Generation**: ReportGenerator service retrieves bucketed data and generates plots


Service Architecture
~~~~~~~~~~~~~~~~~~~~~

**DataStore Service**

The DataStore service (``DataStoreHandler``) handles insertion of accounting records.

Key features:

- Can be run as a single instance or with helper instances (master/worker pattern)
- ``RunBucketing`` option controls whether the instance performs bucketing
- Master instance: creates buckets, runs compaction
- Helper instances: only insert records, no bucketing

Configuration example::

    install service Accounting DataStore
    # Helper instance (optional):
    install service Accounting DataStore -m DataStore -p RunBucketing=False

The service provides:

- ``getRegisteredTypes()``: Lists all registered accounting types
- ``commit()``: Insert a single record
- ``commitRegisters()``: Insert multiple records in a bundle

**ReportGenerator Service**

The ReportGenerator service (``ReportGeneratorHandler``) generates plots and reports from accounting data.

Key features:

- Uses a local filesystem cache for generated plots
- Requires writable ``DataLocation`` directory
- Supports various plot types and time ranges

Configuration requires a writable directory::

    DataLocation = data/accountingGraphs

The service provides:

- ``listReports()``: List available reports for a type
- ``getReport()``: Retrieve report data
- ``generatePlot()``: Generate a plot
- ``generateDelayedPlot()``: Generate plot asynchronously


Database Layer
~~~~~~~~~~~~~~

**AccountingDB**

The ``AccountingDB`` class is the main database interface extending DIRAC's base DB class.

Key responsibilities:

- Managing type catalog (registered accounting types and their definitions)
- Handling raw record insertion via queue
- Performing bucketing of records into time-based aggregates
- Compacting old buckets for space management
- Retrieving bucketed data for reporting

Database schema uses dynamic table creation. For each of the defined types, tables are created organized in a star schema:

- ``ac_in_{type}``: Raw records pending insertion
- ``ac_type_{type}``: Organized raw records, acts as the central facts table
- ``ac_bucket_{type}``: Time-bucketed aggregated records
- ``ac_key_{type}_{key_name}``: Dimension tables with desciptive attributes


Bucketing process:

- Records are bucketed according to the ``bucketsLength`` definition of each type
- Default bucket granularities: hourly for recent data, daily for older data, weekly for very old data
- Automatic compaction occurs at a randomized time (default: 2:00 AM)

**MultiAccountingDB**

The ``MultiAccountingDB`` class provides support for distributing accounting types across multiple database instances.

Configuration::

    Systems
    {
      Accounting
      {
        Production
        {
          AccountingDB
          {
            Host = db1.example.com
            DBName = accounting_main
          }
          ArchiveDB
          {
            Host = db2.example.com
            DBName = accounting_archive
          }
          MultiDB
          {
            WMSHistory = ArchiveDB
          }
        }
      }
    }

Implementation:

- Routes calls to appropriate database instance based on accounting type
- Allows scaling by distributing load across multiple databases
- Some operations (like bucketing) are executed on all databases


Client Side
~~~~~~~~~~~

**DataStoreClient**

The ``DataStoreClient`` provides a high-level interface for submitting accounting records.

Key features:

- Batches records locally to reduce server interactions
- Supports failover to RequestDB if DataStore is unavailable
- Thread-safe record collection
- Automatic retry mechanism

Usage example::

    from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
    from DIRAC.AccountingSystem.Client.Types.Job import Job

    # Create a job accounting record
    job = Job()
    job.setStartTime(someStartTime)
    job.setEndTime(someEndTime)
    job.setValuesFromDict({
        'User': 'someuser',
        'UserGroup': 'volunteers',
        'CPUTime': 3600,
        'ExecTime': 7200,
        # ... other fields
    })

    # Add to client
    result = gDataStoreClient.addRegister(job)

    # Commit to server
    gDataStoreClient.commit()

The client includes:

- ``addRegister()``: Add a record to the buffer
- ``commit()``: Send buffered records to the server
- ``disableFailover()``: Disable failover to RequestDB

**ReportsClient**

The ``ReportsClient`` provides access to the ReportGenerator service.

Usage example::

    from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
    from DIRAC.Core.Utilities import Time
    from datetime import datetime, timedelta

    reportsClient = ReportsClient()

    # List available reports for Job type
    result = reportsClient.listReports('Job')

    # Generate a plot
    startTime = datetime.utcnow() - timedelta(days=7)
    endTime = datetime.utcnow()
    result = reportsClient.generatePlot(
        typeName='Job',
        reportName='AverageCPUTime',
        startTime=startTime,
        endTime=endTime,
        condDict={'Site': ['LCG.GRIDKA.de']},
        grouping='JobType'
    )

The client provides:

- ``listReports()``: List available reports
- ``getReport()``: Get report data
- ``generatePlot()``: Synchronous plot generation
- ``getPlotToMem()``: Retrieve generated plot to memory
- ``getPlotToDirectory()``: Download plot to directory


----------------
Accounting Types
----------------

BaseAccountingType
~~~~~~~~~~~~~~~~~~

All accounting types inherit from ``BaseAccountingType`` which provides:

- Type definition: key fields (text for classification) and value fields (numeric for measurement)
- Bucket configuration: time granularities for data bucketing
- Value validation: ensures all required fields are filled
- Time management: start and end time handling

Key methods:

- ``setStartTime()`` / ``setEndTime()``: Set time boundaries
- ``setNowAsStartAndEndTime()``: Set current time
- ``setValueByKey()`` / ``setValuesFromDict()``: Set field values
- ``checkValues()``: Validate all fields and timing
- ``getDefinition()``: Get type definition tuple
- ``getValues()``: Get record values tuple

Standard Type Structure
~~~~~~~~~~~~~~~~~~~~~~~

Accounting type classes follow this pattern::

    from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

    class MyType(BaseAccountingType):
        def __init__(self):
            super().__init__()
            self.definitionKeyFields = [
                ('Field1', 'VARCHAR(64)'),   # Text fields
                ('Field2', 'VARCHAR(32)'),
            ]
            self.definitionAccountingFields = [
                ('Metric1', 'INT UNSIGNED'),  # Numeric fields
                ('Metric2', 'BIGINT UNSIGNED'),
            ]
            self.bucketsLength = [
                (691200, 3600),      # <8 days = 1 hour granularity
                (2592000, 86400),    # <30 days = 1 day granularity
                (15552000, 604800),  # <6 months = 1 week granularity
            ]
            self.checkType()

        def checkRecord(self):
            # Optional custom validation
            return S_OK()

Type Registration
~~~~~~~~~~~~~~~~~

Types are automatically discovered and registered by the ``TypeLoader`` utility, which looks for classes inheriting from ``BaseAccountingType`` in the ``AccountingSystem.Client.Types`` module.

When a type is first registered:

- Database creates catalog entry with type definition
- Database creates ``in`` table for raw records
- Database creates bucketed tables according to bucket definition


Built-in Accounting Types
~~~~~~~~~~~~~~~~~~~~~~~~~

**Job**

Tracks job execution metrics:

- Key fields: User, UserGroup, JobGroup, Site, JobType, ProcessingType, Status
- Value fields: CPUTime, NormCPUTime, ExecTime, InputDataSize, OutputDataSize, DiskSpace, etc.
- Categories jobs by type and resource usage

Used by: JobWrapper during job execution, StalledJobAgent

**Pilot**

Tracks pilot job metrics:

- Key fields: GridSite, CE, Queue, UserDN, UserGroup, Status
- Value fields: CPUTime, NormCPUTime, ExecTime, InputSize, OutputSize
- Monitors pilot job performance on different computing elements

Used by: JobAgent during pilot execution

**DataOperation**

Tracks data management operations:

- Key fields: Operation, Source, Destination, User, Status, Channel
- Value fields: TransferSize, TransferTime, Files, Throughput
- Records data transfers, replications, removals

Used by: DataManagement components

**WMSHistory**

Monitors WMS system state:

- Note: Replaced by Monitoring system's WMS monitoring
- Retained for backward compatibility
- Real-time monitoring alternative to time-based accounting

**Network**

Tracks network performance:

- Key fields: Source, Destination, Network
- Value fields: latency, packet loss rate, bandwidth
- Consumes perfSONAR metrics via message queue

Used by: NetworkAgent

**StorageOccupancy**

Monitors storage resource usage:

- Key fields: SE, StorageGroup, Endpoint
- Value fields: TotalSize, FileCount, DirectoryCount
- Tracks storage capacity and usage patterns

**PilotSubmission**

Tracks pilot job submission:

- Key fields: Site, CE, Queue, UserGroup
- Value fields: SubmittedPilots, SuccessfulPilots, FailedPilots, WaitingTime
- Monitors pilot submission efficiency


-----------------
Report Generation
-----------------

Plotters
~~~~~~~~

Plotters (in ``AccountingSystem.private.Plotters``) are responsible for generating specific types of reports for each accounting type.

Each plotter inherits from ``BaseReporter``:

- ``JobPlotter``: Generates job-related reports (CPU time, wall time, data usage)
- ``PilotPlotter``: Generates pilot-related reports
- ``DataOperationPlotter``: Generates data operation reports
- ``NetworkPlotter``: Generates network performance reports

Policies
~~~~~~~~

Policies (in ``AccountingSystem.private.Policies``) implement filtering and aggregation rules:

- ``FilterExecutor``: Executes filter conditions on data
- ``JobPolicy``: Specialized handling for job accounting data

MainReporter
~~~~~~~~~~~~

The ``MainReporter`` class coordinates report generation:

- Validates plot requests
- Applies filters based on conditions
- Retrieves bucketed data from database
- Aggregates and calculates statistics
- Generates plot data for visualization

Plot Request Structure
~~~~~~~~~~~~~~~~~~~~~~

Plot requests typically include::

    {
        'typeName': 'Job',                    # Accounting type
        'reportName': 'AverageCPUTime',       # Specific report
        'startTime': <epoch>,                  # Start time (epoch or datetime)
        'endTime': <epoch>,                    # End time (epoch or datetime)
        'condDict': {                          # Filtering conditions
            'Site': ['LCG.GRIDKA.de'],
            'UserGroup': ['volunteers']
        },
        'grouping': 'JobType',                # Grouping field
        'extraArgs': {                         # Additional parameters
            'lastSeconds': 86400,              # Optional: sliding window
            'ignoreSelected': False            # Plot-specific options
        }
    }


------------------
Failover Mechanism
------------------

The DataStoreClient implements failover to ensure accounting records are not lost if the DataStore service is unavailable.

Failover Process:

1. Record submission fails (service down, timeout, etc.)
2. After ``retryGraceTime``, records are sent to failover
3. Failover uses RequestDB to store records as operations
4. When DataStore becomes available, RequestDB retries the operations
5. Records are eventually committed to AccountingDB

Configuration:

- ``/LocalSite/DisableFailover``: Disable failover mechanism
- ``retryGraceTime``: Time to wait before using failover

To disable accounting entirely:

- ``/LocalSite/DisableAccounting``: All accounting calls no-op


----------------------
Development Guidelines
----------------------

Creating New Accounting Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create new type class in ``AccountingSystem.Client.Types``::

    from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

    class NewMetric(BaseAccountingType):
        def __init__(self):
            super().__init__()
            self.definitionKeyFields = [
                ('Category', 'VARCHAR(64)'),
                ('Component', 'VARCHAR(64)'),
            ]
            self.definitionAccountingFields = [
                ('Duration', 'INT UNSIGNED'),
                ('Count', 'INT UNSIGNED'),
            ]
            self.bucketsLength = [
                (86400 * 8, 3600),    # 1 hour
                (86400 * 30, 86400),   # 1 day
                (86400 * 180, 604800), # 1 week
            ]
            self.checkType()

2. Type will be auto-discovered and registered on first use

3. Records can be created and submitted using DataStoreClient

Using DataStoreClient in Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Best practices:

- Use global ``gDataStoreClient`` instance for consistency
- Set appropriate start and end times for records
- Fill all required fields before committing
- Commit periodically rather than for every record

Example::

    from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient

    def processOperation():
        metric = NewMetric()
        metric.setNowAsStartAndEndTime()
        metric.setValuesFromDict({
            'Category': 'Processing',
            'Component': 'MyModule',
            'Duration': 123,
            'Count': 45
        })

        gDataStoreClient.addRegister(metric)

Generating Reports
~~~~~~~~~~~~~~~~~~~

1. Use ReportsClient to retrieve available reports
2. Specify time range and conditions
3. Choose appropriate grouping for aggregation

The Accounting web application provides a user-friendly interface for generating reports interactively.


--------------------
Database Maintenance
--------------------

Compaction
~~~~~~~~~~

Database compaction:

- Runs automatically at configured time (default: 2:00 AM)
- Can be triggered manually via ``AccountingDB.compactBuckets()``
- Keeps only necessary granularities for time periods
- Frees space by deleting overly detailed old data

Manual compaction trigger::

    from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB
    db = AccountingDB()
    db.compactBuckets()

NOTA BENE: the compaction only starts for those types defining a ``dataTimespan`` larger than 30 days. This is NOT the case for most of the types (only true for WMSHistory and PilotSubmission).
Which effectively means that all "Type" tables normally holds all raw records ever inserted.

Monitoring Database Health
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Key indicators:

- Size of "in" tables: Growing may indicate bucketing issues
- Bucket creation rate: Should match record insertion rate
- Database size: Monitor growth patterns
- Query performance: For report generation

Considerations for Large Installations:

- Use Multi-DB to distribute load
- Consider separate database for high-volume types
- Monitor and tune bucketing frequency
- Ensure sufficient disk space for plot cache

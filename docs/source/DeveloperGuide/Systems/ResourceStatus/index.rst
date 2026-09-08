.. _developer_resource_status:

============================
Architecture Reference Guide
============================

.. contents::
   :depth: 2
   :local:

This document provides technical details for developers working with the ResourceStatusSystem (RSS) internals.


Database Schemas
================

ResourceStatusDB
----------------

The ResourceStatusDB contains two sets of tables (Site and Resource), each with three table types.

Status Tables
~~~~~~~~~~~~~

Current state of all elements. **Primary key:** (Name, StatusType, VO)

.. list-table::
   :header-rows: 1
   :widths: 20 15 20 45

   * - Field
     - Type
     - Default
     - Description
   * - Name
     - VARCHAR(64)
     -
     - Element name (e.g., site name, SE name)
   * - StatusType
     - VARCHAR(128)
     - 'all'
     - Status type (all, ReadAccess, WriteAccess, etc.)
   * - VO
     - VARCHAR(64)
     - 'all'
     - Virtual Organization
   * - Status
     - VARCHAR(8)
     -
     - Current status (Active, Degraded, Probing, Banned, Unknown, Error)
   * - Reason
     - VARCHAR(512)
     - 'Unspecified'
     - Reason for current status
   * - DateEffective
     - DATETIME
     -
     - When status became effective
   * - TokenExpiration
     - DATETIME
     - 9999-12-31 23:59:59
     - When lock expires
   * - ElementType
     - VARCHAR(32)
     -
     - Type (CE, SE, etc.)
   * - LastCheckTime
     - DATETIME
     - 1000-01-01 00:00:00
     - Last policy check
   * - TokenOwner
     - VARCHAR(16)
     - 'rs_svc'
     - Who owns the lock

**Tables:** SiteStatus, ResourceStatus

Log Tables
~~~~~~~~~~

All status changes with auto-increment ID. **Primary key:** ID

Same fields as Status tables plus:

* **ID** (BIGINT, AUTO_INCREMENT) - Unique identifier

**Tables:** SiteLog, ResourceLog

History Tables
~~~~~~~~~~~~~~

Archived logs with duplicates removed. Same schema as Log tables.

**Tables:** SiteHistory, ResourceHistory

ResourceManagementDB
--------------------

Cache tables for metrics used by policies.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Table
     - Purpose
   * - DowntimeCache
     - Scheduled downtimes from GOCDB (DowntimeID, StartDate, EndDate, Severity)
   * - GGUSTicketsCache
     - GGUS support tickets (Tickets JSON, OpenTickets count)
   * - JobCache
     - Job efficiency metrics per site (Efficiency, MaskStatus)
   * - PilotCache
     - Pilot efficiency per CE (Site, CE, Efficiency, Status)
   * - PolicyResult
     - Policy evaluation results (Element, Name, PolicyName, Status, Reason)
   * - SpaceTokenOccupancyCache
     - Storage space usage (Endpoint, Token, Free, Total) — values stored in MB
   * - TransferCache
     - Transfer quality metrics (SourceName, DestinationName, Metric, Value)

Key Methods
~~~~~~~~~~~

insert()
   Insert new records

select()
   Query records with flexible conditions

delete()
   Remove records

addOrModify()
   Insert if not exists, update if exists (by primary key)

addIfNotThere()
   Insert only if record doesn't exist

Policy System
=============

Three-Tier Architecture
-----------------------

.. code-block:: text

   Inspector Agent
        ↓
   PEP (Policy Enforcement Point) ← Entry point
        ↓
   PDP (Policy Decision Point) ← Discovers & runs policies
        ↓
   Policies → Commands → Metrics
        ↓
   StateMachine ← Combines results
        ↓
   Actions ← Execute changes

State Machine
-------------

Valid states ordered by priority (lowest number = highest priority):

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - State
     - Level
     - Description
   * - Unknown
     - 5
     - No information available
   * - Active
     - 4
     - Fully operational
   * - Degraded
     - 3
     - Operational but suboptimal
   * - Probing
     - 2
     - Under testing (transition from Banned)
   * - Banned
     - 1
     - Not usable
   * - Error
     - 0
     - Error state

**Transition Rule:** Transitions from Banned to Active/Degraded/Unknown must go through Probing first.

**Location:** ``PolicySystem/StateMachine.py:28-44``

Policy Implementation
---------------------

Policies inherit from ``PolicyBase`` and implement ``evaluate()``.

**Example:** Job Efficiency Policy

.. code-block:: python

   # Location: Policy/JobEfficiencyPolicy.py
   class JobEfficiencyPolicy(PolicyBase):
       def evaluate(self):
           # Execute command to get job stats
           jobStats = self.command.doCommand()
           efficiency = jobStats['Done'] / jobStats['Total']

           # Apply thresholds
           if efficiency > 0.9:
               return {'Status': 'Active', 'Reason': f'Efficiency: {efficiency:.2%}'}
           elif efficiency > 0.7:
               return {'Status': 'Degraded', 'Reason': f'Low efficiency: {efficiency:.2%}'}
           return {'Status': 'Banned', 'Reason': f'Very low efficiency: {efficiency:.2%}'}

FreeDiskSpace Policy
--------------------

The ``FreeDiskSpacePolicy`` (``Policy/FreeDiskSpacePolicy.py``) evaluates SE occupancy using
configurable thresholds.  Thresholds are passed through as command arguments so they propagate
from the CS configuration all the way to the policy evaluation:

1. ``Configurations.py`` reads ``Unit``, ``Banned_threshold``, ``Degraded_threshold``,
   ``Banned_fraction`` and ``Degraded_fraction`` from the
   Operations CS via ``Operations().getValue("ResourceStatus/Policies/FreeDiskSpace/Banned_threshold", 0.1)``
   and stores them in the policy ``args`` dict.
2. ``FreeDiskSpaceCommand`` reads these values from ``self.args`` in ``_prepareCommand()`` and
   returns them alongside ``Free`` and ``Total`` in both ``doNew()`` and ``doCache()``.
3. ``FreeDiskSpacePolicy._evaluate()`` reads ``Banned_threshold``, ``Degraded_threshold``,
   ``Banned_fraction`` and ``Degraded_fraction`` from the command result dict (with safe defaults)
   and applies the comparison. The SE is set to Banned or Degraded if EITHER the absolute
   threshold OR the fraction threshold is exceeded.

This design keeps thresholds fully configurable per deployment without code changes.
See :ref:`rss_advanced_configuration` for the available CS keys.

Command Implementation
----------------------

Commands inherit from ``Command`` and implement ``doCommand()``.

**Example:** Downtime Command

.. code-block:: python

   # Location: Command/DowntimeCommand.py
   class DowntimeCommand(Command):
       def doCommand(self):
           # Query GOCDB API
           gocdb = GOCDB()
           downtimes = gocdb.getDowntimes(elementName=self.args['name'])

           # Store in cache
           for dt in downtimes:
               self.clients['ResourceManagementClient'].addOrModifyDowntimeCache(
                   name=dt['Name'], downtimeID=dt['ID'],
                   startDate=dt['StartDate'], endDate=dt['EndDate'],
                   severity=dt['Severity']
               )

           return S_OK(downtimes)

Client Architecture
===================

ResourceStatus Client
---------------------

High-level API with caching (Singleton pattern).

**Location:** ``Client/ResourceStatus.py``

getElementStatus(elementName, elementType, statusType=None, vO=None)
   Get status for Storage/Computing Elements, FTS, Catalogs

   .. code-block:: python

      from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
      rs = ResourceStatus()

      # Get all status types for a SE
      rs.getElementStatus('CERN-USER', 'StorageElement')
      # Returns: {'CERN-USER': {'ReadAccess': 'Active', 'WriteAccess': 'Active', ...}}

      # Get specific status type
      rs.getElementStatus('CERN-USER', 'StorageElement', 'ReadAccess')
      # Returns: {'CERN-USER': {'ReadAccess': 'Active'}}

      # Get multiple SEs
      rs.getElementStatus(['CERN-USER', 'PIC-USER'], 'StorageElement', 'ReadAccess')

setElementStatus(elementName, elementType, statusType, status, reason=None, tokenOwner=None)
   Set element status (acquires 1-day token)

   .. code-block:: python

      rs.setElementStatus('CERN-USER', 'StorageElement', 'ReadAccess',
                         'Banned', reason='Maintenance')

**Caching:** Internal RSSCache with configurable lifetime, refreshed automatically on cache miss.

SiteStatus Client
-----------------

Simplified interface for site operations (Singleton pattern).

**Location:** ``Client/SiteStatus.py``

getSiteStatuses(siteNames=None)
   Get status for one or more sites

isUsableSite(siteName, statusType='all')
   Returns True if site is Active or Degraded

getUsableSites(statusType='all')
   Returns list of all usable sites

setSiteStatus(siteName, status, reason=None)
   Set site status manually

Agent Implementation
====================

Inspector Agents
----------------

Check elements periodically based on their status.

**Check Frequency:**

Active/Degraded
   Every 20 minutes

Probing/Banned/Unknown
   Every 20 minutes

Error
   Every 5 minutes

**Implementation Pattern:**

.. code-block:: python

   # Location: Agent/SiteInspectorAgent.py, Agent/ElementInspectorAgent.py
   def execute(self):
       # Get elements needing check (LastCheckTime + lifetime < now)
       elements = self._getElementsToBeChecked()

       # Process in parallel (15 threads)
       with ThreadPoolExecutor(max_workers=15) as executor:
           for element in elements:
               executor.submit(self._execute, element)

   def _execute(self, element):
       pep = PEP(clients=self.clients)
       pep.enforce({
           'element': 'Site',  # or 'Resource'
           'name': element['Name'],
           'statusType': element['StatusType'],
           'status': element['Status'],
           'elementType': element['ElementType']
       })

**Skip Condition:** Elements with ``TokenOwner != 'rs_svc'`` are skipped (manual override active).

All Agents
----------

SiteInspectorAgent
   Evaluates policies for Site elements (PollingTime: 300s, maxThreads: 15)

ElementInspectorAgent
   Evaluates policies for Resource elements (PollingTime: 300s, elementType: Resource)

CacheFeederAgent
   Populates cache tables with metrics (PollingTime: 900s, commands: Downtime, GOCDBSync, FreeDiskSpace, Pilot)

TokenAgent
   Manages token expiration, notifies owners 12h before expiry (PollingTime: 3600s)

EmailAgent
   Sends aggregated status change notifications (PollingTime: 1800s)

SummarizeLogsAgent
   Archives logs to history tables (PollingTime: 300s, keeps 36 months)

RucioRSSAgent
   Synchronizes with Rucio RSE status (PollingTime: 120s)

Status Flow
===========

Complete flow from element check to database update:

.. code-block:: text

   1. Inspector Agent
         ↓ Query elements where LastCheckTime + lifetime < now
   2. Get Current Status from DB
         ↓ (Name, StatusType, Status, TokenOwner, etc.)
   3. PEP.enforce(decisionParams)
         ↓ Skip if TokenOwner != 'rs_svc'
   4. PDP.setup(decisionParams)
         ↓ Discover applicable policies from CS
   5. PDP.takeDecision()
         ↓ Run each policy via PolicyCaller
   6. Policy.evaluate()
         ↓ Execute Command to gather metrics
   7. Command.doCommand()
         ↓ Fetch from cache or external API
   8. StateMachine.orderPolicyResults()
         ↓ Sort by severity (Banned > Active)
   9. PDP discovers applicable actions
         ↓ LogStatusAction, EmailAction, etc.
  10. PEP validates element unchanged
         ↓ Check DB to prevent race conditions
  11. Execute Actions
         ↓
  12. LogStatusAction
         ↓ Update Status table (current state)
         ↓ Insert Log table (history)
         ↓ Set TokenOwner='rs_svc', LastCheckTime=now
  13. EmailAction (optional)
         ↓ Insert into ResourceStatusCache
         ↓ EmailAgent aggregates and sends later

Extension Points
================

Adding Custom Policies
----------------------

**1. Create policy file:**

``MyExtension/ResourceStatusSystem/Policy/MyCustomPolicy.py``

.. code-block:: python

   from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase

   class MyCustomPolicy(PolicyBase):
       def evaluate(self):
           # Execute command to get metrics
           result = self.command.doCommand()

           # Apply custom logic
           if result['metric'] > threshold:
               return {'Status': 'Active', 'Reason': 'All good'}
           return {'Status': 'Degraded', 'Reason': 'Metric below threshold'}

**2. Configure in CS:**

.. code-block:: cfg

   Operations/Defaults/ResourceStatus/Policies
   {
     Site
     {
       all
       {
         policyName = MyCustomPolicy
       }
     }
   }

Adding Custom Commands
----------------------

**1. Create command file:**

``MyExtension/ResourceStatusSystem/Command/MyCustomCommand.py``

.. code-block:: python

   from DIRAC.ResourceStatusSystem.Command.Command import Command

   class MyCustomCommand(Command):
       def doCommand(self):
           # Fetch data from external source
           data = self.fetchFromAPI()

           # Store in cache (optional)
           self.clients['ResourceManagementClient'].addOrModifyCustomCache(data)

           return S_OK(data)

**2. Use in policy:**

Policies automatically discover and execute commands based on naming convention.

Adding Custom Actions
---------------------

**1. Create action file:**

``MyExtension/ResourceStatusSystem/PolicySystem/Actions/MyCustomAction.py``

.. code-block:: python

   from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

   class MyCustomAction(BaseAction):
       def run(self):
           element = self.decisionParams['name']
           newStatus = self.policyCombinedResult['Status']

           # Execute custom action (e.g., call external API)
           self.notifyExternalSystem(element, newStatus)

           return S_OK()

**2. Configure in CS:**

.. code-block:: cfg

   Operations/Defaults/ResourceStatus/PolicyActions += MyCustomAction

Quick Reference
===============

Key Classes
-----------

PEP
   Policy Enforcement Point - ``PolicySystem/PEP.py:73-160``

PDP
   Policy Decision Point - ``PolicySystem/PDP.py``

RSSMachine
   State machine - ``PolicySystem/StateMachine.py:28-44``

ResourceStatus
   High-level API (Singleton) - ``Client/ResourceStatus.py:42-98``

SiteStatus
   Site-specific API (Singleton) - ``Client/SiteStatus.py``

ResourceStatusDB
   Status database access - ``DB/ResourceStatusDB.py``

ResourceManagementDB
   Cache database access - ``DB/ResourceManagementDB.py``

Valid States (Priority Order)
------------------------------

1. Unknown (Level 5)
2. Active (Level 4)
3. Degraded (Level 3)
4. Probing (Level 2)
5. Banned (Level 1)
6. Error (Level 0)

Lower level numbers indicate higher priority when combining policy results.

Configuration Paths
-------------------

Policies
   ``/Operations/Defaults/ResourceStatus/Policies/<Element>/<StatusType>/policyName``

Actions
   ``/Operations/Defaults/ResourceStatus/PolicyActions``

Services
   ``/Systems/ResourceStatus/Services/``

Agents
   ``/Systems/ResourceStatus/Agents/``

.. _agents2CS:

=================================
Agents for discovery of resources
=================================

DIRAC has 2 agents that can be used for discovering and updating resources (e.g. computing elements).
These agents are the::

   Configuration/Bdii2CSAgent
   Configuration/GOCDB2CSAgent

Bdii2CSAgent
------------

The Bdii2CSAgent conacts routinely the BDII for availability of Computing and Storage Elements for a given VO,
of for a few. It detects resources not yet present in the CS and notifies the administrators via email.
It can also be configured to update the Configuration automatically.
For the CEs and SEs already present in the CS, the agent is updating the existing entries if they were changed in the BDII recently.

The Bdii2CSAgent can understand both Glue1 and Glue2 specifications.
You can install more than one of such agents in parallel: a typical case is when some sites publish information
only on Glue2, but not on Glue1.

The agent is by default installed with a `DryRun` option which is set to True.
Remove it, or set to False for automatic updating of the CS.

GOCSB2CSAgent
-------------

The GOCDB2CSAgent looks in GOCDB for published resources. As of today, the agent only looks for Perfsonar endpoints.

The agent is by default installed with a `DryRun` option which is set to True.
Remove it, or set to False for automatic updating of the CS.

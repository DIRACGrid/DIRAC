.. _pilots:

========================
DIRAC pilots
========================

This page describes what are DIRAC pilots, and how they work.
To know how to develop DIRAC pilots, please refer to the Developers documentation

The current version of pilots are sometimes dubbed as "Pilots 2.0", or "the pilots to fly in all the skies".



What's a DIRAC Pilot
====================

First of all, a definition:
- A *pilot* is what creates the possibility to run jobs on a worker node. Or, in other words:
- a script that, at a minimum, setup (VO)DIRAC, sets the local DIRAC configuration, launches the an entity for matching jobs (e.g. the JobAgent)

A pilot can be sent, as a script to be run. Or, it can be fetched.

A pilot can run on every computing resource, e.g.: on CREAM Computing elements,
on DIRAC Computing elements, on Virtual Machines in the form of contextualization script,
or IAAC (Infrastructure as a Client) provided that these machines are properly configured.

A pilot has, at a minimum, to:

- install DIRAC
- configure DIRAC
- run the JobAgent

A pilot has to run on each and every computing resource type, provided that:

- Python 2.6+ on the WN
- It is an OS onto which we can install DIRAC

The same pilot script can be used everywhere.

.. image:: ../../../_static/Systems/WMS/Pilots.png
   :alt: Pilots.
   :align: center



Definitions that help understanding what's a pilot
==================================================

- *TaskQueue*: a queue of JDLs with similar requirements.
- *JobAgent*: a DIRAC agent that matches a DIRAC local configuration with a TaskQueue, and extracts a JDL from it (or more than one).
- *pilot wrapper*: a script that wraps the pilot script with conditions for running the pilot script itself (maybe multiple times).
- *pilot job*: a pilot wrapper sent to a computing element (e.g. CREAM, ARC).

The *pilot* is a "standardized" piece of code. The *pilot wrapper* is not.

An agent like the "SiteDirector" encapsulates the *pilot* in a *pilot wrapper*, then sends it to a Computing Element as a *pilot job*.
But, if you don't have the possibility to send a pilot job (e.g. the case of a Virtual Machine in a cloud),
you can still find a way to start the pilot script by encapsulating it in a pilot wrapper that will be started at boot time,
e.g. by supplying the proper contextualization to the VM.


Administration
==============

The following CS section is used for



Pilots 2.0
===========

In case your VO only uses Grid resources, and the pilots are only sent by SiteDirector and TaksQueueDirector agents,
and you don't plan to have any specific pilot behaviour, you can stop reading here:
the new pilot won't have anything different from the old pilot that you will notice.

Instead, in case you want, for example, to install DIRAC in a different way, or you want your pilot to have some VO specific action,
you should carefully read the RFC 18, and what follows.
You should also keep reading if your resources include IAAS and IAAC type of resources, like Virtual Machines.

The files to consider are in https://github.com/DIRACGrid/DIRAC/tree/rel-v6r12/WorkloadManagementSystem/PilotAgent
The main file in which you should look is
https://github.com/DIRACGrid/DIRAC/blob/rel-v6r12/WorkloadManagementSystem/PilotAgent/dirac-pilot.py
that also contains a good explanation on how the system works.

The system works with "commands", as explained in the RFC. Any command can be added.
If your command is executed before the "InstallDIRAC" command, pay attention that DIRAC functionalities won't be available.

We have introduced a special command named "GetPilotVersion"
in https://github.com/DIRACGrid/DIRAC/blob/rel-v6r12/WorkloadManagementSystem/PilotAgent/pilotCommands.py that you should use,
and possibly extend, in case you want to send/start pilots that don't know beforehand the (VO)DIRAC version they are going to install.
In this case, you have to provide a json file freely accessible that contains the pilot version.
This is tipically the case for VMs in IAAS and IAAC.

Beware that, to send pilots containing a specific list of commands via SiteDirector agents need a SiteDirector extension.

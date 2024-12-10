.. _configurationSystem:

====================
Configuration System
====================

The configuration system serves the configuration to any other client (be it another server or a standard client).
The infrastructure is controller/worker based.

**********
Controller
**********

The controller Server holds the central configuration in a local file. This file is then served to the clients, and synchronized with the worker servers.

the controller server also regularly pings the worker servers to make sure they are still alive. If not, they are removed from the list of CS.

When changes are committed to the controller, a backup of the existing configuration file is made in ``etc/csbackup``.

*******
Workers
*******

worker server registers themselves to the controller when starting.
They synchronize their configuration on a regular bases (every 5 minutes by default).
Note that the worker CS do not hold the configuration in a local file, but only in memory.

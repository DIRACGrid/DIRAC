.. _configurationSystem:

====================
Configuration System
====================

The configuration system serves the configuration to any other client (be it another server or a standard client).
The infrastructure is master/slave based.

******
Master
******

The master Server holds the central configuration in a local file. This file is then served to the clients, and synchronized with the slave servers.

the master server also regularly pings the slave servers to make sure they are still alive. If not, they are removed from the list of CS.

When changes are committed to the master, a backup of the existing configuration file is made in ``etc/csbackup``.

******
Slaves
******

Slave server registers themselves to the master when starting.
They synchronize their configuration on a regular bases (every 5 minutes by default).
Note that the slave CS do not hold the configuration in a local file, but only in memory.
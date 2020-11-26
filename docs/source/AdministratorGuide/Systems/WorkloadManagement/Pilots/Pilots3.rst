.. _pilot3:

==============
DIRAC pilots 3
==============

All concepts defined for Pilot2 are valid also for Pilot3. There are anyway some differences for what regards their usage.

.. meta::
   :keywords: Pilots3, Pilot3, Pilot


Bootstrap
=========

Pilot3 needs a JSON file for bootstrapping. We simply call this file the ``pilot.json`` file.
The ``pilot.json`` file is created starting from information found in the Configuration Service.

The pilot wrapper (the script that starts the pilot, which is effectively equivalent to what SiteDirectors send)
expects to find and download such a ``pilot.json`` file from a known location, or a set of them.
Such a location should be exposed via *https://* by, for example, the DIRAC WebApp webserver.

The (list of) location(s) has to be added in the Operations option *Pilot/pilotFileServer*.
If more than one location is used, add them as a list.
We suggest to add at least the URL of the DIRAC WebApp webserver, but multiple locations are also possible, and advised.

The pilot.json file is always kept in sync with the content of the Configuration Service.
At every configuration update, the pilot.json file content will also be updated.


Starting with DIRAC version v7r2, the file uploads is completely on the balancer (nginx) side.
Make sure your balancer is set to load files by following the :ref:`instructions <configure_nginx>`.

  

Other options that can be set also in the Operations part of the CS include:

+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotRepo*                        | Pointer to git repository of DIRAC pilot   | pilotRepo = https://github.com/DIRACGrid/Pilot.git                      |
|                                    |                                            | The value above is the default                                          |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotVORepo*                      | Pointer to git repository of VO DIRAC      | pilotVORepo = https://github.com/MyDIRAC/VOPilot.git                    |
|                                    | extension of pilot.                        |                                                                         |
|                                    | This option is needed in case you have an  |                                                                         |
|                                    | extension of the pilot                     |                                                                         |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotScriptsPath*                 | Path to the code, inside the Git repository| pilotScriptsPath = Pilot                                                |
|                                    |                                            | This value is the default                                               |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotVOScriptsPath*               | Path to the code, inside the Git repository| pilotVOScriptsPath = VOPilot                                            |
|                                    | of the pilot extension                     |                                                                         |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotRepoBranch*                  | Branch to use, inside the Git repository,  | pilotRepoBranch = master                                                |
|                                    | of the pilot code to be used               | The value above is the default                                          |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotVORepoBranch*                | Branch to use, inside the Git repository,  | pilotVORepoBranch = master                                              |
|                                    | of the pilot code extension to be used     | The value above is the default                                          |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *uploadToWebApp*                   | Whether to try to upload the files to the  | uploadToWebApp = True                                                   |
|                                    | list of server specified                   | The value above is the default                                          |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *workDir*                          | Local directory of the master CS where the | workDir = /tmp/pilotSyncDir                                             |
|                                    | files will be downloaded before the upload | There is no default (so /opt/dirac/runit/Configuration/Server)          |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+

Since DIRAC v7r0, SiteDirectors send "pilots3".


Pilots environment
==================

There is a small number of environment variables that can be set by the pilot to control the behaviour of DIRAC jobs.

DIRAC_PROCESSORS
  The number of processors the pilot can use

DIRAC_JOB_PROCESSORS
  The number of processors the jobs administered by the pilot can use (a subset of DIRAC_PROCESSORS)

DIRAC_WHOLENODE
  A boolean flag indicating the pilot is exploiting the whole node (normally, False)

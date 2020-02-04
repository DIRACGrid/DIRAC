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


If you use the DIRAC webserver please

- add the following option to the WebApp CS section::
       
    /WebApp/StaticDirs=pilot
       
- create the following directory in the DIRAC webserver machine::
   
    mkdir /opt/dirac/webRoot/www/pilot/
  

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
| *pilotVOScriptsPath*               | Path to the code, inside the Git repository| pilotScriptsVOPath = VOPilot                                            |
|                                    | of the pilot extension                     |                                                                         |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotRepoBranch*                  | Branch to use, inside the Git repository,  | pilotRepoBranch = master                                                |
|                                    | of the pilot code to be used               | The value above is the default                                          |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotVORepoBranch*                | Branch to use, inside the Git repository,  | pilotVORepoBranch = master                                              |
|                                    | of the pilot code extension to be used     | The value above is the default                                          |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+


Starting the old Pilot 2 via SiteDirectors
==========================================

Since DIRAC v7r0, SiteDirectors will send by default "pilots3".
To still send Pilot 2 type of pilots, the Pilot3 flag should be set explicitely to False
(see :mod:`~DIRAC.WorkloadManagementSystem.Agent.SiteDirector`).

It should be anyway noted that "Pilot 2" are not maintained anymore, and that their code will be removed in a future version of DIRAC.

In this case, the option ``Operations/[Defaults | Setup]/Pilot/UpdatePilotCStoJSONFile`` could be also set to False.


Pilot logging
=============

Advanced pilot logging comes together with Pilot3. To enable... <to complete>

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
Such a location can be, for example, exposed via *https://* by the DIRAC WebApp webserver. Other protocols (including *file://*) are possible.

The pilot.json file is always kept in sync with the content of the Configuration Service.
At every configuration update, the pilot.json file content will also be updated (if necessary).

Also the Operations option *Pilot/<...>/pilotFileServer* should be set to the webserver(s) chosen for the upload.
If more than one location are used, add them as a list.
We suggest to use simply the DIRAC webserver, but multiple locations are also possible, and advised.

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
|                                    | extension of pilot                         |                                                                         |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotScriptsPath*                 | Path to the code, inside the Git repository| pilotScriptsPath = Pilot                                                |
|                                    |                                            | This value is the default                                               |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *pilotScriptsVOPath*               | Path to the code, inside the Git repository| pilotScriptsVOPath = VOPilot                                            |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+


Starting the old Pilot 2 via SiteDirectors
==========================================

Since DIRAC v7r0, SiteDirectors will send by default "pilots3".
To still send Pilot 2 type of pilots, the Pilot3 flag should be set explicitely to False
(see :mod:`~DIRAC.WorkloadManagementSystem.Agent.SiteDirector`).


Pilot logging
=============

Advanced pilot logging comes together with Pilot3. To enable... <to complete>

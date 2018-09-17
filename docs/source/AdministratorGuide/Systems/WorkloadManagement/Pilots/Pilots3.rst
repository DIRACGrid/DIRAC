.. _pilots3:

========================
DIRAC pilots 3
========================

All concepts defined for Pilots 2 are valid also for Pilots3. There are anyway some differences for what regards their usage.


Bootstrap
=========

Pilots3 need a JSON file for bootstrapping. We simply call this file the *pilot.json* file.
The pilot.json file is created starting from information found in the Configuration Service.

The pilot wrapper (the script that starts the pilot, which is effectively equivalent to what SiteDirectors send)
expects to find (download) such pilot.json file from a known location, which can be for example exposed by the DIRAC WebApp webserver.

The pilot.json file is therefore always kept in sync with the content of the Configuration Service.
From DIRAC v6r20, there is the possibility to set the option *UpdatePilotCStoJSONFile* to True in the configuration of
the Configuration/Server service (please see :ref:`ConfigurationServer` for detais). If this option is set,
at every configuration update, the pilot.json file content will also be updated (if necessary).

If *UpdatePilotCStoJSONFile* is True, then also the option *pilotFileServer* should be set to the webserver chosen for the upload.
We suggest to use simply the DIRAC webserver.


Starting Pilots3 via SiteDirectors
==================================

.. versionadded:: v6r20


Since DIRAC v6r20, SiteDirectors can send "pilots2" or "pilots3". Pilots2 are the default, 
but the "Pilots3" flag can be used for sending Pilots3 files instead, see :ref:`conf-SiteDirector`


Pilot logging
=============

Advanced pilot logging comes together with Pilots3. To enable... <to complete>

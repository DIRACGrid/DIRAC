.. _pilots3:

========================
DIRAC pilots 3
========================

All concepts defined for Pilots 2 are valid also for Pilots3. There are anyway some differences for what regards their usage.


Bootstrap
=========

Pilots3 need a JSON file for bootstrapping. We call this file the pilot.json file.
The pilot.json file is created starting from information found in the Configuration Service.
The pilot wrapper (the script that starts the pilot, which is effectively equivalent to what SiteDirectors send) 
expects to find (download) such pilot.json file from a known location, which can be for example exposed by the DIRAC WebApp webserver.

The pilot.json file is therefore always kept in sync with the content of the Configuration Service. 
To enable the creation of the pilot.json file please modify the following CS option: 

<insert here once decided>

Pilot logging
=============

Advanced pilot logging comes together with Pilots3. To enable... <to complete>

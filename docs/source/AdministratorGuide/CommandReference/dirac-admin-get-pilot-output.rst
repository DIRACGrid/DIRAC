.. _admin_dirac-admin-get-pilot-output:

============================
dirac-admin-get-pilot-output
============================

Retrieve output of a Grid pilot

Usage::

  dirac-admin-get-pilot-output [option|cfgfile] ... PilotID ...

Arguments::

  PilotID:  Grid ID of the pilot

Example::

  $ dirac-admin-get-pilot-output https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
  $ ls -la
  drwxr-xr-x  2 hamar marseill     2048 Feb 21 14:13 pilot_26KCLKBFtxXKHF4_ZrQjkw

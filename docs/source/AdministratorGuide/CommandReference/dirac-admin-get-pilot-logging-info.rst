.. _admin_dirac-admin-get-pilot-logging-info:

==================================
dirac-admin-get-pilot-logging-info
==================================

Retrieve logging info of a Grid pilot

Usage::

  dirac-admin-get-pilot-logging-info [option|cfgfile] ... PilotID ...

Arguments::

  PilotID:  Grid ID of the pilot

Example::

  $ dirac-admin-get-pilot-logging-info https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
  Pilot Reference: %s https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
  ===================== glite-job-logging-info Success =====================

  LOGGING INFORMATION:

  Printing info for the Job : https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw

      ---
  Event: RegJob
  - Arrived                    =    Mon Feb 21 13:27:50 2011 CET
  - Host                       =    marwms.in2p3.fr
  - Jobtype                    =    SIMPLE
  - Level                      =    SYSTEM
  - Ns                         =    https://marwms.in2p3.fr:7443/glite_wms_wmproxy_server
  - Nsubjobs                   =    0
  - Parent                     =    https://marlb.in2p3.fr:9000/WQHVOB1mI4oqrlYz2ZKtgA
  - Priority                   =    asynchronous
  - Seqcode                    =    UI=000000:NS=0000000001:WM=000000:BH=0000000000:JSS=000000:LM=000000:LRMS=000000:APP=000000:LBS=000000
  - Source                     =    NetworkServer

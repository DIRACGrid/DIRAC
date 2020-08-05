.. _admin_dirac-admin-get-pilot-info:

==========================
dirac-admin-get-pilot-info
==========================

Retrieve available info about the given pilot

Usage::

  dirac-admin-get-pilot-info [option|cfgfile] ... PilotID ...

Arguments::

  PilotID:  Grid ID of the pilot

Options::

  -e  --extended               : Get extended printout

Example::

  $  dirac-admin-get-pilot-info https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
  {'https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw': {'AccountingSent': 'False',
                                                          'BenchMark': 0.0,
                                                          'Broker': 'marwms.in2p3.fr',
                                                          'DestinationSite': 'cclcgceli01.in2p3.fr',
                                                          'GridSite': 'LCG.IN2P3.fr',
                                                          'GridType': 'gLite',
                                                          'LastUpdateTime': datetime.datetime(2011, 2, 21, 12, 49, 14),
                                                          'OutputReady': 'False',
                                                          'OwnerDN': '/O=GRID-FR/C=FR/O=CNRS/OU=LPC/CN=Sebastien Guizard',
                                                          'OwnerGroup': '/biomed',
                                                          'ParentID': 0L,
                                                          'PilotID': 2241L,
                                                          'PilotJobReference': 'https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw',
                                                          'PilotStamp': '',
                                                          'Status': 'Done',
                                                          'SubmissionTime': datetime.datetime(2011, 2, 21, 12, 27, 52),
                                                          'TaskQueueID': 399L}}

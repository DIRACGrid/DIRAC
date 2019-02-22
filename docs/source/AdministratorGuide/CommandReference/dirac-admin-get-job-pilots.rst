.. _admin_dirac-admin-get-job-pilots:

==========================
dirac-admin-get-job-pilots
==========================

Retrieve info about pilots that have matched a given Job

Usage::

  dirac-admin-get-job-pilots [option|cfgfile] ... JobID

Arguments::

  JobID:    DIRAC ID of the Job

Example::

  $ dirac-admin-get-job-pilots 1848
  {'https://marlb.in2p3.fr:9000/bqYViq6KrVgGfr6wwgT45Q': {'AccountingSent': 'False',
                                                          'BenchMark': 8.1799999999999997,
                                                          'Broker': 'marwms.in2p3.fr',
                                                          'DestinationSite': 'lpsc-ce.in2p3.fr',
                                                          'GridSite': 'LCG.LPSC.fr',
                                                          'GridType': 'gLite',
                                                          'Jobs': [1848L],
                                                          'LastUpdateTime': datetime.datetime(2011, 2, 21, 12, 39, 10),
                                                          'OutputReady': 'True',
                                                          'OwnerDN': '/O=GRID-FR/C=FR/O=CNRS/OU=LPC/CN=Sebastien Guizard',
                                                          'OwnerGroup': '/biomed',
                                                          'ParentID': 0L,
                                                          'PilotID': 2247L,
                                                          'PilotJobReference': 'https://marlb.in2p3.fr:9000/bqYViq6KrVgGfr6wwgT45Q',
                                                          'PilotStamp': '',
                                                          'Status': 'Done',
                                                          'SubmissionTime': datetime.datetime(2011, 2, 21, 12, 27, 52),
                                                          'TaskQueueID': 399L}}

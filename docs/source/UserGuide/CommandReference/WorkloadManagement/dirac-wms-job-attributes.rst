.. _dirac-wms-job-attributes:

========================
dirac-wms-job-attributes
========================

Retrieve attributes associated with the given DIRAC job

Usage::

  dirac-wms-job-attributes [option|cfgfile] ... JobID ...

Arguments::

  JobID:    DIRAC Job ID

Example::

  $ dirac-wms-job-attributes  1
  {'AccountedFlag': 'False',
   'ApplicationNumStatus': '0',
   'ApplicationStatus': 'Unknown',
   'CPUTime': '0.0',
   'DIRACSetup': 'EELA-Production',
   'DeletedFlag': 'False',
   'EndExecTime': '2011-02-14 11:28:01',
   'FailedFlag': 'False',
   'HeartBeatTime': '2011-02-14 11:28:01',
   'ISandboxReadyFlag': 'False',
   'JobGroup': 'NoGroup',
   'JobID': '1',
   'JobName': 'DIRAC_vhamar_602138',
   'JobSplitType': 'Single',
   'JobType': 'normal',
   'KilledFlag': 'False',
   'LastUpdateTime': '2011-02-14 11:28:11',
   'MasterJobID': '0',
   'MinorStatus': 'Execution Complete',
   'OSandboxReadyFlag': 'False',
   'Owner': 'vhamar',
   'OwnerDN': '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar',
   'OwnerGroup': 'eela_user',
   'RescheduleCounter': '0',
   'RescheduleTime': 'None',
   'RetrievedFlag': 'False',
   'RunNumber': '0',
   'Site': 'EELA.UTFSM.cl',
   'StartExecTime': '2011-02-14 11:27:48',
   'Status': 'Done',
   'SubmissionTime': '2011-02-14 10:12:40',
   'SystemPriority': '0',
   'UserPriority': '1',
   'VerifiedFlag': 'True'}

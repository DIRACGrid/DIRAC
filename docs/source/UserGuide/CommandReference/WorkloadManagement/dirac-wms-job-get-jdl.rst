.. _dirac-wms-job-get-jdl:

=====================
dirac-wms-job-get-jdl
=====================

Retrieve the current JDL of a DIRAC job

Usage::

  dirac-wms-job-get-jdl [option|cfgfile] ... JobID ...

Arguments::

  JobID:    DIRAC Job ID

Options::

  -O  --Original               : Gets the original JDL

Example::

  $ dirac-wms-job-get-jdl 1
  {'Arguments': '-ltrA',
   'CPUTime': '86400',
   'DIRACSetup': 'EELA-Production',
   'Executable': '/bin/ls',
   'JobID': '1',
   'JobName': 'DIRAC_vhamar_602138',
   'JobRequirements': '[             OwnerDN = /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar;            OwnerGroup = eela_user;            Setup = EELA-Production;            UserPriority = 1;            CPUTime = 0        ]',
   'OutputSandbox': ['std.out', 'std.err'],
   'Owner': 'vhamar',
   'OwnerDN': '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar',
   'OwnerGroup': 'eela_user',
   'OwnerName': 'vhamar',
   'Priority': '1'}

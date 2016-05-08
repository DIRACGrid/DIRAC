==================================
dirac-admin-select-requests
==================================

  Select requests from the request management system

Usage::

  dirac-admin-select-requests [option|cfgfile] ... 

 

Options::

  -    --JobID=          : WMS JobID for the request (if applicable) 

  -    --RequestID=      : ID assigned during submission of the request 

  -    --RequestName=    : XML request file name 

  -    --RequestType=    : Type of the request e.g. 'transfer' 

  -    --Status=         : Request status 

  -    --Operation=      : Request operation e.g. 'replicateAndRegister' 

  -    --RequestStart=   : First request to consider (start from 0 by default) 

  -    --Limit=          : Selection limit (default 100) 

  -    --OwnerDN=        : DN of owner (in double quotes) 

  -    --OwnerGroup=     : Owner group 

Example::

  $ dirac-admin-select-requests
  9 request(s) selected with conditions  and limit 100
  ['RequestID', 'RequestName', 'JobID', 'OwnerDN', 'OwnerGroup', 'RequestType', 'Status', 'Operation', 'Error', 'CreationTime', 'LastUpdateTime']
  ['1', 'LFNInputData_44.xml', '44', '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar', 'dirac_user', 'diset', 'Waiting', 'setJobStatusBulk', 'None',   '2010-12-08 22:27:07', '2010-12-08 22:27:08']
  ['1', 'LFNInputData_44.xml', '44', '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar', 'dirac_user', 'diset', 'Waiting', 'setJobParameters', 'None', '2010-12-08 22:27:07', '2010-12-08 22:27:08']
  ['2', 'API_2_23.xml', '23', '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar', 'dirac_user', 'diset', 'Waiting', 'setJobParameters', 'None', '2010-12-08 22:27:07', '2010-12-08 22:27:09']
  ['3', 'API_19_42.xml', '42', '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar', 'dirac_user', 'diset', 'Waiting', 'setJobStatusBulk', 'None', '2010-12-08 22:27:07', '2010-12-08 22:27:09']
  ['3', 'API_19_42.xml', '42', '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar', 'dirac_user', 'diset', 'Waiting', 'setJobParameters', 'None', '2010-12-08 22:27:07', '2010-12-08 22:27:09']
  ['4', 'Accounting.DataStore.1293829522.01.0.145174243188', 'None', 'Unknown', 'Unknown', 'diset', 'Waiting', 'commitRegisters', 'None', '2010-12-31 21:05:22', '2010-12-31 21:56:49']
  ['5', 'Accounting.DataStore.1293840021.45.0.74714473302', 'None', 'Unknown', 'Unknown', 'diset', 'Waiting', 'commitRegisters', 'None', '2011-01-01 00:00:21', '2011-01-01 00:05:39']
  ['6', '1057.xml', '1057', '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar', 'dirac_user', 'register', 'Waiting', 'registerFile', 'None', '2011-01-31 13:31:46', '2011-01-31 13:31:53']
  ['7', '1060.xml', '1060', '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar', 'dirac_user', 'register', 'Waiting', 'registerFile', 'None', '2011-01-31 13:42:33', '2011-01-31 13:42:36']



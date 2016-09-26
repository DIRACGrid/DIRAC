=========================
dirac-dls-sequence
=========================

  Get sequences from Data Logging System Database

Usage::

  dirac-dls-sequence [option|cfgfile] ... LFN 

Arguments::

  LFN:      Logical File Name

Example::

  $ dirac-dls-sequence /lhcb/sub/file2.data
  Sequence 1, Caller dirac-dms-add-file.py , UserName coberger , HostName localhost.localdomain , Group devGroup 
	  DataManager.putAndRegister, 2015-08-19 11:50:13, Successful, TargetSE StorageTest1
		  DataManager.registerFile, 2015-08-19 11:50:14, Successful, TargetSE StorageTest1
			  FileCatalog.addFile, 2015-08-19 11:50:14, Successful, TargetSE StorageTest1
		  StorageElement.putFile, 2015-08-19 11:50:14, Successful, TargetSE StorageTest1
 
=========================
dirac-dms-method-call
=========================

  Get method calls from Data Logging System Database

Usage::

  dirac-dms-method-call [option|cfgfile] ... LFN 

Arguments::

  LFN:      Logical File Name

Example::

  $ dirac-dls-method-call /lhcb/sub/file2.data
	Found 8 method calls
	2015-08-19 11:50:13, DataManager.putAndRegister, TargetSE StorageTest1, Successful, SequenceID 1
	2015-08-19 11:50:14, StorageElement.putFile, TargetSE StorageTest1, Successful, SequenceID 1
	2015-08-19 11:50:14, FileCatalog.addFile, TargetSE StorageTest1, Successful, SequenceID 1
  2015-08-19 11:50:14, DataManager.registerFile, TargetSE StorageTest1, Successful, SequenceID 1
  2015-08-19 12:38:20, StorageElement.removeFile, TargetSE StorageTest1, Successful, SequenceID 2
	2015-08-19 12:38:20, FileCatalog.removeReplica, TargetSE StorageTest1, Successful, SequenceID 2
	2015-08-19 12:38:20, FileCatalog.removeFile, Successful, SequenceID 2
	2015-08-19 12:38:20, DataManager.removeFile, Successful, SequenceID 2
  



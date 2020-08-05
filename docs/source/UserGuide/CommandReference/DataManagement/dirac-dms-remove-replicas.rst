.. _dirac-dms-remove-replicas:

=========================
dirac-dms-remove-replicas
=========================

Remove the given file replica or a list of file replicas from the File Catalog
and from the storage.

Usage::

   dirac-dms-remove-replicas <LFN | fileContainingLFNs> SE [SE]

Example::

  $ dirac-dms-remove-replicas /formation/user/v/vhamar/Test.txt IBCP-disk
  Successfully removed DIRAC-USER replica of /formation/user/v/vhamar/Test.txt

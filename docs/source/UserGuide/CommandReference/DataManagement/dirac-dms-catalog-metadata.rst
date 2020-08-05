.. _dirac-dms-catalog-metadata:

==========================
dirac-dms-catalog-metadata
==========================

Get metadata for the given file specified by its Logical File Name or for a list of files
contained in the specifed file

Usage::

   dirac-dms-catalog-metadata <lfn | fileContainingLfns> [Catalog]

Example::

  $ dirac-dms-catalog-metadata /formation/user/v/vhamar/Example.txt
  FileName                                     Size        GUID                                     Status   Checksum
  /formation/user/v/vhamar/Example.txt         34          EDE6DDA4-3344-3F39-A993-8349BA41EB23     1        eed20d47

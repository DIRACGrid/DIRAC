.. _dirac-dms-clean-directory:

=========================
dirac-dms-clean-directory
=========================

Clean the given directory or a list of directories by removing it and all the
contained files and subdirectories from the physical storage and from the
file catalogs.

Usage::

   dirac-dms-clean-directory <LFN_Directory | fileContainingLFN_Directories>

Example::

  $ dirac-dms-clean-directory /formation/user/v/vhamar/newDir
  Cleaning directory /formation/user/v/vhamar/newDir ...  OK

.. _dirac-dms-data-size:

===================
dirac-dms-data-size
===================

Get the size of the given file or a list of files

Usage::

   dirac-dms-data-size <lfn | fileContainingLfns> <SE> <status>

Options::

  -u  --Unit <value>           :    Unit to use [default GB] (MB,GB,TB,PB)

Example::

  $ dirac-dms-data-size  /formation/user/v/vhamar/Example.txt
  ------------------------------
  Files          |      Size (GB)
  ------------------------------
  1              |            0.0
  ------------------------------

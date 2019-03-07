.. _admin_dirac-stager-show-stats:

=======================
dirac-stager-show-stats
=======================

Reports breakdown of file(s) number/size in different staging states across Storage Elements.
Currently used Cache per SE is also reported. (active pins)

Example::

  dirac-stager-show-stats

   Status               SE                   NumberOfFiles        Size(GB)
  --------------------------------------------------------------------------
   Staged               GRIDKA-RDST          1                    4.5535
   StageSubmitted       GRIDKA-RDST          5                    22.586
   Waiting              PIC-RDST             3                    13.6478

  WARNING: the Size for files with Status=New is not yet determined at the point of selection!

   --------------------- current status of the SE Caches from the DB-----------
   GRIDKA-RDST    :      6 replicas with a size of 29.141 GB.

.. _dirac-dms-user-lfns:

===================
dirac-dms-user-lfns
===================

Get the list of all the user files.

Usage::

  dirac-dms-user-lfns [option|cfgfile] ...

Options::

  -D  --Days <value>           : Match files older than number of days [0]
  -M  --Months <value>         : Match files older than number of months [0]
  -Y  --Years <value>          : Match files older than number of years [0]
  -w  --Wildcard <value>       : Wildcard for matching filenames [All]
  -b  --BaseDir <value>        : Base directory to begin search (default /[vo]/user/[initial]/[username])
  -e  --EmptyDirs              : Create a list of empty directories

Example::

  $ dirac-dms-user-lfns
  /formation/user/v/vhamar: 14 files, 6 sub-directories
  /formation/user/v/vhamar/newDir2: 0 files, 0 sub-directories
  /formation/user/v/vhamar/testDir: 0 files, 0 sub-directories
  /formation/user/v/vhamar/0: 0 files, 6 sub-directories
  /formation/user/v/vhamar/test: 0 files, 0 sub-directories
  /formation/user/v/vhamar/meta-test: 0 files, 0 sub-directories
  /formation/user/v/vhamar/1: 0 files, 4 sub-directories
  /formation/user/v/vhamar/0/994: 1 files, 0 sub-directories
  /formation/user/v/vhamar/0/20: 1 files, 0 sub-directories
  /formation/user/v/vhamar/0/998: 1 files, 0 sub-directories
  /formation/user/v/vhamar/0/45: 1 files, 0 sub-directories
  /formation/user/v/vhamar/0/16: 0 files, 0 sub-directories
  /formation/user/v/vhamar/0/11: 1 files, 0 sub-directories
  /formation/user/v/vhamar/1/1004: 1 files, 0 sub-directories
  /formation/user/v/vhamar/1/1026: 1 files, 0 sub-directories
  /formation/user/v/vhamar/1/1133: 1 files, 0 sub-directories
  /formation/user/v/vhamar/1/1134: 0 files, 0 sub-directories
  22 matched files have been put in formation-user-v-vhamar.lfns

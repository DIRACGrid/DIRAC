.. _dirac-dms-add-file:

==================
dirac-dms-add-file
==================

Upload a file to the grid storage and register it in the File Catalog

Usage::

  dirac-dms-add-file [option|cfgfile] ... LFN Path SE [GUID]

Arguments::

  LFN:      Logical File Name
  Path:     Local path to the file
  SE:       DIRAC Storage Element
  GUID:     GUID to use in the registration (optional)

**OR**

Usage::

  dirac-dms-add-file [option|cfgfile] ... LocalFile

Arguments::

  LocalFile: Path to local file containing all the above, i.e.::

  lfn1 localfile1 SE [GUID1]
  lfn2 localfile2 SE [GUID2]

Options::

  -f  --force                  : Force overwrite of existing file

Example::

  $ dirac-dms-add-file LFN:/formation/user/v/vhamar/Example.txt Example.txt DIRAC-USER
  {'Failed': {},
   'Successful': {'/formationes/user/v/vhamar/Example.txt': {'put': 0.70791220664978027,
                                                             'register': 0.61061787605285645}}}

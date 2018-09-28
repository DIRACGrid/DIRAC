========================
dirac-dms-directory-sync
========================

Provides basic rsync functionality for DIRAC

Usage::

  dirac-dms-directory-sync Source Destination

 e.g.: Download
   dirac-dms-directory-sync LFN Path
  or Upload
   dirac-dms-directory-sync Path LFN SE

Arguments::

  LFN:           Logical File Name (Path to directory)
  Path:          Local path to the file (Path to directory)
  SE:            DIRAC Storage Element



Options::

  -D  --sync                   : Make target directory identical to source
  -j  --parallel <value>       : Multithreaded download and upload

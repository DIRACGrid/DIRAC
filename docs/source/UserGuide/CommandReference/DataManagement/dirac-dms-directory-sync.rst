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

General options::

  -o  --option <value>         : Option=value to add
  -s  --section <value>        : Set base section for relative parsed options
  -c  --cert <value>           : Use server certificate to connect to Core Services
  -d  --debug                  : Set debug mode (-ddd is extra debug)
  -   --autoreload             : Automatically restart if there's any change in the module
  -   --license                : Show DIRAC's LICENSE
  -h  --help                   : Shows this help

Options::

  -D  --sync                   : Make target directory identical to source
  -j  --parallel <value>       : Multithreaded download and upload

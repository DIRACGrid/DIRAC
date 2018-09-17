===================
dirac-dms-find-lfns
===================

Find files in the FileCatalog using file metadata
Usage::

  dirac-dms-find-lfns [options] metaspec [metaspec ...]

Arguments::

 metaspec:    metadata index specification (of the form: "meta=value" or "meta<value", "meta!=value", etc.)

Examples::

  $ dirac-dms-find-lfns Path=/lhcb/user "Size>1000" "CreationDate<2015-05-15"

General options::

  -o  --option <value>         : Option=value to add
  -s  --section <value>        : Set base section for relative parsed options
  -c  --cert <value>           : Use server certificate to connect to Core Services
  -d  --debug                  : Set debug mode (-ddd is extra debug)
  -   --autoreload             : Automatically restart if there's any change in the module
  -   --license                : Show DIRAC's LICENSE
  -h  --help                   : Shows this help

Options::

  -   --Path=                  :     Path to search for
  -   --SE=                    :     (comma-separated list of) SEs/SE-groups to be searched

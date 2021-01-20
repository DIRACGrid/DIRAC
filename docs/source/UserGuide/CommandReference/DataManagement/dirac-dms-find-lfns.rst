.. _dirac-dms-find-lfns:

===================
dirac-dms-find-lfns
===================

Find files in the FileCatalog using file metadata

Usage::

  dirac-dms-find-lfns [options] metaspec [metaspec ...]

Arguments::

 metaspec:    metadata index specification               (of the form: "meta=value" or "meta<value", "meta!=value", etc.)

Examples::

  $ dirac-dms-find-lfns Path=/lhcb/user "Size>1000" "CreationDate<2015-05-15"

Options::

  --Path=                  :     Path to search for
  --SE=                    :     (comma-separated list of) SEs/SE-groups to be searched

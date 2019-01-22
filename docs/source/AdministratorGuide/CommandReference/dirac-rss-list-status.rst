=====================
dirac-rss-list-status
=====================

DIRAC v6r20-pre17

Script that dumps the DB information for the elements into the standard output.
If returns information concerning the StatusType and Status attributes.

Usage::

  dirac-rss-list-status [options]

Verbosity::

    -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..

Options::

  --element=               : Element family to be Synchronized ( Site, Resource or Node )
  --elementType=           : ElementType narrows the search; None if default
  --name=                  : ElementName; None if default
  --tokenOwner=            : Owner of the token; None if default
  --statusType=            : StatusType; None if default
  --status=                : Status; None if default

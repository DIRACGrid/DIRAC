.. _admin_dirac-rss-query-db:

==================
dirac-rss-query-db
==================

DIRAC version: v6r20-pre17

Script that dumps the DB information for the elements into the standard output.
If returns information concerning the StatusType and Status attributes.

Usage::

  dirac-rss-query-db [option] <query> <element> <tableType>

Arguments::

  Queries: [select|add|modify|delete]
  Elements: [site|resource|component|node]
  TableTypes: [status|log|history]

Verbosity::

  -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..

Options::

  --element=               : Element family to be Synchronized ( Site, Resource, Node )
  --tableType=             : A valid table type (Status, Log, History)
  --name=                  : ElementName (comma separated list allowed); None if default
  --statusType=            : A valid StatusType argument (it admits a comma-separated list of statusTypes); None if default
  --status=                : A valid Status argument ( Active, Probing, Degraded, Banned, Unknown, Error ); None if default
  --elementType=           : ElementType narrows the search; None if default
  --reason=                : Decision that triggered the assigned status
  --lastCheckTime=         : Time-stamp setting last time the status & status were checked
  --tokenOwner=            : Owner of the token ( to specify only with select/delete queries

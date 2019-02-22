.. _admin_dirac-rss-set-status:

====================
dirac-rss-set-status
====================

DIRAC v6r20-pre17

Script that facilitates the modification of a element through the command line.
However, the usage of this script will set the element token to the command
issuer with a duration of 1 day.

Options::

  --element=               : Element family to be Synchronized ( Site, Resource or Node )
  --name=                  : Name (or comma-separeted list of names) of the element where the change applies
  --statusType=            : StatusType (or comma-separeted list of names), if none applies to all possible statusTypes
  --status=                : Status to be changed
  --reason=                : Reason to set the Status

.. _admin_dirac-install-component:

=======================
dirac-install-component
=======================

Do the initial installation and configuration of a DIRAC component

Usage::

  dirac-install-component [option|cfgfile] ... System Component|System/Component

Arguments::

  System:  Name of the DIRAC system (ie: WorkloadManagement)
  Service: Name of the DIRAC component (ie: Matcher)

Options::

  -w  --overwrite              : Overwrite the configuration in the global CS
  -m  --module <value>         : Python module name for the component code
  -p  --parameter <value>      : Special component option

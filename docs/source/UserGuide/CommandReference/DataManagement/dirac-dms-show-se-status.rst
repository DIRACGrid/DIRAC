.. _dirac-dms-show-se-status:

========================
dirac-dms-show-se-status
========================

Get status of the available Storage Elements

Usage::

  dirac-dms-show-se-status [<options>]

Options::

  -V  --vo <value>             : Virtual Organization
  -a  --all                    : All Virtual Organizations flag
  -n  --noVO                   : No Virtual Organizations assigned flag

Example::

  $ dirac-dms-show-se-status
  Storage Element               Read Status    Write Status
  DIRAC-USER                         Active          Active
  IN2P3-disk                         Active          Active
  IPSL-IPGP-disk                     Active          Active
  IRES-disk                        InActive        InActive
  M3PEC-disk                         Active          Active
  ProductionSandboxSE                Active          Active

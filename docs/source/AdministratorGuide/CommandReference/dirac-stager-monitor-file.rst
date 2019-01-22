=========================
dirac-stager-monitor-file
=========================

Give monitoring information regarding a staging file uniquely identified with (LFN,SE)

- status
- last update
- jobs requesting this file to be staged
- SRM requestID
- pin expiry time
- pin length

Usage::

  dirac-stager-monitor-file  LFN SE ...

Arguments::

  LFN: LFN of the staging file
  SE: Storage Element for the staging file

.. _dirac-wms-job-kill:

==================
dirac-wms-job-kill
==================

Issue a kill signal to a running DIRAC job

Usage::

  dirac-wms-job-kill [option|cfgfile] ... JobID ...

Arguments::

  JobID:    DIRAC Job ID

Example::

  $ dirac-wms-job-kill 1918
  Killed job 1918

.. Note::

  - jobs will not disappear from JobDB until JobCleaningAgent has deleted them
  - jobs will be deleted "immediately" if they are in the status 'Deleted'
  - USER jobs will be deleted after a grace period if they are in status Killed, Failed, Done

  What happens when you hit the "kill job" button

  - if the job is in status 'Running', 'Matched', 'Stalled' it will be properly killed, and then its
    status will be marked as 'Killed'
  - otherwise, it will be marked directly as 'Killed'.

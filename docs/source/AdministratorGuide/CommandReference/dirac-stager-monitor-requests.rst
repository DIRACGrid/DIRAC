=============================
dirac-stager-monitor-requests
=============================

Report the details of file staging requests, based on selection filters

Usage::

  dirac-stager-monitor-requests  [--status=<Status>] [--se=<SE>] [--limit=<integer>] [--showJobs=YES] ...

Arguments::

  status: file status=(New, Offline, Waiting, Failed, StageSubmitted, Staged).
  se: storage element
  showJobs: whether to ALSO list the jobs asking for these files to be staged
     WARNING: Query may be heavy, please use --limit switch!

Options::

  -   --status=                : Filter per file status=(New, Offline, Waiting, Failed, StageSubmitted, Staged).
                                 If not used, all status values will be taken into account
  -   --se=                    : Filter per Storage Element. If not used, all storage elements will be taken into account.
  -   --limit=                 : Limit the number of entries returned.
  -   --showJobs=              : Whether to ALSO list the jobs asking for these files to be staged

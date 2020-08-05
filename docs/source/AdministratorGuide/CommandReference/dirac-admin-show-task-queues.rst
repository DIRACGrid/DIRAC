.. _admin_dirac-admin-show-task-queues:

============================
dirac-admin-show-task-queues
============================

Show details of currently active Task Queues

Usage::

  dirac-admin-show-task-queues [option|cfgfile]

Options::

  -v  --verbose                : give max details about task queues
  -t  --taskQueue <value>      : show this task queue only

Example::

  $ dirac-admin-show-task-queues
  Getting TQs..
  * TQ 401
          CPUTime: 360
             Jobs: 3
          OwnerDN: /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
       OwnerGroup: dirac_user
         Priority: 1.0
            Setup: Dirac-Production

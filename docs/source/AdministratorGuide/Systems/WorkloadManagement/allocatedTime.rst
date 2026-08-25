.. _allocated_time:

=========================================
Allocated time management
=========================================

A pilot holds a batch slot for a fixed length of time. Everything that has to happen in
that slot -- matching a job, running the payload, uploading what it produced -- has to fit,
and the pieces are sized by components that must agree on how much is left. This page
describes where that budget comes from, who may spend which part of it, and which options
control it.

.. contents:: :local:

Where the budget comes from
===========================

The pilot establishes two numbers before any job is matched: how fast this worker node is,
and how long the batch system will let it run.

.. graphviz::

   digraph seeding {
     rankdir=LR; node [shape=box, fontsize=10]; edge [fontsize=9];
     bench [label="dirac-wms-cpu-normalization\n(benchmarks the node)"];
     norm  [label="/LocalSite/\nCPUNormalizationFactor", shape=cylinder];
     batch [label="batch system\n(sacct, qstat, ...)"];
     cs    [label="queue maxCPUTime\nin the CS", shape=note];
     queue [label="dirac-wms-get-queue-cpu-time"];
     left  [label="/LocalSite/CPUTimeLeft", shape=cylinder];

     bench -> norm;
     batch -> queue [label="seconds left, if reachable"];
     cs    -> queue [label="otherwise, minutes x 60"];
     norm  -> left  [label="multiplied in", style=dashed];
     queue -> left  [label="seconds"];
   }

``dirac-wms-get-queue-cpu-time`` tries three sources in order, and takes the first that
answers:

#. **The batch system**, through the TimeLeft utility -- ``sacct``, ``qstat``, ``bjobs``
   and so on, selected by ``/LocalSite/BatchSystemInfo``. This is the only source that
   knows how much of the slot has already been used. It is unavailable to a payload
   running inside a container, which is why the figure is written to the configuration
   rather than recomputed later.
#. **The queue's** ``maxCPUTime`` **in the CS**, under
   ``/Resources/Sites/<grid>/<site>/CEs/<ce>/Queues/<queue>/``. If no queue is identified,
   the smallest ``maxCPUTime`` among that CE's queues is used instead.
#. ``/Resources/Computing/CEDefaults/MaxCPUTime``, a global fallback.

.. warning::

   These are not in the same unit. ``maxCPUTime`` on a queue is in **minutes** and is
   multiplied by 60; ``CEDefaults/MaxCPUTime`` is in **seconds** and is not. A queue
   configured as though it were seconds will hand out slots sixty times too long.

The result, in seconds, is multiplied by ``CPUNormalizationFactor`` and stored as
``/LocalSite/CPUTimeLeft``. That field is therefore **CPU work**, not seconds: wall-clock
seconds times the power of the node.

.. note::

   ``CPUNormalizationFactor`` comes from a benchmark run on the node itself, divided by
   ``Operations/JobScheduling/CPUNormalizationCorrection``. Everything downstream is
   proportional to it, so an inaccurate benchmark scales the whole budget with it.

Who may spend which part
========================

There is one writer of ``/LocalSite/CPUTimeLeft`` after the pilot: the JobAgent. Everything
else reads what it publishes.

.. graphviz::

   digraph budget {
     rankdir=LR; node [shape=box, fontsize=10]; edge [fontsize=9];
     agent   [label="JobAgent", style=filled, fillcolor="#e8e8e8"];
     cfg     [label="/LocalSite/CPUTimeLeft", shape=cylinder];
     matcher [label="Matcher"];
     wd      [label="Watchdog"];
     payload [label="payload\n(elastic sizing)"];

     cfg     -> agent   [label="read once at\ninitialize()"];
     agent   -> cfg     [label="republished each cycle,\nStopMargin already off"];
     agent   -> matcher [label="CPU work advertised\nby the CE"];
     cfg     -> wd      [label="read at job start"];
     cfg     -> payload [label="read by the application"];
   }

The JobAgent subtracts ``StopMargin`` once, when it first reads the slot. Everything
downstream therefore works from what a payload may actually *consume*, and none of them
needs to know a reserve exists. **Consumers must not subtract it again.**

Then the end of a slot looks like this:

.. graphviz::

   digraph timeline {
     rankdir=LR; node [shape=box, fontsize=10, width=1.4]; edge [fontsize=9];
     run   [label="payload runs", style=filled, fillcolor="#dff0d8"];
     wind  [label="winding down", style=filled, fillcolor="#fcf8e3"];
     up    [label="uploads", style=filled, fillcolor="#f2dede"];
     end   [label="slot ends", shape=plaintext];

     run  -> wind [label="StopSigNumber sent"];
     wind -> up   [label="payload exits,\nor is killed"];
     up   -> end;
   }

``StopSigFinishWork`` and ``StopMargin`` both reserve time at the end of the slot, but they
are not interchangeable:

===================  ===============================  ==========================  ==============
Reserve              Whose work                       When                        Unit
===================  ===============================  ==========================  ==============
StopSigFinishWork    the payload's, finishing the     before the payload exits    CPU work
                     unit of work in progress
StopMargin           the JobWrapper's, uploading      after the payload exits     wall clock
                     the outputs and the logs
===================  ===============================  ==========================  ==============

They also differ in reach. ``StopMargin`` applies to every job and shrinks the budget that
the matcher and the payload are told about. ``StopSigFinishWork`` changes no budget at all:
it only decides how early to tap an opted-in payload on the shoulder, and comes out of that
payload's own share.

The units differ because the work differs. Uploading a file takes the same wall clock on any
node, so ``StopMargin`` is in seconds. How much computation a payload has done, and how much
it needs to stop, are both questions about work, so the ``StopSig`` thresholds are in CPU work
and are divided by the node's ``CPUNormalizationFactor`` before use -- which means one figure means the same thing across
a heterogeneous fleet, where a figure in seconds would not:

=========  ==========  ==============  ==========
CPU work   slow node   typical node    fast node
=========  ==========  ==============  ==========
30         3 s         1 s             1 s
200        20 s        7 s             4 s
1000       100 s       36 s            20 s
8000       800 s       287 s           160 s
=========  ==========  ==============  ==========

The Watchdog stops the payload when the published budget is spent, whether or not it was
signalled first, so a payload that ignores the signal still cannot eat into the uploads.

Choosing the values
===================

All of these live in ``/Systems/WorkloadManagement/<INSTANCE>/JobWrapper`` and are listed in
:ref:`the configuration reference <cs-JobWrapper>`. The same names may be set per job in the
JDL, which takes precedence -- but a JDL is fixed at submission, so only the CS reaches work
that is already in the system.

``StopMargin``
   Measure it rather than guess. The JobWrapper reports ``Completing`` the moment the
   payload exits and ``Done`` when it has finished uploading, so the span between those two
   records in the JobLoggingDB is exactly what this has to cover. Take a high percentile
   rather than the mean, and remember that jobs killed *during* post-processing never reach
   ``Done`` and so are absent from the sample: the true requirement is if anything larger
   than what is measured.

``StopSigRegex``
   A regular expression matched against the command line of this job's payload processes.
   It names the application to signal and nothing else: signalling the shells, wrappers and
   container entrypoints that share the process tree is at best useless and at worst kills
   the job outright. Only descendants of this JobWrapper are considered, so other jobs on
   the same node are never affected however well their command lines match. Leaving it
   unset disables the graceful stop.

``StopSigNumber``
   Whichever signal the application handles. Verify it rather than assume: an application
   that does not handle the signal is killed by it, losing exactly the work the mechanism
   exists to save.

``StopSigStartWork``
   How much work a payload must have done before it is worth interrupting. Below that there
   is nothing to save, and a payload matched into a slot shorter than its own wind-down
   would otherwise be signalled at once and report success having produced nothing. It
   defaults to ``StopSigFinishWork``, so a payload is never stopped having done less work
   than stopping it costs.

``StopSigFinishWork``
   How much computation the application needs to wind down -- typically the cost of one unit
   of work, plus what it takes to close and write its output. The graceful stop does nothing
   until this is set. Err high: reserving too much costs a few units of work at the end of a
   slot, while reserving too little costs everything the payload had not yet written.

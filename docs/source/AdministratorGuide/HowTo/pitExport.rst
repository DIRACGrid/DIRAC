.. pitExport:

=================================================
Exporting data from your experiment site to DIRAC
=================================================

One problem that every VO has to tackle is how to enter the data collected by the experiment to DIRAC. Of course, there are many specificities for each VOs: security constraints, friendship/hatred between offline and online groups, performance, etc. However the goal is always the same: *gridify* an existing file.

Because there is not one solution for all, this page will contain a bunch of recipes and hints how you could perform your experiment site export.

We go with the assumption that you have a machine used as a gateway that can talk to both the Online world and the grid world, and on which you can install DIRAC, and that has a *local* (even NFS) access to the file you want to export.

Basic DIRAC solution
====================

The simplest and straight forward solution is to install a :ref:`requestManagementSystem` on the data export machine, configured locally (and not in the central CS) with its own DB. Having a local configuration ensures that this machine will treat only the data export, and nothing else will do it.

Once this is done, you are basically set: the standard RMS installation comes with a ``PutAndRegister`` operation (see :ref:`rmsOpType`) that does exactly what you want, that is take a local file and make it available on the Grid.

What you are missing is the step to declare to the RMS that it should perform such operations. But this is very VO dependant. Here are a few pointers:

* The best is if you can instrument your online software to tell DIRAC about the new file by using directly DIRAC calls
* You can also find a common interface (a MessageQueue, a DB, a file, etc) between the Online code and a specific DIRAC agent that would consume that info and create the appropriate request
* Monitor your file system (with `inotify <http://man7.org/linux/man-pages/man7/inotify.7.html>`_ for example) to learn about new file and create the appropriate request

You may also want to remove the file from the online system once transferred. This is very easy: just add another Operation to your request. The actual operation type will again depend very much on your Online code and what file access you have. But for simple removal of a local file, a ``PhysicalRemoval`` operation will do just fine.


Old LHCb solution
=================

The LHCb solution was based on the basic DIRAC setup detailed earlier. The Online software was instrumented to create just ``PutAndRegister`` operations. However, the operation did not register the file in our central FileCatalog, but in an LHCbDIRAC specific database declared as a FileCatalog: the ``RawIntegrityDB``. When a file entered this DB, the ``RawIntegrityAgent`` would start monitoring the file to see if it had been properly migrated to tape. Once this is confirmed, the deletion signal was sent to the Online software.

A more complete documentation is available here: https://lhcb-dirac.readthedocs.io/en/latest/AdministratorGuide/Online.html#workflow

This setup was used for many years.

LHCb solution
=============

The main drawback of the previous LHCb solution was the number of time a file would be read from disk, both by the Online software and the DIRAC RMS. This eventually became a bottleneck. Moreover, the advantage of the retry logic of the RMS was not needed, because similar logic is implemented in the Online software. We thus got rid of the RMS, and replaced it with synchronous operations.

A python process running basic xmlrpc exposes the synchronous :py:meth:`~DIRAC.DataManagementSystem.Client.DataManager.DataManager.putAndRegister` method. When the Online software has a new file available, it just calls this xmlrpc service. The big performance boost was obtained by copying the file synchronously directly from memory to the tape system. The rest of the chain with the ``RawIntegrityAgent`` is the same.
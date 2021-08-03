.. set highlighting to console input/output
.. highlight:: console


======================
Job command line tools
======================

In order to submit a job, it should be described in a form of JDL. An example
JDL for a simple job is presented below::

  Executable = "/bin/cp";
  Arguments = "my.file my.copy";
  InputSandbox = {"my.file"};
  StdOutput = "std.out";
  StdError = "std.err";
  OutputSandbox = {"std.out","std.err","my.copy"};
  CPUTime = 10;

This job will take a local file "my.file", put it into the Input Sandbox and then 
copy it to the "my.copy" file on the Grid. In the Output Sandbox the new copy will
be returned together with the job standard output and error files. To submit the job
one should execute::

  $ dirac-wms-job-submit job.jdl
  JobID = 11758

where the job.jdl file contains the job JDL description. The command returns the JobID which
is a unique job identifier within the DIRAC Workload Management System. You can now follow
the status of the job by giving::

  $ dirac-wms-job-status 11758
  JobID=11758 Status=Waiting; MinorStatus=Pilot Agent Submission; Site=CREAM.CNAF.it;    

In the output of the command you get the job Status, Minor Status with more details, and the site
to which the job is destinated.    

Once the job in its final Status ( Done or Failed ), you can retrieve the job outputs by::

  $  dirac-wms-job-get-output 11702
  Job output sandbox retrieved in 11702/

This will retrieve all the files specified in the job Output Sandbox into the directory named
after the job identifier.  
  
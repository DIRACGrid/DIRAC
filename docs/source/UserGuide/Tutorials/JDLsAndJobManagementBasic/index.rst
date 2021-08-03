================================
JDLs and Job Management Basic
================================
 
JDL stands for Job Description Language and it is the standard way of job description in the gLite environment.
DIRAC does not use the JDL objects internally but allows the job description using the JDL syntax. An important
difference is that there is no Requirements attribute which is used in the gLite JDL to select specific resources.
Instead, certain attributes are interpreted as job requirements, e.g. CPUTime, Site, etc.

Simple Jobs
---------------

The following is the description of the job which just lists the working directory - Simple.jdl::

        JobName = "Simple_Job";
        Executable = "/bin/ls";
        Arguments = "-ltr";
        StdOutput = "StdOut";
        StdError = "StdErr";
        OutputSandbox = {"StdOut","StdErr"};

To submit the job::

    dirac-wms-job-submit Simple.jdl

Jobs with Input Sandbox and Output Sandbox
----------------------------------------------

In most cases the job input data or executable files are available locally and should be transfered to the grid to run the job. 
In this case the InputSandbox attribute can be used to move the files together with the job.

- Create InputAndOuputSandbox.jdl::

        JobName    = "InputAndOuputSandbox";
        Executable = "testJob.sh";
        StdOutput = "StdOut";
        StdError = "StdErr";
        InputSandbox = {"testJob.sh"};
        OutputSandbox = {"StdOut","StdErr"};

- And create a simple shell script.

  testJob.sh::

        #!/bin/bash
        /bin/hostname
        /bin/date
        /bin/ls -la
  
- After creation of JDL file the next step is to submit the job, using the command::

        dirac-wms-job-submit InputAndOuputSandbox.jdl


Jobs with Input and Output Data
-----------------------------------

In case where the data, programs, etc are stored in a Grid Storage Element, it can be specified as part of InputSandbox or InputData. 
InputSandbox can be declared as a list, separated by commas with each file between "".

Before the grid file can be used, it should be uploaded first to the Grid. This is done using the following command::

    dirac-dms-add-file <LFN> <local_file> SE 

For example::

        bash-3.2$ dirac-dms-add-file /vo.formation.idgrilles.fr/user/v/vhamar/test.txt test.txt M3PEC-disk -o LogLevel=INFO
        2010-10-17 17:15:04 UTC dirac-dms-add-file.py  WARN: ReplicaManager.__getClientCertGroup: Proxy information does not contain the VOMs information.
        2010-10-17 17:15:05 UTC dirac-dms-add-file.py  INFO: ReplicaManager.putAndRegister: Checksum information not provided. Calculating adler32.
        2010-10-17 17:15:05 UTC dirac-dms-add-file.py  INFO: ReplicaManager.putAndRegister: Checksum calculated to be cc500ba0.
        2010-10-17 17:15:06 UTC dirac-dms-add-file.py  WARN: StorageElement.isValid: The 'operation' argument is not supplied. It should be supplied in the future.
        2010-10-17 17:15:06 UTC dirac-dms-add-file.py  INFO: SRM2Storage.__putFile: Using 1 streams
        2010-10-17 17:15:06 UTC dirac-dms-add-file.py  INFO: SRM2Storage.__putFile: Executing transfer of file:test.txt to srm://se0.m3pec.u-bordeaux1.fr:8446/srm/managerv2?SFN=/dpm/m3pec.u-bordeaux1.fr/home/vo.formation.idgrilles.fr/user/v/vhamar/test.txt
        2010-10-17 17:15:13 UTC dirac-dms-add-file.py  INFO: SRM2Storage.__putFile: Successfully put file to storage.
        2010-10-17 17:15:13 UTC dirac-dms-add-file.py ERROR: StorageElement.getPfnForProtocol: Requested protocol not available for SE. DIP for M3PEC-disk
        2010-10-17 17:15:14 UTC dirac-dms-add-file.py  INFO: ReplicaManger.putAndRegister: Sending accounting took 0.5 seconds
        {'Failed': {},
         'Successful': {'/vo.formation.idgrilles.fr/user/v/vhamar/test.txt': {'put': 7.5088520050048828,
                                                                      'register': 0.40918898582458496}}}

- Use the same testJob.sh shell script as in the previous exercise.

- In the JDL we have to add OutputSE and OutputData::

        JobName = "LFNInputSandbox";
        Executable = "testJob.sh";
        StdOutput = "StdOut";
        StdError = "StdErr";
        InputSandbox = {"testJob.sh","LFN:/vo.formation.idgrilles.fr/user/v/vhamar/test.txt"};
        OutputSandbox = {"StdOut","StdErr"};
        OutputSE = "M3PEC-disk";
        OutputData = {"StdOut"};

- After creation of JDL file the next step is submit a job, using the command::

        dirac-wms-job-submit <JDL>

  The same effect can be achieved with the following JDL LFNInputData.jdl::

        JobName = "LFNInputData";
        Executable = "testJob.sh";
        StdOutput = "StdOut";
        StdError = "StdErr";
        InputSandbox = {"testJob.sh"};
        InputData = {"LFN:/vo.formation.idgrilles.fr/user/v/vhamar/test.txt"};
        OutputSandbox = {"StdOut","StdErr"};
        OutputSE = "M3PEC-disk";
        OutputData = {"StdOut"};

An important difference of specifying input data as InputSandbox or InputData is that in the first case the
data file is always downloaded local to the job running in the Grid. In the InputData case, the file can be
either downloaded locally or accessed remotely using some remote acces protocol, e.g. rfio or dcap, depending
on the policies adopted by your Virtual Organization.  


Managing Jobs
-----------------

Submitting a Job
@@@@@@@@@@@@@@@@@@@@@@

- After creating the JDL file the next step is to submit a job using the command::

        dirac-wms-job-submit <JDL>

  For example::

        bash-3.2$ dirac-wms-job-submit Simple.jdl -o LogLevel=INFO
        2010-10-17 15:34:36 UTC dirac-wms-job-submit.py/DiracAPI  INFO: <=====DIRAC v5r10-pre2=====>
        2010-10-17 15:34:36 UTC dirac-wms-job-submit.py/DiracAPI  INFO: Will submit job to WMS
        JobID = 11

  In the output of the command you get the DIRAC job ID which is a unique job identifier. You will use it later
  for other job operations. 


Getting the job status
@@@@@@@@@@@@@@@@@@@@@@@@@@@@

- The next step is to monitor the job status using the command::

        dirac-wms-job-status <Job_ID>

        bash-3.2$ dirac-wms-job-status 11
        JobID=11 Status=Waiting; MinorStatus=Pilot Agent Submission; Site=ANY;

Retrieving the job output
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

- And finally, after the job achieves status **Done**, you can retrieve the job Output Sandbox::

        dirac-wms-job-get-output [--dir output_directory] <Job_ID>

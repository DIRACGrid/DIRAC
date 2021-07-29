.. set highlighting to console input/output
.. highlight:: console

==============
User Data
==============

Users are managing their data in the distributed computing environment by uploading it to
and downloading it from the Storage Elements, replicating files to have redundant copies.
The data is accessed from the user jobs, and new data files are created while the job execution.
All the files are registered in the File Catalog to be easily discoverable. 
The basic DIRAC commands to manipulate data are described in this section. 

File upload
---------------

The initial data file upload to the Grid Storaget Element is performed by the following example command::

  $ dirac-dms-add-file <LFN> <FILE PATH> <SE> [<GUID>]

where <LFN> is the Logical File Name which will uniquely identify the file on the Grid.
<FILE PATH> is the full or relative path to the local file to be uploaded. <SE>
is the name of the Storage Element where the file will be uploaded. Optionally <GUID> - 
unique identifier - can be provided. For example::

  $ dirac-dms-add-file /dirac/user/u/username/user.file user.file DIRAC-USER 

will upload local file *user.file* to the *DIRAC-USER* Storage Element. The file will
be registered in the File Catalog with the *LFN* /dirac/user/u/username/user.file

File download
---------------

To download a file from the Grid Storage Element one should do::

  $ dirac-dms-get-file <LFN>

giving the file LFN as the command argument. This will discover the file on the Grid
and will download the file to the local current directory.

File replication
------------------

To make another copy of the file on a new Storage Element, the following command should be executed::

  $ dirac-dms-replicate-lfn <LFN> <SE>

This will make a new copy of the file specified by its LFN to the *SE* Storage Element. For example::

  $ dirac-dms-replicate-lfn  /dirac/user/u/username/user.file DIRAC-USER

You can see all the replicas of the given file by executing::

  $ dirac-dms-lfn-replicas <LFN>

Finding Storage Elements
-------------------------

You can find all the Storage Elements available in the system by::

  $ dirac-dms-show-se-status

This will show the Storage Elements together with their current status which will help you to decide
which ones you can use.

Data in user jobs
-------------------

To access data files from the user jobs and make the system save the files produced in the jobs on the Grid,
the job description should contain InputData and OutputData parameters. In case of using job JDL description,
the JDL can look like the following::

  Executable = "/bin/cp";
  Arguments = "my_data.file my_data.copy";
  InputData = {"/dirac/user/a/atsareg/my_data.file"};
  StdOutput = "std.out";
  StdError = "std.err";
  OutputSandbox = {"std.out","std.err","my.copy"};
  OutputData = {"my_data.copy"};
  OutputSE = "DIRAC-USER";
  CPUTime = 10;

For this job execution the input data file with LFN */dirac/user/a/atsareg/my_data.file* will be put into the
working directory of the user executable. The job will produce a new data file *my_data.copy* which will be uploaded
to the *DIRAC-USER* Storage Element and registered with LFN (example) */dirac/user/a/atsareg/0/19/my_data.copy*. The LFN is 
constructed using the standard DIRAC user LFN convention ( */<vo>/user/<initial>/<username>/* ) and the job ID to avoid 
clashes of files with the same name coming from different jobs.  
   
  
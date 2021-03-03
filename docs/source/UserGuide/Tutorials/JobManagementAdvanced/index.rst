.. _advancedJobManagement:

==========================
7. Advanced Job Management
==========================

7.1 Parametric Jobs
-------------------

A parametric job allows to submit a set of jobs in one submission command by specifying parameters for each job.


To define this parameter the attribute "Parameters" must be defined in the JDL, the values that it can take are:
  - A list (strings or numbers).
  - Or, an integer, in this case the attributes ParameterStart and ParameterStep must be defined as integers 
    to create the list of job parameters. 


7.1.1 Parametric Job - JDL
@@@@@@@@@@@@@@@@@@@@@@@@@@

A simple example is to define the list of parameters using a list of values, this list can contain integers or strings:::

   Executable = "testJob.sh";
   JobName = "%n_parametric";
   Arguments = "%s";
   Parameters = {"first","second","third","fourth","fifth"};
   StdOutput = "StdOut_%s";
   StdError = "StdErr_%s";
   InputSandbox = {"testJob.sh"};
   OutputSandbox = {"StdOut_%s","StdErr_%s"};

In this example, 5 jobs will be created corresponding to the *Parameters* list values. Note that other JDL attributes can
contain "%s" placeholder. For each generated job this placeholder will be replaced by one of the values in the *Parameters* list.

In the next example, the JDL attribute values are used to create a list of 20 integers starting from 1 (ParameterStart) with a step 2 (ParameterStep):::

   Executable = "testParametricJob.sh";
   JobName = "Parametric_%n";
   Arguments = "%s";  
   Parameters = 20;
   ParameterStart = 1;
   ParameterStep = 2;
   StdOutput = "StdOut_%n";
   StdError = "StdErr_%n";
   InputSandbox = {"testParametericJob.sh"};
   OutputSandbox = {"StdOut_%n","StdErr_%n"};

Therefore, with this JDL job description will be submitted in at once. As in the previous example, the "%s" placeholder
will be replaced by one of the parameter values.

Parametric jobs are submitted as normal jobs, the command output will be a list of the generated job IDs, for example:::

   $ dirac-wms-job-submit Param.jdl 
   JobID = [1047, 1048, 1049, 1050, 1051]

These are standard DIRAC jobs. The jobs outputs can be retrieved as usual specifying the job IDs:::

   $ dirac-wms-job-get-output 1047 1048 1049 1050 1051



7.1.1 Creating and submitting parametric Jobs using DIRAC APIs
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@


DIRAC APIs are an easy and convenient way to create and submit parametric jobs::

   from DIRAC.Interfaces.API.Job import Job
   from DIRAC.Interfaces.API.Dirac import Dirac
   # or extensions, e.g. from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob for LHCb

   J = Job()
   J.setCPUTime(17800)
   J.setInputSandbox('exe-script.py') # whatever
   J.setParameterSequence("args", ['one', 'two', 'three'])
   J.setParameterSequence("iargs", [1, 2, 3])
   J.setExecutable("exe-script.py", arguments=": testing %(args)s %(iargs)s", logFile='helloWorld_%n.log')
   print Dirac().submitJob(J)


InputData (in the form of LFNs -- Logical File Names) can become also parameters in parametric jobs::

   inputDataList = [ # a list of lists
    [
        '/lhcb/data/data1',
        '/lhcb/data/data2'
    ],
    [
        '/lhcb/data/data3',
        '/lhcb/data/data4'
    ],
    [
        '/lhcb/data/data5',
        '/lhcb/data/data6'
    ]

   J.setParameterSequence('InputData', inputDataList, addToWorkflow=True)

and similarly for InputSandbox::

   inputSBList = [ # a list of lists
    [
        '/localFile.txt',
        '/another/localFile.py',
        '/some/lfn/some/where'
    ]

   J.setParameterSequence('InputSandbox', inputSBList, addToWorkflow=True)


The list of parameters, whatever they are have to have ALL the same lenghth,
e.g. there should not be a parameter of length 2 and another of length 3.




7.2 MPI Jobs
------------

Message Passing Interface (MPI) is commonly used to handle the communications between tasks in parallel applications. 
Two versions and implementations supported in DIRAC are the following:::

 - MPICH-1 : MPICH1
 - MPICH-2 : MPICH2

Users should know that, currently, the MPI jobs can only run on one grid site. So, the maximum number of processors that 
a user can require for a job depends on the capacity and the policy of the sites.

Another important point, is that some applications need all nodes to work with a shared directory, 
in some cases, sites provide such a shared disk space but not always.


7.2.1 MPI Jobs - JDL
@@@@@@@@@@@@@@@@@@@@

To define MPI jobs using DIRAC it is necessary:

- Create a wrapper script, this script prepares the environment variables, the arguments are the mpi program without extension c, for example:::

    $ more application.sh
    #!/bin/bash
    EXECUTABLE=$1
    NUMPROC=$2
    DOMAIN=`hostname -f|cut -d. -f2-10`
    MPICC=`which mpicc`
    MPIRUN=`which mpirun`
    MPIH=`which mpi.h`
    # Optional
    echo "========================================="
    echo "DATE: " `/bin/date`
    echo "Domain: " $DOMAIN
    echo "Executable: " $EXECUTABLE
    echo "Num Proc: " $NUMPROC
    echo "MPICC: " $MPICC  
    echo "MPIRUN: " $MPIRUN 
    echo "MPIH: " $MPIH 
    echo "MPI_SHARED_HOME: " `echo $MPI_SHARED_HOME`
    echo "========================================="
    export x=`echo $MPI_SHARED_HOME`
    echo "Starting MPI script"
    mpdtrace
    if [ $? -eq 0 ]; then
      mpicc -o $EXECUTABLE.o ./EXECUTABLE.c -lm
      if [[   -z "$x" || "$x" == "no" ]]; then
        DIR=$HOME/$TMP_DIR
        export PATH=$PATH:$DIR
        for i in `mpdtrace`;
        do
          ssh $i.$DOMAIN mkdir -p  $DIR
          scp $PWD/$EXECUTABLE* $i.$DOMAIN:$DIR/;
          ssh $i.$DOMAIN ls -la $DIR
        done;
      else
        DIR=$MPI_SHARED_HOME/$TMP_DIR
        mkdir $DIR
        cp $EXECUTABLE.o $DIR;
      fi
      $MPIRUN -np $NUMPROC $DIR/$EXECUTABLE.o
      x=`echo $MPI_SHARED_HOME`;
      if [[  -z "$x" ||  "$x" == "no" ]]; then
        for i in `mpdtrace`;
        do
          ssh $i.$DOMAIN 'rm -rf $DIR';
        done;
      else
        cd ..
        rm -rf $DIR 
      fi
    else
      exit
    fi


- Edit the JDL: 
  - Set the *JobType* attribute to "MPI" 
  - Set *Flavor* attribute to specify which version of MPI libraries you want to use - MPICH2 or MPICH1
  - Set *CPUNumber* attribute

  For example:::

    JobType        = "MPI";
    CPUNumber      = 2;
    Executable     = "application.sh";
    Arguments      = "mpifile 2 ";
    StdOutput      = "StdOut";
    StdError       = "StdErr";
    InputSandbox   = {"application.sh","mpifile.c","inputfile.txt"};
    OutputSandbox  = {"mpifile.o","StdErr","StdOut"};
    Flavor         = "MPICH2"


MPI Jobs are submitted as normal jobs, for example:::

   $ dirac-wms-job-submit mpi.jdl 
   JobID = 1099

To retrieve the job outputs use a usual *dirac-wms-job-get-output* command:::

   $ dirac-wms-job-get-output 1099



7.3 DIRAC API
-------------

The DIRAC API is encapsulated in several Python classes designed to be used easily by users to access
a large fraction of the DIRAC functionality. Using the API classes it is easy to write small scripts
or applications to manage user jobs and data. 

7.3.1 Submitting jobs using APIs
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

- First step, create a Python script specifying job requirements.

  Test-API.py::

      from DIRAC.Interfaces.API.Dirac import Dirac
      from DIRAC.Interfaces.API.Job import Job
      
      j = Job()
      j.setCPUTime(500)
      j.setExecutable('echo',arguments='hello')
      j.setExecutable('ls',arguments='-l')
      j.setExecutable('echo', arguments='hello again')
      j.setName('API')
      
      dirac = Dirac()
      result = dirac.submit(j)
      print 'Submission Result: ',result


- Run the script::

        python Test-API.py

        $ python testAPI.py 
        {'OK': True, 'Value': 196}

7.3.2 Retrieving Job Status
@@@@@@@@@@@@@@@@@@@@@@@@@@@

- Create a script Status-API.py::

        from DIRAC.Interfaces.API.Dirac import Dirac
        from DIRAC.Interfaces.API.Job import Job
        import sys
        dirac = Dirac()
        jobid = sys.argv[1]
        print dirac.status(jobid)

- Execute script::
         
        python Status-API.py <Job_ID>

        $python Status-API.py 196
        {'OK': True, 'Value': {196: {'Status': 'Done', 'MinorStatus': 'Execution Complete', 'Site': 'LCG.IRES.fr'}}}
  

7.3.3 Retrieving Job Output
@@@@@@@@@@@@@@@@@@@@@@@@@@@

- Example Output-API.py::

        from DIRAC.Interfaces.API.Dirac import Dirac
        from DIRAC.Interfaces.API.Job import Job
        import sys
        dirac = Dirac()
        jobid = sys.argv[1]
        print dirac.getOutputSandbox(jobid)
        print dirac.getJobOutputData(jobid)

- Execute script::

        python Output-API.py <Job_ID>

        $python Output-API.py 196


7.3.4 Local submission mode
@@@@@@@@@@@@@@@@@@@@@@@@@@@

The Local submission mode is a very useful tool to check the sanity of your job before submission to the
Grid. The job executable is run locally in exactly the same way ( same input, same output ) as it will do on
the Grid Worker Node. This allows to debug the job in a friendly local environment.

Let's perform this exercise in the python shell.

- Load python shell::

        bash-3.2$ python
        Python 2.5.5 (r255:77872, Mar 25 2010, 14:17:52) 
        [GCC 4.1.2 20080704 (Red Hat 4.1.2-46)] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> from DIRAC.Interfaces.API.Dirac import Dirac
        >>> from DIRAC.Interfaces.API.Job import Job
        >>> j = Job()
        >>> j.setExecutable('echo', arguments='hello')
        {'OK': True, 'Value': ''}
        >>> Dirac().submitJob(j,mode='local')
        2010-10-22 14:41:51 UTC /DiracAPI  INFO: <=====DIRAC v5r10-pre2=====>
        2010-10-22 14:41:51 UTC /DiracAPI  INFO: Executing workflow locally without WMS submission
        2010-10-22 14:41:51 UTC /DiracAPI  INFO: Executing at /afs/in2p3.fr/home/h/hamar/Tests/APIs/Local/Local_zbDHRe_JobDir
        2010-10-22 14:41:51 UTC /DiracAPI  INFO: Preparing environment for site DIRAC.Client.fr to execute job
        2010-10-22 14:41:51 UTC /DiracAPI  INFO: Attempting to submit job to local site: DIRAC.Client.fr
        2010-10-22 14:41:51 UTC /DiracAPI  INFO: Executing: /afs/in2p3.fr/home/h/hamar/DIRAC5/scripts/dirac-jobexec jobDescription.xml -o LogLevel=info
        Executing StepInstance RunScriptStep1 of type ScriptStep1 ['ScriptStep1']
        StepInstance creating module instance  ScriptStep1  of type Script
        2010-10-22 14:41:53 UTC dirac-jobexec.py/Script  INFO: Script Module Instance Name: CodeSegment
        2010-10-22 14:41:53 UTC dirac-jobexec.py/Script  INFO: Command is: /bin/echo hello
        2010-10-22 14:41:53 UTC dirac-jobexec.py/Script  INFO: /bin/echo hello execution completed with status 0
        2010-10-22 14:41:53 UTC dirac-jobexec.py/Script  INFO: Output written to Script1_CodeOutput.log, execution complete.
        2010-10-22 14:41:53 UTC /DiracAPI  INFO: Standard output written to std.out
        {'OK': True, 'Value': 'Execution completed successfully'}

- Exit python shell

- List the directory where you run the python shell, the outputs must be automatically created::

        bash-3.2$ ls
        Local_zbDHRe_JobDir  Script1_CodeOutput.log  std.err  std.out
        bash-3.2$ more Script1_CodeOutput.log 
        <<<<<<<<<< echo hello Standard Output >>>>>>>>>>

        hello


7.3.5 Sending Multiple Jobs
@@@@@@@@@@@@@@@@@@@@@@@@@@@

- Create a Test-API-Multiple.py script, for example::

        from DIRAC.Interfaces.API.Dirac import Dirac
        from DIRAC.Interfaces.API.Job import Job

        j = Job()
        j.setCPUTime(500)
        j.setExecutable('echo',arguments='hello')
        for i in range(5):
          j.setName('API_%d' % i)
          dirac = Dirac()
          jobID = dirac.submitJob(j)
          print 'Submission Result: ',jobID

- Execute the script::

          $ python Test-API-Multiple.py 
          Submission Result:  {'OK': True, 'Value': 176}
          Submission Result:  {'OK': True, 'Value': 177}
          Submission Result:  {'OK': True, 'Value': 178}


7.3.6 Using APIs to create JDL files.
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

- Create a Test-API-JDL.py::

          from DIRAC.Interfaces.API.Job import Job
          j = Job()
          j.setName('APItoJDL')
          j.setOutputSandbox(['*.log','summary.data'])
          j.setInputData(['/vo.formation.idgrilles.fr/user/v/vhamar/test.txt','/vo.formation.idgrilles.fr/user/v/vhamar/test2.txt'])
          j.setOutputData(['/vo.formation.idgrilles.fr/user/v/vhamar/output1.data','/vo.formation.idgrilles.fr/user/v/vhamar/output2.data'],OutputPath='MyFirstAnalysis')
          j.setPlatform("")
          j.setCPUTime(21600)
          j.setDestination('LCG.IN2P3.fr')
          j.setBannedSites(['LCG.ABCD.fr','LCG.EFGH.fr'])
          j.setLogLevel('DEBUG') 
          j.setExecutionEnv({'MYVARIABLE':'TEST'})
          j.setExecutable('echo',arguments='$MYVARIABLE')
          print j._toJDL()

- Run the API::

          $ python Test-API-JDL.py 
 
              Priority = "1";
              Executable = "dirac-jobexec";
              ExecutionEnvironment = "MYVARIABLE=TEST";
              StdError = "std.err";
              LogLevel = "DEBUG";
              BannedSites = 
                  {
                      "LCG.ABCD.fr",
                      "LCG.EFGH.fr"
                  };
              StdOutput = "std.out";
              Site = "LCG.IN2P3.fr";
              Platform = "";
              OutputPath = "MyFirstAnalysis";
              InputSandbox = "jobDescription.xml";
              Arguments = "jobDescription.xml -o LogLevel=DEBUG";
              JobGroup = "vo.formation.idgrilles.fr";
              OutputSandbox = 
                  {
                      "*.log",
                      "summary.data",
                      "Script1_CodeOutput.log",
                      "std.err",
                      "std.out"
                  };
              CPUTime = "21600";
              JobName = "APItoJDL";
              InputData = 
                  {
                      "LFN:/vo.formation.idgrilles.fr/user/v/vhamar/test.txt",
                      "LFN:/vo.formation.idgrilles.fr/user/v/vhamar/test2.txt"
                  };
              JobType = "User";
              



As you can see the parameters added to the job object are represented in the JDL job description.
It can now be used together with the **dirac-wms-job-submit** command line tool.


7.3.7 Submitting MultiProcessor (MP) jobs
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

Jobs that can (or should) run using more than 1 processor should be described as such,
using the "setNumberOfProcessors" method of the API::

      j = Job()
      j.setCPUTime(500)
      j.setExecutable('echo',arguments='hello')
      j.setExecutable('ls',arguments='-l')
      j.setExecutable('echo', arguments='hello again')
      j.setName('MP test')
      j.setNumberOfProcessors(16)

Calling ``Job().setNumberOfProcessors()``, with a value bigger than 1,
will translate into adding also the "MultiProcessor" tag to the job description.

.. versionadded:: v6r20p5

Users can specify in the job descriptions NumberOfProcessors and WholeNode parameters, e.g.::

   NumberOfProcessors = 16;
   WholeNode = True;

This will be translated internally into 16Processors and WholeNode tags.
"MultiProcessor" tag is added automatically to the job description if more than 1 processor is specified.

This would allow resources (WN's) to put flexibly requirements on jobs to be taken, for example, avoiding single-core jobs on a multi-core nodes.


7.3.8 Submitting jobs with specifc requirements (e.g. GPU)
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

<to expand, ~same as for MP jobs, i.e. use Tags>

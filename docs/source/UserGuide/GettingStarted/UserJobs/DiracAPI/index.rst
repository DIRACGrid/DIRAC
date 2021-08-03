.. _user_jobs_api:

.. set highlighting to console input/output
.. highlight:: console

==========================
Jobs with DIRAC Python API
==========================

  The DIRAC API is encapsulated in several Python classes designed to be used easily by users to access a large fraction of the DIRAC functionality. Using the API classes it is easy to write small scripts or applications to manage user jobs and data.

  While it may be exploited directly by users, the DIRAC API also serves as the interface for the Ganga Grid front-end to perform distributed user analysis for LHCb, for example.

  The DIRAC API provide several advantages for the users, those advantages are enumerated below:

    - Provides a transparent and secure way for users to submit jobs to the grid.
    - Allow to debug locally the programs before be submitted to the Grid.
    - A simple, seamless interface to Grid resources allows to run single applications or multiple steps of different applications.
    - The user can perform an analysis using understandable Python code. 
    - Using local job submission the job executable is run locally in exactly the same way ( same input, same output ) as it will do on the Grid Worker Node. This allows to debug the job in a friendly local environment.
    - Using local submission mode the user can check the sanity of the job before submission to the Grid.
    - All the DIRAC API commands may also be executed directly from the Python prompt.
    - Between others advantages.
    
Creating a DIRAC Job using API
==============================

The API allows creating DIRAC jobs using the Job object, specifying job requirements

.. code-block:: python

    # setup DIRAC
    from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
    Script.parseCommandLine(ignoreErrors=False) 
  
    from DIRAC.Interfaces.API.Job import Job
    from DIRAC.Interfaces.API.Dirac import Dirac
    
    dirac = Dirac()
    j = Job() 
    
    j.setCPUTime(500)
    j.setExecutable('/bin/echo hello')
    j.setExecutable('/bin/hostname')
    j.setExecutable('/bin/echo hello again')
    j.setName('API')
    
    jobID = dirac.submitJob(j)
    print('Submission Result: ', jobID)
    
In this example, the job has tree steps from different applications: echo, hostname and echo again.

Submitting jobs
===============
  
To submit the job is just send the job using the script::

   $ python testAPI-Submission.py
   2010-10-20 12:05:49 UTC testAPI-Submission.py/DiracAPI  INFO: <=====DIRAC v5r10-pre2=====>
   2010-10-20 12:05:49 UTC testAPI-Submission.py/DiracAPI  INFO: Will submit job to WMS
   {'OK': True, 'Value': 196}

The script output must return the jobID, this is useful for keeping track of your job IDs.

Job Monitoring
==============

Once you have submitted your jobs to the Grid, a little script can be used to monitor the job status
  
.. code-block:: python

    # setup DIRAC
    from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
    Script.parseCommandLine(ignoreErrors=False) 

    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.Interfaces.API.Job import Job
    import sys
    dirac = Dirac()
    jobid = sys.argv[1]
    print(dirac.status(jobid))

Run it like this::

    python Status-API.py <Job_ID>

    $python Status-API.py 196
    {'OK': True, 'Value': {196: {'Status': 'Done', 'MinorStatus': 'Execution Complete', 'Site': 'LCG.IRES.fr'}}}

  The script output is going to return the status, minor status and the site where the job was executed.

Job Output
==========

When the status of the job is done, the outputs can be retrieved using also a simple script::

.. code-block:: python

    import sys

    # setup DIRAC
    from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
    Script.parseCommandLine(ignoreErrors=False) 
   
    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.Interfaces.API.Job import Job

    dirac = Dirac()
    jobid = sys.argv[1]
    print(dirac.getOutputSandbox(jobid))

And, executing the script::

    % python Output-API.py <Job_ID>

The job output is going to create a directory with the jobID and the output files will be stored inside this directory.

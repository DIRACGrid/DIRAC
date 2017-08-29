================
DIRAC JobWrapper
================

The JobAgent is creating a file that is made from the JobWrapperTemplate.py file. It creates a temporary file using this file as a template, which becomes Wrapper_<jobID> somewhere in the workDirectory. It is this file that is then submitted as a real "job wrapper script".

The JobWrapper is not a job wrapper, but is an object that is used by the job wrapper (i.e. the JobWrapperTemplate’s execute() method) to actually do the work…

The only change made in the "template" file is the following:
wrapperTemplate = wrapperTemplate.replace( "@SITEPYTHON@", str( siteRoot ) )

Then the file is submitted in bash using the defined CE (the InProcessCE in our case)

The sequence executed is ("job" is the JobWrapper object here ;-) ):

.. code-block:: python

   job.initialize( arguments )
   #[…]
   result = job.transferInputSandbox( arguments['Job']['InputSandbox'] )
   #[…]
   result = job.resolveInputData()
   #[…]
   result = job.execute( arguments )
   #[…]
   result = job.processJobOutputs( arguments )
   #[…]
   return job.finalize( arguments )

The watchdog is started in job.execute(). A direct consequence is that the time taken to download the input files is not taken into account for the WallClock time.

A race condition might happen inside this method. The problem here is that we submit the process in detached mode (or in a thread, not clear as here thread may be used for process), wait 10 seconds and expect it to be started. If this fails, the JobWrapperTemplate gives up, but if however the detached process runs, it continues executing as if nothing happened! Tt is there that there is the famous gJobReport.setJobStatus( 'Failed', 'Exception During Execution', sendFlag = False )
which is sometimes causing jobs to go to "Failed" and then continue.

There is a nice "feature" of this complex cascade which is that the jobAgent reports "Job submitted as ..." (meaning the job was submitted to the local CE, i.e. the InProcessCE in our case) _after_ the "job" is actually executed!!!

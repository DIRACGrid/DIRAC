=========================================
Job Scheduling
=========================================

The */Operations/<vo>/<setup>/JobScheduling* section contains all parameters that define DIRAC's behaviour when deciding what job has to be
executed. Here's a list of parameters that can be defined:

=========================  ========================================================  ===============================================================================================
Parameter                  Description                                               Default value
=========================  ========================================================  ===============================================================================================
taskQueueCPUTimeIntervals  Possible cpu time values that the task queues can have.   360, 1800, 3600, 21600, 43200, 86400, 172800, 259200, 345600, 518400, 691200, 864000, 1080000
-------------------------  --------------------------------------------------------  -----------------------------------------------------------------------------------------------
EnableSharesCorrection     Enable automatic correction of the priorities assigned    False
                           to each task queue based on previous history
-------------------------  --------------------------------------------------------  -----------------------------------------------------------------------------------------------
CheckJobLimits             Limit the amount of jobs running at sites based on        False
                           their attributes
-------------------------  --------------------------------------------------------  -----------------------------------------------------------------------------------------------
CheckMatchingDelay         Delay running a job at a site if another job has started  False
                           recently and the conditions are met
=========================  ========================================================  ===============================================================================================

Before enabling the correction of priorities, take a look at :ref:`jobpriorities`. Priorities and how to correct them is explained there.
The configuration of the corrections would be defined under *JobScheduling/ShareCorrections*.

Limiting the number of jobs running
====================================

Once *JobScheduling/EnableJobLimits* is enabled. DIRAC will check how many and what type of jobs are running at the configured sites. If
there are more than a configured threshold, no more jobs of that type will run at that site. To define the limits create a
*JobScheduling/RunningLimit/<Site name>* section for each site a limit has to be applied. Limits are defined by creating a section with the job attribute (like
*JobType*) name, and setting the limits inside. For instance, to define that there can't be more that 150 jobs running with *JobType=MonteCarlo* at site *DIRAC.Somewhere.co*
set *JobScheduling/RunningLimit/DIRAC.Somewhere.co/JobType/MonteCarlo=150*

Setting the matching delay
===========================

DIRAC allows to throttle the amount of jobs that start at a given site. This throttling is defined under *JobScheduling/MatchingDelay*. It is configured similarly as the `Limiting the number of jobs
running`_. But instead of defining the maximum amount of jobs that can run at a site, the minimum seconds between starting jobs is defined.
For instance *JobScheduling/MatchingDelay/DIRAC.Somewhere.co/JobType/MonteCarlo=10* won't allow jobs with *JobType=MonteCarlo* to start at
site *DIRAC.Somewhere.co* with less than 10 seconds between them.

Example
========

An example with all the options under *JobScheduling* follows. Remember that JobScheduling is defined under
*/Operations/<vo>/<setup>/JobScheduling* for multi-VO installations, and */Operations/<setup>/JobScheduling* for single-VO ones::

 JobScheduling
 {
   taskQueueCPUTimeIntervals = 360, 1800, 3600, 21600, 43200, 86400, 172800, 259200, 345600
   EnableSharesCorrection = True
   ShareCorrections
   {
     ShareCorrectorsToStart = WMSHistory
     WMSHistory
     {
       GroupsInstance
       {
         MaxGlobalCorrectionFactor = 3
         WeekSlice
         {
           TimeSpan = 604800
           Weight = 80
           MaxCorrection = 2
         }
         HourSlice
         {
           TimeSpan = 3600
           Weight = 20
           MaxCorrection = 5
         }
       }
       UserGroupInstance
       {
         Group = dirac_user
         MaxGlobalCorrectionFactor = 3
         WeekSlice
         {
           TimeSpan = 604800
           Weight = 80
           MaxCorrection = 2
         }
         HourSlice
         {
           TimeSpan = 3600
           Weight = 20
           MaxCorrection = 5
         }
       }
     }
   }
   CheckJobLimits = True
   RunningLimit
   {
     DIRAC.Somewhere.co
     {
       JobType
       {
         MonteCarlo = 150
         Test = 10
       }
     }
   }
   CheckMatchingDelay = True
   MatchingDelay
   {
     DIRAC.Somewhere.co
     {
       JobType
       {
         MonteCarlo = 10
       }
     }
   }
 }

Transactional bulk job submission
=================================

When submitting parametric jobs (bulk submission), the job description contains a recipe
to generate actual jobs per parameter value according to a formulae in the description.
The jobs are generated by default synchronously in the call to the DIRAC WMS JobManager service.
However, there is a risk that in case of an error jobs are partially generated without
the client knowing it. To avoid this risk, an additional logic to ensure that no unwanted jobs
are left in the system has been added together with DIRAC v6r20.

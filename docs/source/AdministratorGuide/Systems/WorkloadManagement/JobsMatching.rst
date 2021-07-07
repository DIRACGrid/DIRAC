.. _JobsMatching:

==============================================
Matching WNs capabilities to Jobs requirements
==============================================

Pilots determine the WNs capabilities and the JobAgent started by the pilot will contact the Matcher service to match a job, selected from the TaskQueueDB.

Capabilities and requirements include but are not limited to:

* *destination*: a (list of) site name(s)
* *CPUTime*: the (estimated) time, expressed in HS06s
* *platform*: the platform of the WN (which is determined by its OS, and not only), also refer to :ref:`resourcesComputing`
* *generic tags*: read about it in further sections

The JobAgent running on the Worker Node and started by the pilot presents capabilities in the form of a dictionary, like the following example::

   {
     CPUTime:               1200000
     GridCE:                ce-01.somewhere.org
     GridMiddleware:        ARC
     MaxRAM:                2048
     NumberOfProcessors:    1
     OwnerGroup:            diracAdmin,test,user
     PilotBenchmark:        19.5
     PilotInfoReportedFlag: False
     PilotReference:        https://ce-01.somewhere.org:8443/CREAM155256908
     Platform:              x86_64_glibc-2.21
     ReleaseProject:        VO
     ReleaseVersion:        7.2.13
     Setup:                 VO-Certification
     Site:                  DIRAC.somewhere.org
     Tag:                   GPU
   }

The WorkloadManagement/Matcher log will print out at the INFO log level dictionaries of capabilities presented to the service, like the example above.
The matcher will try to match these capabilities to the requirements of jobs, which are stored in the MySQL DB TaskQueueDB.

An example of requirements include the following::

  JobRequirements =
  [
    OwnerDN = "/some/DN/";
    VirtualOrganization = "VO";
    Setup = "VO-Certification";
    CPUTime = 17800;
    OwnerGroup = "user";
    UserPriority = 1;
    Sites = "DIRAC.somewhere.org";
    JobTypes = "User";
    Tags = "MultiProcessor";
  ];

which is what can be visualized in Job JDLs.
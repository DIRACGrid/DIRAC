.. _jobs:

========================
DIRAC jobs
========================

Some definitions for DIRAC jobs:

- *payload* or *workflow*: the executed code. A payload describes how to run one or more application step.
- *payload executor*: a script that runs the payload (e.g. dirac-jobexec)
- *JDL*: a container of payload requirements
- *DIRAC job*: a JDL to which it is assigned a unique identifier inside the DIRAC WMS
- *JobWrapper*: a software module for running a DIRACJob in a controlled way
- *multi-processor payload [job]*: a payload application that will try to use multiple cores on the same node
- *computing slot*: resource allocated by a provider where a pilot wrapper is running (batch job)
- *multi-processor [computing] slot*: allocated resource has more than one OS CPU core available in the same slot as opposed to a *single-processor [computing] slot*

Applications properties are reflected in payload properties.

The DIRAC `APIs <http://diracs.readthedocs.io/en/rel-v6r15/CodeDocumentation/Interfaces/API/API_Module.html>`_ can be used to create and submit jobs. 
Specifically, objects of type `Job <http://dirac.readthedocs.io/en/rel-v6r15/CodeDocumentation/Interfaces/API/Job.html>`_ represents a job. The API class `Dirac <http://diracs.readthedocs.io/en/rel-v6r15/CodeDocumentation/Interfaces/API/Dirac.html>`_ and more specifically the call to `submitJob <http://dirac.readthedocs.io/en/rel-v6r15/CodeDocumentation/Interfaces/API/Dirac.html#DIRAC.Interfaces.API.Dirac.Dirac.submitJob>`_ submits jobs to the DIRAC WMS.

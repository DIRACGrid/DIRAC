===============================
Pilot Monitor
===============================

This is part of the DIRAC Web Portal project. For the description of the DIRAC Web Portal basic functionality look here.

    - `Description`_
    - `Selectors`_
    - `Columns`_
    - `Operations`_

Description
-------------------

The Pilot Monitor is providing information about the Pilot Jobs currently managed by the DIRAC Workload Management System. It shows details of the selected Pilot Jobs and allows certain Pilot Job manipulations.

Selectors
-------------------

Selector widgets are provided in the left-side panel. These are drop-down lists with values that can be selected. A single or several values can be chosen. Once the selection is done press Submit button to refresh the contents of the table in the right-side panel. Use Reset button to clean up the values in all the selector widgets.

The following Selectors are available:

**Status**
    The Pilot Job Status. The following Status values are possible:

**Status Comment**

Submitted     Pilot Job is submitted to the grid WMS, its grid status is not yet obtained
Ready     Pilot Job is accepted by the grid WMS
Scheduled     Pilot Job is assigned to a grid site
Running     Pilot Job has started running at a grid site
Stalled     Pilot Job is stuck in the grid WMS without further advancement, this is typically an indication of the WMS error
Done     Pilot Job is finished by the grid WMS
Aborted     Pilot Job is aborted by the grid WMS
Deleted     Pilot Job is marked for deletion

**Site**
    The Pilot Job destination site in DIRAC nomenclature.

**ComputingElement**
    The end point of the Pilot Job Computing Element.

**Owner**
    The Pilot Job Owner. This is the nickname of the Pilot Job Owner corresponding to the Owner grid certificate DN.

**OwnerGroup**
    The Pilot Job Owner group. This usually corresponds to the Owner VOMS role.

**Broker**
    The instance of the WMS broker that was used to submit the Pilot Job.

**Time Span**
    The Time Span widget allows to select Pilot Jobs with Last Update timestamp in the specified time range.

Columns
-----------------

The information on the selected Pilot Jobs is presented in the right-side panel in a form of a table. Note that not all the available columns are displayed by default. You can choose extra columns to display by choosing them in the menu activated by pressing on a menu button ( small triangle ) in any column title field.

The following columns are provided:

**PilotJobReference**
    Pilot Job grid WMS reference.

**Site**
    The Pilot Job destination site in DIRAC nomenclature.

**ComputingElement**
    The end point of the Pilot Job Computing Element.

**Broker**
    The instance of the WMS broker that was used to submit the Pilot Job.

**Owner**
    The Pilot Job Owner. This is the nickname of the Pilot Job Owner corresponding to the Owner grid certificate DN.

**OwnerDN**
    The Pilot Job Owner grid certificate DN.

**OwnerGroup**
    The Pilot Job Owner group. This usually corresponds to the Owner VOMS role.

**CurrentJobID**
    The ID of the current job in the DIRAC WMS executed by the Pilot Job.

**GridType**
    The type of the middleware of the grid to which the Pilot Job is sent

**Benchmark**
    Estimation of the power of the Worker Node CPU which is running the Pilot Job. If 0, the estimation was not possible.

**TaskQueueID**
    Internal DIRAC WMS identifier of the Task Queue for which the Pilot Job is sent.

**PilotID**
    Internal DIRAC WMS Pilot Job identifier

**ParentID**
    Internal DIRAC WMS identifier of the parent of the Pilot Job in case of bulk (parameteric) job submission

**SubmissionTime**
    Pilot Job submission time stamp

**LastUpdateTime**
    Pilot Job last status update time stamp

Operations
----------------------

Clicking on the line corresponding to a Pilot Job, one can obtain a menu which allows certain operations on the Pilot Job. Currently, the following operations are available.

**Show Jobs**
    Pass to a Job Monitor and select jobs attempted to be executed by the given Pilot Job

**PilotOutput**
    Get the standard output of the finished Pilot Job in a pop-up panel. Note that only successfully finished Pilot Jobs output can be accessed.

.. _dirac_overview:

==================================
Architecture overview
==================================

.. toctree::
   :maxdepth: 2

The DIRAC architecture consists of numerous cooperating Distributed Services and Light Agents.

DIRAC introduced the now widely used concept of Pilot Agents. This allows efficient Workload Management
Systems (WMS) to be built. The workload of the community is optimized in the central Task Queue.
The WMS is carefully designed to be resilient to failures in the ever changing Grid environment.

The DIRAC project includes a versatile Data Management System (DMS) which is optimized for reliable
data transfers. The DMS automates the routine data distribution tasks.

The DIRAC Transformation Management System is built on top of the Workload and Data Management services.
This provides automated data driven submission of processing jobs with workflows of arbitrary complexity

The DIRAC Project has all the necessary components to build Workload and Data management systems
of varying complexity. It offers a complete and powerful Grid solution for other user grid communities.

DIRAC design principles
---------------------------

- DIRAC is conceived as a light grid system.
- Following  the paradigm of a Services Oriented Architecture (SOA), DIRAC is lightweight, robust and scalable.
- It should support a rapid development cycle to accommodate ever-evolving grid opportunities.
- It should be easy to deploy on various platforms and updates in order to bring in bug fixes and new
  functionalities should be transparent or even automatic.
- It must be simple to install, configure and operation of various services. This makes the threshold low for
  new sites to be incorporated into the system.
- The system should automate most of the tasks, which allows all the DIRAC resources to be easily managed
  by a single Production Manager.
- Redundancy
- The information which is vital to the successful system operation is duplicated at several services to
  ensure that at least one copy will be available to client request.This is done for the DIRAC Configuration
  Service and for the File Catalog each of which has several mirrors kept synchronized with the master instance.
- All the important operations for which success is mandatory for the functioning of the system without losses
  are executed in a failover recovery framework which allows retrying them in case of failures. All the information
  necessary for the operation execution is encapsulated in an XML object called request which is stored in one of
  the geographically distributed request databases.
- For the data management operations, for example for initial data file uploading to some grid storage, in case of
  failure the files are stored temporarily in some spare storage element with a failover request to move the data
  to the final destination when it becomes available.
- System state information
   - Keeping the static and dynamic information separately reduces the risk of compromising the static information
     due to system overloading.
   - In DIRAC the static configuration data is made available to all the clients via the Configuration Service (CS)
     which has multiple reservations. Moreover, this information can be cached on the client side for relatively
     short periods without risk of client misbehaviour.
   - The dynamic information is in most cases looked for at its source. This is why, for example, the DIRAC Workload
     Management System is following the "pull" paradigm where the computing resources availability is examined by a
     network of agents running in close connection to the sites.
- Requirements to sites
   - The main responsibility of the sites is to provide resources for the common use in a grid. The resources are
     controlled by the site managers and made available through middleware services (Computing and Storage Elements).
   - DIRAC puts very low requirements on the sites

DIRAC Architecture
-------------------

DIRAC follows the paradigm of a Services Oriented Architecture (SOA).

The DIRAC components can be grouped in the following 4 categories:
   - Resources
   - Services
   - Agents
   - Interfaces

Resources
@@@@@@@@@@@@@@@@
DIRAC covers all the possible resources available to WLCG, OSG, EGI grids

Services
@@@@@@@@@@@@@@@@
 - The DIRAC system is built around a set of loosely coupled services which keep the system state and help to
   carry out workload and data management tasks. The services are passive components which are only reacting to
   the requests of their clients possibly soliciting other services in order to accomplish the requests.
 - Each service has typically a MySQL database backend to store the state information.
 - The number of sites where services are installed is limited to those with well-controlled environment where
   an adequate support can be guaranteed. The services are deployed using system start-up scripts and watchdog
   processes which ensure automatic service restart at boot time and in case of service interruptions or crashes.
   Standard host certificates typically issued by national Grid Certification Authorities are used for the
   service/client authentication.
 - The services accept incoming connections from various clients. These can be user interfaces, agents or running
   jobs. But since services are passive components, they have to be complemented by special applications to
   animate the system.

Agents
@@@@@@@
Agents are light and easy to deploy software components which run as independent processes to fulfill one or
several system functions.

 - All the agents are built in the same framework which organizes the main execution loop and provides a uniform
   way for deployment, configuration, control and logging of the agent activity.
 - Agents run in different environments. Those that are part of the DIRAC subsystems, for example Workload
   Management or Data Distribution, are usually deployed close to the corresponding services. They watch for
   changes in the service states and react accordingly by initiating actions like job
   submission or result retrieval.
 - Agents can run on a gatekeeper node of a site controlled by the DIRAC Workload Management System.
   In this case, they are part of the DIRAC WMS ensuring the pull job scheduling paradigm. Agents can also
   run as part of a job executed on a Worker Node as so called "Pilot Agents".

Interfaces
@@@@@@@@@@
 - The DIRAC main programming language is Python and programming interfaces (APIs) are provided in this language.
 - For the users of the DIRAC system the functionality is available through a command line interface.
 - DIRAC also provides Web interfaces for users and system managers to monitor the system behaviour and to
   control the ongoing tasks. The Web interfaces are based on the DIRAC Web Portal framework which ensures
   secure access to the system service using X509 certificates loaded into the user browsers.

.. versionadded:: 8.0
    DIRAC also has the ability to implement additional interfaces based on the http protocol, see :ref:`apis`.

DIRAC Framework
--------------------

The Dirac framework for building secure SOA based systems provides generic components not specific to LHCb
which can be applied in the contexts of other VOs as well. The framework is written in the Python language
and includes the following components:

 - Configuration System
 - Logging System
 - Monitoring System

Configuration Service
@@@@@@@@@@@@@@@@@@@@@@@@@@@@

The Configuration Service is built in the DISET framework to provide static configuration parameters to
all the distributed DIRAC components. This is the backbone of the whole system and necessitates excellent
reliability. Therefore, it is organized as a single master service where all the parameter
updates are done and multiple read-only slave services which are distributed geographically, on VO-boxes
at Tier-1 LCG sites in the case of LHCb. All the servers are queried by clients in a load balancing way.
This arrangement ensures configuration data consistency together with very good scalability properties.

Logging and Monitoring Services
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

 - All the DIRAC components use the same logging facility which can be configured with one or more
   back-ends including standard output, log files or external service.
 - The amount of the logging information is determined by a configurable level specification.
 - Use of the logger permit report to the Logging Service where all the distributed components are
   encountering system failures.
 - This service accumulates information for the analysis of the behaviour of the whole distributed
   system including third party services provided by the sites and central grid services.
 - The quick error report analysis allows spotting and even fixing the problems before they hit the user.
 - The Monitoring Service collects activity reports from all the DIRAC services and some agents.
   It presents the monitoring data in a variety of ways, e.g. historical plots, summary reports, etc.
   Together with the Logging Service, it provides a complete view of the health of the system for the managers.

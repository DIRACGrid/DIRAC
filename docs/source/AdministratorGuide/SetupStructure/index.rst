==========================================
DIRAC Setup Structure
==========================================

The basic DIRAC components are *Databases*, *Services* and *Agents*. These components are combined 
together to form *Systems*.

  *Databases* 
    keep the persistent state of a *System*. They are accessed by Services and Agents as a 
    kind of shared memory.

  *Services* 
    are passive components listening to incoming client requests and reacting accordingly by
    serving requested information from the *Database* backend or inserting requests on the 
    *Database* backend. *Services* themselves can be clients of other *Services* from the same 
    DIRAC *System* or from other *Systems*.

  *Agents* 
    are the active components which are running continuously invoking periodically their execution 
    methods. Agents are animating the whole system by executing actions, sending requests 
    to the DIRAC or third party services. 
  
  *System* 
    is delivering a complex functionality to the rest of DIRAC, providing a solution for a 
    given class of tasks. Examples of *Systems* are Workload Management System or Configuration System.

To achieve a functional DIRAC installation, cooperation of different *Systems* is required. 
A set of *Systems* providing a complete functionality to the end user form a DIRAC *Setup*.
All DIRAC client installations will point to a particular DIRAC *Setup*. *Setups* can span
multiple server installations. Each server installation belongs to a DIRAC *Instance* that can 
be shared by multiple *Setups*.

*Setup* is the highest level of the DIRAC component hierarchy. *Setups* are combining
together instances of *Systems*. A given user community may have several *Setups*. 
For example, there can be "Production" *Setup* together with "Test" or "Certification" 
*Setups* used for development and testing of the new functionality. An instance of a *System* 
can belong to one or more *Setups*, in other words, different *Setups* can share some *System* 
instances. Multiple *Setups* for the given community share the same Configuration information
which allows them to access the same computing resources.

Each *System* and *Setup* instance has a distinct name. The mapping of *Systems* to
*Setups* is described in the Configuration of the DIRAC installation in the "/DIRAC/Setups"
section. 

*ToDo*
  - image illustrating the structure
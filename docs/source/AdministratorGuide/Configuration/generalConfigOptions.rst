.. _general_config_options:

=======================================
General Component Configuration Options
=======================================

This sections contains the configuration options that all services, agents, or executors share.


* Module: this is the name of the component class to be instantiated. If a component with this name
  also exists in the configuration, its options are inherited by the derived component
* LogLevel: possible values INFO,VERBOSE,DEBUG
* LogBackends: backends to send log messages to, possible values stdout, ESserver

Service Options
---------------

There are several options that can be defined for all the services.

* MaxThreads: max number of service threads (15 by default)
* MinThreads: min number of service threads (1 by default)
* MaxWaitingPetitions: max number of queries to be kept in the service queue (500 by default)
* Port: port the service listens on
* Protocol: service access protocol (dips by default)
* HandlerPath: path to the services handler code, e.g. DIRAC.WorkloadManagementSystem.Service.JobManager

Authorization section
@@@@@@@@@@@@@@@@@@@@@

The Authorization section in the service configuration defines access rules for the service
interface methods.

* Default: default access rules for all the methods not specified explicitly
* <Method>: access rules for a specific method

Access rules are defined in terms of user Group properties, group name or VO name.

Group properties are defined in the Group definition in the Registry section as
Properties option which accepts comma separated list of strings. These string values,
Group properties, can be used in the Method access rules. Group properties are not
limited to some predefined set of values. However, some common values are defined
in the :py:mod:`~DIRAC.Core.Security.Properties` as string constants that can be
used in the code. Use those values unless you need some more specific ones.

Group name can be specified directly in the method access rules as a property of the form
group:<group_name> .

VO name can be also specified directly as a property of the form vo:<vo_name> .

Other possible values are:

* `all` or `any`: for any user
* `authenticated` for registered users

The following example illustrates the general service configuration parameters ::

    Systems
    {
      DataManagementSystem
      {
        Services
        {
          FileCatalog
          {
            Port = 9196
            MaxThreads = 10
            MaxWaitingPetitions = 500
            Authorization
            {
              # By default any registered user can access all the methods except see below
              Default = authenticated
              # only users in the group with SuperDataAdmin property can use this method
              eraseAllData = SuperDataAdmin
            }
          }
          BiomedFileCatalog
          {
            Port = 9197
            MaxThreads = 25
            MaxWaitingPetitions = 250
            Authorization
            {
              # By default only users of the VO biomed can access all the methods except see below
              Default = vo:biomed
              # Only members of the biomed_data_admin can use this method
              eraseAllData = group:biomed_data_admin
              # Only users from VO biomed and belonging to groups with NormalBiomedUser
              # property can use this method
              getReplicas = vo:biomed, NormalBiomedUser
            }
          }
        }
      }
    }

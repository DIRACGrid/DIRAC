.. contents:: Table of contents
   :depth: 3

.. _static_component_monitoring:

Static Component Monitoring
===========================

.. versionadded:: v6r13

As of v6r13, DIRAC includes a Component Monitoring system that logs information about what components are being installed
and uninstalled on which machines, when and by whom. Running this service is mandatory!

This information is accessible from both the system administration CLI and the Component History page in the Web Portal.


Installation
============

The service constitutes of one database (InstalledComponentsDB) and one service (Framework/ComponentMonitoring).
These service and DB may have been installed already when DIRAC was installed the first time.

The script **dirac-populate-component-db** should then be used to populate the DB tables with the necessary information.


Interacting with the static component monitoring
================================================

Using the CLI (dirac-admin-sysadmin-cli), it is possible to check the information about installations
by using the 'show installations' command. This command accepts the following parameters:

- list: Changes the display mode of the results
- current: Show only the components that are still installed
- -n <name>: Show only installations of the component with the given name
- -h <host>: Show only installations in the given host
- -s <system>: Show only installations of components from the given system
- -m <module>: Show only installations of the given module
- -t <type>: Show only installations of the given type
- -itb <date>: Show installations made before the given date ('dd-mm-yyyy')
- -ita <date>: Show installations made after the given date ('dd-mm-yyyy')
- -utb <date>: Show installations of components uninstalled before the given date ('dd-mm-yyyy')
- -uta <date>: Show installations of components uninstalled after the given date ('dd-mm-yyyy')

It is also possible to retrieve the installations history information by using the 'Component History' app provided by the Web Portal.
The app allows to set a number of filters for the query. It is possible to filter by:

- Name: Actual name which the component/s whose information should be retrieved was installed with
- Host: Machine/s in which to look for installations
- System: System/s to which the components should belong. e.g: Framework, Bookkeeping ...
- Module: Module/s of the components. e.g: SystemAdministrator, BookkeepingManager, ...
- Type: Service, agent, executor, ...
- Date and time: It is possible to select a timespan during which the components should have been installed ( it is possible to fill just one of the two available fields )

By pressing the 'Submit' button, a list with all the matching results will be shown ( or all the possible results if no filters were specified ).

Dynamic Component Monitoring
============================

It shows information about running DIRAC components such as CPU, Memory, Running threads etc. The information can be accessed from the 'dirac-admin-sysadmin-cli' using
'show profile'. The following parameters can be used::

 - <system>: The name of the system for example: DataManagementSystem
 - <component>: The component name for example: FileCatalog
 - -s <size>: number of elements to be shown
 - h <host>: name of the host where a specific component is running
 - id <initial date DD/MM/YYYY> the date where from we are interested for the log of a specific component
 - it <initial time hh:mm> the time where from we are interested for the log of a specific component
 - ed <end date DD/MM/YYYY>: the date before we are interested for   the log of a specific component
 - et <end time hh:mm>: the time before we are interested for   the log of a specific component
 - show <size>: log lines of profiling information for a component in the machine <host>

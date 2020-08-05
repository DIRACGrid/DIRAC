.. _dirac-cs-structure:

===================================
DIRAC Configuration 
===================================

The DIRAC Configuration information is written in a *CFG* format.
`diraccfg <https://github.com/DIRACGrid/diraccfg/>`_ is a standalone parser for files written in a *CFG* format.

The DIRAC configuration has a hierarchical structure and can come from different sources.

This section describes the main sections of the DIRAC
configuration and the way how this information is delivered to the consumers.

Configuration structure
------------------------

The DIRAC Configuration is organized in a tree structure. It is divided in sections, which
can also be seen as directories. Each section can contain other sections and options.
The options are the leafs in the configuration tree, which contain the actual configuration data.

At the top level of the Configuration tree there are the following sections:

:ref:`DIRAC <dirac-general-cs>`
  This section contains the most general information about the DIRAC installation.
  
:ref:`Systems <dirac-systems-cs>`
  This section provides configuration data for all the DIRAC Systems, their instances and
  components - services, agents and databases.     

:ref:`Registry <dirac-registry-cs>`
  The *Registry* contains information about DIRAC users, groups and communities (VOs).
  
:ref:`Resources <dirac-resources-cs>`
  The *Resources* section provides description of all the DIRAC computing resources. This
  includes computing and storage elements as well as descriptions of several DIRAC and
  third party services.  
  
:ref:`Operations <dirac-operations-cs>`  
  This section collects various operational parameters needed to run the system.
  
The top level sections are described in details in dedicated chapters of the guide.

Configuration sources
-----------------------

The DIRAC Configuration can be defined in several places with strict rules how the settings
are resolved by the clients. The possible configuration data sources are listed below 
in the order of preference of the option resolution:

*Command line options*
  For all the DIRAC commands there is option '-o' defined which takes one configuration option
  setting. For example::
     
     dirac-wms-job-submit job.jdl -o /DIRAC/Setup=Dirac-Production

*Command line argument specifying a CFG file*
  A config file can be passed to any dirac command with the ``--cfg`` flag::
  
     dirac-wms-job-submit job.jdl --cfg my.cfg

  .. versionchanged:: v7r0

     The passing of ``.cfg`` files was changed to require the ``--cfg`` flag.

  .. deprecated:: v7r0

     If a filename with the ``.cfg`` extension is passed as an argument to any DIRAC command
     it will be interpreted as a configuration file, if the ``DIRAC_NO_CFG`` environment variable is not set.

*Value of $DIRACSYSCONFIG environment variable*
  if the DIRACSYSCONFIG variable is set, it should point to a cfg file (written in *CFG* format)

*$HOME/.dirac.cfg*
  This is the file in the user's home directory with the *CFG* format
  
*$DIRACROOT/etc/dirac.cfg*
  This is the configuration file in the root directory of the DIRAC installation
  
*Configuration Service*
  Configuration data available from the global DIRAC Configuration Service
  
The client needing a configuration option is first looking for it in the command line arguments. 
If the option is not found, the search continues in the user configuration file, then in the
DIRAC installation configuration file and finally in the Configuration Service. These gives
a flexible mechanism of overriding global options by specific local settings.
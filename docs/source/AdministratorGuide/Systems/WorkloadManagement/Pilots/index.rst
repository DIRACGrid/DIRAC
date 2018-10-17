.. _pilots:

========================
DIRAC pilots
========================

This page describes what are DIRAC pilots, and how they work.
To know how to develop DIRAC pilots, please refer to the Developers documentation

The current version of pilots are sometimes dubbed as "Pilots 2.0", or "the pilots to fly in all the skies".

It's in development a new generation of pilots, dubbed "Pilots 3". Pilots3 become, from this version, optional.
Pilots3 development is done in the separate repository https://github.com/DIRACGrid/Pilot
The definitions that follow in this page are still valid for Pilots3. 
Some specific information about Pilots3 can be found in the next sections.



What's a DIRAC Pilot
====================

First of all, a definition:
- A *pilot* is what creates the possibility to run jobs on a worker node. Or, in other words:
- a script that, at a minimum, setup (VO)DIRAC, sets the local DIRAC configuration, launches the an entity for matching jobs (e.g. the JobAgent)

A pilot can be sent, as a script to be run. Or, it can be fetched.

A pilot can run on every computing resource, e.g.: on CREAM Computing elements,
on DIRAC Computing elements, on Virtual Machines in the form of contextualization script,
or IAAC (Infrastructure as a Client) provided that these machines are properly configured.

A pilot has, at a minimum, to:

- install DIRAC
- configure DIRAC
- run the JobAgent

A pilot has to run on each and every computing resource type, provided that:

- Python 2.6+ on the WN
- It is an OS onto which we can install DIRAC

The same pilot script can be used everywhere.

.. image:: Pilots2.png
   :alt: Pilots.
   :align: center



Definitions that help understanding what's a pilot
==================================================

- *TaskQueue*: a queue of JDLs with similar requirements.
- *JobAgent*: a DIRAC agent that matches a DIRAC local configuration with a TaskQueue, and extracts a JDL from it (or more than one).
- *pilot wrapper*: a script that wraps the pilot script with conditions for running the pilot script itself (maybe multiple times).
- *pilot job*: a pilot wrapper sent to a computing element (e.g. CREAM, ARC).

The *pilot* is a "standardized" piece of code. The *pilot wrapper* is not.

An agent like the "SiteDirector" encapsulates the *pilot* in a *pilot wrapper*, then sends it to a Computing Element as a *pilot job*.
But, if you don't have the possibility to send a pilot job (e.g. the case of a Virtual Machine in a cloud),
you can still find a way to start the pilot script by encapsulating it in a pilot wrapper that will be started at boot time,
e.g. by supplying the proper contextualization to the VM.


Administration
==============

The following CS section is used for administering the DIRAC pilots::

   Operations/<Setup>/Pilot

These parameters will be interpreted by the WorkloadManagementSystem/SiteDirector agents, and by the WorkloadManagementSystem/Matcher.
They can also be accessed by other services/agents, e.g. for syncing purposes.

Inside this section, you should define the following options, and give them a meaningful value (here, an example is give)::

   # Needed by the SiteDirector:
   Version = v6r20p14 #Version to install. Add the version of your extension if you have one.
   Project = myVO #Your project name: this will end up in the /LocalSite/ReleaseProject option of the pilot cfg, and will be used at matching time
   Extensions = myVO #The DIRAC extension (if any)
   Installation = mycfg.cfg #For an optional configuration file, used by the installation script.
   # For the Matcher
   CheckVersion = False #True by default, if false any version would be accepted at matching level (this is a check done by the WorkloadManagementSystem/Matcher service).

When the *CheckVersion* option is "True", the version checking done at the Matcher level will be strict,
which means that pilots running different versions from those listed in the *Versions* option will refuse to match any job.
There is anyway the possibility to list more than one version in *Versions*; in this case, all of them will be accepted by the Matcher,
and in this case the pilot will install the first in this list (e.g. if Version=v6r20p14,v6r20p13 then pilots will install version v6r20p14)



Pilot Commands
==============

The system works with "commands", as explained in the RFC 18. Any command can be added.
If your command is executed before the "InstallDIRAC" command, pay attention that DIRAC functionalities won't be available.

Beware that, to send pilot jobs containing a specific list of commands using the SiteDirector agents,
you'll need a SiteDirector extension.

Basically, pilot commands are an implementation of the command pattern.
Commands define a toolbox of pilot capabilities available to the pilot script. Each command implements one function, like:

- Check the environment
- Get the pilot version to install
- Install (VO)DIRAC
- Configure (VO)DIRAC
- In fact, there are several configuration commands
- Configure CPU capabilities
- the famous “dirac-wms-cpu-normalization”
- Run the JobAgent

A custom list of commands can be specified using the --commands option,
but if nothing is selected then the following list will be run::

   'GetPilotVersion', 'CheckWorkerNode', 'InstallDIRAC', 'ConfigureBasics', 'CheckCECapabilities',
   'CheckWNCapabilities', 'ConfigureSite', 'ConfigureArchitecture', 'ConfigureCPURequirements',
   'LaunchAgent'

Communities can easily extend the content of the toolbox, adding more commands.
If necessary, different computing resources types can run different commands.


Pilot options
=============

The pilot can be configured to run in several ways.
Please, refer to https://github.com/DIRACGrid/DIRAC/blob/master/WorkloadManagementSystem/PilotAgent/pilotTools.py#L400
for the full list.



Pilot extensions
================

In case your VO only uses Grid resources, and the pilots are only sent by SiteDirector or TaksQueueDirector agents,
and you don't plan to have any specific pilot behaviour, you can stop reading here.

Instead, in case you want, for example, to install DIRAC in a different way, or you want your pilot to have some VO specific action,
you should carefully read the RFC 18, and what follows.

Pilot commands can be extended. A custom list of commands can be added starting the pilot with the -X option.


Pilots started when not controlled by the SiteDirector
======================================================

You should keep reading if your resources include IAAS and IAAC type of resources, like Virtual Machines.

We have introduced a special command named "GetPilotVersion" that you should use,
and possibly extend, in case you want to send/start pilots that don't know beforehand the (VO)DIRAC version they are going to install.
In this case, you have to provide a json file freely accessible that contains the pilot version.
This is tipically the case for VMs in IAAS and IAAC.

The files to consider are in https://github.com/DIRACGrid/DIRAC/blob/master/WorkloadManagementSystem/PilotAgent
The main file in which you should look is
https://github.com/DIRACGrid/DIRAC/blob/master/WorkloadManagementSystem/PilotAgent/dirac-pilot.py
that also contains a good explanation on how the system works.

You have to provide in this case a pilot wrapper script (which can be written in bash, for example) that will start your pilot script
with the proper environment. If you are on a cloud site, often contextualization of your virtual machine is done by supplying
a script like the following: https://gitlab.cern.ch/mcnab/temp-diracpilot/raw/master/user_data (this one is an example from LHCb)

A simpler example is the following::

  #!/bin/sh
  #
  # Runs as dirac. Sets up to run dirac-pilot.py
  #

  date --utc +"%Y-%m-%d %H:%M:%S %Z vm-pilot Start vm-pilot"

  for i in "$@"
  do
  case $i in
      --dirac-site=*)
      DIRAC_SITE="${i#*=}"
      shift
      ;;
      --lhcb-setup=*)
      LHCBDIRAC_SETUP="${i#*=}"
      shift
      ;;
      --ce-name=*)
      CE_NAME="${i#*=}"
      shift
      ;;
      --vm-uuid=*)
      VM_UUID="${i#*=}"
      shift
      ;;
      --vmtype=*)
      VMTYPE="${i#*=}"
      shift
      ;;
      *)
      # unknown option
      ;;
  esac
  done

  # Default if not given explicitly
  LHCBDIRAC_SETUP=${LHCBDIRAC_SETUP:-LHCb-Production}

  # JOB_ID is used by when reporting LocalJobID by DIRAC watchdog
  #export JOB_ID="$VMTYPE:$VM_UUID"

  # We might be running from cvmfs or from /var/spool/checkout
  export CONTEXTDIR=`readlink -f \`dirname $0\``

  export TMPDIR=/scratch/
  export EDG_WL_SCRATCH=$TMPDIR

  # Needed to find software area
  export VO_LHCB_SW_DIR=/cvmfs/lhcb.cern.ch

  # Clear it to avoid problems ( be careful if there is more than one agent ! )
  rm -rf /tmp/area/*

  # URLs where to get scripts
  DIRAC_INSTALL='https://raw.githubusercontent.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py'
  DIRAC_PILOT='https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/WorkloadManagementSystem/PilotAgent/dirac-pilot.py'
  DIRAC_PILOT_TOOLS='https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/WorkloadManagementSystem/PilotAgent/pilotTools.py'
  DIRAC_PILOT_COMMANDS='https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/WorkloadManagementSystem/PilotAgent/pilotCommands.py'
  LHCbDIRAC_PILOT_COMMANDS='http://svn.cern.ch/guest/dirac/LHCbDIRAC/trunk/LHCbDIRAC/WorkloadManagementSystem/PilotAgent/LHCbPilotCommands.py'

  echo "Getting DIRAC Pilot 2.0 code from lhcbproject for now..."
  DIRAC_INSTALL='https://lhcbproject.web.cern.ch/lhcbproject/Operations/VM/pilot2/dirac-install.py'
  DIRAC_PILOT='https://lhcbproject.web.cern.ch/lhcbproject/Operations/VM/pilot2/dirac-pilot.py'
  DIRAC_PILOT_TOOLS='https://lhcbproject.web.cern.ch/lhcbproject/Operations/VM/pilot2/pilotTools.py'
  DIRAC_PILOT_COMMANDS='https://lhcbproject.web.cern.ch/lhcbproject/Operations/VM/pilot2/pilotCommands.py'

  #
  ##get the necessary scripts
  wget --no-check-certificate -O dirac-install.py $DIRAC_INSTALL
  wget --no-check-certificate -O dirac-pilot.py $DIRAC_PILOT
  wget --no-check-certificate -O pilotTools.py $DIRAC_PILOT_TOOLS
  wget --no-check-certificate -O pilotCommands.py $DIRAC_PILOT_COMMANDS
  wget --no-check-certificate -O LHCbPilotCommands.py $LHCbDIRAC_PILOT_COMMANDS

  #run the dirac-pilot script
  python dirac-pilot.py \
   --debug \
   --setup $LHCBDIRAC_SETUP \
   --project LHCb \
   -o '/LocalSite/SubmitPool=Test' \
   --configurationServer dips://lhcb-conf-dirac.cern.ch:9135/Configuration/Server \
   --Name "$CE_NAME" \
   --MaxCycles 1 \
   --name "$1" \
   --cert \
   --certLocation=/scratch/dirac/etc/grid-security \
   --commandExtensions LHCbPilot \
   --commands LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent

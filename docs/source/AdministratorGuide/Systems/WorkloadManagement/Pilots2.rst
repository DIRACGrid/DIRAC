Pilots 2.0
===========

The main point of DIRAC version v6r12 is the introduction of a new type of pilot, that is, for most parts, an implementation of the points discussed within https://github.com/DIRACGrid/DIRAC/wiki/Pilots-2.0:-generic,-configurable-pilots. These changes will be transparent to VOs.
Also, several changes of the Data Management system are done.

# Changes for the pilot

In case your VO only uses Grid resources, and the pilots are only sent by SiteDirector and TaksQueueDirector agents, and you don't plan to have any specific pilot behaviour, you can stop reading here: the new pilot won't have anything different from the old pilot that you will notice.

Instead, in case you want, for example, to install DIRAC in a different way, or you want your pilot to have some VO specific action, you should carefully read the RFC 18, and what follows.
You should also keep reading if your resources include IAAS and IAAC type of resources, like Virtual Machines.

The files to consider are in https://github.com/DIRACGrid/DIRAC/tree/rel-v6r12/WorkloadManagementSystem/PilotAgent 
The main file in which you should look is https://github.com/DIRACGrid/DIRAC/blob/rel-v6r12/WorkloadManagementSystem/PilotAgent/dirac-pilot.py that also contains a good explanation on how the system works. 

The system works with "commands", as explained in the RFC. Any command can be added. If your command is executed before the "InstallDIRAC" command, pay attention that DIRAC functionalities won't be available.

We have introduced a special command named "GetPilotVersion" in https://github.com/DIRACGrid/DIRAC/blob/rel-v6r12/WorkloadManagementSystem/PilotAgent/pilotCommands.py that you should use, and possibly extend, in case you want to send/start pilots that don't know beforehand the (VO)DIRAC version they are going to install. In this case, you have to provide a json file freely accessible that contains the pilot version. This is tipically the case for VMs in IAAS and IAAC.

Beware that, to send pilots containing a specific list of commands via SiteDirector agents need a SiteDirector extension.

.. _installingVMDIRAC:

==================
Installing VMDIRAC
==================

.. toctree::
   :maxdepth: 2

.. contents:: Table of contents
   :depth: 4

------------
Introduction
------------

VMDIRAC has now been merged into the main DIRAC release (as of v7r3/7.3). It
consists of the following parts:

VirtualMachineDB (Database) - This is the database that stores the details of each VM
managed by VMDIRAC. There should be one instance of this on your system.

CloudDirector (Agent) - This is analogous to the core DIRAC SiteDirector. It inspects
the TaskQueues, works out if there are any compatible jobs waiting (without
existing VMs) for cloud resources and starts VM instances as needed. The VM
details are stored in the database for future reference. You need at least one
CloudDirector, for multi community/VO services, it's advisable to have one
CloudDirector per community.

VirtualMachineManager (Service) - This service provides the virtual machine
life-cycle management interface; the CLI tools contact this service to
list/kill running VMs. It also has an inbuilt thread that will inspect existing
VMs, tidying up any that are stopped or haven't reported back for a long time.

VirtualMachineMonitor (Agent) - This agent monitors the health of the current
VM it is running on an reports it back to the VirtualMachineManager. It should
not be installed on the central DIRAC instance, but started in the cloud VM
instances themselves. This lets the VirtualMachineManager know that the
instance is still alive and also handles stopping the instance after any
tasks/jobs have finished.

VMDIRAC WebApp Extention - This WebApp plugin adds an extra VirtualMachines tab
to the usual DIRAC webinterface for summarising the contents of the database.

---------------
Install VMDIRAC
---------------

On a DIRAC server (generally collocated with the other WorkdloadManagement
system components), configure and install the extra components:

* Server local configuration

  In the server local configuration add the User/Password information to connect to the Cloud endpoint. Also you should put a valid path for the host certificate, *e.g.*:

  ::

      Resources
      {
        Sites
        {
          Cloud
          {
            Cloud.LUPM.fr
            {
              Cloud
              {
                194.214.86.244
                {
                  User = xxxx
                  Password = xxxx
                  HostCert = /opt/dirac/etc/grid-security/hostcert.pem
                  HostKey = /opt/dirac/etc/grid-security/hostkey.pem
                  CreatePublicIP = True
                }
              }
            }
          }
        }
      }

* Install the following components:
  
  * DB: VirtualMachineDB
  * Service: WorkloadManagement_VirtualMachineManager 
  * Agent: WorkloadManagement_CloudDirector

------------------------------------
Setup for using cloudinit and Pilot3
------------------------------------

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Using OpenStack with an application credential
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* The user must have access to the OpenStack cloud and be allowed to start up instances.
* Login to the cloud webinterface with your account.
* Go to the identity -> Application credentials panel
* Create a new credential with any name, no expire and no roles selected
* Copy the ID and secret strings somewhere safe for a moment
*   Put the ID and string into a new file on the DIRAC server running the
    CloudDirectors in the following format (one line, separated by a space): 
    <ID> <Secret>
* Make sure the file is owned by the user running dirac and has 0600 permissions.
* Add the location of this file to the Resource Settings.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
OpenStack Resource Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
On the OpenStack Resource you will need the following:

* user account
* user's ssh key uploaded to the OpenStack server: Associated with the instance for debugging. 
  This key is user specific, not project specific. 
* flavour (ID or name)
* image (ID or name)
* network (ID or name)

  ::
 
     CLOUD.ExampleName.uk
      {
	     Name = [ExampleName]
	     # must be unique, use e.g. hostname of the OpenStack webinterface
        CE = [hostname.example.ac.uk]
        Cloud
        {
          [hostname.example.ac.uk]
          {
            # assuming your cloud is using a standard CA
            CAPath = /etc/pki/tls/cert.pem
            # list your favourite VOs here 
            VO = gridpp
            VO += lz
            NetworkID = [network uuid]
            Network = [name_of_network]
            CEType = OpenStack
            MaxInstances = [maximum number of instances]
            AuthURL = [https://keystone.example.ac.uk:5000/v3]
            Appcred = [path to appcred file created earlier]
            # this might be optional
            CVMFSProxy = http://[your cvmfs proxy cache]:3128
            Images
            {
              [image name, e.g. CentOS-7-x86_64-GenericCloud-1905]
              {
                ImageID = [image uuid]
                FlavorName = [flavour name]
                # this is currently a dummy value
                Platform = [DIRACPlatForm]
              }
            }
            OSKeyName = [ssh key name of the OpenStack user]
            Tenant = [Openstack Project Name]
          }
        }

        CEs
        {
          [hostname.example.ac.uk]
          {
            CEType = Cloud
            Architecture = x86_64
            Queues
            {
              [image name]
              {
                maxCPUTime = 24000000
              }
            }
          }
        }
      }

------------------------------
Configuration - other examples
------------------------------

* In the CS Resources section, configure the cloud endpoint as in this example

  ::

      Resources
      {
        Sites
        {
          Cloud
          {
            Cloud.LUPM.fr
            {
              CE = 194.214.86.244
              Cloud
              {
                194.214.86.244
                {
                  CEType = Cloud
                  ex_security_groups = default
                  ex_force_auth_url = http://194.214.86.244:5000/v3/auth/tokens
                  ex_force_service_region = LUPM-CLOUD
                  # This is the max number of VM instances that will be running in parallel
                  # Each VM can have multiple cores, each one executing a job
                  MaxInstances = 4
                  ex_force_auth_version = 3.x_password
                  ex_tenant_name = dirac
                  ex_domain_name = msfg.fr
                  networks = dirac-net
                  # This is the public key previously uploaded to the Cloud provider
                  # It's needed to ssh connect to VMs
                  keyname = cta_cloud_lupm
                  # If this option is set, public IP are assigned to VMs 
                  # It's needed to ssh connect to VMs
                  ipPool = ext-net
                }
                Images
                {
                  # It can be a public or a private image
                  Centos6-Officielle
                  {
                    ImageID = 35403255-f5f1-4c61-96dc-e59678942c6d
                    FlavorName = m1.medium
                  }
                }
              }
            }
          }
        }
      }


* CS Operation section

  ::
     
      Operations
      {
        CTA
        {
          Cloud
          {
            GenericCloudGroup = cta_genpilot
            GenericCloudUser = arrabito
            user_data_commands = vm-bootstrap
            user_data_commands += vm-bootstrap-functions
            user_data_commands += vm-pilot
            user_data_commands += vm-monitor-agent
            user_data_commands += pilotCommands.py
            user_data_commands += pilotTools.py
            user_data_commands += dirac-install.py
            user_data_commands += power.sh
            user_data_commands += parse-jobagent-log
            user_data_commands += dirac-pilot.py
            user_data_commands += save-payload-logs
            # url from which command scripts are downloaded. Usually the url of the web server
            user_data_commands_base_url = http://cta-dirac.in2p3.fr/DIRAC/defaults
            Project = CTA
            Version = v1r40
          }
        }
      }

* CS Registry section

  The host where VMDIRAC is installed and the certificate of which is used for the VMs, it should have these 2 properties set (as in the example below):

    * Properties = GenericPilot (needed to make pilots running on the VM matching jobs in the TaskQueue)
    * Properties = VmRpcOperation (needed by the VirtualMachineMonitorAgent running on the VM to be authorized to send Heartbeats to the VirtualMachineManager service)
    
    ::
       
      Registry
      {
        Hosts
        {
          dcta-agents01.pic.es
          {
            DN = /DC=org/DC=terena/DC=tcs/C=ES/ST=Barcelona/L=Bellaterra/O=Institut de Fisica dAltes Energies/CN=dcta-agents01.pic.es
            CA = /C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience SSL CA 3
            Properties = FullDelegation
            Properties += CSAdministrator
            Properties += ProxyManagement
            Properties += SiteManager
            Properties += Operator
            Properties += JobAdministrator
            Properties += CSAdministrator
            Properties += TrustedHost
            Properties += GenericPilot
            Properties += VmRpcOperation
          }
        }
      }

----------------------
Install VMDIRAC WebApp
----------------------

* The VirtualMachines panel is now included in the main WebApp release.
* If using the old (non cloudinit) bootstrap, you must Create sym links for the
  old bootstrap scripts:

    ::
       
       $ ll /opt/dirac/webRoot/www/defaults/bootstrap
       total 0
       lrwxrwxrwx 1 dirac dirac 50 Feb 21 08:46 dirac-install.py -> /opt/dirac/pro/DIRAC/Core/scripts/dirac-install.py
       lrwxrwxrwx 1 dirac dirac 71 Feb 21 08:49 dirac-pilot.py -> /opt/dirac/pro/DIRAC/WorkloadManagementSystem/PilotAgent/dirac-pilot.py
       lrwxrwxrwx 1 dirac dirac 76 Feb 21 08:50 parse-jobagent-log -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/parse-jobagent-log
       lrwxrwxrwx 1 dirac dirac 73 Feb 21 08:51 pilotCommands.py -> /opt/dirac/pro/DIRAC/WorkloadManagementSystem/PilotAgent/pilotCommands.py
       lrwxrwxrwx 1 dirac dirac 70 Feb 21 08:51 pilotTools.py -> /opt/dirac/pro/DIRAC/WorkloadManagementSystem/PilotAgent/pilotTools.py
       lrwxrwxrwx 1 dirac dirac 66 Feb 21 08:52 power.sh -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/power.sh
       lrwxrwxrwx 1 dirac dirac 75 Feb 21 08:52 save-payload-logs -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/save-payload-logs
       lrwxrwxrwx 1 dirac dirac 70 Feb 21 11:47 vm-bootstrap -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/vm-bootstrap
       lrwxrwxrwx 1 dirac dirac 80 Feb 21 08:52 vm-bootstrap-functions -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/vm-bootstrap-functions
       lrwxrwxrwx 1 dirac dirac 74 Feb 21 08:53 vm-monitor-agent -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/vm-monitor-agent
       lrwxrwxrwx 1 dirac dirac 66 Feb 21 08:53 vm-pilot -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/vm-pilot

  

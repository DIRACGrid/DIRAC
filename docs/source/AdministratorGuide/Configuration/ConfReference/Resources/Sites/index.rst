.. _cs-site:

Resources / Sites - Subsections
===============================

In this section each DIRAC site available for the users is described. The convention to name the sites consist of 3 strings:

- Grid site name, expressed in uppercase, for example: LCG, EELA
- Institution acronym in uppercase, for example: CPPM
- Country: country where the site is located, expressed in lowercase, for example fr

The three strings are concatenated with "." to produce the name of the sites.

+------------------------------------+-----------------------------------------------+-----------------------------------+
| **Name**                           | **Description**                               | **Example**                       |
+------------------------------------+-----------------------------------------------+-----------------------------------+
| *<DIRAC_SITE_NAME>*                | Subsection named with the site name           | LCG.CPPM.fr                       |
+------------------------------------+-----------------------------------------------+-----------------------------------+
| *<DIRAC_SITE_NAME>/Name*           | Site name gave by the site administrator      | Name = in2p3                      |
|                                    | e.g.: the name of the site in GOCDB (optional)|                                   |
+------------------------------------+-----------------------------------------------+-----------------------------------+
| *<DIRAC_SITE_NAME>/CE*             | List of CEs using CE FQN                      | CE = ce01.in2p3.fr                |
|                                    | These CEs are updated by the BDII2CSAgent     | CE += ce02.in2p3.fr               |
|                                    | in the CEs section                            |                                   |
+------------------------------------+-----------------------------------------------+-----------------------------------+
| *<DIRAC_SITE_NAME>/MoUTierLevel*   | Tier Level (optional)                         | MoUTierLevel = 1                  |
+------------------------------------+-----------------------------------------------+-----------------------------------+
| *<DIRAC_SITE_NAME>/CEs/*           | Subsection used to describe each CE available | CEs                               |
+------------------------------------+-----------------------------------------------+-----------------------------------+
| *<DIRAC_SITE_NAME>/Coordinates*    | Site geographical coordinates (optional)      | Coordinates = -8.637979:41.152461 |
+------------------------------------+-----------------------------------------------+-----------------------------------+
| *<DIRAC_SITE_NAME>/Mail*           | Mail address site responsable (optional)      | Mail = atsareg@in2p3.fr           |
+------------------------------------+-----------------------------------------------+-----------------------------------+
| *<DIRAC_SITE_NAME>/SE*             | Closest SE respect to the CE (optional)       | SE = se01.in2p3.fr                |
+------------------------------------+-----------------------------------------------+-----------------------------------+


CEs  sub-subsection
-------------------

This sub-subsection specify the attributes of each particular CE of the site. Must be noticed than in each DIRAC site can be more than one CE.

+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| **Name**                                       | **Description**                                              | **Example**                    |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>*                                    | Subsection named as the CE fully qualified name              | ce01.in2p3.fr                  |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/architecture*                       | CE architecture                                              | architecture = x86_64          |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/CEType*                             | Type of CE, can take values as LCG or CREAM                  | CEType = ARC                   |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/LocalCEType*                        | Type of 'Inner' CE, normally empty. Default = "InProcess".   | LocalCEType = Pool             |
|                                                | Possibilities: potentially all CE types, but in practice     |                                |
|                                                | the most valid would be: InProcess, Sudo, Singularity, Pool. |                                |
|                                                | Pool CE in turn uses InProcess (Default)                     |                                |
|                                                | or Sudo or Singularity. To specify, use Pool/ce_type.        | LocalCEType = Pool/Singularity |
|                                                | This option can also go at the Queue level.                  |                                |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/OS*                                 | CE operating system in a DIRAC format                        | OS = ScientificLinux_Boron_5.3 |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Pilot*                              | Boolean attributes than indicates if the site accept pilots  | Pilot = True                   |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/SubmissionMode*                     | If the CE is a cream CE the mode of submission               | SubmissionMode = Direct        |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/wnTmpDir*                           | Worker node temporal directory                               | wnTmpDir = /tmp                |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/MaxProcessors*                      | Maximum number of available processors on worker nodes       | MaxProcessors = 12             |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/WholeNode*                          | CE allows *whole node* jobs                                  | WholeNode = True               |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Tag*                                | List of tags specific for the CE                             | Tag = GPU,96RAM                |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/RequiredTag*                        | List of required tags that a job to be eligible must have    | RequiredTag = GPU,96RAM        |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues*                             | Subsection. Queues available for this VO in the CE           | Queues                         |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>*                | Name of the queue exactly how is published                   | jobmanager-pbs-formation       |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/CEQueueName*    | Name of the queue in the corresponding CE if not the same    |                                |
|                                                | as the name of the queue section                             | CEQueueName = pbs-grid         |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/maxCPUTime*     | Maximum time allowed to jobs to run in the queue             | maxCPUTime = 1440              |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/MaxTotalJobs*   | If the CE is a CREAM CE the maximum number of jobs in all    | MaxTotalJobs =200              |
|                                                | the status                                                   |                                |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/MaxWaitingJobs* | If the CE is a CREAM CE the maximum number of jobs in        | MaxWaitingJobs = 70            |
|                                                | waiting status                                               |                                |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/OutputURL*      | If the CE is a CREAM CE the URL where to find the outputs    | OutputURL = gsiftp://localhost |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/SI00*           | CE CPU Scaling Reference                                     | SI00 = 2130                    |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/MaxProcessors*  | overrides *<CE_NAME>/MaxProcessors* at queue level           | MaxProcessors = 12             |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/WholeNode*      | overrides *<CE_NAME>/WholeNode* at queue level               | WholeNode = True               |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/Tag*            | List of tags specific for the Queue                          | Tag = GPU,96RAM                |
+------------------------------------------------+------------------------------------------------------------- +--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/RequiredTag*    | List of required tags that a job to be eligible must have    | RequiredTag = GPU,96RAM        |
+------------------------------------------------+--------------------------------------------------------------+--------------------------------+
| *<CE_NAME>/Queues/<QUEUE_NAME>/LocalCEType*    | Same as *<CE_NAME>/LocalCEType* (see above) but per queue.   | LocalCEType = Pool/Singularity |
+------------------------------------------------+--------------------------------------------------------------+--------------------------------+


An example for this session follows::

  Sites
  {
    LCG
    {
      LCG.CERN.cern
      {
        SE = CERN-RAW
        SE += CERN-RDST
        SE += CERN-USER
        CE = ce503.cern.ch
        CE += ce504.cern.ch
        Name = CERN-PROD
        Coordinates = 06.0458:46.2325
        Mail = grid-cern-prod-admins@cern.ch
        MoUTierLevel = 0
        Description = CERN European Organization for Nuclear Research
        CEs
        {
          ce503.cern.ch
          {
            wnTmpDir = .
            architecture = x86_64
            OS = ScientificCERNSLC_Carbon_6.4
            SI00 = 0
            Pilot = False
            CEType = HTCondorCE
            SubmissionMode = Direct
            Queues
            {
              ce503.cern.ch-condor
              {
                VO = lhcb
                VO += LHCb
                SI00 = 3100
                MaxTotalJobs = 5000
                MaxWaitingJobs = 200
                maxCPUTime = 7776
                LocalCEType = Pool/Singularity
                Tag = MultiProcessor
              }
            }
            VO = lhcb
            MaxRAM = 0
            UseLocalSchedd = False
            DaysToKeepLogs = 1
          }
          ce504.cern.ch
          {
            wnTmpDir = .
            architecture = x86_64
            OS = ScientificCERNSLC_Carbon_6.4
            SI00 = 0
            Pilot = False
            CEType = HTCondorCE
            LocalCEType = Pool
            SubmissionMode = Direct
            Queues
            {
              ce504.cern.ch-condor
              {
                VO = lhcb
                VO += LHCb
                SI00 = 3100
                MaxTotalJobs = 5000
                MaxWaitingJobs = 200
                maxCPUTime = 7776
              }
            }
          }
        }
      }
    }
    DIRAC
    {
      DIRAC.HLTFarm.lhcb
      {
        Name = LHCb-HLTFARM
        CE = OnlineCE.lhcb
        CEs
        {
          OnlineCE.lhcb
          {
            CEType = CREAM
            Queues
            {
              OnlineQueue
              {
                maxCPUTime = 2880
              }
            }
          }
        }
        AssociatedSEs
        {
          Tier1-RDST = CERN-RDST
          Tier1_MC-DST = CERN_MC-DST-EOS
          Tier1-Buffer = CERN-BUFFER
          Tier1-Failover = CERN-EOS-FAILOVER
          Tier1-BUFFER = CERN-BUFFER
          Tier1-USER = CERN-USER
          SE-USER = CERN-USER
        }
      }
    }
    VAC
    {
      VAC.Manchester.uk
      {
        Name = UKI-NORTHGRID-MAN-HEP
        CE = vac01.blackett.manchester.ac.uk
        CE += vac02.blackett.manchester.ac.uk
        Coordinates = -2.2302:53.4669
        Mail = ops@NOSPAMtier2.hep.manchester.ac.uk
        CEs
        {
          vac01.blackett.manchester.ac.uk
          {
            CEType = Vac
            architecture = x86_64
            OS = ScientificSL_Carbon_6.4
            wnTmpDir = /scratch
            SI00 = 2200
            MaxCPUTime = 1000
            Queues
            {
              default
              {
                maxCPUTime = 1000
              }
            }
          }
          vac02.blackett.manchester.ac.uk
          {
            CEType = Vac
            architecture = x86_64
            OS = ScientificSL_Carbon_6.4
            wnTmpDir = /scratch
            SI00 = 2200
            MaxCPUTime = 1000
            Queues
            {
              default
              {
                maxCPUTime = 1000
              }
            }
          }
        }
      }
    }
  }

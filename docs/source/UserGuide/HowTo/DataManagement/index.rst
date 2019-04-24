.. _howto_user_dms:

==============
DataManagement
==============


For an introduction about DataManagement concepts, please see the :ref:`introduction <dms-concepts>`

All the commands mentionned bellow can accept several StorageElements and LFNs as parameters. Please use `--help` for more details.

Basics
======

How to list one's own files
---------------------------

For a user to know its own file list::

    [Dirac prod] chaen $ dirac-dms-user-lfns
    Will search for files in /lhcb/user/c/chaen
    /lhcb/user/c/chaen: 5 files, 2 sub-directories
    /lhcb/user/c/chaen/GangaInputFile: 0 files, 1 sub-directories
    /lhcb/user/c/chaen/GangaInputFile/Job_2: 1 files, 0 sub-directories
    /lhcb/user/c/chaen/subDir: 1 files, 0 sub-directories
    7 matched files have been put in lhcb-user-c-chaen.lfns

How to see the various replicas of a file
-----------------------------------------

To list the SE where a file is stored::

    [DIRAC prod] chaen $ dirac-dms-lfn-replicas /lhcb/user/c/chaen/diracTutorial.txt
    Successful :
        /lhcb/user/c/chaen/diracTutorial.txt :
            CERN-USER : srm://srm-eoslhcb.cern.ch:8443/srm/v2/server?SFN=/eos/lhcb/grid/user/lhcb/user/c/chaen/diracTutorial.txt
             RAL-USER : srm://srm-lhcb.gridpp.rl.ac.uk:8443/srm/managerv2?SFN=/castor/ads.rl.ac.uk/prod/lhcb/user/c/chaen/diracTutorial.txt


How to get the xroot URL for my LFN
-----------------------------------

In order to get an xroot URL usable from for example ROOT::

    [DIRAC prod] chaen $ dirac-dms-lfn-accessURL --Protocol=root,xroot /lhcb/user/c/chaen/diracTutorial.txt RAL-USER
    Successful :
        RAL-USER :
            /lhcb/user/c/chaen/diracTutorial.txt : root://clhcbstager.ads.rl.ac.uk//castor/ads.rl.ac.uk/prod/lhcb/user/c/chaen/diracTutorial.txt?svcClass=lhcbUser


If you do not specify a StorageElement, DIRAC will check the URLs for all the replicas::

    [DIRAC prod] chaen $ dirac-dms-lfn-accessURL --Protocol=root,xroot /lhcb/user/c/chaen/diracTutorial.txt
    Using the following list of SEs: ['CERN-USER', 'RAL-USER']
    Successful :
        CERN-USER :
            /lhcb/user/c/chaen/diracTutorial.txt : root://eoslhcb.cern.ch//eos/lhcb/grid/user/lhcb/user/c/chaen/diracTutorial.txt
        RAL-USER :
            /lhcb/user/c/chaen/diracTutorial.txt : root://clhcbstager.ads.rl.ac.uk//castor/ads.rl.ac.uk/prod/lhcb/user/c/chaen/diracTutorial.txt?svcClass=lhcbUser

How to upload a file to a grid storage
--------------------------------------

To put a local file to a GRID storage::

    [DIRAC prod] chaen $ dirac-dms-add-file /lhcb/user/c/chaen/diracTutorial.txt ./diracTutorial.txt CERN-USER
    Could not obtain GUID from file through Gaudi, using standard DIRAC method

    Uploading ./diracTutorial.txt as /lhcb/user/c/chaen/diracTutorial.txt
    Successfully uploaded ./diracTutorial.txt to CERN-USER (0.7 seconds)

How to replicate an LFN to another storage
------------------------------------------

The file has to be already on a grid storage::

    [DIRAC prod] chaen $ dirac-dms-replicate-lfn  /lhcb/user/c/chaen/diracTutorial.txt RAL-USER
    Successful :
        RAL-USER :
            /lhcb/user/c/chaen/diracTutorial.txt :
                 register : 0.216005086899
                replicate : 5.27293300629

How to have the metadata of an file
-----------------------------------

To get the metadata of an LFN as stored in the catalog::

    [DIRAC prod] chaen $ dirac-dms-lfn-metadata /lhcb/user/c/chaen/diracTutorial.txt
    Successful :
        /lhcb/user/c/chaen/diracTutorial.txt :
                    Checksum : 2a810562
                ChecksumType : Adler32
                CreationDate : 2018-12-20 18:33:40
                      FileID : 390920814
                         GID : 2746
                        GUID : 15C4C7B2-47F3-9BDE-CA19-60A1E348EF90
                        Mode : 775
            ModificationDate : 2018-12-20 18:33:40
                       Owner : chaen
                  OwnerGroup : lhcb_prmgr
                        Size : 14
                      Status : AprioriGood
                         UID : 20269

To get the metadata of the file actually stored, you can use the following command (with or without SE specification)::

    [DIRAC prod] chaen $ dirac-dms-pfn-metadata /lhcb/user/c/chaen/diracTutorial.txt
    Getting replicas for 1 files : completed in 0.1 seconds
    Getting SE metadata of 2 replicas : completed in 1.5 seconds
    Successful :
        /lhcb/user/c/chaen/diracTutorial.txt :
            CERN-USER :
                      Accessible : True
                        Checksum : 2a810562
                       Directory : False
                      Executable : False
                            File : True
                FileSerialNumber : 10376293541461674751
                         GroupID : 1470
                      LastAccess : 2018-12-20 19:33:39
                           Links : 1
                         ModTime : 2018-12-20 19:33:39
                            Mode : 400
                        Readable : True
                            Size : 14
                    StatusChange : 2018-12-20 19:33:39
                          UserID : 56212
                       Writeable : False

           RAL-USER :
                     Accessible : True
                         Cached : 1
                       Checksum : 2a810562
                      Directory : False
                     Executable : False
                           File : True
               FileSerialNumber : 0
                        GroupID : 46
                     LastAccess : Never
                          Links : 1
                           Lost : 0
                       Migrated : 0
                        ModTime : 2018-12-20 19:35:13
                           Mode : 644
                       Readable : True
                           Size : 14
                   StatusChange : 2018-12-20 19:35:13
                    Unavailable : 0
                         UserID : 45
                      Writeable : True

    [DIRAC prod] chaen $ dirac-dms-pfn-metadata /lhcb/user/c/chaen/diracTutorial.txt CERN-USER
    Getting replicas for 1 files : completed in 0.1 seconds
    Getting SE metadata of 1 replicas : completed in 1.0 seconds
    Successful :
        /lhcb/user/c/chaen/diracTutorial.txt :
            CERN-USER :
                      Accessible : True
                        Checksum : 2a810562
                       Directory : False
                      Executable : False
                            File : True
                FileSerialNumber : 10376293541461674751
                         GroupID : 1470
                      LastAccess : 2018-12-20 19:33:39
                           Links : 1
                         ModTime : 2018-12-20 19:33:39
                            Mode : 400
                        Readable : True
                            Size : 14
                    StatusChange : 2018-12-20 19:33:39
                          UserID : 56212
                       Writeable : False


How to remove a replica of a file
---------------------------------

In order to remove one of the replicas::

    [DIRAC prod] chaen $ dirac-dms-remove-replicas /lhcb/user/c/chaen/diracTutorial.txt CERN-USER
    Removing replicas : completed in 1.8 seconds
    Successfully removed 1 replicas from CERN-USER

How to remove a file from the grid
----------------------------------

Watch out, this will remove all the replicas of a file::

    [DIRAC prod] chaen $ dirac-dms-remove-files /lhcb/user/c/chaen/diracTutorial.txt
    Removing 1 files : completed in 1.9 seconds
    Successfully removed 1 files



.. include:: metadata.rst

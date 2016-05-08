========================
4. Data Management Basic
========================


4.1 Getting information
-----------------------

4.1.1 SE availability
@@@@@@@@@@@@@@@@@@@@@

- The first thing is to know which Storage Elements are available for the VO, run the command::

        dirac-dms-show-se-status

  For example::

        $ dirac-dms-show-se-status
        Storage Element               Read Status    Write Status
        DIRAC-USER                         Active          Active
        IN2P3-disk                         Active          Active
        IPSL-IPGP-disk                     Active          Active
        IRES-disk                        InActive        InActive
        M3PEC-disk                         Active          Active
        ProductionSandboxSE                Active          Active

  SE names are defined in the DIRAC Configuration. These are the names that you will use with various data management commands. 

4.2 Uploading a file to the Grid
--------------------------------

- The next step is to upload a file to a Storage Element and register it into the DIRAC File Catalog. Execute the command::

        dirac-dms-add-file <LFN> <FILE> <SE>

  Output must look like this::

        $ dirac-dms-add-file /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt Test-Lyon.orig M3PEC-disk
        {'Failed': {},
         'Successful': {'/vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt': {'put': 8.3242118358612061,
                                                                                    'register': 0.51048803329467773}}}

  Note: The output of this command must be successful before continuing with other exercises.

4.3 Obtaining information about the data
----------------------------------------

4.3.1 Metadata
@@@@@@@@@@@@@@

- After a file is registered into DIRAC File Catalog the metadata could be consulted any time with::

        dirac-dms-catalog-metadata <LFN>

  For example, the metadata for Test-Lyon.txt file is::

        $ dirac-dms-catalog-metadata /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt
        FileName                                                                                             Size       GUID                                     Status   Checksum
        /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt                                               15         1D6155B6-0405-BAB0-5552-7913EFD734A7     1        2ec4058b

4.3.2 File Metadata
@@@@@@@@@@@@@@@@@@@

- More detailed file metadata can be obtained with the following command::

        dirac-dms-lfn-metadata <LFN>

  For example::

        $ dirac-dms-lfn-metadata /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt
        {'Failed': {},
         'Successful': {'/vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt': {'Checksum': '2ec4058b',
                                                                           'ChecksumType': 'Adler32',
                                                                           'CreationDate': datetime.datetime(2010, 10, 17, 20, 31, 31),
                                                                           'CreationDate': datetime.datetime(2010, 10, 17, 20, 31, 31),
                                                                           'FileID': 15L,
                                                                           'GID': 2,
                                                                           'GUID': '1D6155B6-0405-BAB0-5552-7913EFD734A7',
                                                                           'Mode': 509,
                                                                           'ModificationDate': datetime.datetime(2010, 10, 17, 20, 31, 31),
                                                                           'Owner': 'vhamar',
                                                                           'OwnerGroup': 'dirac_user',
                                                                           'Size': 15L,
                                                                           'Status': 1,
                                                                           'UID': 2}}}


4.4 Downloading a file
----------------------

- Retrieve the file previously uploaded to the Grid using the command::
  
        dirac-dms-get-file <LFN>

  Output must be like shown below::

        $ dirac-dms-get-file /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt 
        {'Failed': {},
         'Successful': {'/vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt': '/afs/in2p3.fr/home/h/hamar/Tests/DMS/Test-Lyon.txt'}}


4.5 Data Replication
--------------------

4.5.1 Replicating a file
@@@@@@@@@@@@@@@@@@@@@@@@

- The command used to create another replica of a given file::

        dirac-dms-replicate-lfn <LFN> <SE>

  For example::

        $ dirac-dms-replicate-lfn /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt DIRAC-USER
        {'Failed': {},
         'Successful': {'/vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt': {'register': 0.50833415985107422,
                                                                                   'replicate': 11.878520965576172}}}


4.5.2 Replica information
@@@@@@@@@@@@@@@@@@@@@@@@@


- The following command allows to obtain the replica information for the given file::

        dirac-dms-lfn-replicas <LFN>

  An example ouput is shown below::

        $ dirac-dms-lfn-replicas /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt 
        {'Failed': {},
          'Successful': {'/vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt': {'M3PEC-disk': 'srm://se0.m3pec.u-bordeaux1.fr/dpm/m3pec.u-bordeaux1.fr/home/vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt'}}}


4.5.3 Removing a replica
@@@@@@@@@@@@@@@@@@@@@@@@

- To remove replicas use the command::

        dirac-dms-remove-replicas <LFN> <SE>


  For example::
        $  dirac-dms-remove-replicas /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt IBCP-disk
        Successfully removed DIRAC-USER replica of /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt


4.6 Removing Files
------------------

- Please remove all the files created during the T.P, using this command::

        dirac-dms-remove-files <LFN>


  For example::

        $ dirac-dms-remove-files  /vo.formation.idgrilles.fr/user/v/vhamar/Test-Lyon.txt 
        $

4.7 Getting the list of user files
----------------------------------

- To create a list of all the files stored by the user::

        dirac-dms-user-lfns

  After running the command a file with all the LFNs will be created, the file name is associated with the user's VO::

        $ dirac-dms-user-lfns
         /vo.formation.idgrilles.fr/user/v/vhamar: 0 files, 1 sub-directories 
         /vo.formation.idgrilles.fr/user/v/vhamar/2: 0 files, 3 sub-directories 
         /vo.formation.idgrilles.fr/user/v/vhamar/2/2389: 1 files, 0 sub-directories 
         /vo.formation.idgrilles.fr/user/v/vhamar/2/2390: 1 files, 0 sub-directories 
         /vo.formation.idgrilles.fr/user/v/vhamar/2/2391: 1 files, 0 sub-directories 
         3 matched files have been put in vo.formation.idgrilles.fr-user-v-vhamar.lfns
=========================
File Catalog Interface
=========================

Starting the File Catalog Interface
---------------------------------------

- DIRAC File Catalog Command Line Interface (CLI) can be used to perform all the data management operations.
  You can start the CLI with the command::
 
        dirac-dms-filecatalog-cli
  
  
  For example::

        $ dirac-dms-filecatalog-cli 
        Starting DIRAC FileCatalog client
        File Catalog Client $Revision: 1.17 $Date:     
        FC:/>help
        
        Documented commands (type help <topic>):
        ========================================
        add    chmod  find   guid  ls     pwd       replicate  rmreplica   user
        cd     chown  get    id    meta   register  rm         size      
        chgrp  exit   group  lcd   mkdir  replicas  rmdir      unregister

        Undocumented commands:
        ======================
        help

        FC:/>

Basic File Catalog operations
---------------------------------

- Changing directory::
  
     FC:/>cd /vo.formation.idgrilles.fr/user/a/atsareg
     FC:/vo.formation.idgrilles.fr/user/a/atsareg>
     FC:/vo.formation.idgrilles.fr/user/a/atsareg>cd
     FC:/>cd /vo.formation.idgrilles.fr/user/a/atsareg
     FC:/vo.formation.idgrilles.fr/user/a/atsareg>cd ..
     FC:/vo.formation.idgrilles.fr/user/a>cd -

- Listing directory::

     FC:/vo.formation.idgrilles.fr/user/a/atsareg>ls -l
     -rwxrwxr-x 0 atsareg dirac_user      856 2010-10-24 18:35:18 test.txt

- Creating new directory::

     FC:/vo.formation.idgrilles.fr/user/a/atsareg>mkdir newDir
     FC:/vo.formation.idgrilles.fr/user/a/atsareg>ls -l
     -rwxrwxr-x 0 atsareg dirac_user      856 2010-10-24 18:35:18 test.txt
     drwxrwxr-x 0 atsareg dirac_user        0 2010-10-24 11:00:05 newDir

- Changing ownership and permissions::

     FC:/vo.formation.idgrilles.fr/user/a/atsareg>chmod 755 newDir
     FC:/vo.formation.idgrilles.fr/user/a/atsareg>ls -l
     -rwxrwxr-x 0 atsareg dirac_user      856 2010-10-24 18:35:18 test.txt
     drwxr-xr-x 0 atsareg dirac_user        0 2010-10-24 11:00:05 newDir

Managing files and replicas
-------------------------------

- Upload a local file to the grid storage and register it in the catalog::

     add <LFN> <local_file> <SE>

  For example::

     FC:/>cd /vo.formation.idgrilles.fr/user/a/atsareg
     FC:/vo.formation.idgrilles.fr/user/a/atsareg> add test.txt test.txt DIRAC-USER
     File /vo.formation.idgrilles.fr/user/a/atsareg/test.txt successfully uploaded to the DIRAC-USER SE
     FC:/vo.formation.idgrilles.fr/user/a/atsareg> ls -l
     -rwxrwxr-x 0 atsareg dirac_user      856 2010-10-24 18:35:18 test.txt 

- Download grid file to the local directory::

     get <LFN> [<local_directory>]

  For example::
  
     FC:/vo.formation.idgrilles.fr/user/a/atsareg>get test.txt /home/atsareg/data
     File /vo.formation.idgrilles.fr/user/a/atsareg/test.txt successfully downloaded

- Replicate a file registered and stored in a storage element to another storage element::

     replicate <lfn> <SE>

  For example::

     FC:/vo.formation.idgrilles.fr/user/a/atsareg>replicate test.txt M3PEC-disk
     File /vo.formation.idgrilles.fr/user/a/atsareg/test.txt successfully replicated to the M3PEC-disk SE

- List replicas::
   
    replicas <LFN>

  For example::

    FC:/vo.formation.idgrilles.fr/user/a/atsareg>replicas  test.txt
    lfn: /vo.formation.idgrilles.fr/user/a/atsareg/test.txt
    M3PEC-disk      srm://se0.m3pec.u-bordeaux1.fr:8446/srm/managerv2?SFN=/dpm/m3pec.u-bordeaux1.fr/home/vo.formation.idgrilles.fr/user/a/atsareg/test.txt
    DIRAC-USER      dips://dirac.in2p3.fr:9148/DataManagement/StorageElement/vo.formation.idgrilles.fr/user/a/atsareg/test.txt

- Remove replicas::

     rmreplica <LFN> <SE>

  For example::    

   FC:/vo.formation.idgrilles.fr/user/a/atsareg>rmreplica test.txt M3PEC-disk
   lfn: /vo.formation.idgrilles.fr/user/a/atsareg/test.txt
   Replica at M3PEC-disk moved to Trash Bin
   FC:/vo.formation.idgrilles.fr/user/a/atsareg>replicas test.txt
   lfn: /vo.formation.idgrilles.fr/user/a/atsareg/test.txt
   DIRAC-USER      dips://dirac.in2p3.fr:9148/DataManagement/StorageElement/vo.formation.idgrilles.fr/user/a/atsareg/test.txt


- Remove file::

     rm <LFN>

  For example::

     FC:/vo.formation.idgrilles.fr/user/a/atsareg>rm test.txt
     lfn: /vo.formation.idgrilles.fr/user/a/atsareg/test.txt
     File /vo.formation.idgrilles.fr/user/a/atsareg/test.txt removed from the catalog

- Remove directory::

     rmdir <path>

  For example::
 
     FC:/vo.formation.idgrilles.fr/user/a/atsareg>rmdir newDir
     path: /vo.formation.idgrilles.fr/user/a/atsareg/newDir
     Directory /vo.formation.idgrilles.fr/user/a/atsareg/newDir removed from the catalog

Getting extra information
-----------------------------

- Getting file or directory size::

     size <LFN>
     size <dir_path>

  For example::
     
     FC:/vo.formation.idgrilles.fr/user/a/atsareg>size test.txt
     lfn: /vo.formation.idgrilles.fr/user/a/atsareg/test.txt
     Size: 856
     FC:/vo.formation.idgrilles.fr/user/a/atsareg>size ..
     directory: /vo.formation.idgrilles.fr/user/a
     Size: 2358927

- Your current identity::

     id

  For example::

     FC:/vo.formation.idgrilles.fr/user/a/atsareg>id
     user=1(atsareg) group=2(dirac_user)
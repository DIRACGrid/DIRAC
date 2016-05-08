===========================
8. Data Management Advanced
===========================

This section shows how the File Catalog can be used as a Metadata Catalog. The exercises are  performed
using the File Catalog CLI interface. You can start the CLI with the command::
 
        dirac-dms-filecatalog-cli

8.1 User metadata
-----------------

Metadata is the information describing the user data in order to easily select the data sets of interest
for user applications. In the DIRAC File Catalog metadata can be associated with any directory. It is important
that subdirectories are inheriting the metadata of their parents, this allows to reduce the number of the
stored metadata values. Some metadata variables can be declared as indices. Only indexed metadata can be
used in data selections. 

- Adding metadata to a directory::

   meta set  <directory> <metaname> <metavalue>

  For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg>meta set . ATMetaStr Test
   FC:/vo.formation.idgrilles.fr/user/a/atsareg>mkdir testDir
   Successfully created directory: /vo.formation.idgrilles.fr/user/a/atsareg/testDir
   FC:/vo.formation.idgrilles.fr/user/a/atsareg>meta set testDir AnotherMeta AnotherTest

- Getting directory metadata::

   meta get <directory>

  For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg>meta get testDir
           AnotherMeta : AnotherTest
             ATMetaStr : Test

- Creating metadata index::

   meta index <metaname> <metatype> 

  For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg>meta index NewMetaInt int
   Added metadata field NewMetaInt of type int  

  Possible metadata types: int,float,string,date

- Showing existing metadata indices::

   meta show

  For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg>meta show
           ATMetaStr : VARCHAR(128)
           ATMetaInt : INT
          ATMetaDate : DATETIME
           ATMetaSet : MetaSet
          ATMetaInt1 : INT
          NewMetaInt : INT
           ATMetaFlt : float

- Finding files with selection by metadata::

   find <meta selection>

  For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg> find ATMetaInt=10,11 ATMetaInt1<15
   Query: {'ATMetaInt': {'in': [10, 11]}, 'ATMetaInt1': {'<': 15}}
   /vo.formation.idgrilles.fr/user/a/atsareg/newDir/wms_output.py

8.2 File Provenance Metadata
----------------------------

In the File Catalog you can declare ancestor files for a given file. This is often needed
in order to keep track of the derived data provenance path. The ancestor declaration is done
as following::

    ancestorset <descendent> <ancestor>

For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg> ancestorset file2 file1
   FC:/vo.formation.idgrilles.fr/user/a/atsareg> ancestorset file3 file2

Once the chain of ancestors/descendents is created it can be interrogated with the following commands::

   ancestor <file> <depth>
   descendent <file> <depth>

For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg> ancestor file3 2
   /vo.formation.idgrilles.fr/user/a/atsareg/file3
   1      /vo.formation.idgrilles.fr/user/a/atsareg/file2
   2              /vo.formation.idgrilles.fr/user/a/atsareg/file1

   FC:/vo.formation.idgrilles.fr/user/a/atsareg> descendent file1 2
   /vo.formation.idgrilles.fr/user/a/atsareg/file1
   1      /vo.formation.idgrilles.fr/user/a/atsareg/file2
   2              /vo.formation.idgrilles.fr/user/a/atsareg/file3
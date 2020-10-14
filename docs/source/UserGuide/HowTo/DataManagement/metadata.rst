
DFC as a metadata catalog
=========================

This section supposes that the DFC is used as a Metadata Catalog. This is for example not the case of LHCb. Please ask your administrator if you are unsure.
The exercises are  performed using the File Catalog CLI interface. You can start the CLI with the command::
 
        dirac-dms-filecatalog-cli

How to add metadata to a directory
----------------------------------

From the CLI::

   meta set  <directory> <metaname> <metavalue>

For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg>meta set . ATMetaStr Test
   FC:/vo.formation.idgrilles.fr/user/a/atsareg>mkdir testDir
   Successfully created directory: /vo.formation.idgrilles.fr/user/a/atsareg/testDir
   FC:/vo.formation.idgrilles.fr/user/a/atsareg>meta set testDir AnotherMeta AnotherTest

How to get directory metadata
-----------------------------

From the CLI::

   meta get <directory>

For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg>meta get testDir
           AnotherMeta : AnotherTest
             ATMetaStr : Test

How to create metadata index
----------------------------

From the CLI::

   meta index <metaname> <metatype> 

For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg>meta index NewMetaInt int
   Added metadata field NewMetaInt of type int  

  Possible metadata types: int,float,string,date

How to show existing metadata indexes
-------------------------------------

From the CLI::

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

How to find files with selection by metadata
--------------------------------------------

From the CLI::

   find <meta selection>

For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg> find ATMetaInt=10,11 ATMetaInt1<15
   Query: {'ATMetaInt': {'in': [10, 11]}, 'ATMetaInt1': {'<': 15}}
   /vo.formation.idgrilles.fr/user/a/atsareg/newDir/wms_output.py


How to declare file's ancestors
-------------------------------

The ancestor declaration is done as following::

    ancestorset <descendent> <ancestor>

For example::

   FC:/vo.formation.idgrilles.fr/user/a/atsareg> ancestorset file2 file1
   FC:/vo.formation.idgrilles.fr/user/a/atsareg> ancestorset file3 file2

How to query file's ancestors
-----------------------------

It can be interrogated with the following commands::

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

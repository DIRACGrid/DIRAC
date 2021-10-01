Resources / FileCatalogs - Subsections
======================================

This subsection include the definition of the File Catalogs to be used in the installation. In case there is more than one File Catalog defined in this section, the first one in the section will be used as default by the ReplicaManager client.

+---------------------------+-------------------------------------------------+----------------------------+
| **Name**                  | **Description**                                 | **Example**                |
+---------------------------+-------------------------------------------------+----------------------------+
| *FileCatalog*             | Subsection used to configure DIRAC File catalog | FileCatalog                |
+---------------------------+-------------------------------------------------+----------------------------+
| *FileCatalog/AccessType*  | Access type allowed to the particular catalog   | AccessType = Read-Write    |
+---------------------------+-------------------------------------------------+----------------------------+
| *FileCatalog/Status*      | To define the catalog as active or inactive     | Status = Active            |
+---------------------------+-------------------------------------------------+----------------------------+
| *FileCatalog/MetaCatalog* | If the Catalog is a MetaDataCatalog             | MetaCatalog = True         |
+---------------------------+-------------------------------------------------+----------------------------+

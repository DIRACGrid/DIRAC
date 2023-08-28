.. _dfc:

------------------
Dirac File Catalog
------------------

The DIRAC File Catalog (DFC) is a full replica and metadata catalog integrated to DIRAC. It has a very modular structure, allowing for several backends. The interaction with the backend is handled by `Managers` in such a way that the interface exposed to the users remains always the same.

There are two main sets of managers:

  * the historical ones, offering the full range of functionalities and used by most VO
  * and the LHCb ones, where a subsets of the functionalities related to user defined metadata are not tested, but optimized for scaling and consistency. Any VO could of course use it.

The DFC can be used also as a Metadata catalog.
Metadata is the information describing the user data in order to easily select the data sets of interest
for user applications. In the DIRAC File Catalog metadata can be associated with any directory. It is important
that subdirectories are inheriting the metadata of their parents, this allows to reduce the number of the
stored metadata values. Some metadata variables can be declared as indexes. Only indexed metadata can be
used in data selections.
One can declare ancestor files for a given file. This is often needed
in order to keep track of the derived data provenance path.



Installation
------------

The installation and configuration procedure changes slightly between the historical managers and the LHCb ones.

The list of components you need to have installed is:

   * FileCatalogDB: if you want the standard managers, you should use `FileCatalogDB.sql`, but `FilecatalogWithFkAndPsDB.sql` if you want the LHCb ones
   * FileCatalogHandler: just the interface to the DB


FileCatalogDB
-------------

No special configuration there.

FileCatalogHandler
------------------

All the configuration of the DFC takes place there.

* `DatasetManager`: default `DatasetManager` Manager for the dataset
* `DefaultUmask`: default `0775` Umask in octal
* `DirectoryManager`: default `DirectoryLevelTree` Manager for the Directories
* `DirectoryMetadata`: default `DirectoryMetadata` Manager for the directory metadata
* `FileManager`: default `FileManager` Manager for the files
* `FileMetadata`: default `FileMetadata` Manager for the file metadata
* `GlobalReadAccess`: default `True`. If set to True, anyone can read anything
* `LFNPFNConvention`: default `Strong`.
* `ResolvePFN`: default `True`. Deprecated
* `SecurityManager`: default `NoSecurityManager`. Manager for authentication
* `SEManager`: default `SEManagerDB`. Manager for the storage elements
* `UniqueGUID`: default `False`. If `True`, the GUID has to be unique through the namespace
* `UserGroupManager`: default `UserAndGroupManagerDB`. Managers for groups and users
* `ValidFileStatus`: default `[AprioriGood,Trash,Removing,Probing]`. Status that are valid for Files
* `ValidReplicaStatus`: default `[AprioriGood,Trash,Removing,Probing]`. Status that are valid for Replicas
* `VisibleFileStatus`: default `[AprioriGood]`. By default, only files in this status are returned
* `VisibleReplicaStatus`: default `[AprioriGood]` By default, only replicas in this status are returned

In order to use the LHCb handlers you should choose:

* `FileManager = FileManagerPs`
* `DirectoryManager = DirectoryClosure`
* `UniqueGUID = True`
* `SecurityManager = VOMSSecurityManager`


Security Manager
----------------

This manager takes care of the access permissions in the DFC. There are several of them:

* `NoSecurityManager` (:py:class:`~DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.NoSecurityManager.NoSecurityManager`): offer yourself to whatever treatment the world reserves you
* `DirectorySecurityManager` (:py:class:`~DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.DirectorySecurityManager.DirectorySecurityManager`): only look at directories for permissions
* `FullSecurityManager` (:py:class:`~DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.FullSecurityManager.FullSecurityManager`): Close to POSIX treatment of security permissions
* `DirectorySecurityManagerWithDelete` (:py:class:`~DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.DirectorySecurityManagerWithDelete.DirectorySecurityManagerWithDelete`): same as `DirectorySecurityManager` but consider the parent's
  directory write bit for removal
* `VOMSSecurityManager` (:py:class:`~DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.VOMSSecurityManager.VOMSSecurityManager`):
  implements a 3-level posix permission (directory-file-replica),
  and groups the dirac group using their VOMS roles. Basically, if the owner does not match,
  the groups are used. But the group doing the request and the one owning the file do not need
  to be the same: it is enough if they share the same VOMS role.


LFN PFN convention
------------------

The DFC encourages to use a convention for naming physical file names (PFNs) such that they contain the logical file
name (LFN) as their trailing part. In this case there is a clear one-to-one correspondence between the LFNs and PFNs
which simplifies a lot data integrity management. If the LFNPFNConvention option is set to `Strong`, this convention
is imposed: the PFNs are not stored in the DFC and they are constructed on the fly following the convention.

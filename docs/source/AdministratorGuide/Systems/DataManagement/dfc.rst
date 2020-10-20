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
* `SecurityPolicy` : if `SecurityManager = PolicyBasedSecurityManager`, path to the policy to use
* `SEManager`: default `SEManagerDB`. Managers for the strage elements
* `UniqueGUID`: default `False`. If `True`, the GUID has to be unique through the namespace
* `UserGroupManager`: default `UserAndGroupManagerDB`. Managers for groups and users
* `ValidFileStatus`: default `[AprioriGood,Trash,Removing,Probing]`. Status that are valid for Files
* `ValidReplicaStatus`: default `[AprioriGoodTrashRemovingProbing]`. Status that are valid for Replicas
* `VisibleFileStatus`: default `[AprioriGood]`. By default, only files in this status are returned
* `VisibleReplicaStatus`: default `[AprioriGood]` By default, only replicas in this status are returned

In order to use the LHCb handler you should:

* `FileManager = FileManagerPs`
* `DirectoryManager = DirectoryClosure`
* `UniqueGUID = True`
* `SecurityManager = PolicyBasedSecurityManager`
* `SecurityPolicy = DIRAC/DataManagementSystem/DB/FileCatalogComponents/SecurityPolicies/VOMSPolicy`


Security Manager
----------------

This manager takes care of the access permissions in the DFC. There are several of them:

* `NoSecurityManager`: offer yourself to whatever treatment the world reserves you
* `DirectorySecurityManager`: only look at directories for permissions
* `FullSecurityManager`:
* `DirectorySecurityManagerWithDelete`: same as `DirectorySecurityManager` but consider the parent's directory write bit for removal
* `PolicyBasedSecurityManager`: based on plugins. It will evaluate the permissions based on the path, the identity doing the request, and the action itself on a per method bases. Currently, only the `VOMSPolicy` exists in DIRAC.

The `VOMSPolicy` (:py:class:`~DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityPolicies.VOMSPolicy.VOMSPolicy`) implements a 3-level posix permission (directory-file-replica), and groups the dirac group using their VOMS roles. Basically, if the owner does not match, the groups are used. But the group doing the request and the one owning the file do not need to be the same: it is enough if they share the same VOMS role.




LFN PFN convention
------------------

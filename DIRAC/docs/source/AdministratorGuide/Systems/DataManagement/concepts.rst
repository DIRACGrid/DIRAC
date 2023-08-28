.. _dms-concepts:


--------
Concepts
--------

The whole DataManagement System (DMS) of DIRAC relies on a few key concepts:

* Logical File Name (LFN): the LFN is the name of a file, a path. It uniquely identifies a File throughout the DIRAC namespace. A file can have one or several `Replica`.
* Replica: This is a physical copy of an LFN. It is stored at a `StorageElement`. The couple `(LFN,StorageElement)` uniquely identifies a physical copy of a file.
* StorageElement: This represents a physical storage endpoint.
* Catalog: This is the namespace of the DataManagement. Files and their metadata are listed there

Systems in DIRAC (other than DMS) or users, when dealing with files, only have to care about LFNs. If, for some (unlikely) reasons, they need to address a specific replica, then they should use the couple `(LFN, StorageElement name)`. At no point, anywhere, is there a protocol or a URL leaking out of the low level of the DMS.


Logical File Names
------------------

The LFN is the unique identifier of a file throughout the namespace. It takes the form of a path, where the first directory should be the VO name. For example `/lhcb/user/c/chaen/myFile.txt`.


StorageElements
---------------


For details on how to configure them, please see :ref:`resourcesStorageElement`.


DIRAC provides an abstraction to the storage endpoints called `StorageElement`. They are described in the CS, together with all the configuration necessary to physically access the files. There is never any URL leaking from the StorageElement to the rest of the system.



.. _dmsCatalog:

Catalogs
---------

The concept of Catalogs is just the one of a `Namespace`. it is a place where you list your files and their metadata (size, checksum, list of SEs where they are stored, etc). DIRAC supports having several catalogs: in general, any operation done to one catalog will be performed to the others.

For more details, please see :ref:`resourcesCatalog`.

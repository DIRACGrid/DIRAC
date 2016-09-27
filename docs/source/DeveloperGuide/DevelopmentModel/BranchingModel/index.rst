.. _branching_model:

====================================
Branching Model
====================================

DIRAC release branches live in the central repository of the *Github* service.

DIRAC releases nomenclature
-----------------------------

Release version name conventions
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

The DIRAC release versions have the form ``vXrYpZ``, where ``X``, ``Y`` and ``Z`` are incrementally
increasing interger numbers. For example, v1r0p1. ``X`` corresponds to the major release number,
``Y`` corresponds to the minor release number and ``Z`` corresponds to the patch number ( see below ).
It is possible that the patch number is not present in the release version, for example v6r7 . 

The version of prereleases used in the DIRAC certification procedure is constructed in a form: 
``vXrY-preZ``, where the ``vXrY`` part corresponds to the new release being tested and ``Z`` 
denotes the prerelease number.

Release versions are used as tags in Git terms to mark the set of codes corresponding to the
given release.

Release types
@@@@@@@@@@@@@@

We distinguish *releases*, *patches* and *pre-releases*. Releases in turn can be *major* and *minor*.

*major release*
  major releases are created when there is an important change in the DIRAC functionality involving
  changes in the service interfaces making some of the previous major release clients incompatible 
  with the new services. DIRAC clients and services belonging to the same major release are still
  compatible even if they belong to different normal releases. In the release version the major
  release is denoted by the number following the initial letter "v".
  
*minor release*      
  minor releases are created whenever a significant new functionality is added or important changes
  of the existing functionality are done. Minor releases necessitate certification step in order to make
  the new code available in production for users. In the release version minor releases are denoted
  by the number following the letter "r".
  
*patch*
  patches are created for a minor and/or obvious bug fixes and minor functionality changes. This
  is the responsibility of the developer to ensure that the patch changes only fix known problems 
  and do not introduce new bugs or undesirable side effects. Patch releases are not subject to the
  full release certification procedure. Patches are applied to the existing releases. In the release 
  version patch releases are denoted by the number following the letter "p".

*pre-release*
  the DIRAC certification procedure goes through a series of *pre-releases* used to test carefully the
  code to be released in production. The prerelease versions have a form ``vXrY-preZ``.  

Release branches
-------------------------

The following branches are used in managing DIRAC releases:

*integration branch*
  this branch is used to assemble new code developments that will be eventually released as a new major or
  minor release. 
  
*release branches*
  the release branches contain the code corresponding to a given major or minor release. This is the production
  code which is distributed for the DIRAC installations. The release branches are created when a new minor
  release is done. The patches are incorporated into the release branches. The release branch names have the
  form ``rel-vXrY``, where ``vXrY`` part corresponds to the branch minor release version.  
  
*master branch*
  the master branch corresponds to the current stable production code. It is a copy of the corresponding
  release branch.   

These branches are the only ones maintained in the central Git repository
by the release managers. They are used to build DIRAC releases. They also serve 
as reference code used by developers as a starting point for their work. 

Feature branches
----------------------

These are the branches where all the actual developments are happening. 
They can be started from *release/integration* and will be merged back to them
eventually if the contribution are accepted. Their name should reflect the
feature being developed and should not be "integration" or "master" to avoid
confusions. 

Feature branches are used to develop new features for a future release or
making patches to the already created releases. A feature branch will exist as long as 
the feature is in development but will eventually be merged into *release/integration* 
or discarded in case the feature is no longer relevant. Feature branches exist only in 
the developer repositories and never in the *release* repository.

Working on and contributing code to the DIRAC Project is described in :ref:`contributing_code` .  

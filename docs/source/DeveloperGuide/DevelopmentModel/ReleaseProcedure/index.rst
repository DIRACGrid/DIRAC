.. _release_procedure:

=============================
Making DIRAC releases
=============================

This section is describing the procedure to follow by release managers
when preparing new DIRAC releases. The procedure consists of several
steps:

- Merge *Pull Requests* 
- Propagate patches to downstream release
- Make release notes
- Tag *release* branches with release version tags
- Update the state of *release* and *integration* branches in
  the central repository
- Update DIRAC software project description   
- Build and upload release tar files

The release steps are described in this chapter.

Starting
---------

For simplicity and reproducibility, it's probably a good idea to start from a fresh copy in a clean directory.
This means that, you may want to start by moving to a temporary directory and issue the following:
  
  > mkdir $(date +"20%y%m%d") && cd $(date +"20%y%m%d")
  
which will create a clean directory with today's date. We then clone the DIRAC repository and rename the created "origin" remote in "release":
  
  > git clone git@github.com:DIRACGrid/DIRAC.git
  > git remote rename origin release
  


Merging *Pull Requests*
--------------------------

The new code and patch contribution are made in the form of *Github* *Pull Request*.
The *PR* are provided by the developers and are publicly available on the Web. 
The *PR*'s should be first reviewed by the release managers as well as by other
developers to possibly spot evident problems ( relevance of the new features,
conventions, typos, etc ). After the review the *PR* can be merged using the
*Github* tools. After that the remote release branch is in the state ready to
be tagged with the new version. 

Propagating patches
---------------------

In the DIRAC Development Model several release branches can coexist in production.
This means that patches applied to older branches must be propagated to the newer
release branches. This is done in the local Git repository of the release manager.
Let's take an example of a patch created against *release* branch *rel-v6r10* while
the new release branch *rel-v6r11* is already in production. This can be accomplished
by the following sequence of commands::
  
  > git fetch release
 
This will bring all the changes from the central repository including all the 
*release* branches.::
  
  > git checkout -b rel-v6r10 release/rel-v6r10
  > vim release.notes
  
We create local branch from the the remote one containing the patch. Release notes
must be updated to create a new section for the new patch release describing the
new changes. Now we can make a local branch corresponding to a downstream branch
and merge the commits from the patches::
  
  > git checkout -b rel-v6r11 release/rel-v6r11
  > git merge --no-ff rel-v6r10

Note that if the release branches already exist in the repository, they can be rebased
on the remote counterparts instead of recreating them:::

  > git fetch release
  > git checkout rel-v6r10
  > git rebase --no-ff release/rel-v6r10 

This will bring the patches into the local release branch, you can now update the release 
notes and proceed with tagging and uploading. All the patches must be also propagated
to the *integration* branch::

  > git checkout -b integration release/integration
  > git merge --no-ff rel-v6r11  


Release notes
--------------

Release notes are contained in the *release.notes* file. Each release version has a dedicated
section in this file, for example::

  [v6r10p1]
  
  *Core
  BUGFIX: typo in the dirac-install script
  
  *WMS
  CHANGE: JobAgent - handle multi-core worker nodes 

The section title as taken into the square brackets. Change notes are collected per subsystem
denoted by a name starting with \*. Each change record starts with one of the follow header
words: FIX:, BUGFIX:, CHANGE:, NEW: for fixes, bug fixes, small changes and new features
correspondingly.   

Release notes for the given branch should be made in this branch.

Tagging and uploading release branches
----------------------------------------

Once the local release and integration branches have got all the necessary
changes they can be tagged with the new version tags::

  > git checkout rel-v6r10
  > git tag v6r10p11
  > git checkout rel-v6r11
  > git tag v6r11p1
  
Note that if even the patch was made to an upstream release branch, the subsequent
release branch must also receive a new patch release tag. Multiple patches can be
add in one release operation. If the downstream release branch has got its own patches,
those should be described in its release notes under the v6r11p1 section. 

Once the tags are done, the updated branches and new tags must be pushed to the
central repository::

  > git push --tags release rel-v6r10
  > git push --tags release rel-v6r11

Note that we have not yet pushed the *integration* branch. We have to update
first the *releases.cfg* file with the description of dependencies on the
new versions of the DIRAC modules ( see :ref:`dirac_projects` ).

The *integration* branch is also receiving new features to go into the next release.
Therefore, it is used to tag *prerelease* versions that can be then installed
with standard tools on test DIRAC servers, for example::

  > git checkout integration
  > git tag v7r0-pre12
  
After the *releases.cfg* file is updated in the *integration* branch and prerelease
tags are made, the branch can be pushed in the usual way ::
 
  > git push --tags release integration     

How to make a distribution
-----------------------------

Once the release branches are tagged and pushed, the new release versions are
properly described in the *release.cfg* file in the *integration* branch and
also pushed to the central repository, the tar archives containing the new
codes can be created. Just execute *dirac-distribution* command with the appropriate 
flags. For instance::

 dirac-distribution -r v6r10p11 -l DIRAC 
 
You can also pass the releases.cfg to use via command line using the *-C* switch. *dirac-distribution* 
will generate a set of tarballs, release and md5 files. Please copy those to your installation source 
so *dirac-install* can find them. 

The command will compile tar files as well as release notes in *html* and *pdf* formats.
In the end of its execution, the *dirac-distribution* will print out a command that can be 
used to upload generated release files to a predefined repository ( see :ref:`dirac_projects` ).

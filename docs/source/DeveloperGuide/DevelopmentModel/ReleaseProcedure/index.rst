.. _release_procedure:

=============================
Making DIRAC releases
=============================

This section is describing the procedure to follow by release managers
when preparing new DIRAC releases (including patches). 

Prerequisites
=============

The release manager needs to:

- be aware of the DIRAC repository structure and branching.
- have push access to the "release" repository, so the one on GitHub (being part of the project "owners")

The release manager of LHCbDIRAC has the triple role of:

1. creating the release
2. making basic verifications
3. deploying DIRAC tarballs


1. Creating the release(s)
==========================

The procedure consists of several steps:

- Merge *Pull Requests* 
- Propagate patches to downstream release
- Make release notes
- Tag *release* branches with release version tags
- Update the state of *release* and *integration* branches in
  the central repository
- Update DIRAC software project description   
- Build and upload release tar files

The release steps are described in this chapter. First, just a note on *Pull Requests* on GitHub:

The new code and patch contribution are made in the form of *Github* *Pull Request*.
The *PR* are provided by the developers and are publicly available on the Web. 
The *PR*'s should be first reviewed by the release managers as well as by other
developers to possibly spot evident problems ( relevance of the new features,
conventions, typos, etc ). The PRs are also reviewed by autimated tools, like Travis (not limited to).
After the review the *PR* can be merged using the *Github* tools. 
After that the remote release branch is in the state ready to be tagged with the new version. 


Release notes
``````````````

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



Working with code and tags
---------------------------

For simplicity and reproducibility, it's probably a good idea to start from a fresh copy in a clean directory.
This means that, you may want to start by moving to a temporary directory and issue the following::
  
  > mkdir $(date +"20%y%m%d") && cd $(date +"20%y%m%d")
  
which will create a clean directory with today's date. We then clone the DIRAC repository and rename the created "origin" remote in "release"::
  
  > git clone git@github.com:DIRACGrid/DIRAC.git
  > cd DIRAC
  > git remote rename origin release
  


Propagating patches
---------------------

In the DIRAC Development Model several release branches can coexist in production.
This means that patches applied to older branches must be propagated to the newer
release branches. This is done in the local Git repository of the release manager.
Let's take an example of a patch created against *release* branch *rel-v6r14* while
the new release branch *rel-v6r15* is already in production. This can be accomplished
by the following sequence of commands, which will bring all the changes from 
the central repository including all the *release* branches.
We now create local branch from the the remote one containing the patch. Release notes
must be updated to create a new section for the new patch release describing the
new changes. Now we can make a local branch corresponding to a downstream branch
and merge the commits from the patches::
  
  > git checkout -b rel-v6r14 release/rel-v6r14
  > vim release.notes
  
We can now start merging PRs, directly from GitHub. At the same time we edit 
the release notes to reflect what has been merged (please see the note below about how to edit this file).
Once finished, save the file. Then we commit the changes (those done to release.notes) and update the current repository::
  
  > git commit -a #this will commit the changes we made to the release notes in rel-v6r14 local branch
  > git fetch release #this will bring in the updated release/rel-v6r14 branch from the github repository
  > git rebase --no-ff release/rel-v6r14 #this will rebase the current rel-v6r14 branch to the content of release/rel-v6r14

You can now proceed with tagging, pushing, and uploading::

  > git tag v6r14p36 #this will create a tag, from the current branch, in the local repository
  > git push --tags release rel-v6r14 #we push to the *release* repository (so to GitHub-hosted one) the tag just created, and the rel-v6r14 branch. 
  
From the previous command, note that due to the fact that we are pushing a branch named *rel-v6r14* 
to the *release* repository, where it already exists a branch named *rel-v6r14*, 
the local branch will override the remote one.

All the patches must now be also propagated to the *upper* branches. 
In this example we are going through, we are supposing that it exists rel-v6r15 branch, 
from which we already derived production tags. We then have to propagate the changes done to 
rel-v61r4 to rel-v6r15. Note that if even the patch was made to an upstream release branch, the subsequent
release branch must also receive a new patch release tag. Multiple patches can be
add in one release operation. If the downstream release branch has got its own patches,
those should be described in its release notes under the v6r14p36 section. ::

  > git checkout -b rel-v6r15 release/rel-v6r15 # We start by checking out the rel-v6r15 branch
  > git merge rel-v6r14 # Merge to rel-v6r15 what we have advanced in rel-v6r14

The last command may result in merge conflicts, which should be resolved "by hand".
One typical conflict is about the content of the release.notes file.

From now on, the process will look very similar to what we have already done for 
creating tag v6r14p36. We should then repeat the process for v6r15::

  > vim release.notes

Merge PRs (if any), then save the release.notes. Then, modify the __init__.py file and define the version. Then::

  > git commit -a #this will commit the changes we made to the release notes in rel-v6r15 local branch
  > git fetch release #this will bring in the updated release/rel-v6r15 branch from the github repository
  > git rebase --no-ff release/rel-v6r15 #this will rebase the current rel-v6r15 branch to the content of release/rel-v6r15
  > git tag v6r15p4 #this will create a tag, from the current branch, in the local repository
  > git push --tags release rel-v6r15 #we push to the *release* repository (so to GitHub-hosted one) the tag just created, and the rel-v6r15 branch. 

The *master* branch of DIRAC always contains the latest stable release. 
If this correspons to rel-v6r15, we should make sure that this is updated:

  > git push release rel-v6r15:master

Repeat the process for every "upper" release branch. 
The *integration* branch is also receiving new features to go into the next release.
Therefore, it is used to tag *prerelease* versions that can be then installed
with standard tools on test DIRAC servers, for example::


  > git checkout -b integration release/integration
  > git merge --no-ff rel-v6r16 #replace with the "last" branch
  > vim releases.cfg #add the created tags
  > git commit -a
  > git push release integration


2. Making basic verifications
=============================

There are a set of basic tests that can be done on releases. 
The first test can be done even before creating a release tarball.

A first test is done automatically by Travis: https://travis-ci.org/DIRACGrid/DIRAC/branches

Travis also runs on all the Pull Requests, so if for all the PRs merged travis didn't show any problem,
there's a good chance (but NOT the certainty) that the created tags are also sane.

A second test is represented by pylint. [pylint -> expand!]


3. Deploying DIRAC tarballs
=============================

Once the release branches are tagged and pushed, the new release versions are
properly described in the *release.cfg* file in the *integration* branch and
also pushed to the central repository, the tar archives containing the new
codes can be created. Just execute *dirac-distribution* command with the appropriate 
flags. For instance::

 dirac-distribution -r v6r14p36 -l DIRAC 
 
You can also pass the releases.cfg to use via command line using the *-C* switch. *dirac-distribution* 
will generate a set of tarballs, release and md5 files. Please copy those to your installation source 
so *dirac-install* can find them. 

The command will compile tar files as well as release notes in *html* and *pdf* formats.
In the end of its execution, the *dirac-distribution* will print out a command that can be 
used to upload generated release files to a predefined repository ( see :ref:`dirac_projects` ).

It's now time to advertise that new releases have been created. Use the DIRAC google forum.


.. _release_procedure:

=============================
Making python2 DIRAC releases
=============================

This section is describing the procedure to follow by release managers
when preparing new DIRAC releases (including patches).

Prerequisites
=============

The release manager needs to:

- be aware of the DIRAC repository structure and branching.
- have push access to the "release" repository, so the one on GitHub (being part of the project "owners")

The release manager of LHCbDIRAC should:

1. Create the releases
2. make basic verifications
3. deploy the DIRAC tarballs
4. propagate to CVMFS


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
conventions, typos, etc ). The PRs are also reviewed by automated tools, like GitHub Actions.
After the review the *PR* can be merged using the *Github* tools.
After that the remote release branch is in the state ready to be tagged with the new version.


Release notes
`````````````

Release notes are contained in the *release.notes* file. Each release version has a dedicated
section in this file, for example::

  [v7r0p8]
  
  *Configuration
  CHANGE: Let update the config, even if the Pilot info is not in the CS

The section title as taken into the square brackets. Change notes are collected per subsystem
denoted by a name starting with \*. Each change record starts with one of the follow header
words: FIX:, BUGFIX:, CHANGE:, NEW: for fixes, bug fixes, small changes and new features
correspondingly.

Release notes for the given branch should be made in this branch.

The release notes for a given branch can be obtained with the
*docs/Tools/GetReleaseNotes.py* script::

  python docs/Tools/GetReleaseNotes.py --branches <branch> [<branch2>...] --date <dateTheLastTagWasMade> [--openPRs]


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

There are several DIRAC extensions, e.g. VMDIRAC and WebAppDIRAC.
The procedure described below applies to all of them.
Make sure that you apply the procedure starting with the DIRAC extensions.

In the DIRAC Development Model several release branches can coexist in production.
This means that patches applied to older branches must be propagated to the newer
release branches. This is done in the local Git repository of the release manager.
Let's take an example of a patch created against *release* branch *rel-6r22* while
the new release branch *rel-v7r0* is already in production. This can be accomplished
by the following sequence of commands, which will bring all the changes from
the central repository including all the *release* branches.
We now create local branch from the the remote one containing the patch. Release notes
must be updated to create a new section for the new patch release describing the
new changes. Now we can make a local branch corresponding to a downstream branch
and merge the commits from the patches::

  > git checkout -b rel-v6r22 release/rel-v6r22
  > vim release.notes

We can now start merging PRs, directly from GitHub. At the same time we edit
the release notes to reflect what has been merged (please see the note below about how to edit this file).
Once finished, save the file. Then, modify the __init__.py and the setup.py files of the root directory and define the version also there.
Then we commit the changes (those done to release.notes, __init__.py, and setup.py) and update the current repository::

  > git commit -a  # this will commit the changes we made to the release notes in rel-v6r22 local branch
  > git fetch release  # this will bring in the updated release/rel-v6r22 branch from the github repository
  > git rebase --no-ff release/rel-v6r22  # this will rebase the current rel-v6r22 branch to the content of release/rel-v6r22

You can now proceed with tagging, pushing, and uploading::

  > git tag -a v6r22p17 -m "v6r22p17"  # this will create an annotated tag, from the current branch, in the local repository
  > git push --tags release rel-v6r22  # we push to the *release* repository (so to GitHub-hosted one) the tag just created, and the rel-v6r22 branch.

From the previous command, note that due to the fact that we are pushing a branch named *rel-v6r22*
to the *release* repository, where it already exists a branch named *rel-v6r22*,
**the local branch will override the remote one**.

All the patches must now be also propagated to the *upper* branches.
In this example we are going through, we are supposing that it exists rel-v7r0 branch,
from which we already derived production tags. We then have to propagate the changes done to
rel-v6r22 to rel-v7r0. Note that if even the patch was made to an upstream release branch, the subsequent
release branch must also receive a new patch release tag. Multiple patches can be
add in one release operation. If the downstream release branch has got its own patches,
those should be described in its release notes under the v6r22p17 section.

The best way of merging one branch into an "upper" one is by creating a Pull Request in GitHub
(where e.g. you merge *rel-v6r22* into *rel-v7r0*).
This may result in merge conflicts, which should be resolved "by hand".
One typical conflict is about the content of the release.notes file.
GitHub Actions will run the checks. If everything's OK the PR can be merged.

From now on, the process will look very similar to what we have already done for
creating tag v6r22p17. We should then repeat the process for v7r0::

  > vim release.notes
  > vim __init__.py
  > vim setup.py

Merge PRs (if any), then save the files above. Then::

  > git commit -a  # this will commit the changes we made to the release notes in rel-v7r0 local branch
  > git fetch release  # this will bring in the updated release/rel-v7r0 branch from the github repository
  > git rebase --no-ff release/rel-v7r0  # this will rebase the current rel-v7r0 branch to the content of release/rel-v7r0
  > git tag v7r0p8  # this will create a tag, from the current branch, in the local repository
  > git push --tags release rel-v7r0  # we push to the *release* repository (so to GitHub-hosted one) the tag just created, and the rel-v7r0 branch.

The *master* branch of DIRAC always contains the latest stable release.
If this corresponds to rel-v7r0, we should make sure that this is updated::

  > git push release rel-v7r0:master

Repeat the process for every "upper" release branch.
When the release branch of the latest stable version is change, i.e. from rel-v7r0 to rel-v7r1, the URL of the CI status badge in the README should be edited.

The *integration* branch is also receiving new features to go into the next release.
The *integration* branch also contains the *releases.cfg* file, which holds all the versions of DIRAC
together with the dependencies among the different packages. 

From the *integration* branch we also do all the tags of *pre-release* versions, that can be then installed
with standard tools on test DIRAC servers. 

The procedure for creating pre-releases is very similar to creating releases. 
Start by creating the PR for merging *rel-v7r0* in *integration*, then::

  > vim release.notes
  > vim __init__.py
  > vim setup.py
  > vim releases.cfg  # add the created tags (all of them, releases and pre-releases)

Merge all the PRs targeting integration that have been approved (if any), then save the files above. Then::

  > git commit -a
  > git fetch release  # this will bring in the updated release/integration branch from the github repository
  > git rebase --no-ff release/integration  # this will rebase the current integration branch to the content of release/integration
  > git tag v7r1-pre3 -m "v7r1-pre3"  # this will create a tag, from the current branch, in the local repository
  > git push --tags release integration


2. Making basic verifications
=============================

There are a set of basic and integration tests that can be done on releases.
The first test can be done even before creating a release tarball.

All unit and integration tests are automatically run by GitHub Actions: https://github.com/DIRACGrid/DIRAC/actions

GitHub actions also runs on all the Pull Requests, so if for all the PRs merged GitHub Actions didn't show any problem,
there's a good chance (but NOT the certainty) that the created tags are also sane.
For this reason, it is better to create a Pull Request for merging branches into upper ones.


3. Deploying DIRAC tarballs
=============================

Once the release and integration branches are tagged and pushed, the new release and pre-release versions are
properly described in the *release.cfg* file in the *integration* branch and
also pushed to the central repository, the tar archives containing the new
codes can be created.

For releasing DIRAC, you need to be in an environment where
*Sencha cmd* has been installed and *extjs* is downloaded.
There's a Docker image that contains all the above dependencies.
It can be found in GitHub package registry or in docker hub::

  docker.pkg.github.com/diracgrid/management/dirac-distribution:latest (https://github.com/DIRACGrid/management/packages/79929)
  diracgrid/dirac-distribution (https://hub.docker.com/r/diracgrid/dirac-distribution)

The image is rebuilt once per week based on the Dockerfile in https://github.com/DIRACGrid/management/blob/master/dirac-distribution/Dockerfile

Pull it and run inside the dirac-distribution command::

  docker pull diracgrid/dirac-distribution
  python3 dirac-distribution.py -r v7r0p8

The above works also for DIRAC extensions, in this case just remember to specify the project name, e.g.::

  python3 dirac-distribution.py --release v10r0-pre11 --project LHCb

You can also pass the releases.cfg to use via command line using the *-relcfg* switch.
*dirac-distribution* will generate a set of tarballs, release notes in *html* and md5 files.

In the end of its execution, the *dirac-distribution* will print out a command that can be
used to upload generated release files to a predefined repository (see :ref:`dirac_projects`).

You can then run this Jenkins check: https://jenkins-dirac.web.cern.ch/view/DIRAC/job/Pilot3_CVM4_pipeline/
If it passes, it's time to advertise that new releases have been created. Use the DIRAC google forum.


4. Propagating to CVMFS [INCOMPLETE]
=====================================

There's a Docker image that contains all the needed dependencies.
It can be found in GitHub package registry or in docker hub::

  docker.pkg.github.com/diracgrid/management/dirac-cvmfs:latest (https://github.com/DIRACGrid/management/packages/342716)
  diracgrid/dirac-cvmfs (https://hub.docker.com/r/diracgrid/dirac-cvmfs)

The image is rebuilt once per week based on the Dockerfile in https://github.com/DIRACGrid/management/blob/master/dirac-cvmfs/Dockerfile

Pull it and ... ::

  docker pull diracgrid/dirac-cvmfs

--> to be expanded

.. _release_procedure:

=====================
Making DIRAC releases
=====================

.. set highlighting to console input/output
.. highlight:: console

This section is describing the procedure to follow by release managers
when preparing new DIRAC releases (including patches).

The new code and patch contribution are made in the form of *Github* *Pull Request*.
The *PR*'s should be first reviewed by the release managers as well as by other
developers to possibly spot evident problems. The PRs are also reviewed by automated tools, like GitHub Actions.
After the review the *PR* can be merged using the *Github* tools.
After that the remote release branch is in the state ready to be tagged with the new version.

.. warning:: These instructions only apply from DIRAC 8.0 series onwards!
   See the `documentation for previous releases <https://dirac.readthedocs.io/en/rel-v7r3/DeveloperGuide/ReleaseProcedure/index.html>`_ for details on how to make legacy style releases.

Prerequisites
=============

The release manager needs to:

- be aware of the DIRAC repository structure and branching.
- have push access to the "release" repository, so the one on GitHub (being part of the project "owners")

The release manager of DIRAC should:

1. Verify if new version of DIRACOS2 is needed
2. Verify the GitHub actions CI is passing
3. Create the release using GitHub Actions
4. Check the releases is on `PyPI <https://pypi.org/project/DIRAC/>`_ and make basic verifications
5. Propagate to CVMFS

1. Verify if new version of DIRACOS2 is needed
==============================================

Code in open Pull Requests might be dependent from a new version of `DIRACOS2 <https://github.com/DIRACGrid/DIRACOS2>`_.
Do verify for that before proceeding.


2. Verify the GitHub actions CI is passing
==========================================

The README on GitHub should show the status of the CI pipelines for all active releases branches.
This should have ran recently and be in a good state before proceeding.

3. Create the release using GitHub Actions
==========================================

The CI can be manually triggered to create a release by going to `here <https://github.com/DIRACGrid/DIRAC/actions/workflows/deployment.yml>`_.
From here click "Run workflow" and choose which branch should be used for a release.
By default the CI will bump the release on the smallest component, i.e. `8.0.0a7` becomes `8.0.0a8` and `8.1.24` becomes `8.1.25`.
To make a new release series this can be overridden manually.

Release notes are automatically generated from the recently merged PRs and committed before creating the tag.

4. Checking releases
====================

The first thing to ensure is that the release appears on `PyPI <https://pypi.org/project/DIRAC/>`_.
TODO: Make a script to performance sanity checks?

5. Propagating to CVMFS [INCOMPLETE]
=====================================

There's a Docker image that contains all the needed dependencies.
It can be found in GitHub package registry or in docker hub::

  ghcr.io/diracgrid/management/dirac-cvmfs:latest (https://github.com/DIRACGrid/management/packages/342716)
  diracgrid/dirac-cvmfs (https://hub.docker.com/r/diracgrid/dirac-cvmfs)

The image is rebuilt once per week based on this `Dockerfile <https://github.com/DIRACGrid/management/blob/master/dirac-cvmfs/Dockerfile>`_

Pull it and ... ::

  $ docker pull diracgrid/dirac-cvmfs

--> to be expanded

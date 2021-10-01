.. _git_management:

====================================
Managing Code with Git
====================================

DIRAC uses Git to manage it's source code. Git is a Distributed Version Control System (DVCS).
That means that there's no central repository like the one Subversion use. Each developer has
a copy of the whole repository. Because there are lots of repositories, code changes travel across
different repositories all the time by merging changes from different branches and repositories.
In any centralized VCS branching/merging is an advanced topic. In Git branching and merging are daily
operations. That allows to manage the code in a much more easy and efficient way. This document is
heavily inspired on `A successful Git branching model <http://nvie.com/posts/a-successful-git-branching-model/>`_


How decentralization works
===========================

Git doesn't have a centralized repository like Subversion do. Each developer has it's own repository.
That means that commits, branches, tags... everything is local. Almost all Git operations are blazingly fast.
By definition only one person works with one repository directly. But people don't develop alone. Git has a
set of operations to send and bring information to/from remote repositories. Users work with their local
repositories and only communicate with remote repositories to publish their changes or to bring other
developer's changes to their repository. In Git *lingo* sending changes to a repository is called *push*
and bringing changes is *pull*.

Git *per-se* doesn't have a central repository but to make things easier we'll define a repository that
will hold the releases and stable branches for DIRAC. Developers will bring changes from that repository
to synchronize their code with the DIRAC releases. To send changes to be released, users will have to push
their changes to a repository where the integration manager can pull the changes from, and send a *Pull Request*.
A *Pull Request* is telling the release manager where to get the changes from to integrate them into the next
DIRAC release.

.. figure:: integrationModel.png
    :align: left
    :alt: Schema on how changes flow between DIRAC and users

    How to publish and retrieve changes to DIRAC (see also `Pro Git Book <http://git-scm.com/book>`_)

Developers use the *developer private* repositories for their daily work. When they want something to be
integrated, they publish the changes to their *developer public* repositories and send a *Pull Request*
to the release manager. The release manager will pull the changes to his/her own repository,
and publish them in the *blessed repository* where the rest of the developers can pull the new changes
to their respective *developer private* repositories.

In practice, the DIRAC Project is using the `Github <http://github.com/DIRACGrid>`_ service to manage
the code integration operations. This will be described in subsequent chapters.


Decentralized but centralized
==============================

Although Git is a distributed VCS, it works best if developers use a single repository as the central
"truth" repository. Note that this repository is *only considered* to be the central one. We will refer
to this repository as *release* since all releases will be generated from this repository.

Each developer can only pull from the *release* repository. Developers can pull new release patches
from the *release* repository into their *private repositories*, work on a new feature, bugfix....
and then push the changes to their *public* repository. Once there are new changes in their public
repositories, they can issue a *pull request* so the changes can be included in central *release*
repository.

The precise instructions on how to create local git development repository and how to contribute
code to the common repository are given in subsequent sections.

.. _externals:

===================================
External Libraries and Dependencies
===================================


The external libraries and dependencies required to run DIRAC Agents, Services, and Clients are
contained in the `DIRACOS <https://github.com/DIRACGrid/DIRACOS/>`_. package for Python 2.7 installations,
and in the `DIRACOS2 <https://github.com/DIRACGrid/DIRACOS2/>`_ package for Python 3 installations.

The default version of ``DIRACOS`` to be used with a given version of DIRAC is defined in the
``DIRAC/releases.cfg`` file. It can be overwritten with the ``--dirac-os-version`` flag for
the `dirac-install` command. The documentation for creating a DIRACOS release or an extension
of DIRACOS can be found `in its own documentation <https://diracos.readthedocs.io>`_.

.. _building_documentation:

==========================
Building the Documentation
==========================

The DIRAC documentation makes use of the ``diracdoctools`` to create the code
documentation, command references, and concatenate the ``ConfigTemplates`` into
one file. The ``diracdoctools`` are located in the ``DIRAC/docs`` folder, but
they can be ``pip installed`` for use outside of DIRAC. In ``ReadTheDocs`` we do
not install DIRAC via `dirac-install`, so we have to install dependencies via
pip, or ``Mock`` the packages if they cannot be installed.


.. literalinclude:: ../../../requirements.txt
   :caption: The docs/requirements.txt used in readthedocs

For DIRAC extensions you also have to install DIRAC as a requirement:

  FIXME

            
In the sphinx configuration file, the functionality can then be called
   
.. literalinclude:: ../../conf.py
   :start-after: # AUTO SETUP START
   :end-before:  # AUTO SETUP END
   :caption: docs/source/conf.py


The configuration for ``diracdoctools` for is done via a configuration file
located in the docs folder. Just copy the file and adapt it to your needs. All
options are mandatory.  If certain features are not used, simply leave the
values empty

.. literalinclude:: ../../../docs.conf
   :caption: docs/docs.conf

The scripts can also be called directly, like this example from the ``Makefile`` shows.

.. literalinclude:: ../../../Makefile
   :caption: docs/Makefile
   :start-after: # AUTO MAKE START
   :end-before:  # AUTO MAKE END


 Code Reference
 --------------

 The code reference is either created by calling ``run`` from
 ``diracdoctools.cmd.codeReference`` or by invoking the script
 ``dirac-docs-build-code.py``. This creates an ``rst`` file for each python
 file, using ``autodoc`` to document all classes inside those modules. The
 actual documentation is build when sphinx is invoked, which must be able to
 import the modules and all their dependencies


 Command Reference
 -----------------

The command references can be created by calling ``run`` from
``diracdoctools.cmd.commandReference``, or by calling the
``dirac-docs-build-command.py`` script. ``[commands.section]`` will result in a
list of commands with links to their documentation, which is based on the output
of their ``--help``. The resulting ``index.rst`` has to be included explicitely
in the documentation. If ``indexFile`` is specified it has to contain all the
links itself, or warnings are generated.

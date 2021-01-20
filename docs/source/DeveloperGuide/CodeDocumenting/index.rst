.. _code_documenting:

==================================
Documenting your developments
==================================

.. contents::

Where should you document your developments? Well, at several places,
indeed, depending on the documentation we are talking about:

Code documentation
------------------

This is quite easy indeed. It's excellent practice to add
`docstring <http://legacy.python.org/dev/peps/pep-0257/>`_ to your
python code. The good part of it is that tools like pyDev can automatically
read it. Also your python shell can (try ``help()``), and so does iPython
(just use ``?`` for example). Python stores every docstring in
the special attribute ``__doc__``.

Pylint will, by default, complain for every method/class/function left without
a docstring.


Release documentation
---------------------

Releases documentation can be found in 2 places: release notes, and github wiki:

  * release notes are automatically created from the first comment in the pull
    requests, please describe the changes between BEGRINRELEASENOTES and
    ENDRELEASENOTES as presented by the template provided

  * The github wiki can contain a section, for each DIRACGrid repository,
    highlighting update operations, for example the DIRAC releases notes are
    linked from the `DIRAC wiki main page <https://github.com/DIRACGrid/DIRAC/wiki>`_.


Full development documentation
------------------------------

As said at the beginning of this guide, this documentation is in git at
`DIRAC/docs <https://github.com/DIRACGrid/DIRAC/tree/integration/docs>`_.
It is very easy to contribute to it, and you are welcome to do that. You don't
even have to clone the repository: github lets you edit it online.
This documentation is written in ``RST`` and it is compiled using
`sphinx <http://sphinx-doc.org/>`_.

Some parts of the documentation can use UML diagrams. They are generated from .uml files
with `plantuml <http://plantuml.com/starting>`_. Sphinx support plantuml but ReadTheDocs
didn't, so you have to convert .uml in .png with ``java -jar plantuml.jar file.uml``.


.. _codedocumenting_parameters:

Component Options documentation
-------------------------------

The agent, service and executor options are documented in their respective
module docstring via literal include of their options in the
ConfigTemplate.cfg::

   .. literalinclude:: ../ConfigTemplate.cfg
     :start-after: ##BEGIN MyComponent
     :end-before: ##END
     :dedent: 2
     :caption: MyComponent options

Around the section in the *ConfigTemplate.cfg* configuring the component the
*##BEGIN MyComponent* and *##END* tags need set so that the include is
restricted to the section belonging to the component. The options *:dedent:* and
*:caption:* are optional, but create a nicer output.

.. _building_documentation:

Building the Documentation
--------------------------

The DIRAC documentation is created using ``sphinx`` and makes use of the
``diracdoctools`` to create the code documentation, command references, and
concatenate the ``ConfigTemplates`` into one file. The ``diracdoctools`` are
located in the ``DIRAC/docs`` folder, but they can be ``pip installed`` for use
outside of DIRAC. In ``ReadTheDocs`` we do not install DIRAC via
:ref:`admin_dirac-install`, so we have to install dependencies via pip, or
``Mock`` the packages if they cannot be installed.

This ``requirements.txt`` shows how the ``diracdoctools`` can be installed from the
GitHub folder directly, and how other dependencies can also be installed.

.. literalinclude:: ../../../requirements.txt
   :caption: The docs/requirements.txt used in readthedocs

For DIRAC extensions you also have to install DIRAC as a requirement, so add
this line to your ``requirements.txt``

  git+https://github.com/DIRACGrid/DIRAC/@integration

Some packages (e.g., FTS3) cannot be installed in READTHEDOCS, so we let sphinx
mock these packages in the ``source/conf.py`` add the import at the top and set
the ``autodoc_mock_imports`` variable.

  from diracdoctools import fakeEnvironment, DIRAC_DOC_MOCK_LIST
  ...
  autodoc_mock_imports = DIRAC_DOC_MOCK_LIST

Using the ``DIRAC_DOC_MOCK_LIST`` you can inherit the packages that are mocked
for DIRAC, and extend it with packages needed to be mocked for your extension.
The ``fakeEnvironment`` sets up a special mock for GSI and simply needs to be
imported.

In the sphinx configuration file (``source/conf.py``), the functionality can
then be called to create code reference, command reference and concatenated CFG
files.

.. literalinclude:: ../../conf.py
   :start-after: # AUTO SETUP START
   :end-before:  # AUTO SETUP END
   :caption: docs/source/conf.py


The configuration for ``diracdoctools`` is done via a configuration file
located in the docs folder. Just copy the file ``DIRAC/docs/docs.conf``  and
adapt it to your needs. All options are mandatory unless otherwise stated.  If
certain features are not used, simply leave the values empty

.. literalinclude:: ../../../docs.conf
   :caption: docs/docs.conf

For local testing of the documentation, the scripts can also be called
directly, like this example from the ``Makefile`` shows.

.. literalinclude:: ../../../Makefile
   :caption: docs/Makefile
   :start-after: # AUTO MAKE START
   :end-before:  # AUTO MAKE END


Code Reference
``````````````

The code reference is either created by calling ``run`` from
``diracdoctools.Cmd.codeReference`` or by invoking the script
``dirac-docs-build-code.py``. This creates an ``rst`` file for each python
file, using ``autodoc`` to document all classes inside those modules. The
actual documentation is build when sphinx is invoked, which must be able to
import the modules and all their dependencies


Command Reference
`````````````````

The command references can be created by calling ``run`` from
``diracdoctools.Cmd.commandReference``, or by calling the
``dirac-docs-build-command.py`` script. ``[commands.section]`` will result in a
list of commands with links to their documentation, which is based on the output
of their ``--help``. The resulting ``index.rst`` has to be included explicitly
in the documentation. If ``indexFile`` is specified it has to contain all the
links itself, or warnings are generated for missing and superfluous entries.

.. note ::
  The parsing of the ``--help`` output is extremely limited and naive. You must not end a line with a
  colon ``:`` unless you intend to create a verbatim block after it.



CFG File
````````

If you developed your own systems, you can concatenate all the settings defined
in ``ConfigTemplate.cfg`` files into one large file. You need a base
``dirac.cfg`` file and a location where to put the final result

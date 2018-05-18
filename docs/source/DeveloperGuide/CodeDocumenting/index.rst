.. _code_documenting:

==================================
Documenting your developments
==================================

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

  * release notes are automatically created from titles of the Pull Requests. So,
    pay attention to what you put in there.

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
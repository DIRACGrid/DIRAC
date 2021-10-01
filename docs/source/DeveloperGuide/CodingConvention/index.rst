.. _coding_conventions:

==================================
Coding Conventions
==================================

Rules and conventions are necessary to insure a minimal coherence and consistency
of the DIRAC software. Compliance with the rules and conventions is mainly based
on the good will of all the contributors, who are working for the success of the
overall project.

Formatting
----------

The DIRAC code is formatted with `black <https://black.readthedocs.io/en/stable/>`_.
This can be configured to run automatically before each commit using `pre-commit`.
We recommend installing `pre-commit` system wide, using your operating system's package manager, e.g. `apt-get install pre-commit` on Debian based system, `dnf install pre-commit` on Fedora like systems or `brew install pre-commit` on macOS.
Once installed it can be enabled for your local clone using:

.. code-block:: bash

   pre-commit install --allow-missing-config

In addition most editors can be configured to automatically format code with black,
see [black's documentation](https://black.readthedocs.io/en/stable/integrations/editors.html)
for details.

Commit messages
---------------

Commit messages must be between 20 and 150 chars, and follow the format
``<type>(<scope>): <subject>``:
* ``type``: docs, feat, fix, refactor, style or test
* ``scope`` (optional): any extra info, (like DMS or whatever)


Code Organization
------------------------------

DIRAC code is organized in packages corresponding to *Systems*. *Systems* packages
are split into the following standard subpackages:

  *Service*
    contains Service Handler modules together with possible auxiliary modules
  *Agent*
    contains Agent modules together with possible auxiliary modules
  *DB*
    contains Database definitions and front-end classes
  *scripts*
    contains System commands codes

Some System packages might also have additional

  *test*
    Any unit tests and other testing codes
  *Web*
    Web portal codes following the same structure as described in
    :doc:`../AddingNewComponents/DevelopingWebPages/index`.

Packages are sets of Python modules and eventually compilable source code
together with the instructions to use, build and test it. Source code files are
maintained in the git code repository.

**R1**
  Each package has a unique name, that should be written such that each word starts
  with an initial capital letter ( "CamelCase" convention ). *Example*:
  *DataManagementSystem*.

Module Coding Conventions
--------------------------------


**R3**
  Each module should define the following variables in its global scope::

    __RCSID__ = "$Id$"

  this is the SVN macro substituted by the module revision number.

  ::

    __docformat__ = "restructedtext en"

  this is a variable specifying the mark-up language used for the module
  inline documentation ( doc strings ). See :doc:`../CodeDocumenting/index`
  for more details on the inline code documentation.

**R4**
  The first executable string in each module is a doc string describing the
  module functionality and giving instructions for its usage. The string is
  using `ReStructedText <http://docutils.sourceforge.net/rst.html>`_ mark-up
  language.

Importing modules
@@@@@@@@@@@@@@@@@@@@@@@@@@@@

**R5**
  Standard python modules are imported using::

    import <ModuleName>

  Public modules from other packages are imported using::

    import DIRAC.<Package[.SubPackage]>.<ModuleName>

Naming conventions
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

Proper naming the code elements is very important for the code clarity especially
in a project with multiple developers. As a general rule, names should be meaningful
but not too long.

**R6**
   Names are usually made of several words, written together without underscore,
   each first letter of a word being uppercased ( *CamelCase* convention ). The
   case of the first letter is specified by other rules. Only alphanumeric
   characters are allowed.

**R7**
   Names are case sensitive, but names that differ only by the case should not
   be used.

**R8**
   Avoid single characters and meaningless names like "jjj", except for local
   loops or array indexes.

**R9**
   Class names must be nouns, or noun phrases. The first letter is capital.

**R10**
   Class data attribute names must be nouns, or noun phrases. The first letter
   is lower case. The last word should represent the type of the variable value if
   it is not clear from the context otherwise. *Examples*: fileList, nameString,
   pilotAgentDict.

**R11**
   Function names and Class method names must be verbs or verb phrases, the first
   letter in lower case. *Examples*: getDataMember, executeThisPieceOfCode.

**R12**
   Class data member accessor methods are named after the attribute name with a
   "set" or "get" prefix.

**R13**
   Class data attributes must be considered as private and must never be accessed
   from outside the class. Accessor methods should be provided if necessary.

**R14**
   Private methods of a module or class must start by double underscore to explicitly
   prevent its use from other modules.

Python files
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

**R15**
  Python files should contain a definition of a single class, they may contain
  auxiliary (private) classes if needed. The name of the file should be the same as
  the name of the main class defined in the file

**R16**
  A constructor must always initialize all attributes which may be used in the class.

Methods and arguments
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

**R17**
  Methods must not change their arguments. Use assignment to an internal variable if
  the argument value should be modified.

**R18**
  Methods should consistently return a *Result* (*S_OK* or *S_ERROR*) structure.
  A single return value is only allowed for simple methods that can not fail after
  the code is debugged.

**R19**
  Returned *Result* structures must always be tested for possible failures.

**R20**
  Exception mechanism should be used only to trap "unusual" problems. Use *Result*
  structures instead to report failure details.

Coding style
------------------------------------

It is important to try to get a similar look, for an easier maintenance, as most of
the code writers will eventually be replaced during the lifetime of the project.

Comments and doc strings
@@@@@@@@@@@@@@@@@@@@@@@@@@@@

Comments should be abundant, and must follow the rules of automatic documentation
by the sphinx tool using ReStructedText mark-up.

**R24**
   Each class and method definition should start with the doc strings. See
   :doc:`../CodeDocumenting/index` for more details.

**R25**
   Use blank lines to separate blocks of statements but not blank commented
   lines.

Readability and maintainability
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

**R28**
  When doing lookup in dictionaries, don't use ``dict.has_key(x)`` - it is
  deprecated and much slower than ``x in dict``. Also, in python 3.0 this isn't
  valid.

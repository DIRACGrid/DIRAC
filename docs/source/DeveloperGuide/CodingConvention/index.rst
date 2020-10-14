.. _coding_conventions:

==================================
Coding Conventions
==================================

Rules and conventions are necessary to insure a minimal coherence and consistency
of the DIRAC software. Compliance with the rules and conventions is mainly based
on the good will of all the contributors, who are working for the success of the
overall project.

Pep8, Pycodestyle and autopep8
------------------------------

In order to ensure consistent formatting between developers, it was decided to stick to the Pep8 style guide (https://www.python.org/dev/peps/pep-0008/), with two differences:
* we use 2 space indentation instead of 4
* we use a line length of 120 instead of 80

This is managed by the setup.cfg at the root of the DIRAC repository.

In order to ensure that the formatting preference of the developer's editor does not play trick, there are two files under `tests/formatting`: `pep8_bad.py` and `pep8_good.py`. The first one contains generic rules and examples of dos and donts. The developer should pass this file through the autoformat of his/her editor. The output should be exactly `pep8_good.py`. We recommand the use of autopep8 for the autoformatting::

      [chaen@pclhcb31 formatting]$ pycodestyle pep8_bad.py
      pep8_bad.py:11:121: E501 line too long (153 > 120 characters)
      pep8_bad.py:15:121: E501 line too long (124 > 120 characters)
      pep8_bad.py:26:1: E303 too many blank lines (3)
      pep8_bad.py:28:23: E401 multiple imports on one line
      pep8_bad.py:73:3: E741 ambiguous variable name 'l'
      pep8_bad.py:74:3: E741 ambiguous variable name 'O'
      pep8_bad.py:75:3: E741 ambiguous variable name 'I'
      pep8_bad.py:79:42: E251 unexpected spaces around keyword / parameter equals
      pep8_bad.py:79:44: E251 unexpected spaces around keyword / parameter equals
      pep8_bad.py:79:62: E251 unexpected spaces around keyword / parameter equals
      pep8_bad.py:79:64: E251 unexpected spaces around keyword / parameter equals
      pep8_bad.py:79:82: E231 missing whitespace after ','
      pep8_bad.py:79:89: E231 missing whitespace after ','
      pep8_bad.py:79:99: E231 missing whitespace after ','
      pep8_bad.py:79:106: E231 missing whitespace after ','
      pep8_bad.py:79:117: E231 missing whitespace after ','
      pep8_bad.py:79:121: E501 line too long (153 > 120 characters)
      pep8_bad.py:79:126: E231 missing whitespace after ','
      pep8_bad.py:79:148: E251 unexpected spaces around keyword / parameter equals
      pep8_bad.py:79:150: E251 unexpected spaces around keyword / parameter equals
      pep8_bad.py:108:1: E303 too many blank lines (3)


      [chaen@pclhcb31 formatting]$ autopep8 pep8_bad.py > myAutoFormat.py

      [chaen@pclhcb31 formatting]$ pycodestyle myAutoFormat.py
      myAutoFormat.py:11:121: E501 line too long (153 > 120 characters)
      myAutoFormat.py:74:3: E741 ambiguous variable name 'l'
      myAutoFormat.py:75:3: E741 ambiguous variable name 'O'
      myAutoFormat.py:76:3: E741 ambiguous variable name 'I'

      [chaen@pclhcb31 formatting]$ diff myAutoFormat.py pep8_good.py
      [chaen@pclhcb31 formatting]$

Note that pycodestyle will still complain about the ambiguous variable in the good file, since autopep8 will not remove them. Also, autopep8 will not modify comment inside docstrings, hence the first warning on the good file.

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

General lay-out
@@@@@@@@@@@@@@@@@@@@@@@@@@@@

**R21**
  The length of any line should be preferably limited to 120 characters to allow
  debugging on any terminal.

**R22**
  Each block is indented by **two** spaces.

**R23**
   When declaring methods with multiple arguments, consider putting one argument
   per line. This allows inline comments and helps to stay within the 120 column
   limit.

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

**R26**
   Use spaces to separate operator from its operands.

**R27**
   Method invocations should have arguments separated, at least by one space. In
   case there are long or many arguments, put them each on a different line.

**R28**
  When doing lookup in dictionaries, don't use ``dict.has_key(x)`` - it is
  deprecated and much slower than ``x in dict``. Also, in python 3.0 this isn't
  valid.

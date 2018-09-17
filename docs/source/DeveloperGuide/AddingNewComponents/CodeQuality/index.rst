.. _code_quality:

==========================================
Code quality
==========================================

DIRAC code should be coded following the conventions explained in :ref:`coding_conventions`.
There are automatic tools that can help you to follow general good code quality rules.

Specifically, `pylint <http://www.pylint.org/>`_, a static code analyzer, can be used.
Pylint can give you nice suggestions, and might force you to code in a "standard" way.
In any case, to use pylint on DIRAC code we have to supply a configuration file, otherwise pylint will assume that we are coding with standard rules, which is not fully the case: just to say, our choice was to use 2 spaces instead of 4, which is non-standard.

A pylint config file for DIRAC can be found `here <https://github.com/DIRACGrid/DIRAC/blob/integration/.pylintrc>`_

Exercise:
---------

Start a new branch from an existing remote one (call it, for example, codeQualityFixes).
Run pylint (e.g. via pylint-gui) using the DIRAC.pylint.rc file on a file or 2.
Then, commit your changes to your branch. Push this branch to origin, and then ask for a Pull Request using the DIRACGrid github page.

Remember to choose the correct remote branch on which your branch should be merged.

Remember to add a line or 2 of documentation for your PR.

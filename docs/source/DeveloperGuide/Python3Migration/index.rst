.. _python_3_migration:

==================
Python 3 Migration
==================

At the end of 2019 the Python Software Foundation ended their support of cPython 2.7.
RedHat will continue to provide support of their CentOS 8 RPM until June 2024 however the maintenance burden of using Python 2.7 is rapidly increasing as libraries drop support.
This also complicates any migration to Python 3 as it becomes necessary to make major updates to dependencies at the same time.
In addition, DIRACOS has two dependencies which might cause issues as a result of being deprecated:

- CentOS 6 source RPMs are used and these will stop being supported in November 2020.
- The maintainers of the Python Package Index and pip have said they will drop support if either:

  - bugs in Python 2.7 itself make this necessary (which is unlikely)
  - Python 2 usage reduces to a level where pip maintainers feel it is OK to drop support

While neither of these are likely to affect DIRAC in the immediate future, DIRAC has a relatively slow release cycle and therefore Python 2 will still be used for several years after the migration begins.

Migration strategy
------------------

The generally accepted migration strategy:

#. Add support for Python 2 and Python 3 by slowly modernising the code base
#. Once practical, start running unit tests with Python 3 even if they're allowed to fail
#. Once tests pass, start supporting both Python versions so any remaining bugs can be found
#. Drop support for Python 2

In the case of DIRAC, it is also necessary to move to using JSON for serialising the messages sent between between servers.
This will be available as a technology preview in v7r2.

DIRACOS
^^^^^^^

Once it becomes possible to run some DIRAC services with a client that uses Python 3 a suitable DIRACOS release will be needed.
This will be DIRACOS version 2 and the ``--dirac-os-version=v2rX`` flag to `dirac-install` becomes the way to create a Python 3 based DIRAC installation.
This will be based upon `conda-forge <https://conda-forge.org/>`_ and `conda-pack <https://conda.github.io/conda-pack/>`_ and provides several benefits over DIRACOS version 1 while maintaining bit-for-bit reproducibility:

- **Faster:** Creating a new build will take under ten minutes instead of the multiple hours currently required.
- **Distribution independent:** The binaries provided by conda-forge are independent of the Linux distribution allowing DIRAC to only have a minimum ``glibc`` requirement.
- **Alternative architectures:** There is already a small demand for running DIRAC on alternative architectures such as ARM and POWER PC and these platforms are already supported by conda-forge.
- **Easier to extend:** Extensions will be able to contain any additional packages, even if it contains significant compiled code.
- **Greater flexibility:** Currently it is time consuming to modify or add new packages to DIRACOS, especially if a CentOS 6 SRPM doesn't exist. With a conda based DIRACOS it will be possible to make significant changes quickly, such as trying a higher performance `PyPy <https://www.pypy.org/features.html>`_ based build.

2to3
^^^^

When Python 3 was first envisioned the expectation was that ``2to3`` could be ran on a code base to migrate it in one shot.
This quickly turned out to be impractical for anything other than small projects and this is especially true of DIRAC where a large fraction of the code is not tested automatically due to it depending on external services.
Running ``2to3`` at install time isn't ideal as it make it hard to map line numbers to the code, introduces new bugs and generally makes "ugly" code.
Instead the strategy used by almost every project has been to move to a code base which is compatible with both Python 2 and Python 3 at the same time.
This is not inherently an additional burden as the "modern" style of Python 2 code is compatible with Python 3 so it is beneficial when using both Python versions.

Linting
^^^^^^^

Linters can be very useful for finding some compatibility problems however there will initially be too many issues for them to be included in the CI.
To avoid this and allow the progressive inclusion of fixes, the directories listed in ``tests/py3CheckDirs.txt`` will be linted for Python 3 compatibility in the CI.

Further reading
---------------

The following links contain useful information about migrating to Python 3:

- `The Conservative Python 3 Porting Guide <https://portingguide.readthedocs.io/en/latest/index.html>`_
- `Mercurial's Journey to and Reflections on Python 3 <https://gregoryszorc.com/blog/2020/01/13/mercurial%27s-journey-to-and-reflections-on-python-3/>`_


Recommendations for code
------------------------

The "The Conservative Python 3 Porting Guide" linked above is an excellent source of information.
This sections contains some details that are particularly relevant to DIRAC.

**\_\_future\_\_ imports**
  Since Python 2.1 the ``__future__`` module has been used to allow the behaviour of newer Python versions to be accessed from older interpreters.
  For the Python 3 migration there are three particularly useful ones which should be applied to all new files in DIRAC.
  Note: This only applies to DIRAC v7.x series releases as DIRAC version 8+ only supports Python 3 and has no need for these imports.

  - ``from __future__ import print_function``:
    Replaces the Python 2 style print statement with Python 3's print function.
    This is already used widely in DIRAC and should be safe to apply to any file.
    If it is applied to a file which uses the old style function it is an easy to detect ``SyntaxError`` that will be noticed by any commonly used linter.
  - ``from __future__ import absolute_import``:
    In Python 3 all imports are absolute by default.
    This means that if using ``import my_module`` will not find files called ``my_module.py`` that are next to the current file.
    If a relative import is desired this must be explicit using ``from . import my_module``.
  - ``from __future__ import division``:
    In Python 3 division of integers returns a float when necessary, i.e. ``1 / 2 == 0`` in Python 2 but ``1 / 2 == 0.5`` in Python 3.
    The Python 2 behaviour was a common source of bugs and it is likely safe to use this import in most modules.
    After this import is included, both Python versions will have the same behaviour with ``1 / 2 == 0.5`` however some fixes my be required to use the integer division operator ``1 // 2 == 0`` that is available in both Python versions regardless of if the future import is used.

  These three are all confined to only affecting the current file making it easy to progressively add them to individual files without unexpected side effects in other parts of DIRAC.
  While ``from __future__ import unicode_literals`` also exists, this tends to result in unexpected side effects from ``unicode`` objects being passed to functions that weren't designed to handle them and as a result it is not expected to be useful for DIRAC's Python 3 migration.

**bytes vs str**
  The most difficult change when moving Python 3 is the splitting of the `str` type one for text and one for true binary data.
  This exposes subtle issues in Python 2 that were likely never noticed and an automatic conversion to fix this is inherently impossible.
  More details about this can be found `here <https://portingguide.readthedocs.io/en/latest/strings.html>`_ and in slide 6 to slide 13 of the Python 3 presentation that was given at the `BiLD on 8th October 2020 <https://indico.cern.ch/event/956902/contributions/4021427/>`_.

  In most situations DIRAC is only dealing with ascii or unicode strings and therefore nothing needs to change.
  However many libraries choose to be independent of the character encoding used and therefore return a ``bytes`` object in Python 3 instead of ``str``

  .. code-block:: python

    result = subprocess.check_output(["echo", "Hello"])

    # Bad: Fails on Python 3 with "TypeError: can't concat str to bytes"
    return "Result is" + result

    # Good: Explicitly decode bytes to str (does nothing on Python 2)
    return "Result is" + result.decode()

    # For subprocess functions, the universal_newlines=True argument can be used
    other_result = subprocess.check_output(["echo", "Hello"], universal_newlines=True)
    # Good: other_result is already a str object
    return "Result is" + other_result

  **Checking the type of a string:**

  .. code-block:: python

    # Bad: Types should be check using isinstance
    if type(my_variable) == str:

    # Bad: basestring does not exist in Python 3
    if isinstance(my_variable, basestring):

    # Good: Supports both Python 2 and 3
    if isinstance(my_variable, six.string_types):

  **Reading files:**

  It's preferable to explicitly state if a file is being opened in text mode or binary mode.

  .. code-block:: python

    # Bad: Works but it is unclear if data is expected to bytes or a string
    with open("my_file.txt") as fp:
       data = fp.read().split("\n")

    # Good: File is explicitly in text mode
    with open("my_file.txt", "rt") as fp:
       data = fp.read().split("\n")

    # Bad: Fails on python 3 as "\n" is a string not bytes
    with open("my_file.txt", "rb") as fp:
       data = fp.read().split("\n")

    # Good: Prefix the "\n" to make it a bytes object
    with open("my_file.txt", "rb") as fp:
       data = fp.read().split(b"\n")

  While many guides recommend the use of ``io.open`` this is not suitable for DIRAC as unicode is not handled correctly in all cases.
  See slide 6 from the `aforementioned BiLD meeting <https://indico.cern.ch/event/956902/contributions/4021427/>`_ for more details.

**Dictionaries**
  In Python 3 ``my_dict.keys()``, ``my_dict.values()`` and ``my_dict.items()`` now return an iterator instead of a list.
  This is equivalent to ``my_dict.iterkeys()``, ``my_dict.itervalues()`` and ``my_dict.iteritems()`` in Python 2 and these methods have been removed.

  In almost all cases ``my_dict``, ``my_dict.values()`` and ``my_dict.items()`` should be preferred.
  The is a small overhead in Python 2 when using ``items()`` instead of ``iteritems()`` however this is only applicable when dealing with large dictionaries in tight loops and such code can likely be written as a faster alternative (``six`` provides functions like ``six.iteritems(my_dict)`` if absolutely necessary).

  In rare cases the list object returned might be desirable, if so ``list(my_dict.items())`` can be used.

  The ``haskey`` method has been deprecated since Python 2.2 and is removed in Python 3.
  ``my_dict.has_key("Message")`` should be replaced with ``"Message" in my_dict``

**Other iterators**
  The ``zip``, ``map`` and ``filter`` builtins in Python 3 behave like the iterator variants like ``itertools.izip`` in Python 2.
  In additional the Python 3 ``range`` function is equivalent to the Python 2 function ``xrange``
  The same guidelines apply as with dictionaries.

  .. code-block:: python

    # Bad: Will fail if indexed or iterated over twice in Python 3
    numbers = range(10)

    # Good: Will behave the same way in both Python 2 and Python 3
    numbers = list(range(10))

    # Bad: xrange is not available in Python 3
    for i in xrange(10):

    # Good: Will behave the same way in both Python 2 and Python 3
    for i in range(10):

    # Bad: Will use a lot of memory on Python 2
    for i in range(100000000):

    # Good: Only necessary if running many tens of millions of iterations
    # Such cases should be like be solved with a faster solution
    for i in six.moves.range(100000000):

**Integers**
  In Python 3 all integers allow effectively infinite values, this was equivalent to ``long`` in Python 2.
  As Python 2 automatically promotes numbers to ``long`` when they're too big.
  The main issue with using ``int`` instead of ``long`` is that type checks may fail as shown here:

  .. code-block:: python

    # Bad: Original Python 3 incompatible code
    my_number = long(my_number)
    if isinstance(my_number, long)

    # Bad: Works in Python 3 but will be broken in Python 2 for some inputs
    my_number = int(my_number)
    if isinstance(my_number, int)

    # Good: Works in both Python 2 and Python 3
    my_number = int(my_number)
    if isinstance(my_number, six.integer_types)

  If the number is being passed to an interface which might have broken type checks, ``long`` can be imported from ``past.builtins``.

  Some more examples of using integers:

  .. code-block:: python

    # Bad: long doesn't exist in Python 3
    my_number = long("1000000000000")

    # Good: Will behave the same way in both Python 2 and Python 3
    my_number = int("1000000000000")

    # Good: Automatically promoted to long in Python 2
    my_number = int("1000000000000000000000000000000000")

    # Bad: Won't evaluate to true if the number is too large
    if isinstance(my_number, int):

    # Bad: long doesn't exist in Python 3
    if isinstance(my_number, (int, long)):

    # Good: Will behave the same way in both Python 2 and Python 3
    if isinstance(my_number, six.integer_types):

    # Bad: The L suffix doesn't exist in Python 3
    my_number = 1000000000000000000000000000000000L

    # Good: Will behave the same way in both Python 2 and Python 3
    my_number = 1000000000000000000000000000000000

**Classes**
  In Python 2.2 "new-style" classes were introduced which should always inherit from ``object``.
  The behaviour of "old-style" is almost never desirable or intentional and they were removed from Python 3.
  To ensure new-style classes are always used, all objects should inherit from ``object`` or another "new-style" class.

  .. code-block:: python

    # Bad: Uses an old-style class in Python 2 and a new-style class in Python 3
    class MyClass:

    # Good: Will behave the same way in both Python 2 and Python 3
    class MyClass(object):

    # Good: Will behave the same way in both Python 2 and Python 3
    class MyOtherClass(MyClass):

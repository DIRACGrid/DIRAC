""" Decorators for DIRAC.
"""

import os
import inspect
import functools
import traceback

__RCSID__ = "$Id$"

def deprecated( reason, onlyOnce=False ):
  """ A decorator to mark a class or function as deprecated.

      This will cause a warnings to be generated in the usual log if the item
      is used (instantiated or called).

      If the environment variable ``DIRAC_DEPRECATED_FAIL`` is set to a non-empty value, an exception will be
      raised when the function or class is used.

      The decorator can be used before as class or function, giving a reason,
      for example::

        @deprecated("Use functionTwo instead")
        def functionOne(...):

      If `onlyOnce` is set to true then the warning will only be generated on the
      first call or creation of the item. This is useful for things that are
      likely to get called repeatedly (to prevent generating massive log files);
      for example::

        @deprecated("Use otherClass instead", onlyOnce=True)
        class MyOldClass:

      Parameters
      ----------
      reason : str
        Message to display to the user when the deprecated item is used. This should specify
        what should be used instead.
      onlyOnce : bool
        If set, the deprecation warning will only be displayed on the first use.

      Returns
      -------
      function
        A double-function wrapper around the decorated object as required by the python
        interpreter.
  """
  def decFunc( func, clsName=None ):
    """ Inner function generator.
        Returns a function which wraps the given "func" function,
        which prints a deprecation notice as it is called.
        clsName is used internally for class handling, and should be left
        set to None otherwise.

      Parameters
      ----------
      func : function
        The function to call from the wrapper.
      clsName : string
        If set, the wrapped object is assumed to be the __init__ function of
        a class called "clsName". Set to None for wrapping a normal function.

      Returns
      -------
      function
        A function wrapper which which prints the deprecated warning and calls
        func.
    """

    decFunc.warningEn = True

    @functools.wraps( func )
    def innerFunc( *args, **kwargs ):
      """ Prints a suitable deprectaion notice and calls
          the constructor/function/method.
          All arguments are passed through to the target function.
      """
      # fail calling the function if environment variable is set
      if os.environ.get("DIRAC_DEPRECATED_FAIL", None):
        raise NotImplementedError("ERROR: using deprecated function or class: %s" % reason)
      # Get the details of the deprecated object
      if clsName:
        objName = clsName
        objType = "class"
      else:
        objName = func.__name__
        objType = "object"
        if inspect.isfunction( func ):
          objType = "function"
      if decFunc.warningEn:
        # We take the second to last stack frame,
        # which will be the place which called the deprecated item
        # callDetails is a tuple of (file, lineNum, function, text)
        callDetails = traceback.extract_stack()[-2]
        print "NOTE: %s %s is deprecated (%s)." % ( objName, objType, reason )
        print "NOTE:   Used at %s:%u" % ( callDetails[0], callDetails[1] )
      if onlyOnce:
        decFunc.warningEn = False
      return func( *args, **kwargs )

    # Classes are special, we can decorate them directly,
    # but then calling super( class, inst ) doesn't work as the reference
    # to class becomes a function. Instead we decorate the class __init__
    # function, but then have to override the name otherwise just "__init__ is
    # deprecated" will be printed.
    if inspect.isclass( func ):
      func.__init__ = decFunc( func.__init__, clsName=func.__name__ )
      return func
    return innerFunc

  return decFunc

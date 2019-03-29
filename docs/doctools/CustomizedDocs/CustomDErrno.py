"""Additional docstring for the DErrno module."""


class CustomDErrno(object):  # pylint: disable=too-few-public-methods
  """Add the ERRNO constants to the docstring automatically."""

  def __init__(self):
    """Create a string containing restructured text documentation for all the defined error numbers.

    :param str module: full path to the module
    :param bool replace: wether to replace the full docstring or not
       if you replace it completely add the automodule commands etc.!
    """
    self.module = 'DIRAC.Core.Utilities.DErrno'
    self.replace = False
    self.doc_string = ''
    # Fill the docstring addition with what we want to add
    self.doc_string += '\n'
    self.doc_string += 'ErrorCodes\n'
    self.doc_string += '----------\n'
    from DIRAC.Core.Utilities.DErrno import dErrorCode, dStrError
    for ec in sorted(set(dStrError) & set(dErrorCode)):
      # following the syntax for definition lists, bolding the ERRNO so it looks like other module members
      self.doc_string += '\n%s.\\ **%s** = %d\n    %s' % (self.module, dErrorCode[ec], ec, dStrError[ec])


CUSTOMIZED_DOCSTRINGS['DIRAC.Core.Utilities.DErrno'] = CustomDErrno()  # pylint: disable=undefined-variable

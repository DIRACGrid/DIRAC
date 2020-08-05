"""Additional docstring for the Transformation module."""

import textwrap

MODULE = 'DIRAC.TransformationSystem.Client.Transformation'


class CustomTransformation(object):  # pylint: disable=too-few-public-methods
  """Add the ERRNO constants to the docstring automatically."""

  def __init__(self):
    """Create a string containing restructured text documentation for all the special tranformation parameters

    :param str module: full path to the module
    :param bool replace: wether to replace the full docstring or not
       if you replace it completely add the automodule commands etc.!
    """
    self.module = MODULE
    self.replace = False
    self.doc_string = ''
    # Fill the docstring addition with what we want to add
    self.doc_string += textwrap.dedent("""
                                       Transformation Parameters
                                       -------------------------

                                       Any parameter with ``ParameterName`` can be set for a transformation with a call
                                       to ``setParameterName(parameterValue)``.

                                       The following parameters have a special meaning
                                       """)
    from DIRAC.TransformationSystem.Client.Transformation import Transformation
    trans = Transformation()
    for paramName in sorted(trans.paramTypes):
      self.doc_string += '\n``%s``:\n    Default value: %r' % (paramName, trans.paramValues[paramName])


CUSTOMIZED_DOCSTRINGS[MODULE] = CustomTransformation()  # pylint: disable=undefined-variable

"""
  Defines the plugin to perform evaluation on the lfn name
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id $"

from DIRAC.Resources.Catalog.ConditionPlugins.FCConditionBasePlugin import FCConditionBasePlugin


class FilenamePlugin(FCConditionBasePlugin):
  """
     This plugin is to be used when filtering based on the LFN name
  """

  def __init__(self, conditions):
    """ the condition can be any method of the python string object that can be evaluated
        as True or False:

          * endswith
          * find
          * isalnum
          * isalpha
          * isdigit
          * islower
          * isspace
          * istitle
          * isupper
          * startswith

      It should be written just like if you were calling the python call yourself.
      For example::

        Filename=startswith('/lhcb')
        Filename=istitle()

    """
    super(FilenamePlugin, self).__init__(conditions)

  def eval(self, **kwargs):
    """ evaluate the parameters. The lfn argument is mandatory
    """

    lfn = kwargs.get('lfn')

    if not lfn:
      return False

    evalStr = "'%s'.%s" % (lfn, self.conditions)
    try:
      ret = eval(evalStr)
      # Special case of 'find' which returns -1 if the pattern does not exist
      if self.conditions.startswith('find('):
        ret = False if ret == -1 else True

      return ret
    except BaseException:
      return False

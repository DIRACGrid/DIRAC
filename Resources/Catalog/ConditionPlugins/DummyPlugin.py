"""
  Defines the plugin to return the boolean given as param
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id $"

from DIRAC.Resources.Catalog.ConditionPlugins.FCConditionBasePlugin import FCConditionBasePlugin


class DummyPlugin(FCConditionBasePlugin):
  """
     This plugin is to be used to simply return True or False
  """

  def __init__(self, conditions):
    """ the condition can be True or False:
    """
    super(DummyPlugin, self).__init__(conditions)

  def eval(self, **kwargs):
    """ evaluate whether the conditon is True or False
    """
    return eval(self.conditions)

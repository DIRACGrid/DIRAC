"""
  Defines the plugin to return the boolean given as param
"""
from DIRAC.Resources.Catalog.ConditionPlugins.FCConditionBasePlugin import FCConditionBasePlugin


class DummyPlugin(FCConditionBasePlugin):
    """
    This plugin is to be used to simply return True or False
    """

    def __init__(self, conditions):
        """the condition can be True or False:"""
        super().__init__(conditions)

    def eval(self, **kwargs):
        """evaluate whether the conditon is True or False"""
        return eval(self.conditions)

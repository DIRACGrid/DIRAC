""" Base class for plugins as used in the transformation system
"""
import re

from DIRAC import S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations


class PluginBase:
    """Base class for TS plugins"""

    def __init__(self, plugin, operationsHelper=None):
        """plugin name has to be passed in: it will then be executed as one of the functions below, e.g.
        plugin = 'BySize' will execute TransformationPlugin('BySize')._BySize()
        """
        self.plugin = plugin
        self.params = {}

        if operationsHelper is None:
            self.opsH = Operations()
        else:
            self.opsH = operationsHelper

    def setParameters(self, params):
        """Extensions may re-define it"""
        self.params = params

    def run(self):
        """this is a wrapper to invoke the plugin (self._%s()" % self.plugin)"""
        try:
            evalString = f"self._{self.plugin}()"
            return eval(evalString)  # pylint: disable=eval-used
        except AttributeError as x:
            if re.search(self.plugin, str(x)):
                return S_ERROR("Plugin not found")
            else:
                gLogger.exception("Exception in plugin", self.plugin, lException=x)
                raise AttributeError(x)
        except Exception as x:
            gLogger.exception("Exception in plugin", self.plugin, lException=x)
            raise Exception(x)

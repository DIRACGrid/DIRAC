""" Base class for plugins as used in the transformation system
"""

import re
import ast

from DIRAC import S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

class Plugins( object ):
  
  def __init__( self, plugin, transClient = None, operationsHelper = None ):
    """ plugin name has to be passed in: it will then be executed as one of the functions below, e.g.
        plugin = 'BySize' will execute TransformationPlugin('BySize')._BySize()
    """
    self.plugin = plugin
    self.params = {}

    if not operationsHelper:
      self.opsH = Operations()
    else:
      self.opsH = operationsHelper

  def setParameters( self, params ):
    """ Extensions may re-define it
    """
    self.params = params

  def run( self ):
    """ this is a wrapper to invoke the plugin (self._%s()" % self.plugin)
    """
    try:
      evalString = "self._%s()" % self.plugin
      return ast.literal_eval( evalString )
    except AttributeError, x:
      if re.search( self.plugin, str( x ) ):
        return S_ERROR( "Plugin not found" )
      else:
        raise AttributeError, x
    except Exception, x:
      gLogger.exception( x )
      raise Exception, x


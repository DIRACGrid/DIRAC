"""A set of tools for the consumer system.
   In the future the tools corresponding to the CS operations
   probably should be extracted along with other helpers
   to create a common block of CS reading operationg
   e.g. with PathFinder.py, Registry.py and CSHelper.py
   but also in AgentModule.py and RequestHandler.py
"""

from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client import PathFinder

def getOption( optionName, defaultValue = None ):
  return gConfig.getOption( optionName, defaultValue )

def getConsumerOption( optionName, consumerSection, defaultValue = None ):
  return gConfig.getOption( '%s/%s' % ( consumerSection, optionName ), defaultValue )

def getConsumerSection ( system_ConsumerName ):
  """
  """
  return PathFinder.getConsumerSection( system_ConsumerName )

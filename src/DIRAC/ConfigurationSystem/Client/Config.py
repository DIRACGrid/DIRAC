"""
  Instantiate the global Configuration Object
  gConfig is used everywhere within DIRAC to access Configuration data
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient

#: Global gConfig object of type :class:`~DIRAC.ConfigurationSystem.private.ConfigurationClient.ConfigurationClient`
gConfig = ConfigurationClient()


def getConfig():
  """
  :returns: gConfig
  :rtype: ~DIRAC.ConfigurationSystem.private.ConfigurationClient.ConfigurationClient
  """
  return gConfig

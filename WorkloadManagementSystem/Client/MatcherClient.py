""" Module that contains simple client access to Matcher service
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.JEncode import strToIntDict


@createClient('WorkloadManagement/Matcher')
class MatcherClient(Client):

  """ Exposes the functionality available in the WorkloadManagement/MatcherHandler

      This inherits the DIRAC base Client for direct execution of server functionality.
      The following methods are available (although not visible here).

  """

  def __init__(self, **kwargs):
    """ Simple constructor
    """

    super(MatcherClient, self).__init__(**kwargs)
    self.setServer('WorkloadManagement/Matcher')

  @ignoreEncodeWarning
  def getMatchingTaskQueues(self, resourceDict):
    """ Return all task queues that match the resourceDict
    """
    res = self._getRPC().getMatchingTaskQueues(resourceDict)

    if res["OK"]:
      # Cast the string back to int
      res['Value'] = strToIntDict(res['Value'])
    return res

  @ignoreEncodeWarning
  def getActiveTaskQueues(self):
    """ Return all active task queues
    """
    res = self._getRPC().getActiveTaskQueues()

    if res["OK"]:
      # Cast the string back to int
      res['Value'] = strToIntDict(res['Value'])
    return res

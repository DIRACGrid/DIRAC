""" Command module contains the base "Command" class, base for all RSS commands

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from DIRAC import gLogger, S_OK


class Command(object):
  """
    The Command class is a simple base class for all the commands
    for interacting with the clients
  """

  def __init__(self, args=None, clients=None):

    self.apis = (1 and clients) or {}
    self.masterMode = False
    self.onlyCache = False
    self.metrics = {'failed': []}

    self.args = {'onlyCache': False}
    _args = (1 and args) or {}
    self.args.update(_args)
    self.log = gLogger.getSubLogger(self.__class__.__name__)

  def doNew(self, masterParams=None):
    """ To be extended/replaced by real commands
    """
    return S_OK((self.args, masterParams))

  def doCache(self):
    """ To be extended/replaced by real commands
    """
    return S_OK(self.args)

  def doMaster(self):
    """ To be extended/replaced by real commands
    """
    return S_OK(self.metrics)

  def doCommand(self):
    """ Real commands don't need to extende/replace this method, which is called by clients.

        What is done here is the following:
        if self.masterMode is set to True, then the "doMaster()" method is called.
        if not, then the doCache() method is called, and if this returns an object this is returned,
        and otherwise the "doNew" method is called.
    """

    if self.masterMode:
      self.log.verbose('doMaster')
      return self.returnSObj(self.doMaster())

    self.log.verbose('doCache')
    result = self.doCache()
    if not result['OK']:
      return self.returnERROR(result)
    # We may be interested on running the commands only from the cache,
    # without requesting new values.
    if result['Value'] or self.args['onlyCache']:
      return result

    self.log.verbose('doNew')
    return self.returnSObj(self.doNew())

  def returnERROR(self, s_obj):
    """
      Overwrites S_ERROR message with command name, much easier to debug
    """

    s_obj['Message'] = '%s %s' % (self.__class__.__name__, s_obj['Message'])
    return s_obj

  def returnSObj(self, s_obj):
    """
      Overwrites S_ERROR message with command name, much easier to debug
    """

    if s_obj['OK']:
      return s_obj

    return self.returnERROR(s_obj)

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

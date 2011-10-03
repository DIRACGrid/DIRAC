""" The Pilots_Command class is a command class to know about
    present pilots efficiency
"""

import sys

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Command.Command      import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes
from DIRAC.ResourceStatusSystem.Utilities.Utils      import where

#############################################################################

class PilotsStats_Command(Command):

  def doCommand(self):
    """
    Return getPilotStats from Pilots Client
    """
    super(PilotsStats_Command, self).doCommand()

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient
      self.client = PilotsClient()

    try:
      res = self.client.getPilotsStats(self.args[0], self.args[1], self.args[2])
    except:
      gLogger.exception("Exception when calling PilotsClient for %s %s" %(self.args[0], self.args[1]))
      return {'Result':'Unknown'}

    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class PilotsEff_Command(Command):

  def doCommand(self):
    """
    Return getPilotsEff from Pilots Client
    """
    super(PilotsEff_Command, self).doCommand()

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient
      self.client = PilotsClient()

    try:
      res = self.client.getPilotsEff(self.args[0], self.args[1], self.args[2])
    except:
      gLogger.exception("Exception when calling PilotsClient for %s %s" %(self.args[0], self.args[1]))
      return {'Result':'Unknown'}

    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class PilotsEffSimple_Command(Command):

  def doCommand(self, RSClientIn = None):
    """
    Returns simple pilots efficiency

    :attr:`args`:
        - args[0]: string - should be a ValidRes

        - args[1]: string - should be the name of the ValidRes

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
    super(PilotsEffSimple_Command, self).doCommand()

    if self.args[0] == 'Service':
      
      if self.rsClient is None:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
        self.rsClient = ResourceStatusClient()

      try:
        name = self.rsClient.getGeneralName(self.args[0], self.args[1], 'Site')['Value'][0]
      except:
        gLogger.error("PilotsEffSimple_Command: can't get a general name for %s %s" %(self.args[0], self.args[1]))
        return {'Result':'Unknown'}
      granularity = 'Site'

    elif self.args[0] in [ 'Site', 'Resource' ]:
      name = self.args[1]
      granularity = self.args[0]
    else:
      raise InvalidRes, where(self, self.doCommand)

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient
      self.client = PilotsClient()

    try:
      res = self.client.getPilotsSimpleEff(granularity, name, timeout = self.timeout)
      if res is None:
        return {'Result':'Idle'}
      if res[name] is None:
        return {'Result':'Idle'}
    except:
      gLogger.exception("Exception when calling PilotsClient for %s %s" %(granularity, name))
      return {'Result':'Unknown'}

    return {'Result':res[name]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class PilotsEffSimpleCached_Command(Command):

  def doCommand(self):
    """
    Returns simple pilots efficiency

    :attr:`args`:
       - args[0]: string: should be a ValidRes

       - args[1]: string should be the name of the ValidRes

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
    super(PilotsEffSimpleCached_Command, self).doCommand()

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if self.args[0] == 'Service':
      try:
        name = self.rsClient.getGeneralName(self.args[0], self.args[1], 'Site')['Value'][0]
      except:
        gLogger.error("PilotsEffSimpleCached_Command: can't get a general name for %s %s" %(self.args[0], self.args[1]))
        return {'Result':'Unknown'}
      granularity = 'Site'
    elif self.args[0] in ('Site', 'Sites'):
      name = self.args[1]
      granularity = self.args[0]
    else:
      raise InvalidRes, where(self, self.doCommand)

    try:

      if self.client is None:
        from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
        self.client = ResourceManagementClient(timeout = self.timeout)
      res = self.client.getCachedResult(name, 'PilotsEffSimpleEverySites', 'PE_S', 'NULL')['Value']
      if res == None:
        return {'Result':'Idle'}
      if res == []:
        return {'Result':'Idle'}
    except:
      gLogger.exception("Exception when calling ResourceManagementClient for %s %s" %(granularity, name))
      return {'Result':'Unknown'}

    return {'Result':res[0]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
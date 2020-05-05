""" PolicyBase

  The Policy class is a simple base class for all the policies.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.ResourceStatusSystem.Command.Command import Command

__RCSID__ = '$Id$'


class PolicyBase(object):
  """ Base class for all the policies. Do not instantiate directly.
      To use, you should call `setCommand` on the real policy instance.
  """

  def __init__(self):
    """ Constructor
    """

    self.command = Command()
    self.result = {}

  def setCommand(self, policyCommand):
    """
    Set `self.command`.

    :params:
      :attr:`commandIn`: a command object
    """
    if policyCommand is not None:
      self.command = policyCommand

  def evaluate(self):
    """
    Before use, call `setCommand`.

    Invoking `super(PolicyCLASS, self).evaluate` will invoke
    the command (if necessary) as it is provided and returns the results.
    """

    commandResult = self.command.doCommand()
    return self._evaluate(commandResult)

  @staticmethod
  def _evaluate(commandResult):
    """
      Method that will do the real processing of the policy, it has to be extended
      on the real policies.
    """

    return commandResult

""" The MacroCommand class is a macro class for all the macro commands
    for interacting with multiple the clients
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command

class MacroCommand(Command):
  
  def __init__(self, commands):
    self.__commands = commands
  
  # calls every command in the list of commands
  def doCommand(self, args):
    
    if type(args) is not list:
      args = [args]
    
    if len(args) == len(self.__commands):
      commArgs = [(self.__commands[x], args[x]) for x in range(len(self.__commands))]
    elif len(args) == 1:
      commArgs = [(self.__commands[x], args[0]) for x in range(len(self.__commands))]
    else:
      raise RSSException, where(self, self.doCommand)
    
    res = []
    
    for command, arg in commArgs:
      res.append(command.doCommand(arg))
    return res
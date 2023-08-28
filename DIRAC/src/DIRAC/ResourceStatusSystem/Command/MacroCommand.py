""" MacroCommand

  The MacroCommand class is a macro class for all the macro commands
  for interacting with multiple commands

"""
from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Command.Command import Command


class MacroCommand(Command):
    """
    As of today, it is not used.
    """

    def __init__(self):
        super().__init__()

        self.commands = None
        self.args = None
        self.clients = None

    ################################################################################

    def setCommands(self, commandsListIn=None):
        """
        Method to be called as first at every MacroCommand instantiation.

        :params:

          :attr:`commandsListIn`: a list of command objects
        """
        if not isinstance(commandsListIn, list):
            commandsListIn = [commandsListIn]

        self.commands = commandsListIn

    ################################################################################

    def setArgs(self, argsListIn=None):
        """
        Set the arguments of the commands.

        :params:

          :attr:`argsListIn`: a list of args tuples, or just a tuple of args
        """
        if not isinstance(argsListIn, list):
            argsListIn = [argsListIn]

        self.args = argsListIn
        commArgs = []

        if len(self.args) == len(self.commands):
            commArgs = [(self.commands[x], self.args[x]) for x in range(len(self.commands))]
        elif len(self.args) == 1:
            commArgs = [(self.commands[x], self.args[0]) for x in range(len(self.commands))]
        else:
            gLogger.error("Tuples or `args` provided are nor 1 nor the same number of the commands")

        for command, arg in commArgs:
            command.setArgs(arg)

    ################################################################################

    def setClient(self, clientListIn=None):
        """
        Set `self.clients`. If not set, a standard client will be instantiated.
        Then, set the clients used by the commands.

        :params:
          :attr:`clientListIn`: a list of client object
        """
        if not isinstance(clientListIn, list):
            clientListIn = [clientListIn]

        self.clients = clientListIn
        commArgs = []

        if len(self.clients) == len(self.commands):
            commArgs = [(self.commands[x], self.clients[x]) for x in range(len(self.commands))]
        elif len(self.args) == 1:
            commArgs = [(self.commands[x], self.clients[0]) for x in range(len(self.commands))]
        else:
            gLogger.error("`clients` provided are nor 1 nor the same number of the commands")

        for command, client in commArgs:
            command.setClient(client)

    ################################################################################

    def doCommand(self):
        """
        Calls command.doCommand for every command in the list of self.commands
        """

        res = []

        for command in self.commands:
            res.append(command.doCommand())
        return res

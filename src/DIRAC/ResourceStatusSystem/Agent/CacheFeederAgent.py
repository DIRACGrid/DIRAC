""" CacheFeederAgent

  This agent feeds the Cache tables with the outputs of the cache commands.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN CacheFeederAgent
  :end-before: ##END
  :dedent: 2
  :caption: CacheFeederAgent options
"""
from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient
from DIRAC.ResourceStatusSystem.Command import CommandCaller
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient

AGENT_NAME = "ResourceStatus/CacheFeederAgent"


class CacheFeederAgent(AgentModule):
    """
    The CacheFeederAgent feeds the cache tables for the client and the accounting.
    It runs periodically a set of commands, and stores it's results on the
    tables.
    """

    def __init__(self, *args, **kwargs):
        AgentModule.__init__(self, *args, **kwargs)

        self.commands = {}
        self.clients = {}

        self.cCaller = None
        self.rmClient = None

    def initialize(self):
        """Define the commands to be executed, and instantiate the clients that will be used."""

        res = ObjectLoader().loadObject("DIRAC.ResourceStatusSystem.Client.ResourceStatusClient")
        if not res["OK"]:
            self.log.error(f"Failed to load ResourceStatusClient class: {res['Message']}")
            return res
        rsClass = res["Value"]

        res = ObjectLoader().loadObject("DIRAC.ResourceStatusSystem.Client.ResourceManagementClient")
        if not res["OK"]:
            self.log.error(f"Failed to load ResourceManagementClient class: {res['Message']}")
            return res
        rmClass = res["Value"]

        self.commands["Downtime"] = [{"Downtime": {}}]
        self.commands["GOCDBSync"] = [{"GOCDBSync": {}}]
        self.commands["FreeDiskSpace"] = [{"FreeDiskSpace": {}}]

        # PilotsCommand
        self.commands["Pilot"] = [
            {"Pilot": {"element": "Site", "siteName": None}},
            {"Pilot": {"element": "Resource", "siteName": None}},
        ]

        # FIXME: do not forget about hourly vs Always ...etc
        # AccountingCacheCommand
        #    self.commands[ 'AccountingCache' ] = [
        #                                          {'SuccessfullJobsBySiteSplitted'    :{'hours' :24, 'plotType' :'Job' }},
        #                                          {'FailedJobsBySiteSplitted'         :{'hours' :24, 'plotType' :'Job' }},
        #                                          {'SuccessfullPilotsBySiteSplitted'  :{'hours' :24, 'plotType' :'Pilot' }},
        #                                          {'FailedPilotsBySiteSplitted'       :{'hours' :24, 'plotType' :'Pilot' }},
        #                                          {'SuccessfullPilotsByCESplitted'    :{'hours' :24, 'plotType' :'Pilot' }},
        #                                          {'FailedPilotsByCESplitted'         :{'hours' :24, 'plotType' :'Pilot' }},
        #                                          {'RunningJobsBySiteSplitted'        :{'hours' :24, 'plotType' :'Job' }},
        # #                                          {'RunningJobsBySiteSplitted'        :{'hours' :168, 'plotType' :'Job' }},
        # #                                          {'RunningJobsBySiteSplitted'        :{'hours' :720, 'plotType' :'Job' }},
        # #                                          {'RunningJobsBySiteSplitted'        :{'hours' :8760, 'plotType' :'Job' }},
        #                                          ]

        # VOBOXAvailability
        #    self.commands[ 'VOBOXAvailability' ] = [
        #                                            { 'VOBOXAvailability' : {} }
        #

        # Reuse clients for the commands
        self.clients["GOCDBClient"] = GOCDBClient()
        self.clients["ReportsClient"] = ReportsClient()
        self.clients["ResourceStatusClient"] = rsClass()
        self.clients["ResourceManagementClient"] = rmClass()
        self.clients["WMSAdministrator"] = WMSAdministratorClient()
        self.clients["Pilots"] = PilotManagerClient()

        self.cCaller = CommandCaller

        return S_OK()

    def loadCommand(self, commandModule, commandDict):
        """Loads and executes commands.

        :param str commandModule: Name of the command (e.g. 'Downtime')
        :param dict commandDict: dictionary of {'CommandClass':{arguments}}
        """

        commandName = list(commandDict)[0]
        commandArgs = commandDict[commandName]

        commandTuple = (f"{commandModule}Command", f"{commandName}Command")
        commandObject = self.cCaller.commandInvocation(commandTuple, pArgs=commandArgs, clients=self.clients)

        if not commandObject["OK"]:
            self.log.error(f"Error initializing {commandName}")
            return commandObject
        commandObject = commandObject["Value"]

        # Set master mode
        commandObject.masterMode = True

        self.log.info(f"{commandModule}/{commandName}")

        return S_OK(commandObject)

    def execute(self):
        """Just executes, via `loadCommand`, the commands in self.commands one after the other"""

        for commandModule, commandList in self.commands.items():
            self.log.info(f"{commandModule} module initialization")

            for commandDict in commandList:
                commandObject = self.loadCommand(commandModule, commandDict)
                if not commandObject["OK"]:
                    self.log.error(commandObject["Message"])
                    continue
                commandObject = commandObject["Value"]

                try:
                    results = commandObject.doCommand()
                    if not results["OK"]:
                        self.log.error("Failed to execute command", f"{commandModule}: {results['Message']}")
                        continue
                    results = results["Value"]
                    if not results:
                        self.log.info("Empty results")
                        continue
                    self.log.verbose("Command OK Results")
                    self.log.verbose(results)
                except Exception as excp:  # pylint: disable=broad-except
                    self.log.exception(f"Failed to execute command, with exception: {commandModule}", lException=excp)

        return S_OK()

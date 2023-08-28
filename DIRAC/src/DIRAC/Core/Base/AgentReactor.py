########################################################################
# File :   AgentReactor.py
# Author : Adria Casajus
########################################################################
"""
  DIRAC class to execute Agents

  Agents are the active part any any DIRAC system, they execute in a cyclic
  manner looking at the state of the system and reacting to it by taken
  appropriated actions

  All DIRAC Agents must inherit from the basic class AgentModule

  In the most common case, DIRAC Agents are executed using the dirac-agent command.
  dirac-agent accepts a list positional arguments. These arguments have the form:
  [DIRAC System Name]/[DIRAC Agent Name]
  dirac-agent then:
  - produces a instance of AgentReactor
  - loads the required modules using the AgentReactor.loadAgentModules method
  - starts the execution loop using the AgentReactor.go method

  Agent modules must be placed under the Agent directory of a DIRAC System.
  DIRAC Systems are called XXXSystem where XXX is the [DIRAC System Name], and
  must inherit from the base class AgentModule

"""
import time

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader
from DIRAC.Core.Utilities import ThreadScheduler


class AgentReactor:
    """
    Main interface to DIRAC Agents. It allows to :
    - define a Agents modules to be executed
    - define the number of cycles to execute
    - steer the execution

    Agents are declared via:
    - loadAgentModule(): for a single Agent
    - loadAgentModules(): for a list of Agents

    The number of cycles to execute for a defined Agent can be set via:
    - setAgentModuleCyclesToExecute()

    The execution of the Agents is done with:
    - runNumCycles(): to execute an additional number of cycles
    - go():

    During the execution of the cycles, each of the Agents can be signaled to stop
    by creating a file named "stop_agent" in its Control Directory.

    """

    def __init__(self, baseAgentName):
        self.__agentModules = {}
        self.__loader = ModuleLoader("Agent", PathFinder.getAgentSection)
        self.__tasks = {}
        self.__baseAgentName = baseAgentName
        self.__scheduler = ThreadScheduler.ThreadScheduler(enableReactorThread=False, minPeriod=10)
        self.__alive = True
        self.__running = False

    def loadAgentModules(self, modulesList, hideExceptions=False):
        """
        Load all modules required in moduleList
        """
        result = self.__loader.loadModules(modulesList, hideExceptions=hideExceptions)
        if not result["OK"]:
            return result
        self.__agentModules = self.__loader.getModules()
        for agentName in self.__agentModules:
            agentData = self.__agentModules[agentName]
            agentData["running"] = False
            try:
                instanceObj = agentData["classObj"](agentName, agentData["loadName"], self.__baseAgentName)
                result = instanceObj.am_initialize()
                if not result["OK"]:
                    return S_ERROR(f"Error while calling initialize method of {agentName}: {result['Message']}")
                agentData["instanceObj"] = instanceObj
            except Exception as excp:
                if not hideExceptions:
                    gLogger.exception(f"Can't load agent {agentName}", lException=excp)
                return S_ERROR(f"Can't load agent {agentName}: \n {excp}")
            agentPeriod = instanceObj.am_getPollingTime()
            result = self.__scheduler.addPeriodicTask(
                agentPeriod, instanceObj.am_go, executions=instanceObj.am_getMaxCycles(), elapsedTime=agentPeriod
            )
            if not result["OK"]:
                return result

            taskId = result["Value"]
            self.__tasks[result["Value"]] = agentName
            agentData["taskId"] = taskId
            agentData["running"] = True

        if not self.__agentModules:
            return S_ERROR("No agent module loaded")

        return S_OK()

    def runNumCycles(self, agentName=None, numCycles=1):
        """
        Run all defined agents a given number of cycles
        """
        if agentName:
            self.loadAgentModules([agentName])
        error = ""
        for aName in self.__agentModules:
            result = self.setAgentModuleCyclesToExecute(aName, numCycles)
            if not result["OK"]:
                error = "Failed to set cycles to execute"
                gLogger.error(f"{error}:", aName)
                break
        if error:
            return S_ERROR(error)
        self.go()
        return S_OK()

    def __finalize(self):
        """
        Execute the finalize method of all Agents
        """
        for agentName in self.__agentModules:
            try:
                self.__agentModules[agentName]["instanceObj"].finalize()
            except Exception as excp:
                gLogger.exception(f"Failed to execute finalize for Agent: {agentName}", lException=excp)

    def go(self):
        """
        Main method to control the execution of all configured Agents
        """
        if self.__running:
            return
        self.__running = True
        try:
            while self.__alive:
                self.__checkControlDir()
                timeToNext = self.__scheduler.executeNextTask()
                if timeToNext is None:
                    gLogger.info("No more agent modules to execute. Exiting")
                    break
                time.sleep(min(max(timeToNext, 0.5), 5))
        finally:
            self.__running = False
        self.__finalize()

    def setAgentModuleCyclesToExecute(self, agentName, maxCycles=1):
        """
        Set number of cycles to execute for a given agent (previously defined)
        """
        if agentName not in self.__agentModules:
            return S_ERROR(f"{agentName} has not been loaded")
        if maxCycles:
            try:
                maxCycles += self.__agentModules[agentName]["instanceObj"].am_getCyclesDone()
            except Exception as excp:
                error = "Can not determine number of cycles to execute"
                gLogger.exception(f"{error}: '{maxCycles}'", lException=excp)
                return S_ERROR(error)
        self.__agentModules[agentName]["instanceObj"].am_setOption("MaxCycles", maxCycles)
        self.__scheduler.setNumExecutionsForTask(self.__agentModules[agentName]["taskId"], maxCycles)
        return S_OK()

    def __checkControlDir(self):
        """
        Check for the presence of stop_agent file to stop execution of the corresponding Agent
        """
        for agentName in self.__agentModules:
            if not self.__agentModules[agentName]["running"]:
                continue
            agent = self.__agentModules[agentName]["instanceObj"]

            alive = agent.am_getModuleParam("alive")
            if alive:
                if agent.am_checkStopAgentFile():
                    gLogger.info(f"Found StopAgent file for agent {agentName}")
                    alive = False

            if not alive:
                gLogger.info(f"Stopping agent module {agentName}")
                self.__scheduler.removeTask(self.__agentModules[agentName]["taskId"])
                del self.__tasks[self.__agentModules[agentName]["taskId"]]
                self.__agentModules[agentName]["running"] = False
                agent.am_removeStopAgentFile()

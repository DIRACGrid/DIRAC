""" BaseAction

  Base class for Actions.

"""
from DIRAC import gLogger


class BaseAction:
    """
    Base class for all actions. It defines a constructor an a run main method.
    """

    def __init__(self, name, decisionParams, enforcementResult, singlePolicyResults, clients):

        # enforcementResult supposed to look like:
        # {
        #   'Status'        : <str>,
        #   'Reason'        : <str>,
        #   'PolicyActions' : <list>,
        #   [ 'EndDate' : <str> ]
        # }

        # decisionParams supposed to look like:
        # {
        #   'element'     : None,
        #   'name'        : None,
        #   'elementType' : None,
        #   'statusType'  : None,
        #   'status'      : None,
        #   'reason'      : None,
        #   'tokenOwner'  : None
        # }

        self.actionName = name  # 'BaseAction'
        self.decisionParams = decisionParams
        self.enforcementResult = enforcementResult
        self.singlePolicyResults = singlePolicyResults
        self.clients = clients
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.log.verbose("Running %s action" % self.__class__.__name__)

    def run(self):
        """
        Method to be over written by the real actions
        """

        self.log.warn("%s: you may want to overwrite this method" % self.actionName)

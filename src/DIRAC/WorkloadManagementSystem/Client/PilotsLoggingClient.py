""" Class that contains client access to the PilotsLogging handler. """

from DIRAC.Core.Base.Client import Client, createClient


@createClient("WorkloadManagement/PilotsLogging")
class PilotsLoggingClient(Client):
    """Implementation of interface of Pilots Logging service. Client class should be used to communicate
    with PilotsLogging Service
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setServer("WorkloadManagement/PilotsLogging")

    def addPilotsLogging(self, pilotUUID, timestamp, source, phase, status, messageContent):
        """
        Add new Pilots Logging entry

        :param pilotUUID: Pilot reference
        :param status: Pilot status
        :param minorStatus: Additional status information
        :param timestamp: Date and time of status event
        :param source: Source of status information
        """

        return self._getRPC().addPilotsLogging(pilotUUID, timestamp, source, phase, status, messageContent)

    def deletePilotsLogging(self, pilotUUID):
        """
        Delete all Logging entries for Pilot

        :param pilotUUID: Pilot reference
        """

        return self._getRPC().deletePilotsLogging(pilotUUID)

    def getPilotsLogging(self, pilotUUID):
        """
        Get all Logging entries for Pilot

        :param pilotUUID: Pilot reference
        """

        return self._getRPC().getPilotsLogging(pilotUUID)

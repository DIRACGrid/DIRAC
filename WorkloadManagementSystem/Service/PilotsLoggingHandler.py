""" PilotsLoggingHandler is the implementation of the PilotsLogging service

    The following methods are available in the Service interface

    addPilotsLogging()
    getPilotsLogging
    setPilotsUUIDtoIDMapping()
    addPilotsUUID()
    deletePilotsLogging()

"""

__RCSID__ = "$Id: $"

from DIRAC.WorkloadManagementSystem.DB.PilotsLoggingDB import PilotsLoggingDB
from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import S_OK


class PilotsLoggingHandler( RequestHandler ):
  """Server side functions for Pilots Logging service"""

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    """Initialization of Pilots Logging service
    """
    cls.consumersSet = set()
    cls.pilotsLoggingDB = PilotsLoggingDB()

    result = createConsumer( cls.srv_getCSOption("PilotsLoggingQueue"), callback = cls.consumingCallback )
    if result['OK']:
      cls.consumersSet.add(result)
    else:
      return result
    return S_OK()

  def initialize(self):
    """Initialization of Pilots Logging service
    """
    return S_OK()

  @classmethod
  def consumingCallback(cls, headers, message ):
    """
    Callback function for the MQ Consumer, called for every new message and inserting it into database.

    :param headers: Headers of MQ message (not used)
    :param message: Message represented as a dictionary
    """
    # verify received message format
    if set(message) == set(['pilotUUID', 'timestamp', 'source', 'phase', 'status', 'messageContent']):
      cls.pilotsLoggingDB.addPilotsLogging(message['pilotUUID'], message['timestamp'], message['source'],
                                           message['phase'], message['status'], message['messageContent'])

  types_addPilotsLogging = [ basestring, basestring, basestring, basestring, basestring, basestring ]
  def export_addPilotsLogging( self, pilotUUID, timestamp, source, phase, status, messageContent ):
    """
    Add new Pilots Logging entry

    :param pilotUUID: Pilot reference
    :param status: Pilot status
    :param minorStatus: Additional status information
    :param timeStamp: Date and time of status event
    :param source: Source of statu information
    """

    return PilotsLoggingHandler.pilotsLoggingDB.addPilotsLogging(pilotUUID, timestamp, source, phase, status,
                                                                 messageContent)

  types_getPilotsLogging = [ basestring ]
  def export_getPilotsLogging( self, pilotUUID ):
    """
    Get all Logging entries for Pilot

    :param pilotUUID: Pilot reference
    """

    return PilotsLoggingHandler.pilotsLoggingDB.getPilotsLogging( pilotUUID )

  types_deletePilotsLogging = [ [basestring,  list] ]
  def export_deletePilotsLogging( self, pilotUUID ):
    """
    Delete all Logging entries for Pilot

    :param pilotUUID: Pilot reference
    """

    return PilotsLoggingHandler.pilotsLoggingDB.deletePilotsLogging( pilotUUID )

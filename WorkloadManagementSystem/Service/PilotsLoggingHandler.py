""" PilotsLoggingHandler is the implementation of the PilotsLogging service

    The following methods are available in the Service interface

    addPilotsLogging()
    getPilotsLogging
    deletePilotsLogging()

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler

from DIRAC.WorkloadManagementSystem.DB.PilotsLoggingDB import PilotsLoggingDB
from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer


class PilotsLoggingHandler(RequestHandler):
  """Server side functions for Pilots Logging service"""

  consumerSet = None
  pilotsLoggingDB = None

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """Initialization of Pilots Logging service
    """
    cls.consumersSet = set()
    cls.pilotsLoggingDB = PilotsLoggingDB()
    queue = cls.srv_getCSOption("PilotsLoggingQueue")
    # This is pretty awful hack. Somehow, for uknown reason, I cannot access CS with srv_getCSOption.
    # The only way is using full CS path, so I'm using it as a backup solution.
    if not queue:
      queue = gConfig.getValue(serviceInfoDict['serviceSectionPath'] + "/PilotsLoggingQueue")
    result = createConsumer(queue, callback=cls.consumingCallback)
    if result['OK']:
      cls.consumersSet.add(result['Value'])
    else:
      return result
    return S_OK()

  @classmethod
  def consumingCallback(cls, headers, message):
    """
    Callback function for the MQ Consumer, called for every new message and inserting it into database.

    :param headers: Headers of MQ message (not used)
    :param message: Message represented as a dictionary
    """
    # verify received message format
    if set(message) == set(['pilotUUID', 'timestamp', 'source', 'phase', 'status', 'messageContent']):
      cls.pilotsLoggingDB.addPilotsLogging(message['pilotUUID'], message['timestamp'], message['source'],
                                           message['phase'], message['status'], message['messageContent'])

  types_addPilotsLogging = [basestring, basestring, basestring, basestring, basestring, basestring]

  def export_addPilotsLogging(self, pilotUUID, timestamp, source, phase, status, messageContent):
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

  types_getPilotsLogging = [basestring]

  def export_getPilotsLogging(self, pilotUUID):
    """
    Get all Logging entries for Pilot

    :param pilotUUID: Pilot reference
    """

    return PilotsLoggingHandler.pilotsLoggingDB.getPilotsLogging(pilotUUID)

  types_deletePilotsLogging = [(basestring, list)]

  def export_deletePilotsLogging(self, pilotUUID):
    """
    Delete all Logging entries for Pilot

    :param pilotUUID: Pilot reference
    """

    return PilotsLoggingHandler.pilotsLoggingDB.deletePilotsLogging(pilotUUID)

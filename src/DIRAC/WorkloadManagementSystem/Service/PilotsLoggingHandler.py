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

import six
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.DISET.RequestHandler import RequestHandler

from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer


class PilotsLoggingHandler(RequestHandler):
  """Server side functions for Pilots Logging service"""

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """Initialization of Pilots Logging service
    """
    cls.consumersSet = set()
    try:
      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotsLoggingDB", "PilotsLoggingDB")
      if not result['OK']:
        return result
      cls.pilotsLoggingDB = result['Value']()

    except RuntimeError as excp:
      return S_ERROR("Can't connect to DB: %s" % excp)

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
      cls.pilotsLoggingDB.addPilotsLogging(
          message['pilotUUID'], message['timestamp'], message['source'],
          message['phase'], message['status'], message['messageContent'])

  types_addPilotsLogging = [six.string_types, six.string_types, six.string_types,
                            six.string_types, six.string_types, six.string_types]

  @classmethod
  def export_addPilotsLogging(cls, pilotUUID, timestamp, source, phase, status, messageContent):
    """
    Add new Pilots Logging entry

    :param pilotUUID: Pilot reference
    :param status: Pilot status
    :param minorStatus: Additional status information
    :param timeStamp: Date and time of status event
    :param source: Source of statu information
    """

    return cls.pilotsLoggingDB.addPilotsLogging(
        pilotUUID, timestamp, source, phase, status, messageContent)

  types_getPilotsLogging = [six.string_types]

  @classmethod
  def export_getPilotsLogging(cls, pilotUUID):
    """
    Get all Logging entries for Pilot

    :param pilotUUID: Pilot reference
    """

    return cls.pilotsLoggingDB.getPilotsLogging(pilotUUID)

  types_deletePilotsLogging = [six.string_types + (list,)]

  @classmethod
  def export_deletePilotsLogging(cls, pilotUUID):
    """
    Delete all Logging entries for Pilot

    :param pilotUUID: Pilot reference
    """

    return cls.pilotsLoggingDB.deletePilotsLogging(pilotUUID)

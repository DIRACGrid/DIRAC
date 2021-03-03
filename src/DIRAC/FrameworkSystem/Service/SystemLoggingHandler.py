"""
SystemLoggingHandler is the implementation of the Logging service
in the DISET framework.

The following methods are available in the Service interface::

    addMessages()

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.FrameworkSystem.private.standardLogging.Message import tupleToMessage
from DIRAC.FrameworkSystem.DB.SystemLoggingDB import SystemLoggingDB


# This is a global instance of the SystemLoggingDB class
gLogDB = False


def initializeSystemLoggingHandler(serviceInfo):
  """ Check that we can connect to the DB and that the tables are properly created or updated

      :param dict serviceInfo: service information dictionary

      :return: S_OK()/S_ERROR()
  """
  global gLogDB
  gLogDB = SystemLoggingDB()
  res = gLogDB._connect()
  if not res['OK']:
    return res

  return S_OK()


class SystemLoggingHandler(RequestHandler):
  """ This is server
  """

  def __addMessage(self, messageObject, site, nodeFQDN):
    """ This is the function that actually adds the Message to the log Database

        :param messageObject: message object
        :param str site: site name
        :param str nodeFQDN: nodeFQDN

        :return: S_OK()/S_ERROR()
    """
    credentials = self.getRemoteCredentials()
    userDN = credentials.get('DN', 'unknown')
    userGroup = credentials.get('group', 'unknown')

    remoteAddress = self.getRemoteAddress()[0]
    return gLogDB.insertMessage(messageObject, site, nodeFQDN, userDN, userGroup, remoteAddress)

  types_addMessages = [list, six.string_types, six.string_types]

  def export_addMessages(self, messagesList, site, nodeFQDN):
    """ This is the interface to the service

        :param list messagesList: contains a list of Message Objects.
        :param str site: site name
        :param str nodeFQDN: nodeFQDN

        :return: S_OK()/S_ERROR() -- S_ERROR if an exception was raised
    """
    for messageTuple in messagesList:
      messageObject = tupleToMessage(messageTuple)
      result = self.__addMessage(messageObject, site, nodeFQDN)
      if not result['OK']:
        gLogger.error('The Log Message could not be inserted into the DB',
                      'because: "%s"' % result['Message'])
        return S_ERROR(result['Message'])
    return S_OK()

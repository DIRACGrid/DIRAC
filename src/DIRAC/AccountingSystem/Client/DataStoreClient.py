""" Module that holds the DataStore Client class
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import time
import random
import copy
import threading

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Core.Utilities.DErrno import ERMSUKN


@createClient("Accounting/DataStore")
class DataStoreClient(Client):
  """
    Class providing front end access to DIRAC Accounting DataStore Service
     - It allows to reduce the interactions with the server by building and list of
    pending Registers to be sent that are sent in a bundle using the commit method.
     - In case the DataStore is down Registers are sent as DISET requests.
  """

  def __init__(self, retryGraceTime=0, **kwargs):
    """ Simple constructor

        :params int retryGraceTime: the seconds to wait before sending records to the failover
    """
    super(DataStoreClient, self).__init__(**kwargs)
    self.setServer('Accounting/DataStore')

    self.__maxRecordsInABundle = 5000
    self.__registersList = []
    self.__maxTimeRetrying = retryGraceTime
    self.__lastSuccessfulCommit = time.time()
    self.__failoverEnabled = not gConfig.getValue('/LocalSite/DisableFailover', False)
    self.__registersListLock = threading.RLock()
    self.__commitTimer = threading.Timer(5, self.commit)

  def __checkBaseType(self, obj):
    """ Check to find that the class inherits from the Base Type
    """
    for parent in obj.__bases__:
      if parent.__name__ == "BaseAccountingType":
        return True
      if self.__checkBaseType(parent):
        return True
    return False

  def addRegister(self, register):
    """ Add a register to the list to be sent

        :params BaseAccountingType register: the accounting record to send.
    """
    if not self.__checkBaseType(register.__class__):
      return S_ERROR("register is not a valid type (has to inherit from BaseAccountingType")
    retVal = register.checkValues()
    if not retVal['OK']:
      return retVal
    if gConfig.getValue('/LocalSite/DisableAccounting', False):
      return S_OK()

    self.__registersList.append(copy.deepcopy(list(register.getValues())))

    return S_OK()

  def disableFailover(self):
    self.__failoverEnabled = False

  def commit(self):
    """
    Send the registers in a bundle mode
    """
    rpcClient = self._getRPC()
    sent = 0

    # create a local reference and prevent other running commits
    # to take the same data second time
    self.__registersListLock.acquire()
    registersList = self.__registersList
    self.__registersList = []
    self.__registersListLock.release()

    try:
      while registersList:
        registersToSend = registersList[:self.__maxRecordsInABundle]
        retVal = rpcClient.commitRegisters(registersToSend)
        if retVal['OK']:
          self.__lastSuccessfulCommit = time.time()
        else:
          if self.__failoverEnabled and time.time() - self.__lastSuccessfulCommit > self.__maxTimeRetrying:
            gLogger.warn('Error sending accounting record', retVal['Message'])
            gLogger.verbose("Sending accounting records to failover")
            result = _sendToFailover(retVal['rpcStub'])
            if not result['OK']:
              return result
            gLogger.debug("Sent accounting record to failover")
          else:
            gLogger.error('Error sending accounting record', retVal['Message'])
            return S_ERROR("Cannot commit data to DataStore service")
        sent += len(registersToSend)
        del registersList[:self.__maxRecordsInABundle]
    except Exception as e:  # pylint: disable=broad-except
      gLogger.exception("Error committing", lException=e)
      return S_ERROR("Error committing %s" % repr(e).replace(',)', ')'))
    finally:
      # if something is left because of an error return it to the main list
      self.__registersList.extend(registersList)

    return S_OK(sent)

  def delayedCommit(self):
    """
    If needed start a timer that will run the commit later
    allowing to send more registers at once (reduces overheads).
    """

    if not self.__commitTimer.is_alive():
      self.__commitTimer = threading.Timer(5, self.commit)
      self.__commitTimer.start()

    return S_OK()

  def remove(self, register):
    """
    Remove a Register from the Accounting DataStore
    """
    if not self.__checkBaseType(register.__class__):
      return S_ERROR("register is not a valid type (has to inherit from BaseAccountingType")
    retVal = register.checkValues()
    if not retVal['OK']:
      return retVal
    if gConfig.getValue('/LocalSite/DisableAccounting', False):
      return S_OK()
    return self._getRPC().remove(*register.getValues())


def _sendToFailover(rpcStub):
  """ Create a ForwardDISET operation for failover
  """
  try:
    request = Request()
    request.RequestName = "Accounting.DataStore.%s.%s" % (time.time(), random.random())
    forwardDISETOp = Operation()
    forwardDISETOp.Type = "ForwardDISET"
    forwardDISETOp.Arguments = DEncode.encode(rpcStub)
    request.addOperation(forwardDISETOp)

    return ReqClient().putRequest(request)

  # We catch all the exceptions, because it should never crash
  except Exception as e:  # pylint: disable=broad-except
    return S_ERROR(ERMSUKN, "Exception sending accounting failover request: %s" % repr(e))


gDataStoreClient = DataStoreClient()

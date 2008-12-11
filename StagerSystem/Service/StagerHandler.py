########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Service/StagerHandler.py,v 1.14 2008/12/11 16:08:21 acsmith Exp $
########################################################################

"""
    StagerHandler is the implementation of the StagerDB in the DISET framework
"""

__RCSID__ = "$Id: StagerHandler.py,v 1.14 2008/12/11 16:08:21 acsmith Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.StagerSystem.DB.StagerDB import StagerDB

# This is a global instance of the StagerDB class
stagerDB = False

def initializeStagerHandler(serviceInfo):
  global stagerDB
  stagerDB = StagerDB()
  return S_OK()

class StagerHandler(RequestHandler):

  types_setRequest = [ListType,StringType,StringType,StringType,IntType]
  def export_setRequest(self,lfns,storageElement,source,callbackMethod,taskID):
    """
        This method allows stage requests to be set into the StagerDB
    """
    try:
      res = stagerDB.setRequest(lfns,storageElement,source,callbackMethod,taskID)
      if res['OK']:
        gLogger.info('StagerHandler.setRequest: Successfully set stage request')
      else:
        gLogger.error('StagerHandler.setRequest: Failed to set stage request',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.setRequest: Exception when setting request'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_getFilesWithStatus = [StringType]
  def export_getFilesWithStatus(self,status):
    """
        This method allows to retrieve files with the supplied status
    """
    try:
      res = stagerDB.getFilesWithStatus(status)
      if res['OK']:
        gLogger.info('StagerHandler.getFilesWithStatus: Successfully get files with %s status' % status)
      else:
        gLogger.error('StagerHandler.getFilesWithStatus: Failed to get files with %s status' % status,res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.getFilesWithStatus: Exception when getting files with %s status' % status
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_updateFilesStatus = [ListType,StringType]
  def export_updateFilesStatus(self,fileIDs,status):
    """
        This method sets the status for the supplied files
    """
    try:
      res = stagerDB.updateFilesStatus(fileIDs,status)
      if res['OK']:
        gLogger.info('StagerHandler.updateFilesStatus: Successfully updated files to %s status' % status)
      else:
        gLogger.error('StagerHandler.updateFilesStatus: Failed to update files to %s status' % status,res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.updateFilesStatus: Exception when updating files to %s status' % status
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_updateFileInformation = [ListType]
  def export_updateFileInformation(self,fileTuples):
    """
        This method sets the pfn and size for the supplied files
    """
    try:
      res = stagerDB.updateFileInformation(fileTuples)
      if res['OK']:
        gLogger.info('StagerHandler.updateFileInformation: Successfully updated file information')
      else:
        gLogger.error('StagerHandler.updateFileInformation: Failed to update file information',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.updateFileInformation: Exception when updating file information'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_insertStageRequests = [ListType,[IntType,LongType]]
  def export_insertStageRequests(self,fileIDs,requestID):
    """
        This method inserts stage requests for the supplied files
    """
    try:
      res = stagerDB.insertStageRequests(fileIDs,requestID)
      if res['OK']:
        gLogger.info('StagerHandler.insertStageRequests: Successfully inserted stage requests')
      else:
        gLogger.error('StagerHandler.insertStageRequests: Failed to insert stage requests',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.insertStageRequests: Exception when inserting stage requests'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

  types_insertPins = [ListType,IntType,IntType]
  def export_insertPins(self,fileIDs,requestID,pinLifeTime):
    """
        This method inserts pins for the supplied files
    """
    try:
      res = stagerDB.insertPins(fileIDs,requestID,pinLifeTime)
      if res['OK']:
        gLogger.info('StagerHandler.insertPins: Successfully inserted pins information')
      else:
        gLogger.error('StagerHandler.insertPins: Failed to insert pin information',res['Message'])
      return res
    except Exception,x:
      errMsg = 'StagerHandler.insertPins: Exception when inserting pin information'
      gLogger.exception(errMsg,'',x)
      return S_ERROR(errMsg)

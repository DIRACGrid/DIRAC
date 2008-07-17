# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/RequestClient.py,v 1.5 2008/07/17 15:14:14 acsmith Exp $

"""
  This is the client implementation for the RequestDB using the DISET framework.
"""

__RCSID__ = "$Id: RequestClient.py,v 1.5 2008/07/17 15:14:14 acsmith Exp $"

from types import *
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import randomize, fromChar
from DIRAC.ConfigurationSystem.Client import PathFinder

class RequestClient:

  def __init__(self,useCertificates = False):
    """ Constructor of the RequestClient class
    """
    voBoxUrls = fromChar(PathFinder.getServiceURL("RequestManagement/voBoxURLs"))
    self.voBoxUrls = []
    if voBoxUrls:
      self.voBoxUrls = randomize(voBoxUrls.split(','))

    local = PathFinder.getServiceURL("RequestManagement/localURL")
    self.local = False
    if local:
      self.local = local

    central = PathFinder.getServiceURL("RequestManagement/centralURL")
    self.central = False
    if central:
      self.central = central

  ########################################################################
  #
  # These are the methods operating on existing requests and have fixed URLs
  #

  def updateRequest(self,requestName,requestString,url):
    """ Update the request at the supplied url
    """
    try:
      gLogger.info("RequestDBClient.updateRequest: Attempting to update %s at %s." % (requestName,url))
      requestRPCClient = RPCClient(url)
      res = requestRPCClient.updateRequest(requestName,requestString)
      return res
    except Exception,x:
      errStr = "Request.updateRequest: Exception while updating request."
      gLogger.exception(errStr,requestName,str(x))
      return S_ERROR(errStr)

  def deleteRequest(self,requestName,url):
    """ Delete the request at the supplied url
    """
    try:
      gLogger.info("RequestDBClient.deleteRequest: Attempting to delete %s at %s." % (requestName,url))
      requestRPCClient = RPCClient(url)
      res = requestRPCClient.deleteRequest(requestName)
      return res
    except Exception,x:
      errStr = "Request.deleteRequest: Exception while deleting request."
      gLogger.exception(errStr,requestName,str(x))
      return S_ERROR(errStr)

  def setRequestStatus(self,requestName,requestStatus,url):
    """ Set the status of a request
    """
    try:
      gLogger.info("RequestDBClient.setRequestStatus: Attempting to set %s to %s." % (requestName,requestStatus))
      requestRPCClient = RPCClient(url)
      res = requestRPCClient.setRequestStatus(requestName,requestStatus)
      return res
    except Exception,x:
      errStr = "Request.setRequestStatus: Exception while setting request status."
      gLogger.exception(errStr,requestName,str(x))
      return S_ERROR(errStr)

  ##############################################################################
  #
  # These are the methods which require URL manipulation
  #

  def setRequest(self,requestName,requestString,url=''):
    """ Set request. URL can be supplied if not a all VOBOXes will be tried in random order.
    """
    try:
      if not url:
        urls = self.voBoxUrls
      else:
        urls = [url]
      for url in urls:
        requestRPCClient = RPCClient(url)
        res = requestRPCClient.setRequest(requestName,requestString)
        if res['OK']:
          gLogger.info("Succeded setting request  %s at %s" % (requestName,url))
          res["Server"] = url
          return res
        else:
          errKey = "Failed setting request at %s" % url
          errExpl = " : for %s because: %s" % (requestName,res['Message'])
          gLogger.error(errKey,errExpl)
      errKey = "Completely failed setting request"
      errExpl = " : %s\n%s" % (requestName,requestString)
      gLogger.fatal(errKey,errExpl)
      return S_ERROR(errKey)
    except Exception,x:
      errKey = "Completely failed setting request"
      gLogger.exception(errKey,requestName,str(x))
      return S_ERROR(errKey)

  def getRequest(self,requestType,url=''):
    """ Get request from RequestDB.
        First try the local repository then if none available or error try random repository
    """
    try:
      if not url:
        urls = self.voBoxUrls
      else:
        urls = [url]
      for url in urls:
        gLogger.info("RequestDBClient.getRequest: Attempting to get request.", "%s %s" % (url,requestType))
        requestRPCClient = RPCClient(url)
        res = requestRPCClient.getRequest(requestType)
        if res['OK']:
          if res['Value']:
            gLogger.info("Got '%s' request from RequestDB (%s)" % (requestType,url))
            res['Value']['Server'] = url
            return res
          else:
            gLogger.info("Found no '%s' requests on RequestDB (%s)" % (requestType,url))
        else:
          errKey = "Failed getting request from %s" % url
          errExpl = " : %s : %s" % (requestType,res['Message'])
          gLogger.error(errKey,errExpl)
      return res
    except Exception,x:
      errKey = "Failed to get request"
      gLogger.exception(errKey,'',str(x))
      return S_ERROR(errKey)

  def serveRequest(self,requestType='',url=''):
    """ Get a request from RequestDB.
    """
    try:
      if not url:
        urls = self.voBoxUrls
      else:
        urls = [url]
      for url in urls:
        gLogger.info("RequestDBClient.serveRequest: Attempting to obtain request.", "%s %s" % (url,requestType))
        requestRPCClient = RPCClient(url)
        res = requestRPCClient.serveRequest(requestType)
        if res['OK']:
          if res['Value']:
            gLogger.info("Got '%s' request from RequestDB (%s)" % (requestType,url))
            res['Value']['Server'] = url
            return res
          else:
            gLogger.info("Found no '%s' requests on RequestDB (%s)" % (requestType,url))
        else:
          errKey = "Failed getting request from %s" % url
          errExpl = " : %s : %s" % (requestType,res['Message'])
          gLogger.error(errKey,errExpl)
      return res
    except Exception,x:
      errKey = "Failed to get request"
      gLogger.exception(errKey,'',str(x))
      return S_ERROR(errKey)

  def getDBSummary(self,url=''):
    """ Get the summary of requests in the RequestDBs. If a URL is not supplied will get status for all.
    """
    try:
      if not url:
        urls = self.voBoxUrls
      else:
        urls = [url]
      urlDict = {}
      for url in urls:
        requestRPCClient = RPCClient(url)
        urlDict[url] = {}
        result = requestRPCClient.getDBSummary()
        if result['OK']:
          gLogger.info("Succeded getting request summary at %s" % url)
          urlDict[url] = result['Value']
        else:
          errKey = "Failed getting request summary"
          errExpl = " : at %s because %s" % (url,result['Message'])
          gLogger.error(errKey,errExpl)
      return S_OK(urlDict)
    except Exception,x:
      errKey = "Failed getting request summary"
      gLogger.exception(errKey,'',str(x))
      return S_ERROR(errKey)

"""
  This is the client implementation for the RequestDB using the DISET framework.
  A.Smith 23/5/07

  :deprecated:
"""

from types import *
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import randomize

class RequestClient:

  def __init__(self, useCertificates = False):
    """ Constructor of the RequestClient class
    """
    self.localUrl = gConfig.getValue('/Systems/DataManagement/Development/Services/RequestDB/localURL')
    self.centralUrl = gConfig.getValue('/Systems/DataManagement/Development/Services/RequestDB/centralURL')
    voBoxUrls = gConfig.getValue('/Systems/DataManagement/Development/Services/RequestDB/voBoxURLs')
    self.voBoxUrls = randomize(voBoxUrls).remove(self.localUrl)

  def setRequest(self,requestType,requestName,requestString,requestStatus='ToDo',url=''):
    """ Set request. URL can be supplied if not a all VOBOXes will be tried in random order.
    """
    try:
      urls = []
      if url:
        urls[url]
      urls.append(self.voBoxUrls)
      for url in urls:
        requestRPCClient = RPCClient(url)
        res = requestRPCClient.setRequest(requestType,requestName,requestStatus,requestString)
        if res['OK']:
          gLogger.info("Succeded setting request for %s at %s" % (requestName,url))
          res["Server"] = url
          return res
        else:
          errKey = "Failed setting request at %s" % url
          errExpl = " : for %s because: %s" % (requestName,res['Message'])
          gLogger.error(errKey,errExpl)
      errKey = "Completely failed setting request"
      errExpl = " : %s\n%s\n%s" % (requestName,requestType,requestString)
      gLogger.fatal(errKey,errExpl)
      return S_ERROR(errKey)
    except Exception,x:
      errKey = "Completely failed setting request"
      errExpl = " : for %s with exception %s" % (requestName,str(x))
      gLogger.exception(errKey,errExpl)
      return S_ERROR(errKey)

  def setRequestStatus(self,requestType,requestName,status,url=''):
    """ Set the status of a request
    """
    #Must know the URL of the VOBox where this request resides.
    #This allows VOBoxes to request work from others and here update their status.
    if not url:
      return S_ERROR('No URL Supplied')
    requestRPCClient = RPCClient(url,timeout=120)
    try:
      gLogger.info("Setting the status for %s to '%s' at %s" % (requestName,status,url))
      res = requestRPCClient.setRequestStatus(requestType,requestName,status)
      if res['OK']:
        gLogger.info("Succeded setting the status for "+requestName+" to '"+status+"'")
      else:
        errKey = "Failed setting request status at %s" % url
        errExpl = " : for %s to %s because: %s" % (requestName,status,res['Message'])
        gLogger.error(errKey,errExpl)
        res['Message'] = errKey+errExpl
      return res
    except Exception,x:
      errKey = "Failed setting request status at %s" % url
      errExpl = " : for %s with exception %s" % (requestName,str(x))
      gLogger.exception(errKey,errExpl)
      return S_ERROR(errKey+errExpl)

  def getRequest(self,requestType,status):
    """ Get request from RequestDB.
        First try the local repository then if none available or error try random repository
    """
    try:
      #Create list with two RequestDB URLs to try
      url = self.localUrl
      urls = [url]
      urls.append(self.voBoxUrls.pop())

      for url in urls:
        requestRPCClient = RPCClient(url,timeout=120)
        res = requestRPCClient.getRequest(requestType,status)
        if res['OK']:
          if res['Request']:
            gLogger.info("Got '%s' request from RequestDB (%s) with status '%s'" % (requestType,url,status))
            res['Server'] = url
            return res
          else:
            gLogger.info("Found no '%s' requests on RequestDB (%s) with status '%s'" % (requestType,url,status))
        else:
          errKey = "Failed getting request from %s" % url
          errExpl = " : %s of %s because: %s" % (requestType,status,res['Message'])
          gLogger.error(errKey,errExpl)
      return res
    except Exception,x:
      errKey = "Failed to get request"
      errExpl = " : %s" %str(x)
      gLogger.exception(errKey,errExpl)
      return S_ERROR(errKey+errExpl)


  def getRequestSummary(self,url=''):
    """ Get the summary of requests in the RequestDBs. If a URL is not supplied will get status for all.
    """
    try:
      if url:
        urls = [url]
      else:
        urls = self.voBoxUrls
      res = S_OK()
      for url in urls:
        requestRPCClient = RPCClient(url,timeout=120)
        res['Value'][url] = {}
        result = requestRPCClient.getRequestSummary()
        if result['OK']:
          gLogger.info("Succeded getting request summary at %s" % url)
          res['Value'][url] = result['Value']
        else:
          errKey = "Failed getting request summary"
          errExpl = " : at %s because %s" % (url,result['Message'])
          gLogger.error(errKey,errExpl)
      return res
    except Exception,x:
      errKey = "Failed getting request summary"
      errExpl = " : with exception %s" % str(x)
      gLogger.exception(errKey,errExpl)
      return S_ERROR(errKey+errExpl)

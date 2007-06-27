########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/SandboxDB.py,v 1.2 2007/06/27 12:36:17 atsareg Exp $
########################################################################
""" SandboxDB class is a simple storage using MySQL as a container for
    relatively small sandbox files. The file size is limited to 16MB.
    The following methods are provided

    addLoggingRecord()
    getJobLoggingInfo()
    getWMSTimeStamps()    
"""    

__RCSID__ = "$Id: SandboxDB.py,v 1.2 2007/06/27 12:36:17 atsareg Exp $"

import re, os, sys
import time, datetime
from types import *

from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Base.DB import DB  

#############################################################################
class SandboxDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """

    DB.__init__(self,'ISandboxDB','WorkloadManagement/SandboxDB',maxQueueSize)

#############################################################################
  def storeSandboxFile(self,jobID,filename,body,sandbox='in'):  
    """ Store input sandbox ASCII file for jobID with the name filename which
        is given with its string body 
    """
    
    if len(body) > 16*1024*1024:
      return S_ERROR('File size too large %.2f MB for file %s' % \
                     (len(body)/1024./1024.,filename))
    
    prefix = "O"
    if sandbox == 'in':
      prefix = "I"
      
    # Check that the file does not exist already
    req = "SELECT FileName FROM %sSandbox WHERE JobID=%d AND FileName='%s'" % \
          (prefix,int(jobID),filename)
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) > 0:
      # Remove the already existing file - overwrite
      self.gLogger.warn('Overwriting file %s for job %d' % (filename,int(jobID)))
      req = "DELETE FROM %sSandbox WHERE JobID=%d AND FileName='%s'" % \
            (prefix,int(jobID),filename)    
      result = self._update(req)       
      if not result['OK']:
        return result           
    
    body = body.replace("\\","\\\\")
    body = body.replace('\0','\\0')            
    body = body.replace("'","\\'")
    body = body.replace('"','\\"')  
    req = "INSERT INTO %sSandbox(JobId,FileName,FileBody) VALUES (%s,'%s','%s')" % \
          (prefix,jobID,filename,body)
    return self._update(req) 

#############################################################################
  def getSandboxFile(self,jobID,filename,sandbox='in'):  
    """ Store input sandbox ASCII file for jobID with the name filename which
        is given with its string body 
    """
    
    prefix = "O"
    if sandbox == 'in':
      prefix = "I"
    
    req = "SELECT FileBody FROM %sSandbox WHERE JobID=%d AND FileName='%s'" % \
          (prefix, int(jobID), filename)
          
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      return S_ERROR('Sandbox file not found')       
      
    body = result['Value'][0][0]
    return S_OK(body)
    
#############################################################################
  def getFileNames(self,jobID,sandbox='in'):     
    """ Get file names for a given job in a given sandbox
    """ 
    
    prefix = "O"
    if sandbox == 'in':
      prefix = "I"
      
    req = "SELECT FileName FROM %sSandbox WHERE JobID=%d" % (prefix,int(jobID))
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      return S_ERROR('No files found for job %d' % int(jobID))
      
    fileList = [ x[0] for x in result['Value']]         
    return S_OK(fileList)

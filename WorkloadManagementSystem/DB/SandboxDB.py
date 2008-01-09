########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/SandboxDB.py,v 1.7 2008/01/09 09:12:02 atsareg Exp $
########################################################################
""" SandboxDB class is a simple storage using MySQL as a container for
    relatively small sandbox files. The file size is limited to 16MB.
    The following methods are provided

    addLoggingRecord()
    getJobLoggingInfo()
    getWMSTimeStamps()
"""

__RCSID__ = "$Id: SandboxDB.py,v 1.7 2008/01/09 09:12:02 atsareg Exp $"

import re, os, sys
import time, datetime
from types import *

from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

#############################################################################
class SandboxDB(DB):

  def __init__( self, sandbox_type, maxQueueSize=10 ):
    """ Standard Constructor
    """

    DB.__init__(self,sandbox_type,'WorkloadManagement/SandboxDB',maxQueueSize)

    self.maxSize = 16
    result = gConfig.getOption( self.cs_path+'/MaxSize')
    if not result['OK']:
      self.log.warn('Failed to get the Maximum Size limit')
      self.log.error('Using default value '+str(self.maxSize))
    else:
      self.maxSize = int(result['Value'])

#############################################################################
  def storeFile(self,jobID,filename,fileString,sandbox):
    """ Store input sandbox ASCII file for jobID with the name filename which
        is given with its string body
    """

    fileSize = len(fileString)
    if fileSize > self.maxSize*1024*1024:
      return S_ERROR('File size too large %.2f MB for file %s' % \
                     (fileSize/1024./1024.,filename))


    # Check that the file does not exist already
    req = "SELECT FileName FROM %s WHERE JobID=%d AND FileName='%s'" % \
          (sandbox,int(jobID),filename)
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) > 0:
      # Remove the already existing file - overwrite
      gLogger.warn('Overwriting file %s for job %d' % (filename,int(jobID)))
      req = "DELETE FROM %s WHERE JobID=%d AND FileName='%s'" % \
            (sandbox,int(jobID),filename)
      result = self._update(req)
      if not result['OK']:
        return result

    inFields = ['JobID','FileName','FileBody']
    inValues = [jobID,filename,fileString]

    result = self._insert(sandbox,inFields,inValues)
    return result

#############################################################################
  def getSandboxFile(self,jobID,filename,sandbox):
    """ Store input sandbox ASCII file for jobID with the name filename which
        is given with its string body
    """

    req = "SELECT FileBody FROM %s WHERE JobID=%d AND FileName='%s'" % \
          (sandbox, int(jobID), filename)

    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      return S_ERROR('Sandbox file not found')

    body = result['Value'][0][0]
    return S_OK(body)

#############################################################################
  def getFileNames(self,jobID,sandbox):
    """ Get file names for a given job in a given sandbox
    """

    req = "SELECT FileName FROM %s WHERE JobID=%d" % (sandbox,int(jobID))
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      return S_ERROR('No files found for job %d' % int(jobID))

    fileList = [ x[0] for x in result['Value']]
    return S_OK(fileList)

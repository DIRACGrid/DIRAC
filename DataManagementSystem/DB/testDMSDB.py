########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/DB/DataIntegrityDB.py $
########################################################################
__RCSID__   = "$Id: ...................DataIntegrityDB.py 28966 2010-10-05 13:24:36Z acsmith $"
__VERSION__ = "$Revision: 1.10 $"
""" testDMSDB class is a front-end to the testDMS Database. """

import re, os, sys
import time, datetime
from types import *

from DIRAC import gConfig,gLogger,S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
 
#############################################################################
class testDMSDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """
    DB.__init__(self,'testDMSDB','DataManagement/testDMSDB',maxQueueSize)
    #self._query(req)
    #self._update(req)
    #self._insert()

#############################################################################
    
  def insertSomething(self,user,files):
      req = "INSERT INTO userAccount (userName,numOfFiles) VALUES ('%s',%d)" % (user,files)
      res = self._update(req)
      if not res['OK']:
          gLogger.error("Failed to insert user files",res['Message']) 
      else:
          gLogger.info("Successfully inserted %d files" % res['Value'])
      return res

  def querySomething(self,user):
      req = "SELECT userName,numOfFiles from userAccount where userName='%s'" % (user)
      res = self._query(req)
      if not res['OK']:
          gLogger.error("Failed to query user files",res['Message']) 
          return res
      userDict = {}
      for userName,numFiles in res['Value']:
          userDict[userName] = numFiles
          gLogger.info("Succesfully found %d files for user %s" % (numFiles,user))
      return S_OK(userDict)

  # {'OK':True,'Value':value} = S_OK(value)
  # {'OK':False,'Message':message} = S_ERROR(message)
  
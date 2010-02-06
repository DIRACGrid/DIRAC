########################################################################
# $HeadURL:  $
########################################################################

""" The SystemAdministratorClient is a class representing the client of the DIRAC
    SystemAdministrator service
""" 

__RCSID__ = "$Id:  $"

import re, time, random, os, types, getpass
from DIRAC.Core.DISET.RPCClient  import RPCClient
from DIRAC import S_OK, S_ERROR, gLogger,gConfig
from DIRAC.Core.Base.Client import Client


class SystemAdministratorClient(Client):

  def __init__(self,url=None):
    """ Constructor function.
    """
    
    if url:
      self.serverURL = url
    else:  
      self.serverURL = 'Framework/SystemAdministrator'
      
  def getDatabases(self,password=None):
    """ Get the installed databases
    """
    if not password:
      pword = getpass.getpass('MySQL root password: ')
    else:
      pword = password  
    server = RPCClient(self.serverURL)
    return server.getDatabases(pword)    
  
  def installMySQL(self,rootpwd=None,diracpwd=None):
    """ Install the MySQL database on the server side
    """
    if not rootpwd:
      rpword = getpass.getpass('MySQL root password: ')
    else:
      rpword = rootpwd
    if not diracpwd:
      dpword = getpass.getpass('MySQL Dirac password: ')
    else:
      dpword = diracpwd
      
    server = RPCClient(self.serverURL)
    return server.installMySQL(rpword,dpword)
  
  def installDatabase(self,database,rootpwd=None):
    """ Install the MySQL database on the server side
    """
    if not rootpwd:
      rpword = getpass.getpass('MySQL root password: ')
    else:
      rpword = rootpwd
      
    server = RPCClient(self.serverURL)
    return server.installDatabase(rpword,database)      
    
    
    
    
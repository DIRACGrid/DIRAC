""" This is the Data Integrity Client which allows the simple reporting of problematic file and replicas to the IntegrityDB and their status correctly updated in the FileCatalog.""" 
__RCSID__ = "$Id$"

from DIRAC                                                import S_OK, S_ERROR,gLogger,gConfig
from DIRAC.Core.Base.Client                               import Client
import re,os,types

class StorageUsageClient(Client):

  def __init__(self):
    self.setServer('DataManagement/StorageUsage')
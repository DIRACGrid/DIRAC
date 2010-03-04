########################################################################
# $HeadURL$
########################################################################

""" The FileCatalogClient is a class representing the client of the DIRAC
    File Catalog 
""" 

__RCSID__ = "$Id$"

import re, time, random, os, types

from DIRAC import S_OK, S_ERROR, gLogger,gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Base.Client import Client


class FileCatalogClient(Client):

  def __init__(self):
    """ Constructor function.
    """
    self.serverURL = 'DataManagement/FileCatalog'

  def getReplicas( self, lfns, allStatus='allStatus' ):
    return RPCClient(self.serverURL).getReplicas(lfns)

  def listDirectory(self, lfn, verbose=False ):
    return RPCClient(self.serverURL).listDirectory(lfn,verbose)
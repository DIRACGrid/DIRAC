########################################################################
# $HeadURL$
########################################################################

""" The SystemAdministratorClient is a class representing the client of the DIRAC
    SystemAdministrator service. It has also methods to update the Configuration
    Service with the DIRAC components options
"""

__RCSID__ = "$Id$"

import re, time, random, os, types, getpass
from DIRAC.Core.DISET.RPCClient  import RPCClient
from DIRAC import S_OK, S_ERROR, gLogger, gConfig, rootPath
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

class SystemAdministratorClient( Client ):

  def __init__( self, host ):
    """ Constructor function. Takes a mandatory host parameter 
    """
    self.setServer( 'dips://%s:9162/Framework/SystemAdministrator' % host )

""" Client-side transfer class for monitoring system
"""

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC import S_ERROR, S_OK

class SiteMappingClient:

  ###########################################################################
  def __init__(self):
    self.transferClient = TransferClient('Monitoring/SiteMapping')

  ###########################################################################  
  def getFile(self, fileName, outputDir):
    """ Retrieves a single file and puts it in the output directory
    """
    outputFile = '%s/%s' % (outputDir, fileName)
    result = self.transferClient.receiveFile(outputFile, fileName)
    return result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#



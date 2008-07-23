""" Client-side transfer class for monitoring system
"""

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC import S_ERROR, S_OK

enumerationFile = 'enum.tmp'

class SiteMappingClient:

  ###########################################################################
  def __init__(self):
    self.transferClient = TransferClient('Monitoring/SiteMapping')
    
  ###########################################################################
  def updateTimeSeries(self):
    """ Calls SiteMappingHandler.updateTimeSeries() to update any time series data.
    """
    siteRPC = RPCClient('Monitoring/SiteMapping')
    result = siteRPC.updateTimeSeries()
    return result
  
  ###########################################################################  
  def getFile(self, fileName, outputDir):
    """ Retrieves a single file and puts it in the output directory
    """
    outputFile = '%s/%s' % (outputDir, fileName)
    result = self.transferClient.receiveFile(outputFile, {'Type' : 'File', 'Data' : fileName})
    return result

  ###########################################################################    
  def getFiles(self, fileList, outputDir):
    """ Retrieves multiple files in the list
    """
    failedFiles = []
    for f in fileList:
      result = self.getFile(f, outputDir)
      if not result['OK']:
        failedFiles.append(f)
    if len(failedFiles) != 0:
      return S_ERROR('The following files could not be received: %s' % failedFiles)
      
    return S_OK('All files were successfully received.')

  ###########################################################################  
  def getSection(self, section, outputDir):
    """ Retrieves all files corresponding to a given section
        Note: this will force a section update
    """
    tmpFile = '%s/%s' % (outputDir, enumerationFile)
    result = self.transferClient.receiveFile(tmpFile, {'Type' : 'Section', 'Data': section})
    if not result['OK']:
      return S_ERROR('Failed to enumerate section file list. Error: %s' % result)
    fileList = self.readFileList(tmpFile)
    self.getFiles(fileList, outputDir)
    return result

  ###########################################################################  
  def getAssociation(self, fileName, outputDir):
    """ Retrieves all files from the same section as fileName
        Note: this will force a section update
    """
    tmpFile = '%s/%s' % (outputDir, enumerationFile)
    result = self.transferClient.receiveFile(tmpFile, {'Type' : 'Association', 'Data': fileName})
    if not result['OK']:
      return S_ERROR('Failed to enumerate section file list. Error: %s' % result)
    fileList = self.readFileList(tmpFile)
    self.getFiles(fileList, outputDir)
    return result

  ###########################################################################  
  def getAllSections(self, outputDir):
    """ Forces a global update and retrieves all files
    """
    tmpFile = '%s/%s' % (outputDir, enumerationFile)
    result = self.transferClient.receiveFile(tmpFile, {'Type': 'All', 'Data' : False})
    if not result['OK']:
      return S_ERROR('Failed to enumerate file list.')
    fileList = self.readFileList(tmpFile)
    self.getFiles(fileList, outputDir)
    return result

  ###########################################################################  
  def readFileList(self, inputFile):
    """ Generates a list of files for use in section enumeration
    """
    fin = open(inputFile, 'r')
    rawData = fin.read()
    fin.close()
    
    fileList = rawData.split('\n')
    del fileList[len(fileList) - 1]
    return fileList

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#



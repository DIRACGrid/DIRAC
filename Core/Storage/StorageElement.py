""" This is the StorageElement class.

    self.name is the resolved name of the StorageElement i.e CERN-tape
    self.options is dictionary containing the general options defined in the CS e.g. self.options['Backend] = 'Castor2'
    self.storages is a list of the stub objects created by StorageFactory for the protocols found in the CS.
    self.localProtocols is a list of the local protocols that were created by StorageFactory
    self.remoteProtocols is a list of the remote protocols that were created by StorageFactory
    self.protocolOptions is a list of dictionaries containing the options found in the CS. (should be removed)
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.Utilities.List import sortList
from DIRAC.Core.Utilities.File import getSize
import re, time,os

class StorageElement:

  def __init__(self,name):
    self.valid = True
    res = StorageFactory().getStorages(name)
    if not res['OK']:
      self.valid = False
    factoryDict = res['Value']
    self.name = factoryDict['StorageName']
    self.options = factoryDict['StorageOptions']
    self.localProtocols = factoryDict['LocalProtocols']
    self.remoteProtocols = factoryDict['RemoteProtocols']
    self.storages = factoryDict['StorageObjects']
    self.protocolOptions = factoryDict['ProtocolOptions']

  def dump(self):
    """
      Dump to the logger a sumary of the StorageElement items
    """
    gLogger.info("StorageElement.dump: Perparing dump for StorageElement %s." % self.name)
    i = 1
    outStr = "\n\n============ Options ============\n"
    for key in sortList(self.options.keys()):
      outStr = "%s%s: %s\n" % (outStr,key.ljust(15),self.options[key])

    for storage in self.storages:
      outStr = "%s============Protocol %s ============\n" % (outStr,i)
      res = storage.getParameters()
      storageParameters = res['Value']
      for key in sortList(storageParameters.keys()):
        outStr = "%s%s: %s\n" % (outStr,key.ljust(15),storageParameters[key])
      i = i + 1
    gLogger.info(outStr)

  def isValid(self):
    gLogger.info("StorageElement.isValid: Determining whether the StorageElement %s is valid for use." % self.name)
    return S_OK(self.valid)

  def getStorageElementName(self):
    return S_OK(self.name)
  #################################################################################################
  #
  # These are the basic get functions to get information about the supported protocols
  #

  def getProtocols(self):
    """ Get the list of all the protocols defined for this Storage Element
    """
    gLogger.info("StorageElement.getProtocols: Obtaining all protocols for %s." % self.name)
    allProtocols = self.localProtocols+self.remoteProtocols
    return S_OK(allProtocols)

  def getRemoteProtocols(self):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    gLogger.info("StorageElement.getRemoteProtocols: Obtaining remote protocols for %s." % self.name)
    return S_OK(self.remoteProtocols)

  def getLocalProtocols(self):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    gLogger.info("StorageElement.getLocalProtocols: Obtaining local protocols for %s." % self.name)
    return S_OK(self.localProtocols)

  #################################################################################################
  #
  # These are the basic get functions for storage configuration
  #

  def getStorageElementOption(self,option):
    """ Get the value for the option supplied from self.options
    """
    gLogger.info("StorageElement.getStorageElementOption: Obtaining %s option for Storage Element %s." % (option,self.name))
    if self.options.has_key(option):
      optionValue = self.options[option]
      return S_OK(optionValue)
    else:
      errStr = "StorageElement.getStorageElementOption: Option not defined for SE."
      gLogger.error(errStr,"%s for %s" % (option,self.name))
      return S_ERROR(errStr)

  def getStorageParameters(self,protocol):
    """ Get protocol specific options
    """
    gLogger.info("StorageElement.getStorageParameters: Obtaining storage parameters for %s protocol %s." % (self.name,protocol))
    res = self.getProtocols()
    availableProtocols = res['Value']
    if not protocol in availableProtocols:
      errStr = "StorageElement.getStorageParameters: Requested protocol not available for SE."
      gLogger.error(errStr,'%s for %s' % (protocol,self.name))
      return S_ERROR(errStr)
    for storage in self.storages:
      res = storage.getParameters()
      storageParameters = res['Value']
      if storageParameters['ProtocolName'] == protocol:
        return S_OK(storageParameters)
    errStr = "StorageElement.getStorageParameters: Requested protocol supported but no object found."
    gLogger.error(errStr,"%s for %s" % (protocol,self.name))
    return S_ERROR(errStr)

  def isLocalSE(self):
    """ Test if the Storage Element is local in the current context
    """
    gLogger.info("StorageElement.isLocalSE: Determining whether %s is a local SE." % self.name)
    configStr = '/DIRAC/Site'
    localSite = gConfig.getValue(configStr)
    configStr = '/Resources/SiteLocalSEMapping/%s' % localSite
    localSEs = gConfig.getValue(configStr,[])
    if self.name in localSEs:
      return S_OK(True)
    else:
      return S_OK(False)

  #################################################################################################
  #
  # These are the basic get functions for pfn manipulation
  #

  def getPfnForProtocol(self,pfn,protocol,withPort=True):
    """ Transform the input pfn into another with the given protocol for the Storage Element.
    """
    res = self.getProtocols()
    availableProtocols = res['Value']
    if not protocol in availableProtocols:
      errStr = "StorageElement.getPfnForProtocol: Requested protocol not available for SE."
      gLogger.error(errStr,'%s for %s' % (protocol,self.name))
      return S_ERROR(errStr)
    # Check all available storages for required protocol then contruct the PFN
    for storage in self.storages:
      res = storage.getParameters()
      if protocol == res['Value']['ProtocolName']:
        pfnDict = pfnparse(pfn)
        res = storage.getProtocolPfn(pfnDict,withPort)
        return res
    errStr = "StorageElement.getPfnForProtocol: Requested protocol supported but no object found."
    gLogger.error(errStr,"%s for %s" % (protocol,self.name))
    return S_ERROR(errStr)

  def getPfnPath(self,pfn):
    """  Get the part of the PFN path below the basic storage path.
         This path must coinside with the LFN of the file in order to be compliant with the LHCb conventions.
    """
    res = pfnparse(pfn)
    if not res['OK']:
      return res
    fullPfnPath = '%s/%s' % (res['Value']['Path'],res['Value']['FileName'])

    # Check all available storages and check whether the pfn is for that protocol
    pfnPath = ''
    for storage in self.storages:
      res = storage.isPfnForProtocol(pfn)
      if res['OK']:
        if res['Value']:
          res = storage.getParameters()
          saPath = res['Value']['Path']
          if not saPath:
            # If the sa path doesn't exist then the pfn path is the entire string
            pfnPath = fullPfnPath
          else:
            if re.search(saPath,fullPfnPath):
              # Remove the sa path from the fullPfnPath
              pfnPath = fullPfnPath.replace(saPath,'')
      if pfnPath:
        return S_OK(pfnPath)
    # This should never happen. DANGER!!
    errStr = "StorageElement.getPfnPath: Failed to get the pfn path for any of the protocols!!"
    gLogger.error(errStr)
    return S_ERROR(errStr)


  #################################################################################################
  #
  # These are the methods that implement the StorageElement functionality
  #

  def removeDirectory(self,directoryUrl):
    """ This method removes the contents of a directory on the storage including files and subdirectories.

        'directoryUrl' is the directory on the storage to be removed
    """
    localSE = self.isLocalSE()['Value']
    for storage in self.storages:
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      # If the SE is not local then we can't use local protocols
      if protocolName in self.remoteProtocol:
        useProtocol = True
      elif localSE:
        useProtocol = True
      else:
        useProtocol = False
        gLogger.info("StorageElement.removeDirectory: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        res = self.getPfnForProtocol(directoryUrl, protocolName)
        if res['OK']:
          directory = res['Value']
          res = storage.removeDirectory(directory)
          if res['OK']:
            if res['Value']['Successful'].has_key(directory):
              return S_OK(res['Value']['Successful'][directory])
    # If we get here we tried all the protocols and failed with all of them
    errStr = "StorageElement.removeDirectory: Failed to create directory for all protocols."
    gLogger.error(errStr,directoryUrl)
    return S_ERROR(errStr)

  def makeDirectory(self,directoryUrl):
    """ This will recursively create the directories on the storage until the desired path

        'directoryUrl' is a string containing the directory to be created
    """
    localSE = self.isLocalSE()['Value']
    for storage in self.storages:
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      # If the SE is not local then we can't use local protocols
      if protocolName in self.remoteProtocol:
        useProtocol = True
      elif localSE:
        useProtocol = True
      else:
        useProtocol = False
        gLogger.info("StorageElement.makeDirectory: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        res = self.getPfnForProtocol(directoryUrl, protocolName)
        if res['OK']:
          directory = res['Value']
          res = storage.createDirectory(directory)
          if res['OK']:
            if res['Value']['Successful'].has_key(directory):
              return S_OK()
    # If we get here we tried all the protocols and failed with all of them
    errStr = "StorageElement.makeDirectory: Failed to create directory for all protocols."
    gLogger.error(errStr,directoryUrl)
    return S_ERROR(errStr)

  def getAccessUrl(self,pfns):
    """ This method obtains a tURL for a pfn (or list of pfns)

        'pfn' is the physical file name (as registered in the LFC)
    """
    if type(pfns) == types.StringType:
      pfnsToGet = [pfns]
    elif type(pfns) == types.ListType:
      pfnsToGet = pfns
    else:
      return S_ERROR("StorageElement.getTurls: Supplied pfns must be string or list of strings")

    pfnToProtocolPfn = {}
    turlDict = {}
    # We try all the available storages for this SE
    for storage in self.storages:
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      # First get all the protocol pfns we for this protocol
      for pfn in pfnsToGet:
        res = self.getPfnForProtocol(pfn, protocolName)
        if res['OK']:
          protocolPfn = res['Value']
          pfnToProtocolPfn[protocolPfn] = pfn
      # If there is anything to get
      if len(pfnToProtocolPfn.keys()) > 0:
        res = storage.getTransportURL(pfnToProtocolPfn.keys(),protocols=self.localProtocols)
        if not res['OK']:
          infoStr = "StorageElement.getTurls%s." % res['Message']
          gLogger.error(infoStr)
        else:
          # Obtain the success or failure for each attempted pfn
          for protocolPfn in pfnToProtocolPfn.keys():
            if res['Value']['Successful'].has_key(protocolPfn):
              turl = res['Value']['Successful'][protocolPfn]
              pfn = pfnToProtocolPfn[protocolPfn]
              # Populate a dictionary with the supplied pfn and obtained turl
              turlDict[pfn] = turl
              # Remove this pfn from the list so we don't try it again
              pfnsToGet.remove(pfn)
            else:
              # This means that obtaining a tURL for this pfn failed
              pfn = pfnToProtocolPfn[protocolPfn]
              infoStr = "StorageElement.getTurls: Failed to get tURL: %s" % res['Value']['Failed'][protocolPfn]
              gLogger.error(infoStr,'%s for protocol %s' % (pfn,protocolName))
    successful = turlDict
    failed = {}
    # Check if there are any pfns which still remain to get
    if len(pfnsToGet) > 0:
      # If we get here we tried all the protocols and failed with all of them
      for pfn in pfnsToGet:
        failed[pfn] = True
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFile(self,pfn,catalogueFileSize,localPath = ''):
    """ This method will obtain a local copy of a file from the SE

        'pfn' is the physical file name (as registered in the LFC)
        'catalogueFileSize' is the size from the catalogue
    """
    localSE = self.isLocalSE()['Value']

    res  = pfnparse(pfn)
    if not res['OK']:
      errStr = "StorageElement.getFile: Failed to parse supplied PFN."
      gLogger.error(errStr,"%s: %s" % (pfn,res['Message']))
      return S_ERROR(errStr)
    pfnDict = res['Value']
    fileName = os.path.basename(pfn)

    # Try all of the storages one by one until success
    for storage in self.storages:
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      # If the SE is not local then we can't use local protocols
      if protocolName in self.remoteProtocols:
        useProtocol = True
      elif localSE:
        useProtocol = True
      else:
        useProtocol = False
        gLogger.info("StorageElement.makeDirectory: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        res = storage.getProtocolPfn(pfnDict,True)
        if not res['OK']:
          infoStr = "StorageElement.getFile%s." % res['Message']
          gLogger.error(infoStr,'%s for protocol %s' % (pfn,protocolName))
        else:
          protocolPfn = res['Value']
          protocolSize = None
          ###############################################################################################
          # Pre-transfer check. Check if file exists, get the size and check against the catalogue
          res = storage.exists(protocolPfn)
          if res['OK']:
            if res['Value']['Successful'].has_key(protocolPfn):
              fileExists = res['Value']['Successful'][protocolPfn]
              if not fileExists:
                errStr = "StorageElement.getFile: Source file does not exist."
                gLogger.error(errStr,'%s for protocol %s' % (protocolPfn,protocolName))
                return S_ERROR(errStr)
              else:
                infoStr = "StorageElement.getFile: File exists, checking size."
                gLogger.info(infoStr,'%s for protocol %s' % (protocolPfn,protocolName))
                res = storage.getFileSize(protocolPfn)
                if res['OK']:
                  if res['Value']['Successful'].has_key(protocolPfn):
                    protocolSize = res['Value']['Successful'][protocolPfn]
                    if not catalogueFileSize == protocolSize:
                      infoStr ="StorageElement.getFile: Physical file size and catalogue file size mismatch."
                      gLogger.error(infoStr,'%s : Physical %s, Catalogue %s' % (protocolPfn,protocolSize,catalogueFileSize))
                      return S_ERROR('StorageElement.getFile: Physical file zero size')
                    if protocolSize == 0:
                      infoStr ="StorageElement.getFile: Physical file found with zero size."
                      gLogger.error(infoStr,'%s at %s' % (protocolPfn,self.name))
                      res = storage.removeFile(protocolPfn)
                      if res['OK']:
                        if res['Value']['Successful'].has_key(protocolPfn):
                          infoStr ="StorageElement.getFile: Removed zero size file from storage."
                          gLogger.info(infoStr,'%s with protocol %s' % (protocolPfn,protocolName))
                        else:
                          infoStr ="StorageElement.getFile: Failed to removed zero size file from storage."
                          gLogger.error(infoStr,'%s with protocol %s' % (protocolPfn,protocolName))
                      else:
                        infoStr ="StorageElement.getFile: Failed to removed zero size file from storage."
                        gLogger.error(infoStr,'%s with protocol %s' % (protocolPfn,protocolName))
                      return S_ERROR('StorageElement.getFile: Physical file zero size')
                    else:
                      infoStr ="StorageElement.getFile: %s file size: %s." %  (protocolPfn,protocolSize)
                      gLogger.info(infoStr)
                else:
                  infoStr = "StorageElement.getFile: Failed to get remote file size."
                  gLogger.error(infoStr,'%s for protocol %s' % (protocolPfn,protocolName))
            else:
              infoStr = "StorageElement.getFile: Failed to determine whether file exists."
              gLogger.error(infoStr,'%s for protocol %s' % (protocolPfn,protocolName))
          else:
            infoStr = "StorageElement.getFile: Failed to determine whether file exists."
            gLogger.error(infoStr,'%s for protocol %s' % (protocolPfn,protocolName))

          ###########################################################################################
          # If the check was successful (i.e. we obtained correctly the file size) perform the transfer
          if protocolSize:
            if not localPath:
              localPath = '%s/%s' % (os.getcwd(),fileName)
            fileTuple = (protocolPfn,localPath,protocolSize)
            res = storage.getFile(fileTuple)
            if res['OK']:
              if res['Value']['Successful'].has_key(protocolPfn):
                infoStr ="StorageElement.getFile: Got local copy of file. Checking size..."
                gLogger.info(infoStr)
                ##############################################################
                # Post-transfer check. Check the local file size and remove it if not correct
                localSize = getSize(localPath)
                if localSize == protocolSize:
                  infoStr ="StorageElement.getFile: Local file size correct."
                  gLogger.info(infoStr,'%s with protocol %s' % (localPath,protocolName))
                  # woohoo, good work!!!
                  return S_OK(localPath)
              ##############################################################
              # If the transfer failed or the sizes don't match clean up the mess
              if os.path.exists(localPath):
                os.remove(localPath)
            else:
              infoStr = "StorageElement.getFile%s" % res['Message']
              gLogger.error(infoStr)

    # If we get here we tried all the protocols and failed with all of them
    errStr = "StorageElement.putFile: Failed to get file for all protocols."
    gLogger.error(errStr,pfn)
    return S_ERROR(errStr)

  def putFile(self,file,directoryPath,alternativeFileName=None):
    """ This method will upload a local file to the SE

        'file' is the full local path to the file e.g. /opt/dirac/file/for/upload.file
        'directoryPath' is the path on the storage the file will be put
        'alternativeFileName' is the target file name
    """
    if alternativeFileName:
      gLogger.info("StorageElement.putFile: Attempting to put %s to %s with file name %s." % (file,directoryPath,alternativeFileName))
    else:
      gLogger.info("StorageElement.putFile: Attempting to put %s to %s." % (file,directoryPath))

    size = getSize(file)
    fileName = os.path.basename(file)
    if size == -1:
      infoStr = "StorageElement.putFile: Failed to get file size"
      gLogger.error(infoStr,file)
      return S_ERROR(infoStr)
    elif size == 0:
      infoStr ="StorageElement.putFile: File is zero size"
      gLogger.error(infoStr,file)
      return S_ERROR(infoStr)

    gLogger.info("StorageElement.putFile: Determined file size of %s to be %s." % (file,size))

    # The method will try all storages
    for storage in self.storages:
      # Get the parameters for the current storage
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      gLogger.info("StorageElement.putFile: Attempting to put file with %s." % protocolName)
      # If the SE is not local then we can't use local protocols
      if protocolName in self.remoteProtocols:
        useProtocol = True
      elif localSE:
        useProtocol = True
      else:
        useProtocol = False
        gLogger.info("StorageElement.putDirectory: %s not appropriate for use." % protocolName)
      if useProtocol:
        res =  storage.getCurrentURL(directoryPath)
        if res['OK']:
          destinationDirectory = res['Value']
          res = storage.createDirectory(destinationDirectory)
          if not res['OK']:
            infoStr ="StorageElement.putFile: Failed to create directory."
            gLogger.error(infoStr,'%s with protocol %s' % (directoryPath,protocolName))
          else:
            storage.changeDirectory(directoryPath)
        if res['OK']:
          # Obtain the full URL for the file from the file name and the cwd on the storage
          if alternativeFileName:
            res = storage.getCurrentURL(alternativeFileName)
          else:
            res = storage.getCurrentURL(fileName)
          if not res['OK']:
            infoStr ="StorageElement.putFile: Failed to get the file URL."
            gLogger.error(infoStr,' With protocol %s' % protocolName)
          else:
            destUrl = res['Value']
            ##############################################################
            # Pre-transfer check. Check if file already exists and remove it
            res = storage.exists(destUrl)
            if res['OK']:
              if res['Value']['Successful'].has_key(destUrl):
                fileExists = res['Value']['Successful'][destUrl]
                if not fileExists:
                  infoStr = "StorageElement.putFile: pre-transfer check completed successfully"
                  gLogger.info(infoStr)
                else:
                  infoStr = "StorageElement.putFile: pre-transfer check failed. File already exists. Removing..."
                  gLogger.info(infoStr)
                  res = storage.removeFile(destUrl)
                  if res['OK']:
                    if res['Value']['Successful'].has_key(destUrl):
                      infoStr ="StorageElement.putFile: Removed pre-existing file from storage."
                      gLogger.info(infoStr,'%s with protocol %s' % (destUrl,protocolName))
                    else:
                      infoStr ="StorageElement.putFile: Failed to remove pre-existing file from storage."
                      gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))
                  else:
                    infoStr ="StorageElement.putFile: Failed to remove pre-existing file from storage."
                    gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))
              else:
                infoStr ="StorageElement.putFile: Failed to find pre-existance of file."
                gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))
            else:
              infoStr ="StorageElement.putFile: Failed to find pre-existance of file."
              gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))
            ##############################################################
            # Perform the transfer here....
            fileTuple = (file,destUrl,size)
            res = storage.putFile(fileTuple)
            if res['OK']:
              if res['Value']['Successful'].has_key(destUrl):
                infoStr = "StorageElement.putFile: Successfully put %s with protocol %s." % (destUrl,protocolName)
                gLogger.info(infoStr)
                return S_OK(destUrl)
              else:
                errStr ="StorageElement.putFile: Failed to put file."
                errMessage = res['Value']['Failed'][destUrl]
                gLogger.error(errStr,'%s with protocol %s: %s' % (destUrl,protocolName,errMessage))
            # If the transfer completely failed
            else:
              errStr = "StorageElement.putFile: Completely failed to put file: %s" % res['Message']
              gLogger.error(errStr,"%s with protocol %s" % (destUrl,protocolName))
    # If we get here we tried all the protocols and failed with all of them
    errStr = "StorageElement.putFile: Failed to put file for all protocols."
    gLogger.error(errStr,file)
    return S_ERROR(errStr)



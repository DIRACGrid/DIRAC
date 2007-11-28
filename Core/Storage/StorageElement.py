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
import re, time

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

  def testStorageFactory(self,dict):
    return StorageFactory().getStorage(dict)

  def dump(self):
    """
      Dump to the logger a sumary of the StorageElement items
    """
    outStr = "\nStorage Element %s:\n" % (self.name)
    i = 1
    outStr = "%s============ Options ============\n" % (outStr)
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
    return S_OK(self.valid)

  #################################################################################################
  #
  # These are the basic get functions to get information about the supported protocols
  #

  def getProtocols(self):
    """ Get the list of all the protocols defined for this Storage Element
    """
    allProtocols = self.localProtocols+self.remoteProtocols
    return S_OK(allProtocols)

  def getRemoteProtocols(self):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    return S_OK(self.remoteProtocols)

  def getLocalProtocols(self):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    return S_OK(self.localProtocols)

  def getPrimaryProtocol(self):
    """ Get the primary protocol option
    """
    res = self.getStorageElementOption('PrimaryProtocol')
    return res

  def getLocalProtocol(self):
    """ Get the local protocol option
    """
    res = self.getStorageElementOption('LocalProtocol')
    return res

  def getStorageElementOption(self,option):
    """ Get the value for the option supplied from self.options
    """
    if self.options.has_key(option):
      optionValue = self.options[option]
      return S_OK(optionValue)
    else:
      errStr = "StorageElement.getStorageElementOption: %s not defined for %s." % (option,self.name)
      gLogger.error(errStr)
      return S_ERROR(errStr)

  def getStorageParameters(self,protocol):
    """ Get protocol specific options
    """
    res = self.getProtocols()
    availableProtocols = res['Value']
    if not protocol in availableProtocols:
      errStr = "StorageElement.getStorageParameters: %s protocol is not supported for %s." % (protocol,self.name)
      gLogger.error(errStr)
      return S_ERROR(errStr)
    for storage in self.storages:
      res = storage.getParameters()
      storageParameters = res['Value']
      if storageParameters['ProtocolName'] == protocol:
        return S_OK(storageParameters)
    errStr = "StorageElement.getStorageParameters: %s found in %s protocols list but no object found." % (protocol,self.name)
    gLogger.error(errStr)
    return S_ERROR(errStr)

  def isLocalSE(self):
    """ Test if the Storage Element is local in the current context
    """
    localSE = gConfig.getValue('/LocalSite/LocalSE',[])
    if self.name in localSE:
      return S_OK(True)
    else:
      return S_OK(False)

  #################################################################################################
  #
  # These are the methods that implement the StorageElement functionality
  #

  def getFile(self,pfn,catalogueFileSize):
    """ This method will obtain a local copy of a file from the SE

        'pfn' is the physical file name (as registered in the LFC)
        'catalogueFileSize' is the size from the catalogue
    """
    pfnDict = pfnparse(pfn)
    fileName = os.path.basename(pfn)

    # If the file is on a local storage element try the local protocols first
    for storage in self.storages:
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      protocolPfn = storage.getProtocolPfn(pfnDict)
      protocolSize = None

      ###############################################################################################
      # Pre-transfer check. Check if file exists, get the size and check against the catalogue
      res = storage.exists(protocolPfn)
      if res['OK']:
        if res['Value']['Successful'].has_key(protocolPfn):
          fileExists = res['Value']['Successful'][destUrl]
          if not fileExists:
            infoStr = "StorageElement.getFile: File does not exist."
            gLogger.error(infoStr,'%s for protocol %s' % (protocolPfn,protocolName))
            return S_ERROR('StorageElement.getFile: Physical file does not exist')
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
                  gLogger.error(infoStr,'%s' % protocolPfn)
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

  def putFile(self,file,alternativePath=None,desiredProtocol=None):
    """ This method will upload a local file to the SE

        'file' is the full local path to the file e.g. /opt/dirac/file/for/upload.file
        'alternativePath' is the path on the storage the file will be put
        'desiredProtocol' is a protocol name
    """
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

    # The method will try all storages
    for storage in self.storages:
      # Get the parameters for the current storage
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      # If we only wish to try a single protocol then perform check here
      tryProtocol = False
      if desiredProtocol:
        if desiredProtocol == protocolName:
          tryProtocol = True
      else:
        tryProtocol = True
      if tryProtocol:
        res = S_OK()
        # If we require to create an alternative path create it then move there in the storage
        if alternativePath:
          res = storage.makeDirectory(alternativePath)
          if not res['OK']:
            infoStr ="StorageElement.putFile: Failed to create directory."
            gLogger.error(infoStr,'%s with protocol %s' % (alternativePath,protocolName))
          else:
            storage.changeDirectory(alternativePath)
        if res['OK']:
          # Obtain the full URL for the file from the file name and the cwd on the storage
          res = storage.getCurrentURL(fileName)
          if not res['OK']:
            infoStr ="StorageElement.putFile: Failed to get the file URL."
            gLogger.error(infoStr,'%s with protocol %s' % (fileName,protocolName))
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
            fileTuple = (fileName,destUrl,size)
            res = storage.putFile(fileTuple)
            if res['OK']:
              if res['Value']['Successful'].has_key(destUrl):
                ##############################################################
                # Post-transfer check. Check the file size and remove it if not correct
                res = storage.getFileSize(destUrl)
                if res['OK']:
                  if res['Value']['Successful'].has_key(destUrl):
                    # If the file size matched we are finished
                    if res['Value']['Successful'][destUrl] == size:
                      infoStr ="StorageElement.putFile: Destination file size correct."
                      gLogger.info(infoStr,'%s with protocol %s' % (destUrl,protocolName))
                      # woohoo, good work!!!
                      return S_OK(destUrl)
                    else:
                      infoStr ="StorageElement.putFile: Destination file size does not match. Removing..."
                      gLogger.info(infoStr)
                  else:
                    infoStr ="StorageElement.putFile: Failed to get destination file size. Removing..."
                    gLogger.info(infoStr)
                else:
                  infoStr ="StorageElement.putFile: Failed to get destination file size. Removing..."
                  gLogger.info(infoStr)
              else:
                infoStr ="StorageElement.putFile: Failed to put file."
                gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))

              ##############################################################
              # If the transfer failed or the sizes don't match clean up the mess
              infoStr ="StorageElement.putFile: Removing failed file remnant."
              gLogger.info(infoStr,'%s with protocol %s' % (destUrl,protocolName))
              res = storage.removeFile(destUrl)
              if res['OK']:
                if res['Value']['Successful'].has_key(destUrl):
                  infoStr ="StorageElement.putFile: Removed failed file remnant from storage."
                  gLogger.info(infoStr,'%s with protocol %s' % (destUrl,protocolName))
                else:
                  infoStr ="StorageElement.putFile: Failed to remove failed file remnant from storage."
                  gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))
              else:
                infoStr ="StorageElement.putFile: Failed to remove failed file remnant from storage."
                gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))

            ##############################################################
            # If the transfer completely failed
            else:
              infoStr = "StorageElement.putFile%s" % res['Message']
              gLogger.error(infoStr)

    # If we get here we tried all the protocols and failed with all of them
    errStr = "StorageElement.putFile: Failed to put file for all protocols."
    gLogger.error(errStr,file)
    return S_ERROR(errStr)

  #################################################################################################
  # Below this line things aren't implemented
  #
  def __getPfnDict(self,pfn,protocol):
    """ Gets the dictionary describing the PFN in terms of protocol
    """
    errStr = "StorageElement.__getPfnDict: Implement me."
    gLogger.error(errStr)
    return S_ERROR(errStr)

  def getPfnForProtocol(self,pfn,protocol):
    """ Transform the input pfn into another with the given protocol for the Storage Element.
        The new PFN is built on the fly based on the information provided in the Configuration Service
    """
    errStr = "StorageElement.getPfnForProtocol: Implement me."
    gLogger.error(errStr)
    return S_ERROR(errStr)

  def getPfnPath(self,pfn):
    """  Get the part of the PFN path below the basic storage path.
         This path must coinside with the LFN of the file in order to be
         compliant with the LHCb conventions.
    """
    errStr = "StorageElement.getPfnPath: Implement me."
    gLogger.error(errStr)
    return S_ERROR(errStr)

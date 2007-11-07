########################################################################
# $Id: DIPStorage.py,v 1.2 2007/11/07 14:18:35 atsareg Exp $
########################################################################

""" DIPStorage class is the client of the DIRAC Storage Element.

    The following methods are available in the Service interface

    getMetadata()
    get()
    getDir()
    put()
    putDir()
    remove()

"""

__RCSID__ = "$Id: DIPStorage.py,v 1.2 2007/11/07 14:18:35 atsareg Exp $"

from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC import gLogger, S_OK, S_ERROR
import re
import os
import time
import string

class DIPStorage:

  def __init__(self,url):
    """
    """
    self.transferClient = TransferClient(url)
    # to avoid timeouts on the CS refresher - should be fixed elsewhere
    time.sleep(0.02)
    self.serviceClient = RPCClient(url)
    # to avoid timeouts on the CS refresher - should be fixed elsewhere
    time.sleep(0.02)

    self.cwd = '/'

  def getParameters(self):
    """  Get the storage access parameters: protocol, host, root directory
    """
    pass

  def makeDir(self,newdir):
    """  Make a new directory on the remote storage
    """

    return S_OK()

  def listDirectory(self,dirPath='',mode=''):
    """ Make listing of the directory dirPath
    """
    result = self.serviceClient.listDirectory(dirPath,mode)
    return result

  def exists(self,path):
    """ Check the existence of the given path. Returns some basic
        metadata as well ( type )
    """

    result = self.serviceClient.getMetadata(fname)
    return result['Exists']

  def isDirectory(self,path):
    """  Checks if the given path is a directory
    """

    result = self.serviceClient.getMetadata(fname)
    if not result['Exists']:
      return False
    else:
      return result['Type'] == "Directory"

  def isFile(self,path):
    """  Checks if the given path is a file
    """

    result = self.serviceClient.getMetadata(fname)
    if not result['Exists']:
      return False
    else:
      return result['Type'] == "File"

  def changeDirectory(self,newdir):
    """  Changes the default current directory to newdir. This
         has no effect on the storage status.
    """

    self.cwd = newdir

  def getMetadata(self,fname):
    """ Get the fname metadata
    """

    result = self.serviceClient.getMetadata(fname)
    return result

  def remove(self,fname):
    """ Remove file fname from the storage
    """

    result = self.serviceClient.remove(fname,'')
    return result

  def removeDirectory(self,dirName):
    """ Remove the entire directory from the storage
    """

    result = self.serviceClient.removeDirectory(dirName,'')
    return result

  def removeFileList(self,fileList):
    """ Remove files in the given list from the storage
    """

    result = self.serviceClient.removeFileList(fileList,'')
    return result

  def put(self,fname):
    """ Send file with the name fname to the Storage Element
    """

    bname = os.path.basename(fname)
    sendName = self.cwd+'/'+bname

    print sendName,fname
    result = self.transferClient.sendFile(fname,sendName)
    return result

  def putDirectory(self,dname):
    """ Upload a directory dname to the storage current directory
    """

    bname = os.path.basename(dname)
    sendName = self.cwd
    result = self.transferClient.sendBulk([dname],sendName)
    return result

  def putFileList(self,fileList):
    """ Upload files in the fileList to the current directory
    """
    sendName = self.cwd
    result = self.transferClient.sendBulk(fileList,sendName)
    return result

  def get(self,fname):
    """ Get file with the name fname from the Storage Element
    """
    bname = os.path.basename(fname)
    result = self.transferClient.receiveFile(bname,fname)
    return result

  def getDirectory(self,dname):
    """ Get file directory dname from the storage
    """

    cwd = os.getcwd()
    result = self.transferClient.receiveBulk(cwd,dname)
    return result

  def getFileList(self,fileList):
    """ Get files in the given fileList from the storage
    """
    fileListString = '--FileList--'+string.join(fileList,':')
    print fileListString
    cwd = os.getcwd()
    result = self.transferClient.receiveBulk(cwd,fileListString)
    return result

  def getAdminInfo(self):
    """  Get the storage service administration information
    """

    # The contents of the information is to be defined
    # Available and used space, number of files, usage stats, etc

    result = self.serviceClient.getAdminInfo()
    return result
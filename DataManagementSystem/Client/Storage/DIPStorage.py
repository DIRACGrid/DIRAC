########################################################################
# $Id: DIPStorage.py,v 1.2 2008/02/15 10:25:59 atsareg Exp $
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

__RCSID__ = "$Id: DIPStorage.py,v 1.2 2008/02/15 10:25:59 atsareg Exp $"

from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC import gLogger, S_OK, S_ERROR
import re,os

class DIPStorage:

  def __init__(self,storageName,protocol,path,host,port,spaceToken,wspath):
    """
    """

    self.protocolName = 'DIP'
    self.name = storageName
    self.protocol = protocol
    self.path = path
    self.host = host
    self.port = port
    self.wspath = wspath
    self.spaceToken = spaceToken

    url = protocol+"://"+host+":"+port+"/"+path
    self.transferClient = TransferClient(url)
    self.serviceClient = RPCClient(url)

    self.cwd = '/'
    self.isok = True

  def isOK(self):
    return self.isok

  def getParameters(self):
    """ This gets all the storage specific parameters pass when instantiating the storage
    """
    parameterDict = {}
    parameterDict['StorageName'] = self.name
    parameterDict['ProtocolName'] = self.protocolName
    parameterDict['Protocol'] = self.protocol
    parameterDict['Host'] = self.host
    parameterDict['Path'] = self.path
    parameterDict['Port'] = self.port
    parameterDict['SpaceToken'] = self.spaceToken
    parameterDict['WSUrl'] = self.wspath
    return S_OK(parameterDict)

  def getCurrentURL(self,fileName):
    """ Obtain the current file URL from the current working directory and the filename
    """
    if fileName:
      if fileName[0] == '/':
        fileName = fileName.lstrip('/')
    try:
      fullUrl = '%s://%s:%s%s%s/%s' % (self.protocol,self.host,self.port,self.wspath,self.cwd,fileName)
      return S_OK(fullUrl)
    except Exception,x:
      errStr = "Failed to create URL %s" % x
      return S_ERROR(errStr)

  def createDirectory(self,dirpath):
    """ Create the remote directory
    """

    print dirpath
    return S_OK()

  def chdir(self,newdir):

    self.cwd = newdir

  def exists(self,fname):
    """
    """

    result = self.serviceClient.exists(fname)
    return result

  def getMetadata(self,fname):
    """
    """

    result = self.serviceClient.getMetadata(fname)
    return result

  def remove(self,fname):
    """ Remove file fname from the storage
    """

    result = self.serviceClient.remove(fname,'')
    return result

  def put(self,fname):
    """ Send file with the name fname to the Storage Element
    """

    bname = os.path.basename(fname)
    sendName = self.cwd+'/'+bname

    print sendName,fname
    result = self.transferClient.sendFile(fname,sendName)
    return result

  def putDir(self,dname):
    """ Upload a directory dname to the storage current directory
    """

    bname = os.path.basename(dname)
    sendName = self.cwd+'/'+bname
    result = self.transferClient.sendBulk([dname],sendName)
    return result

  def putFileList(self,fileList):
    """ Upload files in the fileList to the current directory
    """

    return S_OK()

  def get(self,fname):
    """ Get file with the name fname from the Storage Element
    """
    bname = os.path.basename(fname)
    result = self.transferClient.receiveFile(bname,fname)
    return result

  def getDir(self,dname):
    """ Get file directory dname from the storage
    """

    return S_OK()
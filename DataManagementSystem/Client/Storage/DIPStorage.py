########################################################################
# $Id: DIPStorage.py,v 1.1 2007/12/11 17:50:35 acsmith Exp $
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

__RCSID__ = "$Id: DIPStorage.py,v 1.1 2007/12/11 17:50:35 acsmith Exp $"

from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC import gLogger, S_OK, S_ERROR
import re,os

class DIPStorage:

  def __init__(self,url):
    """
    """
    self.transferClient = TransferClient(url)
    self.serviceClient = RPCClient(url)

    self.cwd = '/'

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
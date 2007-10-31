""" This is the SRM2 StorageClass

    Implemented:
                getParameters()
                getUrl()
    Not implemented:

"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Subprocess import pythonCall
import types, re

DEBUG = 0

class SRM2Storage(StorageBase):

  def __init__(self,storageName,protocol,path,host,port,spaceToken):
    self.name = storageName
    self.protocolName = 'SRM2'
    self.protocol = protocol
    self.path = path
    self.host = host
    self.port = port
    self.spaceToken = spaceToken

    apply(StorageBase.__init__,(self,self.name,self.path))

    self.timeout = 100
    self.long_timeout = 600

    try:
      import lcg_util
      infoStr = 'Using lcg_util from: %s' % lcg_util.__file__
      gLogger.info(infoStr)

      # setting some variables for use with lcg_utils
      self.nobdii = 0
      self.defaulttype = 2
      self.srctype = 0
      dsttype = self.defaulttype
      self.vo = 'lhcb'
      self.nbstreams = 4
      self.verbose = 1
      self.timeout = 60
      self.src_spacetokendesc = ''
      self.conf_file = 'ignored'
      self.insecure = 0
      self. src_file = 'file:/etc/group'
      self.dest_spacetokendesc = self.spaceToken
    except Exception,x:
      errStr = "SRM2Storage.__init__: Failed to import lcg_util: %s" % (x)
      gLogger.exception(errStr)

    try:
      import gfal
      infoStr = "Using gfal from: %s" % gfal.__file__
      gLogger.info(infoStr)
    except Exception,x:
      errStr = "SRM2Storage.__init__: Failed to import gfal: %s" % (x)
      gLogger.exception(errStr)


  def getProtocol(self):
    return self.protocolName

  ################################################################################
  #
  # The methods below are URL manipulation methods
  #

  def getPFNBase(self,withPort=False):
    """ This will get the pfn base. This is then appended with the LFN in LHCb convention.
    """
    if withPort:
      pfnBase = 'srm://%s:%s%s' % (self.host,self.port,self.path)
    else:
      pfnBase = 'srm://%s%s' % (self.host,self.path)
    return S_OK(pfnBase)

  def getUrl(self,path,withPort=False):
    """ This gets the URL for path supplied. With port is optional.
    """
    path = self.getPath(path)
    if withPort:
      url = 'srm://%s:%s%s' % (self.host,self.port,path)
    else:
      url = 'srm://%s%s' % (self.host,path)
    return S_OK(url)

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
    return S_OK(parameterDict)

  ################################################################################
  #
  # The methods below are for removal operations
  #

  def remove(self,fname):
    """ Remove from the physical storage the files provided (can be single file or list of files)
    """
    if type(fname) == types.StringType:
      urls = [fname]
    else:
      urls = fname

    gfalDict = {}
    gfalDict['surls'] = urls
    gfalDict['nbfiles'] =  len(urls)

    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1

    print gfalDict
    errCode,gfalObject,errMessage = gfal.gfal_init(dict)
    if errCode == 0:
      gLogger.debug("SRM2Storage.remove: Initialised gfal_init.")
      """
      print 'Performing: %s with gfal object: %s' % ('gfal.gfal_deletesurls()',gfalObject)
      errCode,gfalObject,errMessage = gfal.gfal_deletesurls(gfalObject)
      if errCode == 0:
        print 'Performing: %s with gfal object: %s' % ('gfal.gfal_get_results()',gfalObject)
        errCode,gfalObject,listOfResults = gfal.gfal_get_results(gfalObject)
        if errCode < 0:
          print 'Error with code %s: %s' % (errCode,os.strerror(errCode))
        else:
          print 'GFAL object: %s' % gfalObject
          for dict in listOfResults:
            if dict['status'] == 0:
              print dict['surl'],dict['turl']
            else:
              print 'Error with SURL: %s %s' % (dict['surl'],os.strerror(dict['status']))
      else:
        print 'Error code: %s' % errCode
        print 'Error message: %s' % errMessage
      """
    else:
      errStr = "SRM2Storage.remove: Failed to instanciate gfal_init: %s" % errMessage
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return S_OK()







  ################################################################################
  #
  # The methods below are not yet finished.
  #

  def put(self,fname):
    """Put file to the current directory

       Put the file to the current directory on the GRIDFTP storage. The file
       can be a local file or a URL with the gsiftp protocol.
    """
    errCode,errStr = lcg_util.lcg_cp3(src_file, dest_file, defaulttype,srctype, dsttype, nobdii, vo,nbstreams,conf_file,insecure,verbose,timeout,src_spacetokendesc,dest_spacetokendesc)
    return S_OK('Implement me')


  def makeDir(self,newdir):

    curr_cwd = self.cwd
    if newdir:
      if newdir[-1] == '/':
         newdir = newdir[:-1]

    self.changeDir(newdir)
    # Check if the directory already exists
    #exists = self.exists('dirac_directory')
    exists = 1

    if exists :
      self.cwd = curr_cwd
      if os.path.exists('dirac_directory'):
        os.remove('dirac_directory')
      return S_OK()

    url = self.getUrl(newdir)

    dfile = open("dirac_directory",'w')
    dfile.write("This is a DIRAC system directory")
    dfile.close()

    result = self.put("dirac_directory")
    if result['Status'] != 'OK':
      # Check if the file is being migrated to tape
      #if re.search("Device or resource busy",result['Message']):
      #  result = S_OK()
      #else:
      if result['Message'] == "File exists":
        result = S_OK()
      else:
        print "Making directory",newdir,"failed"

    self.cwd = curr_cwd
    if os.path.exists("dirac_directory"):
      os.remove("dirac_directory")

    return result

  def ls(self, path):
    result = S_ERROR("ls not yet implemented")
    return resulT

  def isdir(self,rdir):
    return self.exists(rdir+'/dirac_directory')

  def isfile(self,fname):
    return self.exists(fname)

  def exists(self,fpath):
    """ Check if the file exists on the storage
    """

    comm = "lcg-exist --vo lhcb "+self.getUrl(fpath)
    #print comm
    status,output,error,pythonError = exeCommand(comm,self.timeout)

    if re.search(' exists',output):
      return 1
    else:
      return 0

  def get(self,fname):
    """Get file

       Get a copy of the file fname in the current local directory
    """
    #self.touchProxy(12)
    source_url = self.getUrl(fname)
    dest_url = 'file:///'+os.getcwd()+'/'+os.path.basename(fname)
    verbose = ''
    if DEBUG:
      verbose = '-v'
    comm = 'lhcb-lcg-cp '+verbose+' --vo lhcb '+source_url+' '+dest_url
    if DEBUG:
      print comm
    status,out,error,pythonError = exeCommand(comm, self.long_timeout)
    if DEBUG:
      print "Status:",status
      print "Output:",out
      print "Error:",error
    if not status:
      return S_OK()
    else:
      if pythonError == 2:
        return S_ERROR("Timeout while get() call execution: file "+fname+' timeout '+str(self.long_timeout))
      else:
        return S_ERROR( "Failed to get file "+fname+ " with lcg-cp error "+str(status)+\
                        '\nOutput: '+out+'\nError: '+error )


  def getDir(self,gdir):
    print "Not yet implemented"

  def removeDir(self,rdir):
    print "Not yet implemented"

  def fsize(self, fname):
    """ Get the file size in the storage
    """
    url = self.getUrl(fname)
    return S_OK('Implement me')

  def getMetaData(self,fnames):
    """ Get all metadata associated with a file
    """
    return S_OK('Implement me')
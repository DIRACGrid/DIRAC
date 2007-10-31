""" This is the SRM2 StorageClass

    Implemented:
                getParameters()
                getUrl()
    Not implemented:

"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Subprocess import pythonCall
from stat import *
import types, re,os

loadPath = os.environ['LD_LIBRARY_PATH']
newLoadPath = '%s:%s' % ('/afs/cern.ch/project/gd/egee/glite/ui_PPS_glite3.1_UPDATE06/globus/lib',loadPath)
os.environ['LD_LIBRARY_PATH'] = newLoadPath
try:
  import lcg_util
  infoStr = 'Using lcg_util from: %s' % lcg_util.__file__
  gLogger.info(infoStr)
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

    # setting some variables for use with lcg_utils
    self.nobdii = 1
    self.defaulttype = 2
    self.vo = 'lhcb'
    self.nbstreams = 4
    self.verbose = 1
    self.conf_file = 'ignored'
    self.insecure = 0

  def getProtocol(self):
    return self.protocolName

  def getName(self):
    return self.name

  def getPath(self):
    return self.path
 
  def getHost(self):
    return self.host

  def getPort(self):
    return self.port

  def getSpaceToken(self):
    return self.spaceToken

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
    # If the filename supplied already contains the storage base path then do not add it again
    if re.search(self.path,path):
      if withPort:
        url = 'srm://%s%s' % (self.host,path)
      else:
        url = 'srm://%s:%s%s' % (self.host,self.port,path)
    # If it is not prepend it to the file name 
    else:
      pfnBase = self.getPFNBase(withPort)['Value']
      url = '%s%s' % (pfnBase,path)     
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
    gfalDict['timeout'] = 1000  

    errCode,gfalObject,errMessage = gfal.gfal_init(gfalDict)
    if not errCode == 0:
      errStr = "SRM2Storage:remove: Failed to initialise gfal_init: %s" % errMessage
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.debug("SRM2Storage.remove: Initialised gfal_init.")

    errCode,gfalObject,errMessage = gfal.gfal_deletesurls(gfalObject)
    if not errCode == 0:
      errStr = "SRM2Storage.remove: Failed to perform gfal_deletesurls: %s" % errMessage
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.debug("SRM2Storage.remove: Performed gfal_deletesurls.")

    numberOfResults,gfalObject,listOfResults = gfal.gfal_get_results(gfalObject)
    if numberOfResults <= 0:
      errStr = "SRM2Storage.remove: Did not obtain results with gfal_get_results."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.debug("SRM2Storage.remove: Retrieved %s results from gfal_get_results." % numberOfResults) 

    successRemove = []
    failRemove = []
    for dict in listOfResults:
      if dict['status'] == 0:
        successRemove.append(dict['surl'])
        gLogger.info("SRM2Storage.remove: %s removed successfully." % dict['surl'])
      elif dict['status'] == 2:
        # This is the case where the file doesn't exist. Should not be accounted or retried.
        gLogger.info("SRM2Storage.remove: %s %s." % (dict['surl'],os.strerror(dict['status'])))  
      else:
        failRemove.append(dict['surl'])
        gLogger.info("SRM2Storage.remove: %s %s." % (dict['surl'],os.strerror(dict['status'])))
 
    resDict = {'Success':successRemove,'Failed':failRemove}
    return S_OK(resDict)

  def removeDir(self,directoryPath):
    """ Remove the contents of the directory from the physical storage 
    """
    res = self.ls(directoryPath)
    if not res['OK']:
      return res
    resDict = res['Value'] 
    if directoryPath in resDict['FailedPaths']:
      errStr = "SRM2Storage.removeDir: Failed to list the contents of %s." % directoryPath
      gLogger.error(errStr)
      return S_ERROR(errStr)

    surlsInDir = []
    if not resDict['PathDetails'][directoryPath]['Directory']:
      errStr = "SRM2Storage.removeDir: The supplied path was not a directory."
      gLogger.error(errStr)
      return S_ERROR(errStr)
        
    surlsInDir = []
    filesDict = resDict['PathDetails'][directoryPath]['Files']
    surlsInDir = filesDict.keys()
    res = self.remove(surlsInDir)
    if not res['OK']:
      return res

    sizeRemoved = 0
    
    for surl in surlsInDir:
      if surl in res['Value']['Success']:
        sizeRemoved += filesDict[surl]['Size']  
    res['SizeRemoved'] = sizeRemoved
    return res

  ################################################################################
  #   
  # The methods below are for listing operations
  #

  def ls(self,fname):
    """ The supplied argmuments should either be a list of files or a directory. It should not be a list of directories.
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
    gfalDict['srmv2_lslevels'] = 1
    gfalDict['timeout'] = 1000
    
    errCode,gfalObject,errMessage = gfal.gfal_init(gfalDict)
    if not errCode == 0:
      errStr = "SRM2Storage:ls: Failed to initialise gfal_init: %s" % errMessage
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.debug("SRM2Storage:ls: Initialised gfal_init.")

    errCode,gfalObject,errMessage = gfal.gfal_ls(gfalObject)
    if not errCode == 0:
      errStr = "SRM2Storage.ls: Failed to perform gfal_ls: %s" % errMessage
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.debug("SRM2Storage.ls: Performed gfal_ls.")
      
    numberOfResults,gfalObject,listOfResults = gfal.gfal_get_results(gfalObject)
    if numberOfResults <= 0:
      errStr = "SRM2Storage.ls: Did not obtain results with gfal_get_results."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.debug("SRM2Storage.ls: Retrieved %s results from gfal_get_results." % numberOfResults)

    pathsDict = {}
    failedList = []
    # Each of the original paths can be a file or a directory
    for pathDict in listOfResults:
      if pathDict['status'] == 0:
        pathSURL = self.getUrl(pathDict['surl'])['Value']
        pathLocality = pathDict['locality']
        if re.search('ONLINE',pathLocality):
          pathCached = 1
        else:
          pathCached = 0
        if re.search('NEARLINE',pathLocality):
          pathMigrated = 1
        else:
          pathMigrated = 0
        pathStat = pathDict['stat']
        pathSize = pathStat[ST_SIZE]
        pathIsDir = S_ISDIR(pathStat[ST_MODE])
       
        if not pathIsDir:
          # In the case that the path supplied is a file
          fileDict = {'Directory':0,'Size':pathSize,'Cached':pathCached,'Migrated':pathMigrated}
          pathsDict[pathSURL] = fileDict
          gLogger.info("SRM2Storage.ls: %s %s %s." % (pathSURL.ljust(125),str(pathSize).ljust(10),pathLocality))

        else:
          gLogger.info("SRM2Storage.ls: %s" % (pathSURL))
          pathsDict[pathSURL] = {'Directory':1}
          subDirs = [] 
          subFiles = {}
          if pathDict.has_key('subpaths'):
            subPaths = pathDict['subpaths'] 

            # Parse the subpaths for the directory            
            for subPathDict in subPaths:
              subPathSURL = self.getUrl(subPathDict['surl'])['Value']
              subPathLocality = subPathDict['locality']
              if re.search('ONLINE',subPathLocality):
                subPathCached = 1          
              else:
                subPathCached = 0
              if re.search('NEARLINE',subPathLocality):
                subPathMigrated = 1  
              else:  
                subPathMigrated = 0  
              subPathStat = subPathDict['stat']
              subPathSize = subPathStat[ST_SIZE]
              subPathIsDir = S_ISDIR(subPathStat[ST_MODE])

              if subPathIsDir:
                # If the subpath is a directory
                subDirs.append(subPathSURL)
                gLogger.info("SRM2Storage.ls:\t%s" % (subPathSURL)) 
              else:
                # In the case that the subPath is a file
                fileDict = {'Directory':0,'Size':subPathSize,'Cached':subPathCached,'Migrated':subPathMigrated}
                subFiles[subPathSURL] = fileDict
                gLogger.info("SRM2Storage.ls:\t%s %s %s." % (subPathSURL.ljust(125),str(subPathSize).ljust(10),subPathLocality))              

          # Keep the infomation about this path's subpaths
          pathsDict[pathSURL]['SubDirs'] = subDirs
          pathsDict[pathSURL]['Files'] = subFiles           
          gLogger.info("SRM2Storage.ls:") 
      else:
        pathSURL = self.getUrl(pathDict['surl'])['Value']
        pathExplanation = pathDict['explanation']
        errorStr = os.strerror(pathDict['status'])
        gLogger.info("SRM2Storage.ls: %s : %s : %s ." % (pathSURL,pathExplanation,errorStr))
        failedList.append(pathSURL)
    resDict = {'PathDetails': pathsDict,'FailedPaths': failedList}
    return S_OK(resDict) 


  ################################################################################
  #
  # The methods below are not yet finished.
  #

  def put(self,fname):
    """ Put file to the current directory
    """
    dest_file = fname 
    src_file = 'file:///etc/group'
    srctype = 0
    dsttype = self.defaulttype
    src_spacetokendesc = ''
    dest_spacetokendesc = self.spaceToken
    errCode,errStr = lcg_util.lcg_cp3(src_file, dest_file, self.defaulttype, srctype, dsttype, self.nobdii, self.vo, self.nbstreams, self.conf_file, self.insecure, self.verbose, self.timeout,src_spacetokendesc,dest_spacetokendesc)
    if not errCode == 0:
      errStr = "SRM2Storage.put: Failed to put %s: %s." % (src_file,errStr)
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return S_OK()


  def makeDir(self,newdir):
    dfile = open("dirac_directory",'w')
    dfile.write("This is a DIRAC system directory")
    dfile.close()
    result = self.put("dirac_directory")
    return result

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
    """ Get a copt of the local file in the current local directory.
    """
    src_file = fname
    dest_file = 'file://%s/%s' % (os.curdir(),os.path.basename(fname))
    defaulttype = 2
    srctype = 2
    dsttype = 0
    nobdii = 1
    vo = 'lhcb'
    nbstreams = 4
    conf_file = 'ignore'
    insecure = 1
    verbose = 1
    timeout = 0
    src_spacetokendesc = self.spaceToken
    dest_spacetokendesc = '' 
    errCode,errStr = lcg_util.lcg_cp3(src_file, dest_file, defaulttype,srctype, dsttype, nobdii, vo,nbstreams,conf_file,insecure,verbose,timeout,src_spacetokendesc,dest_spacetokendesc)

  def getDir(self,gdir):
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

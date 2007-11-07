###############################################################################
# $Id: LcgFileCatalogClient.py,v 1.1 2007/11/07 13:52:11 acsmith Exp $
###############################################################################

""" Class for the LCG File Catalog Client"""

import os, re, string, commands, types
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC import gConfig
import DIRAC

__RCSID__ = "$Id: LcgFileCatalogClient.py,v 1.1 2007/11/07 13:52:11 acsmith Exp $"

try:
  import lfc
except ImportError, x:
  print "Failed to import lfc module !"  
  print str(x)
  
DEBUG = 0

class LcgFileCatalogClient:

  def __init__(self,infosys=None,host=None):

    self.host = host
    
    result = gConfig.getOption('/DIRAC/Setup')
    if not result['OK']:
      gLogger.fatal('Failed to get the /DIRAC/Setup')
      return
    setup = result['Value']
#    if infosys is None:
#      if not os.environ.has_key('LCG_GFAL_INFOSYS'):
#        infosys = cfgSvc.get(mode,'LCG_GFAL_INFOSYS')
#        os.environ['LCG_GFAL_INFOSYS'] = infosys
#    else:
#      os.environ['LCG_GFAL_INFOSYS'] = infosys
#    os.environ['LCG_CATALOG_TYPE'] = 'lfc'
#    if self.host is None:
#      self.host = cfgSvc.get(mode,'LFC_HOST')
#      os.environ['LFC_HOST'] = self.host
#    else:
#      os.environ['LFC_HOST'] = self.host  


    os.environ['LFC_HOST'] = host
    os.environ['LCG_GFAL_INFOSYS'] = infosys       

    result = gConfig.getOption('/DIRAC/Site')
    if not result['OK']:
      gLogger.error('Failed to get the /DIRAC/Site')
      self.site = 'Unknown'  
    else:  
      self.site = result['Value']
        
    self.prefix = '/grid'
    
    # Find out if the LCG utilities are from DIRAC installation
    # and set the corresponding path if this is the case
    dirac_path = DIRAC.__path__[0]
    self.lcg_path = dirac_path.replace('/DIRAC/python/DIRAC','/lcg')
    #self.lcg_path = "/home/saborido/LHCb/dirac/lcg"
    if not os.path.exists(self.lcg_path):
      self.lcg_path = ''

    self.timeout = 30
    self.session = False
    self.name = "LFC"
       
  def getName(self):
    return self.name
	
  def __make_comm(self,comm):
  
    result = ''
    if self.lcg_path:
      result = result + 'sh -c "source '+self.lcg_path+ \
               '/etc/profile.d/globus.sh '+ self.lcg_path+'; '
    result = result + comm
    if self.lcg_path:
      result = result + '"'
    
    return result   	
    
  def __make_comm_globus(self,comm):
  
    result = ''
    if self.lcg_path:
      result = result + 'sh -c "source '+self.lcg_path+ \
               '/etc/profile.d/globus.sh '+ self.lcg_path+'; '+ \
               '$AUX_LIBRARY_PATH/ld-linux.so.2 '+ \
               '--library-path ${AUX_LIBRARY_PATH}:${LCG_LIBRARY_PATH} '+ \
               '$GLOBUS_LOCATION/bin/'
    result = result + comm
    if self.lcg_path:
      result = result + '"'
    
    return result 
    
  def openSession(self):
    """Open the LFC client/server session"""
  
    #if not self.session:
    sessionName = 'DIRAC at %s' % self.site
    #sessionName = 'DIRAC_'+DIRAC.DIRAC_VERSION+' '+DIRAC.DIRAC_BUILD+' at '+self.site
    lfc.lfc_startsess(self.host,sessionName)
    self.session = True
      
  def closeSession(self): 
    """Close the LFC client/server session"""
  
    lfc.lfc_endsess()
    self.session = False  
    
  def setAuthorizationId(self,dn):
    """Set authorization id for the proxy-less LFC communication"""
    
    lfc.lfc_client_setAuthorizationId(0,0,'GSI',dn)    
     
  def _getSeFromPfn(self,pfn):
    se = ''
    reg = re.compile(r'(.*://)([-a-zA-Z0-9\.]*)(/)')
    m = reg.match(pfn)
    if m:
      se = m.group(2)
    return se

  def ls(self,path):
    """Make a directory listing """
    
    path = self.prefix+path

    result = {}
#    lfcapi = os.access('/home/atsareg/work/lcg/opt/lcg/bin/lfc-ls',os.X_OK)
#
#    if ( lfcapi == 0 ):
#      result['Status']= "Error"
#      result['Message'] = 'LFC API not installed'
#      return result

    #comm = '/opt/lcg/bin/lfc-ls ' + path
    comm = self.__make_comm('lfc-ls '+path)
    status,out=commands.getstatusoutput(comm)
    out = out.strip()

    result = {}
    res = re.search('No such file or directory',out)
    if res is not None:
      return S_ERROR( out )
    else:
      result = S_OK()
      result['Message'] = ""
      result['DirList'] = out.split('\n')
      return result

  def exists(self,path):
    """ Check if the path exists """
    
    path_pre = self.prefix+path

    result = S_OK()
    result['Exists'] = 0

    """ test for existence of the file/directory """
    amode = 0
    value = lfc.lfc_access(path_pre,amode)
    if (value == 0):
       result = S_OK()
       result['Exists'] = 1
    else:
       errno = lfc.cvar.serrno
       mess = lfc.sstrerror(errno).lower()
       if ( mess.find("no such file or directory") >= 0 ):
          result = S_OK()
          result['Exists'] = 0
       else:
          result = S_ERROR( lfc.sstrerror(errno) )
          result['Exists'] = 0

    return result

  def existsDir(self,path):
    """ Check if the directory path exists """

    result = self.exists(path)
    return result

  def existsGuid(self,guid):
    """ Check if the guid exists """

    result = S_OK()
    result['Exists'] = 0

    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg('',guid,fstat)
    if (value == 0):
       result = S_OK()
       result['Exists'] = 1
    else:
       errno = lfc.cvar.serrno
       mess = lfc.sstrerror(errno).lower()
       if ( mess.find("no such file or directory") >= 0 ):
          result = S_OK()
          result['Exists'] = 0
       else:
          result = S_ERROR( lfc.sstrerror(errno) )

    return result

  def addFiles(self,listOfTuples):
    """Add (register) a list of files to the catalog within a session"""

    result = S_OK()

    self.openSession()

    listOfMessages = []
    result['Message'] = "There is a message per added file in result['listOfMessages']"

    for etuple in listOfTuples:
      lfn,pfn,size,se,guid = etuple
      res = self.addFile(lfn,pfn,size,se,guid)
      if ( res['Status'] == 'OK' ):
         listOfMessages.append("File " + pfn + " added to the catalog")
      else:
         listOfMessages.append("Failed to add file " + pfn + "\n" + res['Message'])
         result['Status']= "Error"
      
    self.closeSession()

    result['listOfMessages'] = listOfMessages
    return result

  def addFile(self,lfn,pfn,size,se,guid):
    """Add (register) a file to the catalog"""

    if DEBUG:
      print "addFile:",lfn,pfn,size,se,guid
    
    result = {}

    #Check we can get the Storage Element Hostname
    if ( not se ):
       se = self._getSeFromPfn(pfn)
       if ( not se ):
          return S_ERROR( "'se' must be a fully qualified Storage Element hostname" )

    #Check the size of the file is correctly specified
    try:
      lsize = long(size)
    except:
      return S_ERROR( "The size of the file must be an 'int','long' or 'string'" )

        #Check the GUID is provided and is unique
    if ( not guid ):
       return S_ERROR( "No GUID provided" )
    else:
       tguid = self.existsGuid(guid)
       if ( tguid['Status'] == "OK" and tguid['Exists'] == 1 ):
          return S_ERROR( "The GUID already exists" )

    #Check the LFN does not exist yet
    tlfn = self.exists(lfn)
    if ( tlfn['Status'] == 'OK' and tlfn['Exists'] == 1 ):
       return S_ERROR( "The LFN already exists, use function addPfn instead" )

    #################### Start a transaction ################
    lfc.lfc_starttrans(self.host,'Transaction: DIRAC_'+DIRAC.DIRAC_VERSION+' '+ \
                       DIRAC.DIRAC_BUILD+' at '+self.site)

    #Make the directories recursively if needed
    bdir = os.path.dirname(lfn)
    ex = self.existsDir(bdir)
    if not ex['Exists']:
       result = self.makedirs(bdir)
       if result['Status'] != 'OK':
          lfc.lfc_aborttrans()
          return result

    #Create a new file in the LFC (WE HAVE TO THINK ABOUT THE ACCESS PERMISSION)
    lfnadd = self.prefix+lfn
    value = lfc.lfc_creatg(lfnadd,guid,0664)
    if (value != 0):
       errno = lfc.cvar.serrno
       lfc.lfc_aborttrans()
       return S_ERROR( "Failed to create guid\n" + lfc.sstrerror(errno) )

    #Set the size of the file
    value = lfc.lfc_setfsizeg(guid,lsize,'','')
    if (value != 0):
       errno = lfc.cvar.serrno
       lfc.lfc_aborttrans()
       return S_ERROR( "Failed to set the file size\n" + lfc.sstrerror(errno) )

    #################### End the transaction ################
    lfc.lfc_endtrans()

    #Finally, register the pfn replica
    res = self.addPfn(lfn,pfn,se)
    if ( res['Status'] == 'OK' ):
       result = S_OK()
       result['Message'] = "File added to the catalog"
    else:
       result = res
       self.removeLfn(lfn)

    return result

  def addPfns(self,listOfTuples):
    """Add a list of replicas (pfn) to the catalog within a session"""

    result = S_OK()

    self.openSession()

    listOfMessages = []
    result['Message'] = "There is a message per added file in result['listOfMessages']"

    for etuple in listOfTuples:
      lfn,pfn,se = etuple
      res = self.addPfn(lfn,pfn,se)
      if res['OK']:
         listOfMessages.append("File " + pfn + " inserted in the catalog")
      else:
         listOfMessages.append("Failed to insert file " + pfn + "\n" + res['Message'])
      
    self.closeSession()

    result['listOfMessages'] = listOfMessages
    return result

  def addPfn(self,lfn,pfn,se,guid=''):
    """Add a replica (pfn) of an existing (lfn) file"""
    
    if DEBUG:
      print "addPfn:",lfn,pfn,se
        
    if not guid:
       tst1 = self.getGuidByLfn(lfn)
       if ( not tst1.has_key('GUID') ):
          return S_ERROR( "There is no GUID for file "+lfn )
       else:
          guid = tst1['GUID']

    fid = lfc.lfc_fileid()
    status = '-'
    f_type = 'D'
    poolname = ''
    fs = ''

    if ( not se ):
       se = self._getSeFromPfn(pfn)
       if ( not se ):
          return S_ERROR( "'se' must be a fully qualified Storage Element hostname" )

    value = lfc.lfc_addreplica(guid,fid,se,pfn,status,f_type,poolname,fs)

    if (value == 0):
        result = S_OK()
        result['Message'] = "Inserted in the catalog"
        return result
    else:
        errno = lfc.cvar.serrno
        result = S_ERROR(lfc.sstrerror(errno))
        if result['Message'] == "File exists":
          # This is not an error but a duplicate registration - to review !
          result = S_OK()
          result['Message'] = "File already exists" 
        return result

  def removeLfns(self,listOfLfn):
    """Remove a list of files (lfn) from the catalog"""

    result = S_OK()

    self.openSession()

    listOfMessages = []
    result['Message'] = "There is a message per added file in result['listOfMessages']"

    for lfn in listOfLfn:
      res = self.removeLfn(lfn)
      if ( res['Status'] == 'OK' ):
         listOfMessages.append("File " + lfn + " removed from the catalog")
      else:
         listOfMessages.append("Failed to remove file " + lfn + "\n" + res['Message'])
         result['Status']= "Error"
      
    self.closeSession()

    result['listOfMessages'] = listOfMessages
    return result

  def removeLfn(self,lfn):
    """Remove a file (lfn) from the catalog"""

    if ( self.exists(lfn)['Exists'] == 1):
        lfnrem = self.prefix+lfn
        value = lfc.lfc_unlink(lfnrem)
        if (value == 0):
            result = S_OK()
            result['Message'] = "Removed from the catalog"
            return result
        else:
            errno = lfc.cvar.serrno
            return S_ERROR( lfc.sstrerror(errno) )
    else:
        result = S_OK()
        result['Message'] = "File does not exist"
        return result

  def rmFile(self,lfn):
    """Remove a file (lfn) from the catalog"""

    result = self.removeLfn(lfn)
    return result

  def rmFiles(self,listOfLfn):
    """Remove a list of files (lfn) from the catalog"""

    result = self.removeLfns(listOfLfn)
    return result

  def removePfns(self,listOfPfn):
    """Remove a list of replicas (pfn) from the catalog"""

    result = S_OK()

    self.openSession()

    listOfMessages = []
    result['Message'] = "There is a message per added file in result['listOfMessages']"

    for pfn in listOfPfn:
      res = self.removePfn('',pfn)
      if ( res['Status'] == 'OK' ):
         listOfMessages.append("Replica " + pfn + " removed from the catalog")
      else:
         listOfMessages.append("Failed to remove replica " + pfn + "\n" + res['Message'])
         result['Status']= "Error"
      
    self.closeSession()

    result['listOfMessages'] = listOfMessages
    return result

  def removePfn(self,lfn,pfn):
    """Remove the pfn replica"""

    result = {}

    tst = self.getGuidByPfn(pfn)
    if ( not tst.has_key('GUID') ):
       result = S_OK()
       result['Message'] = "There is no file "+pfn
       return result

    guid = tst['GUID']

    fid = lfc.lfc_fileid()
    value = lfc.lfc_delreplica(guid,fid,pfn)
    if (value == 0):
       return S_OK()
    else:
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

  def getFileSize(self,lfn):
    """ Get the file size """

    lfn = self.prefix+lfn

    result = {}
    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg(lfn,'',fstat)
    if (value == 0):
       result = S_OK()
       result['FileSize'] = fstat.filesize
       return result
    else:
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

  def mkdir(self,dirname):
    """Make a directory in the catalog"""

    result = {}
    # WE HAVE TO THINK MORE ABOUT PERMISSIONS
    dirname = self.prefix+dirname
    value = lfc.lfc_mkdir(dirname, 0775)
    if (value == 0):
       return S_OK()
    else:
       errno = lfc.cvar.serrno
       return S_ERROR( "Failed to make directory " + dirname + " : " + lfc.sstrerror(errno) )

  def makedirs(self,path):
    """Make all the directories recursively in the path"""

    dir = os.path.dirname(path)
    res = self.existsDir(path)
 
    if res['Status'] != "OK":
      result = S_ERROR('Makedirs failed for directory '+path+": "+res['Message'])
      return result  
    if res['Exists']:
      return S_OK()

    res = self.existsDir(dir)
        
    if res['Exists']:
      result = self.mkdir(path)
    else:
      result = self.makedirs(dir)
      result = self.mkdir(path)

    return result

  def rmdir(self,dirname):
    """Remove an empty directory from the catalog"""

    dirname = self.prefix+dirname
    value = lfc.lfc_rmdir(dirname)
    if (value == 0):
       return S_OK()
    else:
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    return result

  def getPfnsByLfn(self,lfn,opensession=False):
    """  Returns a list of replicas for the given lfn through the getReplica method (DIRAC3-ised)
    """
    # This still allows a session to be opened
    if opensession: self.openSession()

    repdict = {}
    fullLfn = '%s%s' % (self.prefix,lfn)
    value,replicaObjects = lfc.lfc_getreplica(fullLfn,'','')

    if not (value == 0):
       errno = lfc.cvar.serrno
       if opensession: self.closeSession()
       return S_ERROR(lfc.sstrerror(errno))
 
    for replica in replicaObjects:
      SE = replica.host
      pfn = replica.sfn
      repdict[SE] = pfn.strip()

    if opensession: self.closeSession()
    return S_OK(repdict)

  def getPfnsByLfnList(self,lfns):
    """ Returns replicas for a list of lfns (DIRAC3-ised)
    """
    resdict = {}
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.openSession()
    for lfn in lfns:
      res = self.getPfnsByLfn(lfn,False)
      if res['OK']:
        resdict[lfn] = res['Value']
      else:
        resdict[lfn] = {}
    if self.session:
      self.closeSession()
    return S_OK(resdict)    

  def getPfnsInDir(self,path):
    """Returns a list of replicas for the given directory"""
    
    result = S_OK()

    tlfn = self.existsDir(path)
    
    if ( tlfn['Status'] == 'OK' and tlfn['Exists'] == 0 ):
       return S_ERROR( "Directory "+path+" does not exist in catalog" )
    
    
    result = self.ls(path)
    dirlist = result['DirList']
    
    repdict = {}
    resdict = {}
    path_pre = self.prefix+path

    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg(path_pre,'',fstat)
    nbfiles = fstat.nlink
    direc = lfc.lfc_opendirg(path_pre,'')

    for num in range(nbfiles):
       entry,lista = lfc.lfc_readdirxr(direc,"")
       lfn = path+"/"+dirlist[num]  
       repdict = {}
       # if lfn is a directory, lista will be None
       if lista:
         for rep in lista:
            SE  = rep.host
            sfn = rep.sfn
            repdict[SE] = sfn
         resdict[lfn] = repdict   
              
    lfc.lfc_closedir(direc)
    result["Replicas"] = resdict
    return result

  def getPfnsByGuid(self,guid):
    """Returns a list of replicas for the given guid using lfc_getreplica"""

    self.openSession()

    result = {}

    tguid = self.existsGuid(guid)
    if ( tguid['Status'] == "OK" and tguid['Exists'] == 0 ):
       self.closeSession()
       return S_ERROR( "The GUID "+guid+" does not exist." )

    repdict = {}
    nbentries,listreplicas = lfc.lfc_getreplica('',guid,'')

    if listreplicas:
       result = S_OK()
    else:
       errno = lfc.cvar.serrno
       self.closeSession()
       return S_ERROR(lfc.sstrerror(errno))

    for line in range(len(listreplicas)):
       SE = listreplicas[line].host
       sfn = listreplicas[line].sfn
       repdict[SE] = sfn.strip()

    self.closeSession()
    result["Replicas"] = repdict
    return result

  def getPfnsByGuid_listreplica(self,guid):
    """Returns a list of replicas for the given guid"""

    result = {}

    tguid = self.existsGuid(guid)
    if ( tguid['Status'] == "OK" and tguid['Exists'] == 0 ):
       return S_ERROR( "The GUID "+guid+" does not exist." )

    repdict = {}
    list = lfc.lfc_list()
    listreplicas = lfc.lfc_listreplica('',guid,lfc.CNS_LIST_BEGIN,list)

    if listreplicas: 
       result = S_OK()
    else:
       lfc.lfc_listreplica('',guid,lfc.CNS_LIST_END,list)
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    while listreplicas:
       SE = listreplicas.host
       sfn = listreplicas.sfn
       repdict[SE] = sfn.strip()
       listreplicas = lfc.lfc_listreplica('',guid,lfc.CNS_LIST_CONTINUE,list)
    else:
       lfc.lfc_listreplica('',guid,lfc.CNS_LIST_END,list)

    result["Replicas"] = repdict
    return result

  def getPfnsByPfn(self,pfn):
    """Returns a list of replicas for the given pfn"""


    tst = self.getGuidByPfn(pfn)
    if ( not tst.has_key('GUID') ):
       return S_ERROR( "There is no GUID for file "+pfn )

    guid = tst['GUID']
    return self.getPfnsByGuid(guid)

  def getLfnsByGuid(self,guid):
    """Returns a list of lfns for the given guid"""

    tguid = self.existsGuid(guid)
    if ( tguid['Status'] == "OK" and tguid['Exists'] == 0 ):
       return S_ERROR( "The GUID "+guid+" does not exist." )

    list = lfc.lfc_list()
    lfnlist = []
    listlinks = lfc.lfc_listlinks('',guid,lfc.CNS_LIST_BEGIN,list)

    if listlinks: 
       result = S_OK()
    else:
       lfc.lfc_listlinks('',guid,lfc.CNS_LIST_END,list)
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    while listlinks:
       ll = listlinks.path
       if re.search ('^'+self.prefix,ll):
          ll = listlinks.path.replace(self.prefix,"",1)
       lfnlist.append(ll)
       listlinks = lfc.lfc_listlinks('',guid,lfc.CNS_LIST_CONTINUE,list)
    else:
       lfc.lfc_listlinks('',guid,lfc.CNS_LIST_END,list)

    result["LFN"] = lfnlist
    return result

  def getLfnsByPfn(self,pfn):
    """Returns a list of lfns for the given pfn"""

    result = {}

    res = self.getGuidByPfn(pfn)
    if ( res['Status'] == 'OK' ):
       guid = res['GUID']
    else:
       result = res
       return result

    return self.getLfnsByGuid(guid)

  def getGuidByLfn(self,lfn):
    """Get the GUID for the given lfn"""

    result = {}

    fstat = lfc.lfc_filestatg()
    lfng = self.prefix+lfn
    value = lfc.lfc_statg(lfng,'',fstat)
    if (value == 0):
       result = S_OK()
       result['GUID'] = fstat.guid
       return result
    else:
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )
       
  def getGUIDsByLfnList(self,lfns):
    """Get GUIDs for list of lfns
    """
    
    resdict = {}
    self.openSession()
    for lfn in lfns:
      result = self.getGuidByLfn(lfn)
      if result['Status'] == 'OK':
        resdict[lfn] = result['GUID']
      else:
        resdict[lfn] = {} 

    self.closeSession()
    result = S_OK()
    result['Guids'] = resdict
    return result     

  def getGuidByPfn(self,pfn):
    """Get the GUID for the given pfn"""

    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statr(pfn,fstat)
    if (value == 0):
       result = S_OK()
       result['GUID'] = fstat.guid
       return result
    else:
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

  def getACL(self,lfn):
    """Get the ACL for the given lfn"""

    result = S_OK()
    lfng = self.prefix+lfn
    nentries, acl_list = lfc.lfc_getacl(lfng, lfc.CA_MAXACLENTRIES)
    if (nentries == -1):
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    dictu = {}
    dictg = {}
    for acl in acl_list:
       if ( acl.a_type == lfc.CNS_ACL_USER_OBJ ):
          result["owner"] = acl.a_id, acl.a_perm
       elif ( acl.a_type == lfc.CNS_ACL_USER ):
          dictu[str(acl.a_id)] = acl.a_perm
       elif ( acl.a_type == lfc.CNS_ACL_GROUP_OBJ ):
          result["groupowner"] = acl.a_id, acl.a_perm
       elif ( acl.a_type == lfc.CNS_ACL_GROUP ):
          dictg[str(acl.a_id)] = acl.a_perm
       elif ( acl.a_type == lfc.CNS_ACL_MASK ):
          result["mask"] = acl.a_perm
       elif ( acl.a_type == lfc.CNS_ACL_OTHER ):
          result["other"] = acl.a_perm
       elif ( acl.a_type == lfc.CNS_ACL_DEFAULT ):
          result["default"] = acl.a_perm
          
       result["users"] = dictu
       result["groups"] = dictg

    return result

  def setMask(self,lfn,perm):
    """Set a mask for a given lfn"""

    result = S_OK()
    lfng = self.prefix+lfn
    nentries, acl_list = lfc.lfc_getacl(lfng, lfc.CA_MAXACLENTRIES)
    if (nentries == -1):
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    ind = 0
    indrm = -1
    for acl in acl_list:
       if ( acl.a_type == lfc.CNS_ACL_MASK ): indrm = ind
       ind = ind + 1

    if ( indrm != -1 ): del acl_list[indrm]

    acl_mask = lfc.lfc_acl()
    acl_mask.a_type = lfc.CNS_ACL_MASK
    acl_mask.a_id = 0
    acl_mask.a_perm = perm
    acl_list.append(acl_mask)

    value = lfc.lfc_setacl(lfng,acl_list)
    if value != 0:
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    result['Message'] = "Set mask " + str(perm) + " for " + lfng
    return result

  def addUserACL(self,lfn,uid,perm):
    """Add a user ACL for a given lfn"""

    result = S_OK()
    lfng = self.prefix+lfn
    nentries, acl_list = lfc.lfc_getacl(lfng, lfc.CA_MAXACLENTRIES)
    if (nentries == -1):
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    acl_user = lfc.lfc_acl()
    acl_user.a_type = lfc.CNS_ACL_USER
    acl_user.a_id = uid
    acl_user.a_perm = perm
    acl_list.append(acl_user)

    #We need to add a mask only for the first additional user.
    #Adding a mask when not needed produces an error.
    addmask = 1
    for acl in acl_list:
       if ( acl.a_type == lfc.CNS_ACL_MASK ): addmask = 0

    if addmask:
       acl_mask = lfc.lfc_acl()
       acl_mask.a_type = lfc.CNS_ACL_MASK
       acl_mask.a_id = 0
       acl_mask.a_perm = perm
       acl_list.append(acl_mask)

    value = lfc.lfc_setacl(lfng,acl_list)
    if value != 0:
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    result['Message'] = "ACL " + str(perm) + " added for user " + str(uid)
    return result

  def delUserACL(self,lfn,uid):
    """Remove a user ACL for a given lfn"""

    result = S_OK()
    lfng = self.prefix+lfn
    nentries, acl_list = lfc.lfc_getacl(lfng, lfc.CA_MAXACLENTRIES)
    if (nentries == -1):
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    ind = 0
    indrm = -1
    for acl in acl_list:
       if ( acl.a_type==lfc.CNS_ACL_USER and acl.a_id==uid ): indrm = ind
       ind = ind + 1

    result['Message'] = "User " + str(uid) + " does not have an ACL"
    if ( indrm != -1 ):
       del acl_list[indrm]
       value = lfc.lfc_setacl(lfng,acl_list)
       if value != 0:
          errno = lfc.cvar.serrno
          return S_ERROR( lfc.sstrerror(errno) )
       else:
          result['Message'] = "ACL removed for user " + str(uid)

    return result

  def addGroupACL(self,lfn,gid,perm):
    """Add a group ACL for a given lfn"""

    result = S_OK()
    lfng = self.prefix+lfn
    nentries, acl_list = lfc.lfc_getacl(lfng, lfc.CA_MAXACLENTRIES)
    if (nentries == -1):
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    acl_group = lfc.lfc_acl()
    acl_group.a_type = lfc.CNS_ACL_GROUP
    acl_group.a_id = gid
    acl_group.a_perm = perm
    acl_list.append(acl_group)

    value = lfc.lfc_setacl(lfng,acl_list)
    if value != 0:
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    result['Message'] = "ACL " + str(perm) + " added for group " + str(gid)
    return result

  def delGroupACL(self,lfn,gid):
    """Remove a group ACL for a given lfn"""

    result = S_OK()
    lfng = self.prefix+lfn
    nentries, acl_list = lfc.lfc_getacl(lfng, lfc.CA_MAXACLENTRIES)
    if (nentries == -1):
       errno = lfc.cvar.serrno
       return S_ERROR( lfc.sstrerror(errno) )

    ind = 0
    indrm = -1
    for acl in acl_list:
       if ( acl.a_type==lfc.CNS_ACL_GROUP and acl.a_id==gid ): indrm = ind
       ind = ind + 1

    result['Message'] = "Group " + str(gid) + " does not have an ACL"
    if ( indrm != -1 ):
       del acl_list[indrm]
       value = lfc.lfc_setacl(lfng,acl_list)
       if value != 0:
          errno = lfc.cvar.serrno
          return S_ERROR( lfc.sstrerror(errno) )
       else:
          result['Message'] = "ACL removed for group " + str(gid)

    return result

  def showTags(self,dirname):
    return S_ERROR( 'Function not yet implemented' )

  def addTag(self,dirname, tagname):
    return S_ERROR( 'Function not yet implemented' )

  def removeTag(self,dirname, tagname):
    return S_ERROR( 'Function not yet implemented' )

  def addTagValue(self,file, tagname, var, value):
    return S_ERROR( 'Function not yet implemented' )

  def updateTagVal(self,file, tagname, var, value):
    return S_ERROR( 'Function not yet implemented' )

  def removeTagValue(self,dirname, tagname, var):
    return S_ERROR( 'Function not yet implemented' )


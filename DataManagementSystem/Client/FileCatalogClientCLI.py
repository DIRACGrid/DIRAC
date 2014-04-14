#!/usr/bin/env python
""" File Catalog Client Command Line Interface. """

__RCSID__ = "$Id$"

import stat
import cmd
import commands
import os.path
import time
import sys
from types  import DictType, ListType
from DIRAC  import gConfig
from DIRAC.Core.Security import CS
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Utilities.PrettyPrint import int_with_commas, printTable

from DIRAC.DataManagementSystem.Client.CmdDirCompletion.AbstractFileSystem import DFCFileSystem, UnixLikeFileSystem
from DIRAC.DataManagementSystem.Client.CmdDirCompletion.DirectoryCompletion import DirectoryCompletion

class DirectoryListing:
  
  def __init__(self):
    
    self.entries = []
  
  def addFile(self,name,fileDict,repDict,numericid):
    """ Pretty print of the file ls output
    """        
    perm = fileDict['Mode']
    date = fileDict['ModificationDate']
    #nlinks = fileDict.get('NumberOfLinks',0)
    nreplicas = len( repDict )
    size = fileDict['Size']
    if fileDict.has_key('Owner'):
      uname = fileDict['Owner']
    elif fileDict.has_key('OwnerDN'):
      result = CS.getUsernameForDN(fileDict['OwnerDN'])
      if result['OK']:
        uname = result['Value']
      else:
        uname = 'unknown' 
    else:
      uname = 'unknown'
    if numericid:
      uname = str(fileDict['UID'])
    if fileDict.has_key('OwnerGroup'):
      gname = fileDict['OwnerGroup']
    elif fileDict.has_key('OwnerRole'):
      groups = CS.getGroupsWithVOMSAttribute('/'+fileDict['OwnerRole'])
      if groups: 
        if len(groups) > 1:
          gname = groups[0]
          default_group = gConfig.getValue('/Registry/DefaultGroup','unknown')
          if default_group in groups:
            gname = default_group
        else:
          gname = groups[0]
      else:
        gname = 'unknown' 
    else:
      gname = 'unknown'     
    if numericid:
      gname = str(fileDict['GID'])
    
    self.entries.append( ('-'+self.__getModeString(perm),nreplicas,uname,gname,size,date,name) )
    
  def addDirectory(self,name,dirDict,numericid):
    """ Pretty print of the file ls output
    """    
    perm = dirDict['Mode']
    date = dirDict['ModificationDate']
    nlinks = 0
    size = 0
    if dirDict.has_key('Owner'):
      uname = dirDict['Owner']
    elif dirDict.has_key('OwnerDN'):
      result = CS.getUsernameForDN(dirDict['OwnerDN'])
      if result['OK']:
        uname = result['Value']
      else:
        uname = 'unknown'
    else:
      uname = 'unknown'
    if numericid:
      uname = str(dirDict['UID'])
    if dirDict.has_key('OwnerGroup'):
      gname = dirDict['OwnerGroup']
    elif dirDict.has_key('OwnerRole'):
      groups = CS.getGroupsWithVOMSAttribute('/'+dirDict['OwnerRole'])
      if groups:
        if len(groups) > 1:
          gname = groups[0]
          default_group = gConfig.getValue('/Registry/DefaultGroup','unknown')
          if default_group in groups:
            gname = default_group
        else:
          gname = groups[0]
      else:
        gname = 'unknown'
    if numericid:
      gname = str(dirDict['GID'])
    
    self.entries.append( ('d'+self.__getModeString(perm),nlinks,uname,gname,size,date,name) )  
    
  def addDataset(self,name,datasetDict,numericid):
    """ Pretty print of the dataset ls output
    """    
    perm = datasetDict['Mode']
    date = datasetDict['ModificationDate']
    size = datasetDict['TotalSize']
    if datasetDict.has_key('Owner'):
      uname = datasetDict['Owner']
    elif datasetDict.has_key('OwnerDN'):
      result = CS.getUsernameForDN(datasetDict['OwnerDN'])
      if result['OK']:
        uname = result['Value']
      else:
        uname = 'unknown'
    else:
      uname = 'unknown'
    if numericid:
      uname = str( datasetDict['UID'] )
      
    gname = 'unknown'  
    if datasetDict.has_key('OwnerGroup'):
      gname = datasetDict['OwnerGroup']
    if numericid:
      gname = str( datasetDict ['GID'] )
    
    numberOfFiles = datasetDict ['NumberOfFiles']
        
    self.entries.append( ('s'+self.__getModeString(perm),numberOfFiles,uname,gname,size,date,name) )   
    
  def __getModeString(self,perm):
    """ Get string representation of the file/directory mode
    """  
    
    pstring = ''
    if perm & stat.S_IRUSR:
      pstring += 'r'
    else:
      pstring += '-'
    if perm & stat.S_IWUSR:
      pstring += 'w'
    else:
      pstring += '-'
    if perm & stat.S_IXUSR:
      pstring += 'x'
    else:
      pstring += '-'    
    if perm & stat.S_IRGRP:
      pstring += 'r'
    else:
      pstring += '-'
    if perm & stat.S_IWGRP:
      pstring += 'w'
    else:
      pstring += '-'
    if perm & stat.S_IXGRP:
      pstring += 'x'
    else:
      pstring += '-'    
    if perm & stat.S_IROTH:
      pstring += 'r'
    else:
      pstring += '-'
    if perm & stat.S_IWOTH:
      pstring += 'w'
    else:
      pstring += '-'
    if perm & stat.S_IXOTH:
      pstring += 'x'
    else:
      pstring += '-'    
      
    return pstring  
  
  def printListing(self,reverse,timeorder):
    """
    """
    if timeorder:
      if reverse:
        self.entries.sort(key=lambda x: x[5]) 
      else:  
        self.entries.sort(key=lambda x: x[5],reverse=True) 
    else:  
      if reverse:
        self.entries.sort(key=lambda x: x[6],reverse=True) 
      else:  
        self.entries.sort(key=lambda x: x[6]) 
        
    # Determine the field widths
    wList = [ 0 for _x in range(7) ]
    for d in self.entries:
      for i in range(7):
        if len(str(d[i])) > wList[i]:
          wList[i] = len(str(d[i]))
        
    for e in self.entries:
      print str(e[0]),
      print str(e[1]).rjust(wList[1]),
      print str(e[2]).ljust(wList[2]),
      print str(e[3]).ljust(wList[3]),
      print str(e[4]).rjust(wList[4]),
      print str(e[5]).rjust(wList[5]),
      print str(e[6])

  def addSimpleFile(self,name):
    """ Add single files to be sorted later"""
    self.entries.append(name)

  def printOrdered(self):
    """ print the ordered list"""
    self.entries.sort()
    for entry in self.entries:
      print entry

class FileCatalogClientCLI(cmd.Cmd):
  """ usage: FileCatalogClientCLI.py xmlrpc-url.

    The URL should use HTTP protocol, and specify a port.  e.g.::

        http://localhost:7777

    This provides a command line interface to the FileCatalog Exported API::

        ls(path) - lists the directory path

    The command line interface to these functions can be listed by typing "help"
    at the prompt.

    Other modules which want access to the FileCatalog API should simply make
    their own internal connection to the XMLRPC server using code like::

        server = xmlrpclib.Server(xmlrpc_url)
        server.exported_function(args)
  """

  intro = """
File Catalog Client $Revision: 1.17 $Date: 
            """

  def __init__(self, client):
    cmd.Cmd.__init__(self)
    self.fc = client
    self.cwd = '/'
    self.prompt = 'FC:'+self.cwd+'> '
    self.previous_cwd = '/'

    self.dfc_fs = DFCFileSystem(self.fc)
    self.lfn_dc = DirectoryCompletion(self.dfc_fs)

    self.ul_fs = UnixLikeFileSystem()
    self.ul_dc = DirectoryCompletion(self.ul_fs)

  def getPath(self,apath):

    if apath.find('/') == 0:
      path = apath
    else:
      path = self.cwd+'/'+apath
      path = path.replace('//','/')

    return os.path.normpath(path)
  
  def do_register(self,args):
    """ Register a record to the File Catalog
    
        usage:
          register file <lfn> <pfn> <size> <SE> [<guid>]  - register new file record in the catalog
          register replica <lfn> <pfn> <SE>   - register new replica in the catalog
    """
    
    argss = args.split()
    if (len(argss)==0):
      print self.do_register.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'file':
      if (len(argss) < 4):
        print self.do_register.__doc__
        return
      return self.registerFile(argss)
    elif option == 'pfn' or option == "replica":
      # TODO
      # Is the __doc__ not complete ?
      if (len(argss) != 3):
        print self.do_register.__doc__
        return
      return self.registerReplica(argss)
    else:
      print "Unknown option:",option

  # An Auto Completion For ``register``
  _available_register_cmd = ['file', 'replica']
  def complete_register(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) >= 2 and (args[1] in self._available_register_cmd):
      # if 'register file' or 'register replica' exists,
      # try to do LFN auto completion.
      cur_path = ""
      if (len(args) == 3):
        cur_path = args[2]
      result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)
      return result

    result = [i for i in self._available_register_cmd if i.startswith(text)]
    return result
  
  def do_add(self,args):
    """ Upload a new file to a SE and register in the File Catalog
    
        usage:
        
          add <lfn> <pfn> <SE> [<guid>] 
    """
    
    # ToDo - adding directories
    
    argss = args.split()
    
    if len(argss) < 3:
      print "Error: insufficient number of arguments"
      return
    
    lfn = argss[0]
    lfn = self.getPath(lfn)
    pfn = argss[1]
    se = argss[2]
    guid = None
    if len(argss)>3:
      guid = argss[3]
        
    dirac = Dirac()
    result = dirac.addFile(lfn,pfn,se,guid,printOutput=False)
    if not result['OK']:
      print 'Error: %s' %(result['Message'])
    else:
      print "File %s successfully uploaded to the %s SE" % (lfn,se)  

  def complete_add(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_get(self,args):
    """ Download file from grid and store in a local directory
    
        usage:
        
          get <lfn> [<local_directory>] 
    """
    
    argss = args.split()
    if (len(argss)==0):
      print self.do_get.__doc__
      return
    lfn = argss[0]
    lfn = self.getPath(lfn)
    dir_ = ''
    if len(argss)>1:
      dir_ = argss[1]
        
    dirac = Dirac()
    localCWD = ''
    if dir_:
      localCWD = os.getcwd()
      os.chdir(dir_)
    result = dirac.getFile(lfn)
    if localCWD:
      os.chdir(localCWD)
      
    if not result['OK']:
      print 'Error: %s' %(result['Message'])
    else:
      print "File %s successfully downloaded" % lfn      

  def complete_get(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

  def do_unregister(self,args):
    """ Unregister records in the File Catalog
    
        usage:
          unregister replica  <lfn> <se>
          unregister file <lfn>
          unregister dir <path>
    """        
    argss = args.split()
    if (len(argss)==0):
      print self.do_unregister.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'replica':
      if (len(argss) != 2):
        print self.do_unregister.__doc__
        return
      return self.removeReplica(argss)
    elif option == 'file': 
      if (len(argss) != 1):
        print self.do_unregister.__doc__
        return
      return self.removeFile(argss)
    elif option == "dir" or option == "directory":
      if (len(argss) != 1):
        print self.do_unregister.__doc__
        return
      return self.removeDirectory(argss)    
    else:
      print "Error: illegal option %s" % option

  # An Auto Completion For ``register``
  _available_unregister_cmd = ['replica', 'file', 'dir', 'directory']
  def complete_unregister(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) >= 2 and (args[1] in self._available_unregister_cmd):
      # if 'unregister file' or 'unregister replica' and so on exists,
      # try to do LFN auto completion.
      cur_path = ""
      if (len(args) == 3):
        cur_path = args[2]
      result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)
      return result

    result = [i for i in self._available_unregister_cmd if i.startswith(text)]
    return result
      
  def do_rmreplica(self,args):
    """ Remove LFN replica from the storage and from the File Catalog
    
        usage:
          rmreplica <lfn> <se>
    """        
    argss = args.split()
    if (len(argss) != 2):
      print self.do_rmreplica.__doc__
      return
    lfn = argss[0]
    lfn = self.getPath(lfn)
    print "lfn:",lfn
    se = argss[1]
    try:
      result =  self.fc.setReplicaStatus( {lfn:{'SE':se,'Status':'Trash'}} )
      if result['OK']:
        print "Replica at",se,"moved to Trash Bin"
      else:
        print "Failed to remove replica at",se
        print result['Message']
    except Exception, x:
      print "Error: rmreplica failed with exception: ", x

  def complete_rmreplica(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

    
  def do_rm(self,args):
    """ Remove file from the storage and from the File Catalog
    
        usage:
          rm <lfn>
          
        NB: this method is not fully implemented !    
    """  
    # Not yet really implemented
    argss = args.split()
    if len(argss) != 1:
      print self.do_rm.__doc__
      return
    self.removeFile(argss)

  def complete_rm(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
    
  def do_rmdir(self,args):
    """ Remove directory from the storage and from the File Catalog
    
        usage:
          rmdir <path>
          
        NB: this method is not fully implemented !  
    """  
    # Not yet really implemented yet
    argss = args.split()
    if len(argss) != 1:
      print self.do_rmdir.__doc__
      return
    self.removeDirectory(argss)  

  def complete_rmdir(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
          
  def removeReplica(self,args):
    """ Remove replica from the catalog
    """          
    
    path = args[0]
    lfn = self.getPath(path)
    print "lfn:",lfn
    rmse = args[1]
    try:
      result =  self.fc.removeReplica( {lfn:{'SE':rmse}} )
      if result['OK']:
        if 'Failed' in result['Value']:
          if lfn in result['Value']['Failed']:
            print "ERROR: %s" % ( result['Value']['Failed'][lfn])
          elif  lfn in result['Value']['Successful']:
            print "File %s at %s removed from the catalog" %( lfn, rmse )
          else:
            "ERROR: Unexpected returned value %s" % result['Value']
        else:
          print "File %s at %s removed from the catalog" %( lfn, rmse )
      else:
        print "Failed to remove replica at",rmse
        print result['Message']
    except Exception, x:
      print "Error: rmpfn failed with exception: ", x
      
  def removeFile(self,args):
    """ Remove file from the catalog
    """  
    
    path = args[0]
    lfn = self.getPath(path)
    print "lfn:",lfn
    try:
      result =  self.fc.removeFile(lfn)
      if result['OK']:
        if 'Failed' in result['Value']:
          if lfn in result['Value']['Failed']:
            print "ERROR: %s" % ( result['Value']['Failed'][lfn] )
          elif lfn in result['Value']['Successful']:
            print "File",lfn,"removed from the catalog"
          else:
            print "ERROR: Unexpected result %s" % result['Value']
        else:
          print "File",lfn,"removed from the catalog"
      else:
        print "Failed to remove file from the catalog"  
        print result['Message']
    except Exception, x:
      print "Error: rm failed with exception: ", x       
      
  def removeDirectory(self,args):
    """ Remove file from the catalog
    """  
    
    path = args[0]
    lfn = self.getPath(path)
    print "lfn:",lfn
    try:
      result =  self.fc.removeDirectory(lfn)
      if result['OK']:
        if result['Value']['Successful']:
          print "Directory",lfn,"removed from the catalog"
        elif result['Value']['Failed']:
          print "ERROR:", result['Value']['Failed'][lfn]  
      else:
        print "Failed to remove directory from the catalog"  
        print result['Message']
    except Exception, x:
      print "Error: rm failed with exception: ", x            
      
  def do_replicate(self,args):
    """ Replicate a given file to a given SE
        
        usage:
          replicate <LFN> <SE> [<SourceSE>]
    """
    argss = args.split()
    if len(argss) < 2:
      print "Error: unsufficient number of arguments"
      return
    lfn = argss[0]
    lfn = self.getPath(lfn)
    se = argss[1]
    sourceSE = ''
    if len(argss)>2:
      sourceSE=argss[2]
    try:
      dirac = Dirac()
      result = dirac.replicateFile(lfn,se,sourceSE,printOutput=True)      
      if not result['OK']:
        print 'Error: %s' %(result['Message'])
      elif not result['Value']:
        print "Replica is already present at the target SE"
      else:  
        print "File %s successfully replicated to the %s SE" % (lfn,se)  
    except Exception, x:
      print "Error: replicate failed with exception: ", x      
      
  def complete_replicate(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

  def do_replicas(self,args):
    """ Get replicas for the given file specified by its LFN

        usage: replicas <lfn>
    """
    argss = args.split()
    if (len(argss) == 0):
      print self.do_replicas.__doc__
      return
    apath = argss[0]
    path = self.getPath(apath)
    print "lfn:",path
    try:
      result =  self.fc.getReplicas(path)    
      if result['OK']:
        if result['Value']['Successful']:
          for se,entry in result['Value']['Successful'][path].items():
            print se.ljust(15),entry
      else:
        print "Replicas: ",result['Message']
    except Exception, x:
      print "replicas failed: ", x

  def complete_replicas(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
        
  def registerFile(self,args):
    """ Add a file to the catatlog 

        usage: add <lfn> <pfn> <size> <SE> [<guid>]
    """      
       
    path = args[0]
    infoDict = {}
    lfn = self.getPath(path)
    infoDict['PFN'] = args[1]
    infoDict['Size'] = int(args[2])
    infoDict['SE'] = args[3]
    if len(args) == 5:
      guid = args[4]
    else:
      _status,guid = commands.getstatusoutput('uuidgen')
    infoDict['GUID'] = guid
    infoDict['Checksum'] = ''    
      
    fileDict = {}
    fileDict[lfn] = infoDict  
      
    try:
      result = self.fc.addFile(fileDict)         
      if not result['OK']:
        print "Failed to add file to the catalog: ",
        print result['Message']
      elif result['Value']['Failed']:
        if result['Value']['Failed'].has_key(lfn):
          print 'Failed to add file:',result['Value']['Failed'][lfn]  
      elif result['Value']['Successful']:
        if result['Value']['Successful'].has_key(lfn):
          print "File successfully added to the catalog"    
    except Exception, x:
      print "add file failed: ", str(x)    
    
  def registerReplica(self,args):
    """ Add a file to the catatlog 

        usage: addpfn <lfn> <pfn> <SE> 
    """      
    path = args[0]
    infoDict = {}
    lfn = self.getPath(path)
    infoDict['PFN'] = args[1]
    if infoDict['PFN'] == "''" or infoDict['PFN'] == '""':
      infoDict['PFN'] = ''
    infoDict['SE'] = args[2]
      
    repDict = {}
    repDict[lfn] = infoDict    
      
    try:
      result = self.fc.addReplica(repDict)                    
      if not result['OK']:
        print "Failed to add replica to the catalog: ",
        print result['Message']
      elif result['Value']['Failed']:
        print 'Failed to add replica:',result['Value']['Failed'][lfn]   
      else:
        print "Replica added successfully:", result['Value']['Successful'][lfn]    
    except Exception, x:
      print "add pfn failed: ", str(x)    
      
  def do_ancestorset(self,args):
    """ Set ancestors for the given file
    
        usage: ancestorset <lfn> <ancestor_lfn> [<ancestor_lfn>...]
    """            
    
    argss = args.split()    
    if (len(argss) == 0):
      print self.do_ancestorset.__doc__
      return 
    lfn = argss[0]
    if lfn[0] != '/':
      lfn = self.cwd + '/' + lfn
    ancestors = argss[1:]
    tmpList = []
    for a in ancestors:
      if a[0] != '/':
        a = self.cwd + '/' + a
      tmpList.append(a)
    ancestors = tmpList       
    
    try:
      result = self.fc.addFileAncestors({lfn:{'Ancestors':ancestors}})
      if not result['OK']:
        print "Failed to add file ancestors to the catalog: ",
        print result['Message']
      elif result['Value']['Failed']:
        print "Failed to add file ancestors to the catalog: ",
        print result['Value']['Failed'][lfn]
      else:
        print "Added %d ancestors to file %s" % (len(ancestors),lfn)
    except Exception, x:
      print "Exception while adding ancestors: ", str(x)                
                         
  def complete_ancestorset(self, text, line, begidx, endidx):

    args = line.split()

    if ( len(args) == 1 ):
      cur_path = ""
    elif ( len(args) > 1 ):
      # If the line ends with ' '
      # this means a new parameter begin.
      if line.endswith(' '):
        cur_path = ""
      else:
        cur_path = args[-1]

    result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_ancestor(self,args):
    """ Get ancestors of the given file
    
        usage: ancestor <lfn> [depth]
    """            
    
    argss = args.split()
    if (len(argss) == 0):
      print self.do_ancestor.__doc__
      return
    lfn = argss[0]
    if lfn[0] != '/':
      lfn = self.cwd + '/' + lfn
    depth = [1]
    if len(argss) > 1:
      depth = int(argss[1])
      depth = range(1,depth+1)
        
    try:      
      result = self.fc.getFileAncestors([lfn],depth)
      if not result['OK']:
        print "ERROR: Failed to get ancestors: ",
        print result['Message']       
      elif result['Value']['Failed']:
        print "Failed to get ancestors: ",
        print result['Value']['Failed'][lfn]
      else:
        depthDict = {}  
        depSet = set()    
        for lfn,ancestorDict in  result['Value']['Successful'].items():
          for ancestor,dep in ancestorDict.items():     
            depthDict.setdefault(dep,[])
            depthDict[dep].append(ancestor)
            depSet.add(dep)
        depList = list(depSet)
        depList.sort()
        print lfn   
        for dep in depList:
          for lfn in depthDict[dep]:      
            print dep,' '*dep*5, lfn
    except Exception, x:
      print "Exception while getting ancestors: ", str(x)    

  def complete_ancestor(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
                                                                     
  def do_descendent(self,args):
    """ Get descendents of the given file
    
        usage: descendent <lfn> [depth]
    """            
    
    argss = args.split()
    if (len(argss) == 0):
      print self.do_descendent.__doc__
      return
    lfn = argss[0]
    if lfn[0] != '/':
      lfn = self.cwd + '/' + lfn
    depth = [1]
    if len(argss) > 1:
      depth = int(argss[1])
      depth = range(1,depth+1)
        
    try:
      result = self.fc.getFileDescendents([lfn],depth)
      if not result['OK']:
        print "ERROR: Failed to get descendents: ",
        print result['Message']       
      elif result['Value']['Failed']:
        print "Failed to get descendents: ",
        print result['Value']['Failed'][lfn]
      else:
        depthDict = {}  
        depSet = set()    
        for lfn,descDict in  result['Value']['Successful'].items():
          for desc,dep in descDict.items():     
            depthDict.setdefault(dep,[])
            depthDict[dep].append(desc)
            depSet.add(dep)
        depList = list(depSet)
        depList.sort()
        print lfn   
        for dep in depList:
          for lfn in depthDict[dep]:      
            print dep,' '*dep*5, lfn
    except Exception, x:
      print "Exception while getting descendents: ", str(x)              

  def complete_descendent(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
#######################################################################################
# User and group methods      
      
  def do_user(self,args):
    """ User related commands
    
        usage:
          user add <username>  - register new user in the catalog
          user delete <username>  - delete user from the catalog
          user show - show all users registered in the catalog
    """    
    argss = args.split()
    if (len(argss)==0):
      print self.do_user.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'add':
      if (len(argss)!=1):
        print self.do_user.__doc__
        return
      return self.registerUser(argss) 
    elif option == 'delete':
      if (len(argss)!=1):
        print self.do_user.__doc__
        return
      return self.deleteUser(argss) 
    elif option == "show":
      result = self.fc.getUsers()
      if not result['OK']:
        print ("Error: %s" % result['Message'])            
      else:  
        if not result['Value']:
          print "No entries found"
        else:  
          for user,id_ in result['Value'].items():
            print user.rjust(20),':',id_
    else:
      print "Unknown option:",option

  # completion for ``user``
  _available_user_cmd = ['add', 'delete', 'show']
  def complete_user(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) == 2 and (args[1] in self._available_user_cmd):
      # if the sub command exists,
      # Don't need any auto completion
      return result

    result = [i for i in self._available_user_cmd if i.startswith(text)]
    return result
    
  def do_group(self,args):
    """ Group related commands
    
        usage:
          group add <groupname>  - register new group in the catalog
          group delete <groupname>  - delete group from the catalog
          group show - how all groups registered in the catalog
    """    
    argss = args.split()
    if (len(argss)==0):
      print self.do_group.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'add':
      if (len(argss)!=1):
        print self.do_group.__doc__
        return
      return self.registerGroup(argss) 
    elif option == 'delete':
      if (len(argss)!=1):
        print self.do_group.__doc__
        return
      return self.deleteGroup(argss) 
    elif option == "show":
      result = self.fc.getGroups()
      if not result['OK']:
        print ("Error: %s" % result['Message'])            
      else:  
        if not result['Value']:
          print "No entries found"
        else:  
          for user,id_ in result['Value'].items():
            print user.rjust(20),':',id_
    else:
      print "Unknown option:",option  
  
  # completion for ``group``
  _available_group_cmd = ['add', 'delete', 'show']
  def complete_group(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) == 2 and (args[1] in self._available_group_cmd):
      # if the sub command exists,
      # Don't need any auto completion
      return result

    result = [i for i in self._available_group_cmd if i.startswith(text)]
    return result
  def registerUser(self,argss):
    """ Add new user to the File Catalog
    
        usage: adduser <user_name>
    """
 
    username = argss[0] 
    
    result =  self.fc.addUser(username)
    if not result['OK']:
      print ("Error: %s" % result['Message'])
    else:
      print "User ID:",result['Value']  
      
  def deleteUser(self,args):
    """ Delete user from the File Catalog
    
        usage: deleteuser <user_name>
    """
 
    username = args[0] 
    
    result =  self.fc.deleteUser(username)
    if not result['OK']:
      print ("Error: %s" % result['Message'])    
      
  def registerGroup(self,argss):
    """ Add new group to the File Catalog
    
        usage: addgroup <group_name>
    """
 
    gname = argss[0] 
    
    result =  self.fc.addGroup(gname)
    if not result['OK']:
      print ("Error: %s" % result['Message'])
    else:
      print "Group ID:",result['Value']    
      
  def deleteGroup(self,args):
    """ Delete group from the File Catalog
    
        usage: deletegroup <group_name>
    """
 
    gname = args[0] 
    
    result =  self.fc.deleteGroup(gname)
    if not result['OK']:
      print ("Error: %s" % result['Message'])         
         
  def do_mkdir(self,args):
    """ Make directory
    
        usage: mkdir <path>
    """
    
    argss = args.split()
    if (len(argss)==0):
      print self.do_group.__doc__
      return
    path = argss[0] 
    if path.find('/') == 0:
      newdir = path
    else:
      newdir = self.cwd + '/' + path
      
    newdir = newdir.replace(r'//','/')
    
    result =  self.fc.createDirectory(newdir)    
    if result['OK']:
      if result['Value']['Successful']:
        if result['Value']['Successful'].has_key(newdir):
          print "Successfully created directory:", newdir
      elif result['Value']['Failed']:
        if result['Value']['Failed'].has_key(newdir):  
          print 'Failed to create directory:',result['Value']['Failed'][newdir]
    else:
      print 'Failed to create directory:',result['Message']

  def complete_mkdir(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

  def do_cd(self,args):
    """ Change directory to <path>
    
        usage: cd <path>
               cd -
    """
 
    argss = args.split()
    if len(argss) == 0:
      path = '/'
    else:  
      path = argss[0] 
      
    if path == '-':
      path = self.previous_cwd
      
    newcwd = self.getPath(path)
    if len(newcwd)>1 and not newcwd.find('..') == 0 :
      newcwd=newcwd.rstrip("/")
    
    result =  self.fc.isDirectory(newcwd)        
    if result['OK']:
      if result['Value']['Successful']:
        if result['Value']['Successful'][newcwd]:
        #if result['Type'] == "Directory":
          self.previous_cwd = self.cwd
          self.cwd = newcwd
          self.prompt = 'FC:'+self.cwd+'>'
        else:
          print newcwd,'does not exist or is not a directory'
      else:
        print newcwd,'is not found'
    else:
      print 'Server failed to find the directory',newcwd

  def complete_cd(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_id(self,args):
    """ Get user identity
    """
    result = getProxyInfo()
    if not result['OK']:
      print "Error: %s" % result['Message']
      return
    user = result['Value']['username']
    group = result['Value']['group']
    result = self.fc.getUsers()
    if not result['OK']:
      print "Error: %s" % result['Message']
      return
    userDict = result['Value']
    result = self.fc.getGroups()
    if not result['OK']:
      print "Error: %s" % result['Message']
      return
    groupDict = result['Value']    
    idUser = userDict.get(user,0)
    idGroup = groupDict.get(group,0)
    print "user=%d(%s) group=%d(%s)" % (idUser,user,idGroup,group)
      
  def do_lcd(self,args):
    """ Change local directory
    
        usage:
          lcd <local_directory>
    """    
    argss = args.split()
    if (len(argss) != 1):
      print self.do_lcd.__doc__
      return
    localDir = argss[0]
    try:
      os.chdir(localDir)
      newDir = os.getcwd()
      print "Local directory: %s" % newDir
    except:
      print "%s seems not a directory" % localDir

  def complete_lcd(self, text, line, begidx, endidx):
    # TODO
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.ul_dc.parse_text_line(text, cur_path, self.cwd)

    return result
          
  def do_pwd(self,args):
    """ Print out the current directory
    
        usage: pwd
    """
    print self.cwd      

  def do_ls(self,args):
    """ Lists directory entries at <path> 

        usage: ls [-ltrn] <path>
    """
    
    argss = args.split()
    # Get switches
    _long = False
    reverse = False
    timeorder = False
    numericid = False
    path = self.cwd
    if len(argss) > 0:
      if argss[0][0] == '-':
        if 'l' in argss[0]:
          _long = True
        if 'r' in  argss[0]:
          reverse = True
        if 't' in argss[0]:
          timeorder = True
        if 'n' in argss[0]:
          numericid = True  
        del argss[0]  
          
      # Get path    
      if argss:        
        path = argss[0]       
        if path[0] != '/':
          path = self.cwd+'/'+path      
    path = path.replace(r'//','/')

    # remove last character if it is "/"    
    if path[-1] == '/' and path != '/':
      path = path[:-1]
    
    # Check if the target path is a file
    result =  self.fc.isFile(path)          
    if not result['OK']:
      print "Error: can not verify path"
      return
    elif path in result['Value']['Successful'] and result['Value']['Successful'][path]:
      result = self.fc.getFileMetadata(path)      
      dList = DirectoryListing()
      fileDict = result['Value']['Successful'][path]
      dList.addFile(os.path.basename(path),fileDict,{},numericid)
      dList.printListing(reverse,timeorder)
      return         
    
    # Get directory contents now
    try:
      result =  self.fc.listDirectory(path,_long)                   
      dList = DirectoryListing()
      if result['OK']:
        if result['Value']['Successful']:
          for entry in result['Value']['Successful'][path]['Files']:
            fname = entry.split('/')[-1]
            # print entry, fname
            # fname = entry.replace(self.cwd,'').replace('/','')
            if _long:
              fileDict = result['Value']['Successful'][path]['Files'][entry]['MetaData']
              repDict = result['Value']['Successful'][path]['Files'][entry].get( "Replicas", {} )
              if fileDict:
                dList.addFile(fname,fileDict,repDict,numericid)
            else:  
              dList.addSimpleFile(fname)
          for entry in result['Value']['Successful'][path]['SubDirs']:
            dname = entry.split('/')[-1]
            # print entry, dname
            # dname = entry.replace(self.cwd,'').replace('/','')  
            if _long:
              dirDict = result['Value']['Successful'][path]['SubDirs'][entry]
              if dirDict:
                dList.addDirectory(dname,dirDict,numericid)
            else:    
              dList.addSimpleFile(dname)
          
          for entry in result['Value']['Successful'][path]['Links']:
            pass
          
          if 'Datasets' in result['Value']['Successful'][path]:
            for entry in result['Value']['Successful'][path]['Datasets']:
              dname = os.path.basename( entry )    
              if _long:
                dsDict = result['Value']['Successful'][path]['Datasets'][entry]['Metadata']  
                if dsDict:
                  dList.addDataset(dname,dsDict,numericid)
              else:    
                dList.addSimpleFile(dname)
              
          if _long:
            dList.printListing(reverse,timeorder)      
          else:
            dList.printOrdered()
      else:
        print "Error:",result['Message']
    except Exception, x:
      print "Error:", str(x)

  def complete_ls(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_cnt = 0

    if (len(args) > 1):
      if ( args[1][0] == "-"):
        index_cnt = 1

    # the first argument -- LFN.
    if (1+index_cnt<=len(args)<=2+index_cnt):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_cnt) or (len(args)==2+index_cnt and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_cnt):
          cur_path = args[1+index_cnt]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_chown(self,args):
    """ Change owner of the given path

        usage: chown [-R] <owner> <path> 
    """         
    
    argss = args.split()
    recursive = False
    if (len(argss) == 0):
      print self.do_chown.__doc__
      return
    if argss[0] == '-R':
      recursive = True
      del argss[0]
    if (len(argss) != 2):
      print self.do_chown.__doc__
      return
    owner = argss[0]
    path = argss[1]
    lfn = self.getPath(path)
    pathDict = {}
    pathDict[lfn] = {'Owner':owner}
    
    try:
      result = self.fc.changePathOwner(pathDict,recursive)        
      if not result['OK']:
        print "Error:",result['Message']
        return
      if lfn in result['Value']['Failed']:
        print "Error:",result['Value']['Failed'][lfn]
        return  
    except Exception, x:
      print "Exception:", str(x)         

  def complete_chown(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_counter = 0+1

    if '-R' in args:
      index_counter = 1+1

    # the first argument -- LFN.
    if ((1+index_counter) <=len(args)<= (2+index_counter)):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_counter) or (len(args)==2+index_counter and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_counter):
          cur_path = args[1+index_counter]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_chgrp(self,args):
    """ Change group of the given path

        usage: chgrp [-R] <group> <path> 
    """         
    
    argss = args.split()
    recursive = False
    if (len(argss) == 0):
      print self.do_chgrp.__doc__
      return
    if argss[0] == '-R':
      recursive = True
      del argss[0]
    if (len(argss) != 2):
      print self.do_chgrp.__doc__
      return
    group = argss[0]
    path = argss[1]
    lfn = self.getPath(path)
    pathDict = {}
    pathDict[lfn] = {"Group":group}
    
    try:
      result = self.fc.changePathGroup(pathDict,recursive)         
      if not result['OK']:
        print "Error:",result['Message']
        return
      if lfn in result['Value']['Failed']:
        print "Error:",result['Value']['Failed'][lfn]
        return  
    except Exception, x:
      print "Exception:", str(x)    

  def complete_chgrp(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_counter = 0+1

    if '-R' in args:
      index_counter = 1+1

    # the first argument -- LFN.
    if ((1+index_counter) <=len(args)<= (2+index_counter)):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_counter) or (len(args)==2+index_counter and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_counter):
          cur_path = args[1+index_counter]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_chmod(self,args):
    """ Change permissions of the given path
        usage: chmod [-R] <mode> <path> 
    """         
    
    argss = args.split()
    recursive = False
    if (len(argss) < 2):
      print self.do_chmod.__doc__
      return
    if argss[0] == '-R':
      recursive = True
      del argss[0]
    mode = argss[0]
    path = argss[1]
    lfn = self.getPath(path)
    pathDict = {}
    # treat mode as octal 
    pathDict[lfn] = {"Mode":eval('0'+mode)}
    
    try:
      result = self.fc.changePathMode(pathDict,recursive)             
      if not result['OK']:
        print "Error:",result['Message']
        return
      if lfn in result['Value']['Failed']:
        print "Error:",result['Value']['Failed'][lfn]
        return  
    except Exception, x:
      print "Exception:", str(x)       
      
  def complete_chmod(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_counter = 0+1

    if '-R' in args:
      index_counter = 1+1

    # the first argument -- LFN.
    if ((1+index_counter) <=len(args)<= (2+index_counter)):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_counter) or (len(args)==2+index_counter and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_counter):
          cur_path = args[1+index_counter]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_size(self,args):
    """ Get file or directory size. If -l switch is specified, get also the total
        size per Storage Element 

        usage: size [-l] [-f] <lfn>|<dir_path>
        
        Switches:
           -l  long output including per SE report
           -f  use raw file information and not the storage tables  
    """      
    
    argss = args.split()
    _long = False
    fromFiles = False
    if len(argss) > 0:
      if argss[0] == '-l':
        _long = True
        del argss[0]
    if len(argss) > 0:
      if argss[0] == '-f':
        fromFiles = True
        del argss[0]    
        
    if len(argss) == 1:
      path = argss[0]
      if path == '.':
        path = self.cwd    
    else:
      path = self.cwd
    path = self.getPath(path)
    
    try:
      result = self.fc.isFile(path)
      if not result['OK']:
        print "Error:",result['Message']
      if result['Value']['Successful']:
        if result['Value']['Successful'][path]:  
          print "lfn:",path
          result =  self.fc.getFileSize(path)
          if result['OK']:
            if result['Value']['Successful']:
              print "Size:",result['Value']['Successful'][path]
            else:
              print "File size failed:", result['Value']['Failed'][path]  
          else:
            print "File size failed:",result['Message']
        else:
          print "directory:",path
          result =  self.fc.getDirectorySize( path, _long, fromFiles )          
          if result['OK']:
            if result['Value']['Successful']:
              print "Logical Size:",int_with_commas(result['Value']['Successful'][path]['LogicalSize']), \
                    "Files:",result['Value']['Successful'][path]['LogicalFiles'], \
                    "Directories:",result['Value']['Successful'][path]['LogicalDirectories']
              if _long:
                fields = ['StorageElement','Size','Replicas']
                values = []
                if "PhysicalSize" in result['Value']['Successful'][path]:
                  print 
                  totalSize = result['Value']['Successful'][path]['PhysicalSize']['TotalSize']
                  totalFiles = result['Value']['Successful'][path]['PhysicalSize']['TotalFiles'] 
                  for se,sdata in result['Value']['Successful'][path]['PhysicalSize'].items():
                    if not se.startswith("Total"):
                      size = sdata['Size']
                      nfiles = sdata['Files']
                      #print se.rjust(20),':',int_with_commas(size).ljust(25),"Files:",nfiles
                      values.append( (se, int_with_commas(size), str(nfiles)) )
                  #print '='*60
                  #print 'Total'.rjust(20),':',int_with_commas(totalSize).ljust(25),"Files:",totalFiles
                  values.append( ('Total', int_with_commas(totalSize), str(totalFiles)) )
                  printTable(fields,values)  
              if "QueryTime" in result['Value']:
                print "Query time %.2f sec" % result['Value']['QueryTime']
            else:
              print "Directory size failed:", result['Value']['Failed'][path]
          else:
            print "Directory size failed:",result['Message']  
      else:
        print "Failed to determine path type"        
    except Exception, x:
      print "Size failed: ", x

  def complete_size(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_counter = 0

    if '-l' in args:
      index_counter = 1

    # the first argument -- LFN.
    if ((1+index_counter) <=len(args)<= (2+index_counter)):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_counter) or (len(args)==2+index_counter and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_counter):
          cur_path = args[1+index_counter]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_guid(self,args):
    """ Get the file GUID 

        usage: guid <lfn> 
    """      
    
    argss = args.split()
    if (len(argss) == 0):
      print self.do_guid.__doc__
      return
    path = argss[0]
    try:
      result =  self.fc.getFileMetadata(path)
      if result['OK']:
        if result['Value']['Successful']:
          print "GUID:",result['Value']['Successful'][path]['GUID']
        else:
          print "ERROR: getting guid failed"  
      else:
        print "ERROR:",result['Message']
    except Exception, x:
      print "guid failed: ", x   

  def complete_guid(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

##################################################################################
#  Metadata methods
      
  def do_meta(self,args):
    """ Metadata related operations
    
        Usage:
          meta index [-d|-f|-r] <metaname> [<metatype>]  - add new metadata index. Possible types are:
                                                           'int', 'float', 'string', 'date';
                                                         -d  directory metadata
                                                         -f  file metadata
                                                         -r  remove the specified metadata index
          meta set <path> <metaname> <metavalue> - set metadata value for directory or file
          meta remove <path> <metaname>  - remove metadata value for directory or file
          meta get [-e] [<path>] - get metadata for the given directory or file
          meta tags <path> <metaname> where <meta_selection> - get values (tags) of the given metaname compatible with 
                                                        the metadata selection
          meta show - show all defined metadata indice
    
    """    
    argss = args.split()
    if (len(argss)==0):
      print self.do_meta.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'set':
      if (len(argss) != 3):
        print self.do_meta.__doc__
        return
      return self.setMeta(argss)
    elif option == 'get':
      return self.getMeta(argss)  
    elif option[:3] == 'tag':
      # TODO
      if (len(argss) == 0):
        print self.do_meta.__doc__
        return
      return self.metaTag(argss)    
    elif option == 'index':
      if (len(argss) < 1):
        print self.do_meta.__doc__
        return
      return self.registerMeta(argss)
    elif option == 'metaset':
      # TODO
      if (len(argss) == 0):
        print self.do_meta.__doc__
        return
      return self.registerMetaset(argss)
    elif option == 'show':
      return self.showMeta()
    elif option == 'remove' or option == "rm":
      if (len(argss) != 2):
        print self.do_meta.__doc__
        return
      return self.removeMeta(argss) 
    else:
      print "Unknown option:",option  

  # auto completion for ``meta``
  # TODO: what's the doc for metaset?
  _available_meta_cmd = ["set", "get", "tag", "tags", 
                         "index", "metaset","show",
                         "rm", "remove"]
  _meta_cmd_need_lfn = ["set", "get",
                        "rm", "remove"]
  def complete_meta(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) >= 2 and (args[1] in self._available_meta_cmd):
      # if the sub command is not in self._meta_cmd_need_lfn
      # Don't need any auto completion
      if (args[1] in self._meta_cmd_need_lfn):
        # TODO
        if (len(args) == 2):
          cur_path = ""
        elif ( len(args) > 2 ):
          # If the line ends with ' '
          # this means a new parameter begin.
          if line.endswith(' '):
            cur_path = ""
          else:
            cur_path = args[-1]
          
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)
        pass
      return result

    result = [i for i in self._available_meta_cmd if i.startswith(text)]
    return result
            
  def removeMeta(self,argss):
    """ Remove the specified metadata for a directory or file
    """    
    apath = argss[0]
    path = self.getPath(apath)
    if len(argss) < 2:
      print "Error: no metadata is specified for removal"
      return
    
    metadata = argss[1:]
    result = self.fc.removeMetadata(path,metadata)
    if not result['OK']:
      print "Error:", result['Message']
      if "FailedMetadata" in result:
        for meta,error in result['FailedMetadata']:
          print meta,';',error
     
  def setMeta(self,argss):
    """ Set metadata value for a directory
    """      
    if len(argss) != 3:
      print "Error: command requires 3 arguments, %d given" % len(argss)
      return
    path = argss[0]
    if path == '.':
      path = self.cwd
    elif path[0] != '/':
      path = self.cwd+'/'+path  
    meta = argss[1]
    value = argss[2]
    print path,meta,value
    metadict = {}
    metadict[meta]=value
    result = self.fc.setMetadata(path,metadict)
    if not result['OK']:
      print ("Error: %s" % result['Message'])     
      
  def getMeta(self,argss):
    """ Get metadata for the given directory
    """            
    expandFlag = False
    dirFlag = True
    if len(argss) == 0:
      path ='.'
    else:  
      if argss[0] == "-e":
        expandFlag = True
        del argss[0]
      if len(argss) == 0:
        path ='.'  
      else:  
        path = argss[0]
        dirFlag = False
    if path == '.':
      path = self.cwd
    elif path[0] != '/':
      path = self.getPath(path)

    path = path.rstrip( '/' )
      
    if not dirFlag:
      # Have to decide if it is a file or not
      result = self.fc.isFile(path)
      if not result['OK']:
        print "ERROR: Failed to contact the catalog"      
      if not result['Value']['Successful']:
        print "ERROR: Path not found"
      dirFlag = not result['Value']['Successful'][path]        
        
    if dirFlag:    
      result = self.fc.getDirectoryMetadata(path)      
      if not result['OK']:
        print ("Error: %s" % result['Message']) 
        return
      if result['Value']:
        metaDict = result['MetadataOwner']
        metaTypeDict = result['MetadataType']
        for meta, value in result['Value'].items():
          setFlag = metaDict[meta] != 'OwnParameter' and metaTypeDict[meta] == "MetaSet"
          prefix = ''
          if setFlag:
            prefix = "+"
          if metaDict[meta] == 'ParentMetadata':
            prefix += "*"
            print (prefix+meta).rjust(20),':',value
          elif metaDict[meta] == 'OwnMetadata':
            prefix += "!"
            print (prefix+meta).rjust(20),':',value   
          else:
            print meta.rjust(20),':',value 
          if setFlag and expandFlag:
            result = self.fc.getMetadataSet(value,expandFlag)
            if not result['OK']:
              print ("Error: %s" % result['Message']) 
              return
            for m,v in result['Value'].items():
              print " "*10,m.rjust(20),':',v      
      else:
        print "No metadata defined for directory"   
    else:
      result = self.fc.getFileUserMetadata(path)      
      if not result['OK']:
        print ("Error: %s" % result['Message']) 
        return
      if result['Value']:      
        for meta,value in result['Value'].items():
          print meta.rjust(20),':', value
      else:
        print "No metadata found"        
      
  def metaTag(self,argss):
    """ Get values of a given metadata tag compatible with the given selection
    """    
    path =  argss[0]
    del argss[0]
    tag = argss[0]
    del argss[0]
    path = self.getPath(path)
    
    # Evaluate the selection dictionary
    metaDict = {}
    if argss:
      if argss[0].lower() == 'where':
        result = self.fc.getMetadataFields()        
        if not result['OK']:
          print ("Error: %s" % result['Message']) 
          return
        if not result['Value']:
          print "Error: no metadata fields defined"
          return
        typeDictfm = result['Value']['FileMetaFields']
        typeDict = result['Value']['DirectoryMetaFields']

        
        del argss[0]
        for arg in argss:
          try:
            name,value = arg.split('=')
            if not name in typeDict:
              if not name in typeDictfm:
                print "Error: metadata field %s not defined" % name
              else:
                print 'No support for meta data at File level yet: %s' % name
              return
            mtype = typeDict[name]
            mvalue = value
            if mtype[0:3].lower() == 'int':
              mvalue = int(value)
            if mtype[0:5].lower() == 'float':
              mvalue = float(value)
            metaDict[name] = mvalue
          except Exception,x:
            print "Error:",str(x)
            return  
      else:
        print "Error: WHERE keyword is not found after the metadata tag name"
        return
      
    result = self.fc.getCompatibleMetadata( metaDict, path )  
    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return
    tagDict = result['Value']
    if tag in tagDict:
      if tagDict[tag]:
        print "Possible values for %s:" % tag
        for v in tagDict[tag]:
          print v
      else:
        print "No compatible values found for %s" % tag       

  def showMeta(self):
    """ Show defined metadata indices
    """
    result = self.fc.getMetadataFields()  
    if not result['OK']:
      print ("Error: %s" % result['Message'])            
    else:
      if not result['Value']:
        print "Metadata for file or directory found"
      else:
        if len(result['Value']['FileMetaFields']):
          print "\n","File metadata".rjust(20,":")
          for meta, metatype in result['Value']['FileMetaFields'].items():
            metatype = metatype.replace("int","integer").replace("INT","integer").replace("VARCHAR(128)","string")
            print meta.rjust(20),':',metatype
        if len(result['Value']['DirectoryMetaFields']):
          print "\n","Directory metadata".rjust(20),":"
          for meta,metatype in result['Value']['DirectoryMetaFields'].items():
            metatype = metatype.replace("int","integer").replace("INT","integer").replace("VARCHAR(128)","string")
            print meta.rjust(20),':',metatype

    result = self.fc.listMetadataSets()
    if not result['OK']:
      print 'Error: %s' % result['Message']
    else:
      if not result['Value']:
        print "No Metadata sets defined"
      else:  
        print "\n","Metadata sets".rjust(20),":"
        for metasetname, values in result['Value'].items():
          metastr = ''
          for key,val in values.items():
            metastr += "%s : %s, " % (key, val)
          metastr.rstrip(",")  
          print "%s -> %s" % (metasetname.rjust(20), metastr)

  def registerMeta(self,argss):
    """ Add metadata field. 
    """

    if len(argss) < 2:
      print "Unsufficient number of arguments"
      return

    fdType = '-d'
    removeFlag = False
    if argss[0].lower() in ['-d','-f']:
      fdType = argss[0]
      del argss[0]
    if argss[0].lower() == '-r':
      removeFlag = True
      del argss[0]

    if len(argss) < 2 and not removeFlag:
      print "Unsufficient number of arguments"
      return

    mname = argss[0]
    if removeFlag:
      result = self.fc.deleteMetadataField(mname)
      if not result['OK']:
        print "Error:", result['Message']
      return

    mtype = argss[1]

    if mtype.lower()[:3] == 'int':
      rtype = 'INT'
    elif mtype.lower()[:7] == 'varchar':
      rtype = mtype
    elif mtype.lower() == 'string':
      rtype = 'VARCHAR(128)'
    elif mtype.lower() == 'float':
      rtype = 'FLOAT'
    elif mtype.lower() == 'date':
      rtype = 'DATETIME'
    elif mtype.lower() == 'metaset':
      rtype = 'MetaSet'
    else:
      print "Error: illegal metadata type %s" % mtype
      return

    result =  self.fc.addMetadataField(mname,rtype,fdType)
    if not result['OK']:
      print ("Error: %s" % result['Message'])
    else:
      print "Added metadata field %s of type %s" % (mname,mtype)   
 
  def registerMetaset(self,argss):
    """ Add metadata set
    """
    
    setDict = {}
    setName = argss[0]
    del argss[0]
    for arg in argss:
      key,value = arg.split('=')
      setDict[key] = value
      
    result =  self.fc.addMetadataSet(setName,setDict)
    if not result['OK']:
      print ("Error: %s" % result['Message'])  
    else:
      print "Added metadata set %s" % setName  
    
  def do_find(self,args):
    """ Find all files satisfying the given metadata information 
    
        usage: find [-q] <path> <meta_name>=<meta_value> [<meta_name>=<meta_value>]
    """   
   
    argss = args.split()
    if (len(argss) < 1):
      print self.do_find.__doc__
      return
    
    verbose = True
    if argss[0] == "-q":
      verbose  = False
      del argss[0]

    path = argss[0]
    path = self.getPath(path)
    del argss[0]
    if argss:
      if argss[0][0] == '{':
        metaDict = eval(argss[0])
      else:  
        metaDict = self.__createQuery(' '.join(argss))
    else:
      metaDict = {}    
    print "Query:",metaDict
          
    result = self.fc.findFilesByMetadata(metaDict,path)
    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return 
    if result['Value']:
      for dir_ in result['Value']:
        print dir_
    else:
      print "No matching data found"      
    if verbose and "QueryTime" in result:
      print "QueryTime %.2f sec" % result['QueryTime']  

  def complete_find(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # skip "-q" optional switch
    if len(args) >= 2 and args[1] == "-q":
      if len(args) > 2 or line.endswith(" "):
        del args[1]

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def __createQuery(self,args):
    """ Create the metadata query out of the command line arguments
    """    
    argss = args.split()
    result = self.fc.getMetadataFields()

    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return None
    if not result['Value']:
      print "Error: no metadata fields defined"
      return None
    typeDict = result['Value']['FileMetaFields']
    typeDict.update(result['Value']['DirectoryMetaFields'])
    
    # Special meta tags    
    typeDict['SE'] = 'VARCHAR'
    typeDict['User'] = 'VARCHAR'
    typeDict['Group'] = 'VARCHAR'
    typeDict['Path'] = 'VARCHAR'
  
    metaDict = {}
    contMode = False
    for arg in argss:
      if not contMode:
        operation = ''
        for op in ['>=','<=','>','<','!=','=']:
          if arg.find(op) != -1:
            operation = op
            break
        if not operation:
          
          print "Error: operation is not found in the query"
          return None
          
        name,value = arg.split(operation)
        if not name in typeDict:
          print "Error: metadata field %s not defined" % name
          return None
        mtype = typeDict[name]
      else:
        value += ' ' + arg
        value = value.replace(contMode,'')
        contMode = False  
      
      if value[0] == '"' or value[0] == "'":
        if value[-1] != '"' and value != "'":
          contMode = value[0]
          continue 
      
      if value.find(',') != -1:
        valueList = [ x.replace("'","").replace('"','') for x in value.split(',') ]
        mvalue = valueList
        if mtype[0:3].lower() == 'int':
          mvalue = [ int(x) for x in valueList if not x in ['Missing','Any'] ]
          mvalue += [ x for x in valueList if x in ['Missing','Any'] ]
        if mtype[0:5].lower() == 'float':
          mvalue = [ float(x) for x in valueList if not x in ['Missing','Any'] ]
          mvalue += [ x for x in valueList if x in ['Missing','Any'] ]
        if operation == "=":
          operation = 'in'
        if operation == "!=":
          operation = 'nin'    
        mvalue = {operation:mvalue}  
      else:            
        mvalue = value.replace("'","").replace('"','')
        if not value in ['Missing','Any']:
          if mtype[0:3].lower() == 'int':
            mvalue = int(value)
          if mtype[0:5].lower() == 'float':
            mvalue = float(value)               
        if operation != '=':     
          mvalue = {operation:mvalue}      
                                
      if name in metaDict:
        if type(metaDict[name]) == DictType:
          if type(mvalue) == DictType:
            op,value = mvalue.items()[0]
            if op in metaDict[name]:
              if type(metaDict[name][op]) == ListType:
                if type(value) == ListType:
                  metaDict[name][op] = uniqueElements(metaDict[name][op] + value)
                else:
                  metaDict[name][op] = uniqueElements(metaDict[name][op].append(value))     
              else:
                if type(value) == ListType:
                  metaDict[name][op] = uniqueElements([metaDict[name][op]] + value)
                else:
                  metaDict[name][op] = uniqueElements([metaDict[name][op],value])       
            else:
              metaDict[name].update(mvalue)
          else:
            if type(mvalue) == ListType:
              metaDict[name].update({'in':mvalue})
            else:  
              metaDict[name].update({'=':mvalue})
        elif type(metaDict[name]) == ListType:   
          if type(mvalue) == DictType:
            metaDict[name] = {'in':metaDict[name]}
            metaDict[name].update(mvalue)
          elif type(mvalue) == ListType:
            metaDict[name] = uniqueElements(metaDict[name] + mvalue)
          else:
            metaDict[name] = uniqueElements(metaDict[name].append(mvalue))      
        else:
          if type(mvalue) == DictType:
            metaDict[name] = {'=':metaDict[name]}
            metaDict[name].update(mvalue)
          elif type(mvalue) == ListType:
            metaDict[name] = uniqueElements([metaDict[name]] + mvalue)
          else:
            metaDict[name] = uniqueElements([metaDict[name],mvalue])          
      else:            
        metaDict[name] = mvalue         
    
    return metaDict 

  def do_dataset( self, args ):
    """ A set of dataset manipulation commands
    
        Usage:
          
          dataset add <dataset_name> <meta_query>          - add a new dataset definition
          dataset annotate <dataset_name> <annotation>     - add annotation to a dataset
          dataset show [-l] [<dataset_name>]               - show existing datasets
          dataset status <dataset_name>                    - display the dataset status
          dataset files <dataset_name>                     - show dataset files     
          dataset rm <dataset_name>                        - remove dataset
          dataset check <dataset_name>                     - check if the dataset parameters are still valid     
          dataset update <dataset_name>                    - update the dataset parameters
          dataset freeze <dataset_name>                    - fix the current contents of the dataset     
          dataset release <dataset_name>                   - release the dynamic dataset
    """
    argss = args.split()
    if (len(argss)==0):
      print self.do_meta.__doc__
      return
    command = argss[0]
    del argss[0]
    if command == "add":
      self.dataset_add( argss )
    elif command == "annotate":
      self.dataset_annotate( argss )    
    elif command == "show":
      self.dataset_show( argss )  
    elif command == "files":
      self.dataset_files( argss )
    elif command == "rm":
      self.dataset_rm( argss )   
    elif command == "check":
      self.dataset_check( argss )
    elif command == "update":
      self.dataset_update( argss )     
    elif command == "freeze":
      self.dataset_freeze( argss )
    elif command == "release":
      self.dataset_release( argss )      
    elif command == "status":
      self.dataset_status( argss )        

  def dataset_add( self, argss ):
    """ Add a new dataset
    """
    datasetName = argss[0]
    metaSelections = ' '.join( argss[1:] )
    metaDict = self.__createQuery( metaSelections )
    datasetName = self.getPath( datasetName )
    
    result = self.fc.addDataset( datasetName, metaDict )
    if not result['OK']:
      print "ERROR: failed to add dataset:", result['Message']
    else:
      print "Successfully added dataset", datasetName  

  def dataset_annotate( self, argss ):
    """ Add a new dataset
    """
    datasetName = argss[0]
    annotation = ' '.join( argss[1:] )
    datasetName = self.getPath( datasetName )
    
    result = self.fc.addDatasetAnnotation( {datasetName: annotation} )
    if not result['OK']:
      print "ERROR: failed to add annotation:", result['Message']
    else:
      print "Successfully added annotation to", datasetName  

  def dataset_status( self, argss ):
    """ Display the dataset status
    """
    datasetName = argss[0]
    result = self.fc.getDatasetParameters( datasetName )
    if not result['OK']:
      print "ERROR: failed to get status of dataset:", result['Message']
    else:
      parDict = result['Value']
      for par,value in parDict.items():
        print par.rjust(20),':',value  

  def dataset_rm( self, argss ):
    """ Remove the given dataset
    """
    datasetName = argss[0]
    result = self.fc.removeDataset( datasetName )
    if not result['OK']:
      print "ERROR: failed to remove dataset:", result['Message']
    else:
      print "Successfully removed dataset", datasetName  

  def dataset_check( self, argss ):
    """ check if the dataset parameters are still valid
    """
    datasetName = argss[0]
    result = self.fc.checkDataset( datasetName )
    if not result['OK']:
      print "ERROR: failed to check dataset:", result['Message']
    else:
      changeDict = result['Value']
      if not changeDict:
        print "Dataset is not changed"
      else:
        print "Dataset changed:"
        for par in changeDict:
          print "   ",par,': ',changeDict[par][0],'->',changeDict[par][1]
          
  def dataset_update( self, argss ):
    """ Update the given dataset parameters
    """
    datasetName = argss[0]
    result = self.fc.updateDataset( datasetName )
    if not result['OK']:
      print "ERROR: failed to update dataset:", result['Message']
    else:
      print "Successfully updated dataset", datasetName            

  def dataset_freeze( self, argss ):
    """ Freeze the given dataset
    """
    datasetName = argss[0]
    result = self.fc.freezeDataset( datasetName )
    if not result['OK']:
      print "ERROR: failed to freeze dataset:", result['Message']
    else:
      print "Successfully frozen dataset", datasetName            

  def dataset_release( self, argss ):
    """ Release the given dataset
    """
    datasetName = argss[0]
    result = self.fc.releaseDataset( datasetName )
    if not result['OK']:
      print "ERROR: failed to release dataset:", result['Message']
    else:
      print "Successfully released dataset", datasetName       

  def dataset_files( self, argss ):
    """ Get the given dataset files
    """
    datasetName = argss[0]
    result = self.fc.getDatasetFiles( datasetName )
    if not result['OK']:
      print "ERROR: failed to get files for dataset:", result['Message']
    else:
      lfnList = result['Value']
      for lfn in lfnList:
        print lfn

  def dataset_show( self, argss ):
    """ Show existing requested datasets
    """
    long_ = False
    if '-l' in argss:
      long_ = True
      del argss[argss.index('-l')]
    datasetName = ''
    if len( argss ) > 0:
      datasetName = argss[0]

    result = self.fc.getDatasets( datasetName )
    if not result['OK']:
      print "ERROR: failed to get datasets"
      return

    datasetDict = result['Value']
    if not long_:
      for dName in datasetDict.keys():
        print dName
    else:
      fields = ['Key','Value']
      datasets = datasetDict.keys()
      dsAnnotations = {}
      resultAnno = self.fc.getDatasetAnnotation( datasets )
      if resultAnno['OK']:
        dsAnnotations = resultAnno['Value']['Successful']
      for dName in datasets:
        records = []
        print '\n'+dName+":"
        print '='*(len(dName)+1)
        for key,value in datasetDict[dName].items():
          records.append( [key,str( value )] )
        if dName in dsAnnotations:
          records.append( [ 'Annotation',dsAnnotations[dName] ] )  
        printTable( fields, records )  

  def do_stats( self, args ):
    """ Get the catalog statistics
    
        Usage:
          stats
    """
    
    try:
      result = self.fc.getCatalogCounters()
    except AttributeError, x:
      print "Error: no statistics available for this type of catalog:", str(x)
      return
      
    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return 
    fields = ['Counter','Number']
    records = []
    for key,value in result['Value'].items():
      records.append( ( key, str(value) ) )
      #print key.rjust(15),':',result['Value'][key]
    printTable( fields, records )    
      
  def do_rebuild( self, args ):
    """ Rebuild auxiliary tables
    
        Usage:
           rebuild <option>
    """
    
    argss = args.split()
    _option = argss[0]
    start = time.time()
    result = self.fc.rebuildDirectoryUsage()
    if not result['OK']:
      print "Error:", result['Message']
      return 
      
    total = time.time() - start
    print "Directory storage info rebuilt in %.2f sec", total    
    
  def do_repair( self, args ):
    """ Repair catalog inconsistencies
    
        Usage:
           repair catalog
    """
    
    argss = args.split()
    _option = argss[0]
    start = time.time()
    result = self.fc.repairCatalog()
    if not result['OK']:
      print "Error:", result['Message']
      return 
      
    total = time.time() - start
    print "Catalog repaired in %.2f sec", total      
      
  def do_exit(self, args):
    """ Exit the shell.

    usage: exit
    """
    sys.exit(0)

  def emptyline(self): 
    pass      
      
if __name__ == "__main__":
  
  if len(sys.argv) > 2:
    print FileCatalogClientCLI.__doc__
    sys.exit(2)      
  elif len(sys.argv) == 2:
    catype = sys.argv[1]
    if catype == "LFC":
      from DIRAC.Resources.Catalog.LcgFileCatalogProxyClient import LcgFileCatalogProxyClient
      cli = FileCatalogClientCLI(LcgFileCatalogProxyClient())
      print "Starting LFC Proxy FileCatalog client"
      cli.cmdloop() 
    elif catype == "DiracFC":
      from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
      cli = FileCatalogClientCLI(FileCatalogClient())
      print "Starting ProcDB FileCatalog client"
      cli.cmdloop()  
    else:
      print "Unknown catalog type", catype
      

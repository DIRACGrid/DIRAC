#!/usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################
""" File Catalog Client Command Line Interface. """

__RCSID__ = "$Id:  $"

import string
import sys
import cmd
import commands
import os.path
from   types import *

from DIRAC                                  import gConfig, gLogger, S_OK, S_ERROR


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
    self.prompt = 'FC:'+self.cwd+'>'

  def getPath(self,apath):
  
    if apath.find('/') == 0:
      path = apath
    else:
      path = self.cwd+'/'+apath

    return path

  def do_cd(self,args):
    """ Change directory to <path>
    
        usage: cd <path>
    """
 
    argss = string.split(args)
    path = argss[0] 
    if path.find('/') == 0:
      newcwd = path
    else:
      newcwd = self.cwd + '/' + path
      
    newcwd = newcwd.replace(r'//','/')
    
    result =  self.fc.isDirectory(newcwd)    
    if result['OK']:
      if result['Value']['Successful']:
        if result['Value']['Successful'][newcwd]:
        #if result['Type'] == "Directory":
          self.cwd = newcwd
          self.prompt = 'FC:'+self.cwd+'>'
        #else:
        #  print newcwd,'is not a directory'
      else:
        print newcwd,'is not found'
    else:
      print 'Server failed to find the directory',newcwd
          
  def do_pwd(self,args):
    """ Print out the current directory
    
        usage: pwd
    """
    print self.cwd      

  def do_ls(self,args):
    """ Lists directory entries at <path> 

        usage: ls [-l] <path>
    """
    
    argss = string.split(args)
    if len(argss) == 2:
      if argss[1].find('/') == 0:
        path = argss[0]+' '+argss[1]
      else:
        path = argss[0]+' '+self.cwd+'/'+argss[1]
    elif len(argss) == 1:
      if argss[0].find('/') == 0:
        path = argss[0]  
      elif argss[0] == '-l':
        path = argss[0] + ' ' + self.cwd 
      else:
        path = self.cwd+'/'+argss[0]
    else:
      path = self.cwd  
      
    path = path.replace(r'//','/')  
    
    try:
      result =  self.fc.listDirectory(path)
      if result['OK']:
        if result['Value']['Successful']:
          for entry in result['Value']['Successful'][path]['Files']:
            print entry.replace(self.cwd,'').replace('/','')
          for entry in result['Value']['Successful'][path]['SubDirs']:
            print entry.replace(self.cwd,'').replace('/','')  
      else:
        print "ls failed: ",result['Message']
    except Exception, x:
      print "ls failed: ", x
      
  def do_replicas(self,args):
    """ Get replicas for the given file specified by its LFN

        usage: replicas <lfn>
    """
    apath = string.split(args)[0]
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
        
  def do_add(self,args):
    """ Add a file to the catatlog 

        usage: add <lfn> <pfn> <size> <SE> [<guid>]
    """      
       
    path = string.split(args)[0]
    infoDict = {}
    lfn = self.getPath(path)
    infoDict['PFN'] = string.split(args)[1]
    infoDict['Size'] = string.split(args)[2]
    infoDict['SE'] = string.split(args)[3]
    if len(args) == 5:
      guid = string.split(args)[4]
    else:
      status,guid = commands.getstatusoutput('uuidgen')
    infoDict['GUID'] = guid
    infoDict['Checksum'] = ''    
      
    fileDict = {}
    fileDict[lfn] = infoDict  
      
    try:
      result = self.fc.addFile(fileDict)          
      if not result['OK']:
        print "Failed to add file to the catalog: ",
        print result['Message']
      elif result['Value']['Failed'][lfn]:
        print 'Failed to add file:',result['Value']['Failed'][lfn]  
    except Exception, x:
      print "add failed: ", str(x)    
    
  def do_addpfn(self,args):
    """ Add a file to the catatlog 

        usage: addpfn <lfn> <pfn> <SE> 
    """      
    path = string.split(args)[0]
    infoDict = {}
    lfn = self.getPath(path)
    infoDict['PFN'] = string.split(args)[1]
    if infoDict['PFN'] == "''" or infoDict['PFN'] == '""':
      infoDict['PFN'] = ''
    infoDict['SE'] = string.split(args)[2]
      
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
      print "addpfn failed: ", str(x)    
      
  def do_size(self,args):
    """ Get the file size 

        usage: size <lfn> 
    """      
    
    path = string.split(args)[0]
    lfn = self.getPath(path)
    print "lfn:",path
    try:
      result =  self.fc.getFileSize(path)
      if result['Status'] == 'OK':
        print "Size:",result['FileSize']
      else:
        print "Size: failed",result['Message']
    except Exception, x:
      print "size failed: ", x
      
  def do_guid(self,args):
    """ Get the file GUID 

        usage: guid <lfn> 
    """      
    
    path = string.split(args)[0]
    lfn = self.getPath(path)
    print "lfn:",path
    try:
      result =  self.fc.getGuidByLfn(path)
      if result['Status'] == 'OK':
        print "GUID:",result['GUID']
      else:
        print "GUID: failed",result['Message']
    except Exception, x:
      print "guid failed: ", x    
      
  def do_rmpfn(self,args):
    """ Remove replica from the catalog

        usage: rmpfn <lfn> <se>
    """          
    
    path = string.split(args)[0]
    lfn = self.getPath(path)
    print "lfn:",lfn
    rmse = string.split(args)[1]
    try:
      result =  self.fc.getPfnsByLfn(lfn)
      if result['Status'] == 'OK':
        done = 0
        for se,entry in result['Replicas'].items():
          if se == rmse:
            result =  self.fc.removePfn(lfn,entry)
            done = 1
            if result['Status'] == 'OK':
              print "Replica",entry,"removed from the catalog"
            else:
              print "Failed to remove replica",entry
              print result['Message']
              
            break    
        if not done:
          print "Replica SE",rmse,"not found"      
      else:
        print "Failed to get replicas",result['Message']
    except Exception, x:
      print "rmpfn failed: ", x
      
  def do_rm(self,args):
    """ Remove file from the catalog

        usage: rmpfn <lfn> 
    """  
    
    path = string.split(args)[0]
    lfn = self.getPath(path)
    print "lfn:",lfn
    try:
      result =  self.fc.rmFile(lfn)
      if result['Status'] == 'OK':
        print "File",lfn,"removed from the catalog"
      else:
        print "Failed to remove file from the catalog"  
        print result['Message']
    except Exception, x:
      print "rm failed: ", x           
      
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
        from DIRAC.DataManagementSystem.Client.FileCatalogClient import FileCatalogClient
        cli = FileCatalogClientCLI(FileCatalogClient())
        print "Starting ProcDB FileCatalog client"
        cli.cmdloop()  
      else:
        print "Unknown catalog type", catype

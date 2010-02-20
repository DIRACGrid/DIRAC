#!/usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################
""" File Catalog Client Command Line Interface. """

__RCSID__ = "$Id:  $"

import stat
import sys
import cmd
import commands
import os.path
from types  import *
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Security import CS

class DirectoryListing:
  
  def __init__(self):
    
    self.entries = []
  
  def addFile(self,name,fileDict,numericid):
    """ Pretty print of the file ls output
    """        
    if fileDict.has_key('MetaData'):
      fileDict = fileDict['MetaData']

    perm = fileDict['Permissions']
    date = fileDict['ModificationTime']
    nlinks = fileDict['NumberOfLinks']
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
    if numericid:
      gname = str(fileDict['GID'])
    
    self.entries.append( ('-'+self.__getModeString(perm),nlinks,uname,gname,size,date,name) )
    
  def addDirectory(self,name,dirDict,numericid):
    """ Pretty print of the file ls output
    """    
    perm = dirDict['Permissions']
    date = dirDict['ModificationTime']
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
    wList = [ 0 for x in range(7) ]
    for d in self.entries:
      for i in range(7):
        if len(str(d[i])) > wList[i]:
          wList[i] = len(str(d[i]))
        
    for e in self.entries:
      print str(e[0]),
      print str(e[1]).rjust(wList[1]),
      print str(e[2]).ljust(wList[2]),
      print str(e[3]).ljust(wList[3]),
      print str(e[4]).rjust(wList[2]),
      print str(e[5]).rjust(wList[3]),
      print str(e[6])
      

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
  
  def do_add(self,args):
    """ Add a record to the File Catalog
    
        usage:
        
          add user <user_name>
          add group <group_name>
          add file <lfn> <pfn> <size> <SE> [<guid>]
          add pfn <lfn> <pfn> <SE> 
    """
    
    argss = args.split()
    option = argss[0]
    del argss[0]
    if option == 'user':
      return self.adduser(argss) 
    elif option == 'group':
      return self.addgroup(argss) 
    elif option == 'file':
      return self.addfile(argss)
    elif option == 'pfn':
      return self.addfile(argss)
    else:
      print "Unknown option:",option
         
  
  def do_delete(self,args):
    """ Delete records from the File Catalog
    
        usage:
        
          delete user <user_name>
          delete group <group_name>
    """
    
    argss = args.split()
    option = argss[0]
    del argss[0]
    if option == 'user':
      return self.deleteuser(argss) 
    elif option == 'group':
      return self.deletegroup(argss) 
    else:
      print "Unknown option:",option
  
  def adduser(self,argss):
    """ Add new user to the File Catalog
    
        usage: adduser <user_name>
    """
 
    username = argss[0] 
    
    result =  self.fc.addUser(username)
    if not result['OK']:
      print ("Error: %s" % result['Message'])
    else:
      print "User ID:",result['Value']  
      
  def deleteuser(self,args):
    """ Delete user from the File Catalog
    
        usage: deleteuser <user_name>
    """
 
    username = args[0] 
    
    result =  self.fc.deleteUser(username)
    if not result['OK']:
      print ("Error: %s" % result['Message'])    
      
  def addgroup(self,argss):
    """ Add new group to the File Catalog
    
        usage: addgroup <group_name>
    """
 
    gname = argss[0] 
    
    result =  self.fc.addGroup(gname)
    if not result['OK']:
      print ("Error: %s" % result['Message'])
    else:
      print "Group ID:",result['Value']    
      
  def deletegroup(self,args):
    """ Delete group from the File Catalog
    
        usage: deletegroup <group_name>
    """
 
    gname = args[0] 
    
    result =  self.fc.deleteGroup(gname)
    if not result['OK']:
      print ("Error: %s" % result['Message'])         
      
  def do_show(self,args):
    """ Show File Catalog info
    
        usage: show <option>
        
        options:
          users - show all the users defined in the catalog
          groups -  how all the groups defined in the catalog
    """
    
    argss = args.split()
    option = argss[0] 
    
    if option == 'users':
      result =  self.fc.getUsers()
    elif option == 'groups':
      result =  self.fc.getGroups()
    else:
      print ('Unknown option: %s' % option)
      return
        
    if not result['OK']:
      print ("Error: %s" % result['Message'])            
    else:  
      if not result['Value']:
        print "No entries found"
      else:  
        for user,id in result['Value'].items():
          print user.rjust(20),':',id
         
  def do_mkdir(self,args):
    """ Make directory
    
        usage: mkdir <path>
    """
    
    argss = args.split()
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
          print "Successfully created directory;", newdir
      elif result['Value']['Failed']:
        if result['Value']['Failed'].has_key(newdir):  
          print 'Failed to create directory:',result['Value']['Failed'][newdir]
    else:
      print 'Failed to create directory:',result['Message']

  def do_cd(self,args):
    """ Change directory to <path>
    
        usage: cd <path>
    """
 
    argss = args.split()
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
        else:
          print newcwd,'does not exist or is not a directory'
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

        usage: ls [-ltrn] <path>
    """
    
    argss = args.split()
    # Get switches
    long = False
    reverse = False
    timeorder = False
    numericid = False
    path = self.cwd
    if len(argss) > 0:
      if argss[0][0] == '-':
        if 'l' in argss[0]:
          long = True
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
    
    # Get directory contents now
    try:
    #while 1:  
      result =  self.fc.listDirectory(path,long)     
      dList = DirectoryListing()
      if result['OK']:
        if result['Value']['Successful']:
          for entry in result['Value']['Successful'][path]['Files']:
            fname = entry.replace(self.cwd,'').replace('/','')
            if long:
              fileDict = result['Value']['Successful'][path]['Files'][entry]
              if fileDict:
                dList.addFile(fname,fileDict,numericid)
            else:  
              print fname
          for entry in result['Value']['Successful'][path]['SubDirs']:
            dname = entry.replace(self.cwd,'').replace('/','')  
            if long:
              dirDict = result['Value']['Successful'][path]['SubDirs'][entry]
              if dirDict:
                dList.addDirectory(dname,dirDict,numericid)
            else:    
              print dname
          for entry in result['Value']['Successful'][path]['Links']:
            pass
              
          if long:
            dList.printListing(reverse,timeorder)      
      else:
        print "ls failed: ",result['Message']
        
    #  break  
    except Exception, x:
      print "ls failed: ", x
      
  def do_replicas(self,args):
    """ Get replicas for the given file specified by its LFN

        usage: replicas <lfn>
    """
    apath = args.split()
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
        
  def addfile(self,args):
    """ Add a file to the catatlog 

        usage: add <lfn> <pfn> <size> <SE> [<guid>]
    """      
       
    path = args[0]
    infoDict = {}
    lfn = self.getPath(path)
    infoDict['PFN'] = args[1]
    infoDict['Size'] = args[2]
    infoDict['SE'] = args[3]
    if len(args) == 5:
      guid = args[4]
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
      elif result['Value']['Failed']:
        if result['Value']['Failed'].has_key(lfn):
          print 'Failed to add file:',result['Value']['Failed'][lfn]  
      elif result['Value']['Successful']:
        if result['Value']['Successful'].has_key(lfn):
          print "File successfully added to the catalog"    
    except Exception, x:
      print "add file failed: ", str(x)    
    
  def addpfn(self,args):
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
      
  def do_chown(self,args):
    """ Change owner of the given path

        usage: chown <owner> <path> 
    """         
    
    argss = args.split()
    owner = argss[0]
    path = argss[1]
    lfn = self.getPath(path)
    pathDict = {}
    pathDict[lfn] = owner
    
    try:
      result = self.fc.changePathOwner(pathDict)         
      if not result['OK']:
        print "chown failed:",result['Message']
    except Exception, x:
      print "chown failed: ", str(x)       
      
  def do_chgrp(self,args):
    """ Change group of the given path

        usage: chgrp <group> <path> 
    """         
    
    argss = args.split()
    group = argss[0]
    path = argss[1]
    lfn = self.getPath(path)
    pathDict = {}
    pathDict[lfn] = group
    
    try:
      result = self.fc.changePathGroup(pathDict)         
      if not result['OK']:
        print "chgrp failed:",result['Message']
    except Exception, x:
      print "chgrp failed: ", str(x)    
      
  def do_chmod(self,args):
    """ Change permissions of the given path

        usage: chmod <mode> <path> 
    """         
    
    argss = args.split()
    mode = argss[0]
    path = argss[1]
    lfn = self.getPath(path)
    pathDict = {}
    # treat mode as octal 
    pathDict[lfn] = eval('0'+mode)
    
    try:
      result = self.fc.changePathMode(pathDict)         
      if not result['OK']:
        print "chmod failed:",result['Message']
    except Exception, x:
      print "chmod failed: ", str(x)       
      
  def do_size(self,args):
    """ Get the file size 

        usage: size <lfn> 
    """      
    
    path = args.split()[0]
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
    
    path = args.split()[0]
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
    
    path = args.split()[0]
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
    
    path = args.split()[0]
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
        
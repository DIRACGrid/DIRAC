""" Base Storage Class has the following methods:

    Implemented:
      changeDir()
      configure()
      getCwd()
      getPath()
      putDir ()

    Not implemented:
      exists()
      fsize()
      get()
      getDir()
      getPFNBase()
      isdir()
      isfile()
      ls()
      makeDir()
      put()
      remove()
      removeDir()
"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class StorageBase:

  def __init__(self,name,rootdir):
    self.configure(name,rootdir)

  def exists(self,path):
    """Check if the given path exists
    """
    print "Storage.exists: implement me!"

  def fsize(self,fname):
    """Get the physical size of the given file
    """
    print "Storage.fsize: implement me!"

  def get(self,path):
    """Get a local copy of the file specified by its path
    """
    print "Storage.get: implement me!"

  def getDir(self,path):
    """Get locally a directory from the physical storage together with all its
       files and subdirectories.
    """
    print "Storage.getDir: implement me!"

  def getPFNBase(self):
    """ Get the base of the URL for the storage.
        This base is usually supplemented by the file LFN to comply with the LHCb conventions.
    """
    print "Storage.getDir: implement me!"

  def isdir(self,dirname):
    """Check if the given path exists and it is a directory
    """
    print "Storage.isdir: implement me!"

  def isfile(self,fname):
    """Check if the given path exists and it is a file
    """
    print "Storage.isfile: implement me!"

  def ls(self,path):
    """ List the supplied path
    """

  def makeDir(self,newdir):
    """ Make a new directory on the physical storage
    """
    print "Storage.makeDir: implement me!"

  def put(self,fname):
    """Put a copy of the local file to the current directory on the
       physicak storage
    """
    print "Storage.put: implement me!"

  def remove(self,path):
    """Remove physically the file specified by its path
    """
    print "Storage.remove: implement me!"

  def removeDir(self,path):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
    """
    print "Storage.removeDir: implement me!"

  ############################################################################################
  #
  # Below this point the methods are implemented
  #

  def configure(self,name,rootdir):
    """ Called when StorageBase is initialised.
    """
    self.name = name
    self.rootdir = rootdir
    self.cwd = self.rootdir

  def changeDir(self,newdir):
    """ Change the current directory
    """
    newcwd = self.getPath(newdir)
    self.cwd = newcwd

  def getCwd(self):
    """ Get the current directory
    """
    return self.cwd

  def getPath(self,fpath):
    """ Get the full path of the given file name resolving the current working directory.
    """
    if fpath == '/':
      return self.rootdir
    res = re.search("^/",fpath)
    if res is not None:
      return '%s%s' % (self.rootdir,fpath)
    else:
      return '%s/%s' % (self.cwd,fpath)

  def putDir (self,directory):
    """Put local directory together with all its files and subdirectories
       to the current directory on the physical storage.
    """

    remote_cwd = self.cwd
    if os.path.isdir(directory):
      dirname = os.path.basename(directory)
      if not self.isdir(dirname):
        self.makeDir (dirname)
        self.changeDir (dirname)
      else:
        self.changeDir (dirname)
    else:
      print "No such directory: ",  directory
      return S_ERROR( "No such directory: "+directory )

    cwd = os.getcwd()
    os.chdir(directory)
    files = os.listdir(".")
    all_OK = True
    some_OK = False
    if len(files) > 0:
      for name in files:
        if not os.path.isdir(name):
          result = self.put(name);
          if result['Status'] != "OK":
            all_OK = False
          else:
            some_OK = True
        else:
          result = self.putDir (name);
          if result['Status'] != "OK":
            all_OK = False
          else:
            some_OK = True

    os.chdir(cwd)
    self.cwd = remote_cwd
    if not all_OK:
      if some_OK:
        return S_ERROR('Not all the files transfered successfully')
      else:
        return S_ERROR('putDir() failed completely')
    else:
      return S_OK()


########################################################################
# $HeadURL$
# Author : Constantino Calancha
########################################################################
"""
  Utilities used in File Catalog Client Command Line Interface and
  COMDIRAC dls command.

"""
__RCSID__ = "$Id$"

import stat
import os.path
from DIRAC  import gConfig
from DIRAC.Core.Security import CS


def pathFromArgument( arg, cwd ):
  """ Normalize arg and make it absolute
  """
  path = os.path.normpath( arg )
  if not os.path.isabs( path ):
    path = os.path.normpath( os.path.join( cwd, path ))
  return path

class DirectoryListing:
  """ Store/Print the information of a listing in the file catalog
  """
  def __init__(self, list_replicas = False):

    self.entries = []
    self.list_rep = list_replicas

  def addFile(self,name,fileDict,repDict,numericid):
    """ Pretty print of the file ls output
    """
    perm = fileDict['Mode']
    date = fileDict['ModificationDate']
    #nlinks = fileDict.get('NumberOfLinks',0)
    nreplicas = len( repDict )
    size = fileDict['Size']
    if 'Owner' in fileDict:
      uname = fileDict['Owner']
    elif 'OwnerDN' in fileDict:
      result = CS.getUsernameForDN(fileDict['OwnerDN'])
      if result['OK']:
        uname = result['Value']
      else:
        uname = 'unknown'
    else:
      uname = 'unknown'
    if numericid:
      uname = str(fileDict['UID'])
    if 'OwnerGroup' in fileDict:
      gname = fileDict['OwnerGroup']
    elif 'OwnerRole' in fileDict:
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

  def addFileWithReplicas( self,name,fileDict,numericid, replicas ):
    """ Pretty print of the file ls output with replica info
    """
    self.addFile( name, fileDict, replicas, numericid )

    self.entries[ -1 ] += tuple( replicas )

  def addDirectory(self,name,dirDict,numericid):
    """ Pretty print of the file ls output
    """
    perm = dirDict['Mode']
    date = dirDict['ModificationDate']
    nlinks = 0
    size = 0
    if 'Owner' in dirDict:
      uname = dirDict['Owner']
    elif 'OwnerDN' in dirDict:
      result = CS.getUsernameForDN(dirDict['OwnerDN'])
      if result['OK']:
        uname = result['Value']
      else:
        uname = 'unknown'
    else:
      uname = 'unknown'
    if numericid:
      uname = str(dirDict['UID'])
    if 'OwnerGroup' in dirDict:
      gname = dirDict['OwnerGroup']
    elif 'OwnerRole' in dirDict:
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
    if 'Owner' in datasetDict:
      uname = datasetDict['Owner']
    elif 'OwnerDN' in datasetDict:
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
    if 'OwnerGroup' in datasetDict:
      gname = datasetDict['OwnerGroup']
    if numericid:
      gname = str( datasetDict ['GID'] )

    numberOfFiles = datasetDict ['NumberOfFiles']

    self.entries.append( ('s'+self.__getModeString(perm),numberOfFiles,uname,gname,size,date,name) )

  @staticmethod
  def __getModeString(perm):
    """ Get string representation of the file/directory mode
    """
    pstring = ''
    pstring += (perm & stat.S_IRUSR and 'r') or '-'
    pstring += (perm & stat.S_IWUSR and 'w') or '-'
    pstring += (perm & stat.S_IXUSR and 'x') or '-'

    pstring += (perm & stat.S_IRGRP and 'r') or '-'
    pstring += (perm & stat.S_IWGRP and 'w') or '-'
    pstring += (perm & stat.S_IXGRP and 'x') or '-'

    pstring += (perm & stat.S_IROTH and 'r') or '-'
    pstring += (perm & stat.S_IWOTH and 'w') or '-'
    pstring += (perm & stat.S_IXOTH and 'x') or '-'

    return pstring

  @staticmethod
  def humanReadableSize(num,suffix='B'):
    """ Translate file size in bytes to human readable

        Powers of 2 are used (1Mi = 2^20 = 1048576 bytes).
    """
    num = int(num)
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
      if abs(num) < 1024.0:
        return "%3.1f%s%s" % (num, unit, suffix)
      num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

  def printListing(self,reverseOrder,timeorder,sizeorder,humanread):
    """ Print the listing of several directories/files
    """
    if timeorder:
      self.entries.sort( key=lambda x: x[5], reverse = not reverseOrder )
    elif sizeorder:
      self.entries.sort( key=lambda x: x[4], reverse = not reverseOrder )
    else:
      self.entries.sort( key=lambda x: x[6], reverse = reverseOrder )

    # Determine the field widths
    wList = [0] * 7
    for d in self.entries:
      for i in range(7):
        if humanread and i == 4:
          humanreadlen = len( str(self.humanReadableSize( d[4] ) ) )
          if humanreadlen > wList[4]:
            wList[4] = humanreadlen
        else:
          if len( str( d[i] ) ) > wList[i]:
            wList[i] = len( str( d[i] ) )

    for e in self.entries:
      size = e[4]
      if humanread:
        size = self.humanReadableSize( e[4] )
      print str( e[0] ),
      if self.list_rep:
        print str( e[1] ).rjust( wList[1] ),
      print str( e[2] ).ljust( wList[2] ),
      print str( e[3] ).ljust( wList[3] ),
      print str( size ).rjust( wList[4] ),
      print str( e[5] ).rjust( wList[5] ),
      print str( e[6] )

  def addSimpleFile(self,name):
    """ Add single files to be sorted later"""
    self.entries.append(name)

  def printOrdered(self):
    """ Print the ordered list"""
    self.entries.sort()
    for entry in self.entries:
      print entry

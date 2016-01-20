#################################################################
# Class DirectoryListing
# Author: A.T.
# Added 02.03.2015
#################################################################

__RCSID__ = "$Id$"

import stat

from DIRAC import gConfig
from DIRAC.Core.Security import CS

class DirectoryListing( object ):

  def __init__( self ):

    self.entries = []

  def addFile( self, name, fileDict, repDict, numericid ):
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

  def addDirectory( self, name, dirDict, numericid ):
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

  def addDataset( self, name, datasetDict, numericid ):
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

  def __getModeString( self, perm ):
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
  
  def humanReadableSize( self, num, suffix = 'B' ):
    """ Translate file size in bytes to human readable

        Powers of 2 are used (1Mi = 2^20 = 1048576 bytes).
    """
    num = int(num)
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
      if abs(num) < 1024.0:
        return "%3.1f%s%s" % (num, unit, suffix)
      num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

  def printListing( self, reverse, timeorder, sizeorder, humanread ):
    """
    """
    if timeorder:
      if reverse:
        self.entries.sort(key=lambda x: x[5])
      else:
        self.entries.sort(key=lambda x: x[5],reverse=True)
    elif sizeorder:
      if reverse:
        self.entries.sort(key=lambda x: x[4])
      else:
        self.entries.sort(key=lambda x: x[4],reverse=True)
    else:  
      if reverse:
        self.entries.sort(key=lambda x: x[6],reverse=True) 
      else:  
        self.entries.sort(key=lambda x: x[6]) 
        
    # Determine the field widths
    wList = [0] * 7
    for d in self.entries:
      for i in range(7):
        if humanread and i == 4:
          humanreadlen = len(str(self.humanReadableSize(d[4])))
          if humanreadlen > wList[4]:
            wList[4] = humanreadlen
        else:
          if len(str(d[i])) > wList[i]:
            wList[i] = len(str(d[i]))

    for e in self.entries:
      size = e[4]
      if humanread:
        size = self.humanReadableSize(e[4])
      print str(e[0]),
      print str(e[1]).rjust(wList[1]),
      print str(e[2]).ljust(wList[2]),
      print str(e[3]).ljust(wList[3]),
      print str(size).rjust(wList[4]),
      print str(e[5]).rjust(wList[5]),
      print str(e[6])

  def addSimpleFile( self, name ):
    """ Add single files to be sorted later"""
    self.entries.append(name)

  def printOrdered( self ):
    """ print the ordered list"""
    self.entries.sort()
    for entry in self.entries:
      print entry
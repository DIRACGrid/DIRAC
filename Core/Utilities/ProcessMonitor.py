########################################################################
# $Id$
# File :   ProcessMonitor.py
# Author : Stuart Paterson
########################################################################

""" The Process Monitor utility allows to calculate cumulative CPU time for a given PID
    and it's process group.  This is only implemented for linux / proc file systems
    but could feasibly be extended in the future.
"""

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id$"

import os, re, platform

class ProcessMonitor:

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger( 'ProcessMonitor' )
    self.osType = platform.uname()

  #############################################################################
  def getCPUConsumed( self, pid ):
    """Returns the CPU consumed for supported platforms when supplied a PID.
    """
    currentOS = self.__checkCurrentOS()
    if currentOS.lower() == 'linux':
      cpuResult = self.getCPUConsumedLinux( pid )
      return cpuResult
    else:
      self.log.warn( 'Platform %s is not supported' % ( currentOS ) )
      return S_ERROR( 'Unsupported platform' )

  #############################################################################
  def getCPUConsumedLinux( self, pid ):
    """Returns the CPU consumed given a PID assuming a proc file system exists.
    """
    masterProcPath = '/proc/%s/stat' % ( pid )
    if not os.path.exists( masterProcPath ):
      return S_ERROR( 'Process %s does not exist' % ( pid ) )

    #Get the current process list
    pidListResult = self.__getProcListLinux()
    if not pidListResult['OK']:
      return pidListResult

    pidList = pidListResult['Value']
    currentCPU = 0
    #Now recursively add all child process CPU contributions
    childCPUResult = self.__getChildCPUConsumedLinux( pid, pidList )
    currentCPU += childCPUResult

    procGroup = self.__getProcGroupLinux( pid )
    if not procGroup['OK']:
      return procGroup

    procGroup = procGroup['Value'].strip()

    #Next add any contributions from orphan processes in same process group
    for pidCheck in pidList:
      info = self.__getProcInfoLinux( pidCheck )
      if info['OK']:
        info = info['Value']
        if info[4] == procGroup:
          contribution = float( info[13] ) / 100 + float( info[14] ) / 100 + float( info[15] ) / 100 + float( info[16] ) / 100
          currentCPU += contribution
          self.log.debug( 'Added %s to CPU total (now %s) from orphan PID %s %s' % ( contribution, currentCPU, info[0], info[1] ) )

    self.log.verbose( 'Final CPU estimate is %s' % currentCPU )
    return S_OK( currentCPU )

  #############################################################################
  def __getProcListLinux( self ):
    """Gets list of process IDs from /proc/*.
    """
    result = shellCall( 10, 'ls -d /proc/[0-9]*' )

    if not result['OK']:
      if not 'Value' in result:
        return result
    procList = result['Value'][1].replace( '/proc', '' ).split( '\n' )

    return S_OK( procList )

  #############################################################################
  def __getChildCPUConsumedLinux( self, pid, pidList ):
    """Adds contributions to CPU total from child processes recursively.
    """
    childCPU = 0
    for pidCheck in pidList:
      info = self.__getProcInfoLinux( pidCheck )
      if info['OK']:
        info = info['Value']
        if info[3] == pid:
          contribution = float( info[13] ) / 100 + float( info[14] ) / 100 + float( info[15] ) / 100 + float( info[16] ) / 100
          childCPU += contribution
          self.log.debug( 'Added %s to CPU total (now %s) from child PID %s %s' % ( contribution, childCPU, info[0], info[1] ) )
          childCPU += self.__getChildCPUConsumedLinux( pidCheck, pidList )

    return childCPU

  #############################################################################
  def __getProcInfoLinux( self, pid ):
    """Attempts to read /proc/PID/stat and returns list of items if ok.
    """
    procPath = '/proc/%s/stat' % ( pid )
    try:
      fopen = open( procPath, 'r' )
      procStat = fopen.readline()
      fopen.close()
    except Exception:
      return S_ERROR( 'Not able to check %s' % pid )
    return S_OK( procStat.split( ' ' ) )

  #############################################################################
  def __getProcGroupLinux( self, pid ):
    """Returns UID for given PID.
    """
    result = shellCall( 10, 'ps --no-headers -o pgrp -p %s' % ( pid ) )
    if not result['OK']:
      if  not 'Value' in result:
        return result

    return S_OK( result['Value'][1] )

  #############################################################################
  def __checkCurrentOS( self ):
    """Checks it is possible to determine CPU consumed with this utility
       for the current OS.
    """
    localOS = None
    self.osType = platform.uname()
    if re.search( 'Darwin', self.osType[0] ):
      localOS = 'Mac'
    elif re.search( 'Windows', self.osType[0] ):
      localOS = 'Windows'
    else:
      localOS = 'Linux'
      self.log.debug( 'Will determine CPU consumed for %s flavour OS' % ( localOS ) )
    return localOS

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

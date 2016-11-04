#!/usr/bin/env python
"""
command line tool to remove local and remote proxies
"""


import sys
import os

import DIRAC
from DIRAC import gLogger, S_OK
from DIRAC.Core.Security import Locations
from DIRAC.Core.Base import Script

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Security import ProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

__RCSID__ = "$Id$"


class Params( object ):
  """
  handles input options for dirac-proxy-destroy
  """
  def __init__( self ):
    """
    creates a Params class with default values
    """
    self.vos = []
    self.delete_all = False

  def addVO( self, voname ):
    """
    adds a VO to be deleted from remote proxies
    """
    self.vos.append( voname )
    return S_OK()

  def setDeleteAll( self, _ ):
    """
    deletes local and remote proxies
    """
    self.delete_all = True
    return S_OK()

  def needsValidProxy( self ):
    """
    returns true if any remote operations are required
    """
    return self.vos or self.delete_all


  # note the magic : and =
  def registerCLISwitches( self ):
    """
    add options to dirac option parser
    """
    Script.setUsageMessage( "Script to delete a dirac proxy. Default: delete local proxy only." )
    Script.registerSwitch( "a", "all", "Delete the local and all uploaded proxies (the nuclear option)", self.setDeleteAll )
    Script.registerSwitch( "v:", "vo=", "Delete uploaded proxy for vo name given", self.addVO )



def getProxyGroups():
  """
  Returns a set of all remote proxy groups stored on the dirac server for the user invoking the command.
  """
  proxies = gProxyManager.getUserProxiesInfo()
  if not proxies[ 'OK' ]:
    raise RuntimeError( 'Could not retrieve uploaded proxy info.' )


  user_groups = set()
  for dn in proxies[ 'Value' ]:
    dn_groups = set( proxies[ 'Value' ][ dn ].keys() )
    user_groups.update( dn_groups )

  return user_groups


def mapVoToGroups( voname ):
  """
  Returns all groups available for a given VO as a set.
  """

  vo_dict =  Registry.getGroupsForVO( voname )
  if not vo_dict[ 'OK' ]:
    raise RuntimeError( 'Could not retrieve groups for vo %s.' % voname )

  return set( vo_dict[ 'Value' ] )



def deleteRemoteProxy( userdn, vogroup ):
  """
  Deletes proxy for a vogroup for the user envoking this function.
  Returns a list of all deleted proxies (if any).
  """
  rpcClient = RPCClient( "Framework/ProxyManager" )
  retVal = rpcClient.deleteProxyBundle( [ ( userdn, vogroup ) ] )

  if retVal[ 'OK' ]:
    gLogger.notice( 'Deleted proxy for %s.' % vogroup )
  else:
    gLogger.error( 'Failed to delete proxy for %s.' % vogroup )


def deleteLocalProxy( proxyLoc ):
  """
  Deletes the local proxy.
  Returns false if no local proxy found.
  """
  try:
    os.unlink( proxyLoc )
  except IOError:
    gLogger.error( 'IOError: Failed to delete local proxy.' )
    return
  except OSError:
    gLogger.error( 'OSError: Failed to delete local proxy.' )
    return
  gLogger.notice( 'Local proxy deleted.' )



def main():
  """
  main program entry point
  """
  options = Params()
  options.registerCLISwitches()

  Script.parseCommandLine( ignoreErrors = True )

  if options.delete_all and options.vos:
    gLogger.error( "-a and -v options are mutually exclusive. Please pick one or the other." )
    return 1

  proxyLoc = Locations.getDefaultProxyLocation()

  if not os.path.exists( proxyLoc ):
    gLogger.error( "No local proxy found in %s, exiting." % proxyLoc )
    return 1

  result = ProxyInfo.getProxyInfo( proxyLoc, True )
  if not result[ 'OK' ]:
    raise RuntimeError( 'Failed to get local proxy info.' )

  if result[ 'Value' ][ 'secondsLeft' ] < 60 and options.needsValidProxy():
    raise RuntimeError( 'Lifetime of local proxy too short, please renew proxy.' )

  userDN=result[ 'Value' ][ 'identity' ]

  if options.delete_all:
    # delete remote proxies
    remote_groups = getProxyGroups()
    if not remote_groups:
      gLogger.notice( 'No remote proxies found.' )
    for vo_group in remote_groups:
      deleteRemoteProxy( userDN, vo_group )
    # delete local proxy
    deleteLocalProxy( proxyLoc )
  elif options.vos:
    vo_groups = set()
    for voname in options.vos:
      vo_groups.update(mapVoToGroups( voname ) )
    # filter set of all groups to only contain groups for which there is a user proxy
    user_groups = getProxyGroups()
    vo_groups.intersection_update( user_groups )
    if not vo_groups:
      gLogger.notice( 'You have no proxies registered for any of the specified VOs.' )
    for group in vo_groups:
      deleteRemoteProxy( userDN, group )
  else:
    deleteLocalProxy( proxyLoc )

  return 0

if __name__ == "__main__":
  try:
    retval = main()
    DIRAC.exit( retval )
  except RuntimeError as rtError:
    gLogger.error( 'Operation failed: %s' % str( rtError ) )
  DIRAC.exit( 1 )

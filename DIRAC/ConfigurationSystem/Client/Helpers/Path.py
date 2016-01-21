########################################################################
# $HeadURL$
# File :   Path.py
# Author : Ricardo Graciani
########################################################################
"""
Some Helper class to build CFG paths from tuples
"""
__RCSID__ = "$Id$"

cfgInstallSection = 'LocalInstallation'
cfgResourceSection = 'Resources'
import os

def cfgPath( *args ):
  """
  Basic method to make a path out of a tuple of string, any of them can be already a path
  """
  path = os.path.join( *[str( k ) for k in args] )  
  return os.path.normpath( path )

def cfgInstallPath( *args ):
  """
  Path to Installation/Configuration Options
  """
  return cfgPath( cfgInstallSection, *args )

def cfgPathToList( arg ):
  """
  Basic method to split a cfgPath in to a list of strings
  """
  from types import StringTypes
  listPath = []
  if type( arg ) not in StringTypes:
    return listPath
  while arg.find( '/' ) == 0:
    arg = arg[1:]
  return arg.split( '/' )

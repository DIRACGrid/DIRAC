#!/usr/bin/env python
''' buildScriptsDocs

  Prototype to build scripts documentation from the scripts docstrings. As the
  scripts are not very uniform, this script is full of hacks to avoid them. They
  will eventually dissapear.

'''

# defined on DIRACDocs/source/Tools/fakeEnvironment
import fakeEnvironment

import getpass
import glob
import os
import shutil
import sys
import tempfile
import commands

from DIRAC           import rootPath
from DIRAC.Core.Base import Script

def getTmpDir():
  ''' Makes a python module on a temporary directory
  '''

  try:
    tmpDir = tempfile.mkdtemp()
  except IOError:
    sys.exit( 'IOError creating tmp dir' )
    
  initFile = os.path.join( tmpDir, '__init__.py' )
  try:
    open( initFile, 'w' ).close()
  except IOError:
    sys.exit( 'Cannot touch %s/__init__.py' % tmpDir )

  # Appends tmpDir to PYTHONPATH, so that we can import the scripts directly
  sys.path.append( tmpDir )

  return tmpDir

def generateCommandReference( tmpDir ):
  ''' Makes a directory where the CommandReference rst are going to be generated 
  '''
  
  commandReferenceDir = os.path.join( tmpDir, 'CommandReference' )
  try:
    os.mkdir( commandReferenceDir )
  except OSError:
    sys.exit( 'Error creating %s' % commandReferenceDir )  

  return commandReferenceDir

def prepareScripts( tmpDir ):
  ''' Given the DIRAC rootPath ( this assumes DIRAC is on the pythonpath ! )
      gets the paths for all scripts 
  '''  
  
  diracPath = os.path.join( rootPath, 'DIRAC' )
  if not os.path.exists( diracPath ):      
    sys.exit( '%s does not exist' % diracPath )
  
  # Get all scripts
  scriptsPath = os.path.join( diracPath, '*', 'scripts', '*.py' )

  scriptsDict = {}

  # Get all scripts on scriptsPath and sorts them, this will make our life easier
  # afterwards
  scripts = glob.glob( scriptsPath )
  scripts.sort()     
  for scriptPath in scripts:
    
    # Few modules still have __init__.py on the scripts directory
    if '__init__' in scriptPath or 'build' in scriptPath:
      continue
        
    # ..rubish/rubish/AnyDIRACSystem/scripts/scriptName.py
    moduleName, _, scriptName = scriptPath.split( '/' )[ -3: ]
    newScriptName = scriptName.replace( '-', '_' )
    newScriptPath = os.path.join( tmpDir, newScriptName ) 
    
    try:
      shutil.copy( scriptPath, newScriptPath )
    except IOError:
      sys.exit( 'Copy from %s to %s failed' % ( scriptPath, newScriptPath ) )
      
    # Let's try EAFP style...  
    # We store the scripts on each module with the NEW name !
    newScriptName = newScriptName.split( '.' )[ 0 ]
    try:
      scriptsDict[ moduleName ].append( newScriptName )
    except KeyError:
      scriptsDict[ moduleName ] = [ newScriptName ]         
      
  return scriptsDict

def writeScriptsDocs( scriptsDict, commandRefPath ):
  ''' Function that iterates over the scripts and imports them. It requires scripts
      to be written in such what they do not crash the import raising exceptions.
  '''
  
  # To avoid the script crashing us, we crash it
  def killMe( *args, **kwargs ): raise NameError( 'KillMe' )
  # getpass also bothers us, we hack it...
  def shutUp(): pass
  getpass.getpass = shutUp
  
  # Create index.rst, just the header, we will append lines afterwards
  adminPath,userPath = createIndexRST( commandRefPath )
  
  # Sort module names to have a nicer presentation
  modules = scriptsDict.keys()
  modules.sort()
  
  scriptPaths = { 'admin' : [], 'user' : [] }
  
  #for module, newScriptNames in scriptsDict.iteritems():
  for module in modules:
    
    # Create module header on the index.rst 
    createModuleRST( adminPath, module )
    createModuleRST( userPath, module )
    
    for newScriptName in scriptsDict[ module ]:
      
      isAdmin = 'admin' in newScriptName
      
      # Before anything crashes, let's write the script rst file. If we can import
      # it, we will populate if afterwards.
      if isAdmin:
        indexPath = adminPath
      else:
        indexPath = userPath
          
      scriptPath = createScriptRST( indexPath, newScriptName )

      if isAdmin:
        scriptPaths[ 'admin' ].append( scriptPath )
      else:
        scriptPaths[ 'user' ].append( scriptPath )  
           
      # This is ugly, but otherwise Script piles up the arguments
      #reload( Script )
      #Script.parseCommandLine = killMe

      docToWrite = ''
      commandName = os.path.basename( scriptPath ).replace('.rst','')
      status, docToWrite = commands.getstatusoutput( commandName+' -h' )
      writeScriptRST( scriptPath, docToWrite )
            
      #script = None
      #try:
      #  script     = __import__( newScriptName )
      #  docToWrite = script.__doc__
      #except SystemExit:
      #  print 'exited: %s' % newScriptName 
      #except NameError, x:
      #  print 'killMe: %s' % newScriptName, str(x)
      #except:
      #  print 'other: %s' % newScriptName

      #if script:
      #  docToWrite = script.__doc__
      #  writeScriptRST( scriptPath, docToWrite )
  
  return scriptPaths    
  
#...............................................................................
# Functions generating rst files

def createIndexRST( commandRefPath ):
  ''' Creates a new index.rst file, which will keep links to all scripts 
  '''  
  
  adminPath = os.path.join( commandRefPath, 'admin.rst' )
  userPath  = os.path.join( commandRefPath, 'user.rst' )
  
  def writeHeader( indexPath ):
    with open( indexPath, 'w' ) as index:    
      index.write( '=' * 40 )
      index.write( '\nCommand reference (|release|)\n' )
      index.write( '=' * 40 )
  
  writeHeader( adminPath )
  writeHeader( userPath )  
    
  return adminPath, userPath
 
def createModuleRST( indexPath, moduleName ):
  ''' Adds the toctree and module name for a given module to index.rst
  '''
  
  with open( indexPath, 'a' ) as index:
    index.write( '\n\n%s\n' % moduleName )
    index.write( '\n.. toctree::' )
    index.write( '\n    :maxdepth: 2\n' )   
    
def createScriptRST( indexPath, newScriptName ):
  ''' Creates realScriptName.rst file and links it to index.rst
  '''
  
  realScriptName = newScriptName.replace( '_', '-' )

  with open( indexPath, 'a' ) as index:  
    index.write( '\n    %s' % realScriptName )

  scriptPath = os.path.join( indexPath.rsplit( '/', 1 )[ 0 ], '%s.rst' % realScriptName )  
  with open( scriptPath, 'w' ) as script:
    script.write( '=' * len( realScriptName ) )
    script.write( '\n%s\n' % realScriptName )
    script.write( '=' * len( realScriptName ) )
    script.write( '\n' )
  
  return scriptPath

def writeScriptRST( scriptPath, docString ):
  ''' Given a scriptPath and a docString, writes the second into the first
  '''
  
  # Some of the scripts do not have docstring !
  if not docString:
    docString = 'NO DOCSTRING'
    print 'NO DOCSTRING: %s' % scriptPath  
  with open( scriptPath, 'a' ) as script:
    script.write( '\n::' )
    lines = docString.split('\n')
    for line in lines:
      script.write( '\n  ' + line )
  
#...............................................................................

def overwriteCommandReference( commandRefPath, scriptPaths ):
  ''' Overwrites the default CommandReference with the latest CommandReference
      generated.
  '''  
    
  whereAmI = os.path.dirname( os.path.abspath( __file__ ) )
  _relativePathAdmin = '../source/AdministratorGuide/CommandReference'
  _relativePathUser  = '../source/UserGuide/CommandReference'
  
  newAdminPath = os.path.abspath( os.path.join( whereAmI, _relativePathAdmin ) )
  newUserPath  = os.path.abspath( os.path.join( whereAmI, _relativePathUser ) )
  
  def cleanPrevious( newPath ):
    
    try:
      shutil.rmtree( newPath )
    except OSError:
      sys.exit( 'OSError removing %s' % newPath )  
    except shutil.Error:
      sys.exit( 'shutil.Error removing %s' % newPath )
  
  cleanPrevious( newAdminPath )
  cleanPrevious( newUserPath )
  
  def copyNew( newPath, scriptPaths, indexName ):
    
    try:
      os.mkdir( newPath )
    except OSError:
      sys.exit( 'Error creating %s' % newPath )
    for scriptPath in scriptPaths[ indexName ]:
      try:
        shutil.copy( scriptPath, newPath )  
      except shutil.Error:
        sys.exit( 'shutil.Error copying %s' % newPath )

  copyNew( newAdminPath, scriptPaths, 'admin' )
  copyNew( newUserPath,  scriptPaths, 'user' ) 
  
  def copyIndex( newPath, commandRefPath, indexName ):

    indexPath    = os.path.abspath( os.path.join( commandRefPath, '%s.rst' % indexName ) )
    newIndexPath = os.path.abspath( os.path.join( newPath, 'index.rst' ) )    
    try:
      shutil.copy( indexPath, newIndexPath )  
    except shutil.Error:
      sys.exit( 'shutil.Errir copying %s' % indexPath )
    
  copyIndex( newAdminPath, commandRefPath, 'admin' )
  copyIndex( newUserPath, commandRefPath, 'user' )    
    
#...............................................................................
# run
  
def run( tmpDir = None ):
  ''' Generates a temp directory where to copy over all scripts renamed so
      that we can import them into python. Once that is done, we import them
      one by one, to get the docstring.
  '''
  
  if tmpDir is None:
    tmpDir = getTmpDir()
    
  commandRefPath = generateCommandReference( tmpDir )
  scriptsDict    = prepareScripts( tmpDir )
  sys.path.append( tmpDir )
  scriptPaths    = writeScriptsDocs( scriptsDict, commandRefPath )
  
  print 'RSTs generated'
  
  overwriteCommandReference( commandRefPath, scriptPaths )
  
  print 'Done'

#...............................................................................
# main
  
if __name__ == "__main__" :

  try:
    tmpDir = sys.argv[ 1 ]  
  except IndexError:  
    tmpDir = None
  
  run( tmpDir )
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
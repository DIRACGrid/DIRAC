#!/usr/bin/env python
''' buildCodeDOC
  
  It accepts as argument the DIRAC version ( or branch name )

'''

# defined on DIRACDocs/source/Tools/fakeEnvironment
import fakeEnvironment

import DIRAC

import os
import pkgutil
import sys
import tempfile

DOCMODULES = [ 'API', 'Client', 'Service', 'Utilities' ]

def getTmpDir():
  ''' Creates a temporary dir and adds it to sys.path so that we can import
      whatever lies there.
  '''

  try:
    tmpDir = tempfile.mkdtemp()
  except IOError:
    sys.exit( 'IOError creating tmp dir' )
      
  sys.path.append( tmpDir )

  return tmpDir

#...............................................................................
# Functions generating rst files

def getCodeDocumentationPath():

  whereAmI = os.path.dirname( os.path.abspath( __file__ ) )
  relativePathToWrite = '../source/CodeDocumentation'
  
  codeDocumentationPath = os.path.abspath( os.path.join( whereAmI, 
                                                         relativePathToWrite ) )

  try:
    os.mkdir( codeDocumentationPath )
  except OSError:
    sys.exit( 'Cannot create %s' % codeDocumentationPath )

  return codeDocumentationPath  

def getDIRACPackages():
  
  pkgpath = os.path.dirname( DIRAC.__file__ )
  packages = [ name for _, name, _ in pkgutil.iter_modules([pkgpath]) ]
  
  packages.sort()
  packages.pop(packages.index('Resources'))
  packages.pop(packages.index('Workflow'))
  
  return packages

def getPackageModules( package ):

  diracPackage = __import__( 'DIRAC.%s' % package, globals(), locals(), [ '*' ] )

  

  pkgpath = os.path.dirname( diracPackage.__file__ )
  modules = [ name for _, name, _ in pkgutil.iter_modules([pkgpath]) ]  
  modules.sort()
  
  return modules

def writeIndexHeader( indexFile, title, depth=2 ):

  indexFile.write( '\n' + '=' * len( title ) )
  indexFile.write( '\n%s\n' % title )
  indexFile.write( '=' * len( title ) )
  indexFile.write( '\n\n.. toctree::' )
  indexFile.write( '\n   :maxdepth: %d\n' % depth )

def writeCodeDocumentationIndexRST( codeDocumentationPath, diracPackages ):
  '''
  '''
    
  indexPath = os.path.join( codeDocumentationPath, 'index.rst' )
  with open( indexPath, 'w' ) as index:
    index.write( '.. _code_documentation:\n\n')
    index.write( 'Code Documentation (|release|)\n' )
    index.write( '------------------------------\n' )
    writeIndexHeader( index, 'Systems', 1 )    
    for diracPackage in diracPackages:
      if "System" in diracPackage:
        index.write( '\n   %s/index.rst\n' % diracPackage )
    writeIndexHeader( index, 'Other', 1 )    
    for diracPackage in ['Interfaces','Core']:
      index.write( '\n   %s/index.rst\n' % diracPackage )      

def writePackageDocumentation( tmpDir, codeDocumentationPath, diracPackage ):
  
  packageDir = os.path.join( codeDocumentationPath, diracPackage ) 
  try:
    os.mkdir( packageDir )
  except OSError:
    sys.exit( 'Cannot create %s' % packageDir )

  modulePackages = getPackageModules( diracPackage )

  indexPath = os.path.join( packageDir, 'index.rst' )
  with open( indexPath, 'w' ) as index:
    titlePackage = diracPackage
    if diracPackage == "Core":
      titlePackage = "Utilities"
    elif diracPackage == "Interfaces":
      titlePackage = "API"  
    writeIndexHeader( index, titlePackage )
    
    for modulePackage in modulePackages:
      if not modulePackage in DOCMODULES:
        continue
      index.write( '\n\n   %s/index.rst' % modulePackage )
      packageModPath = os.path.join( packageDir, modulePackage )
        
      try:
        os.mkdir( packageModPath )
      except OSError:
        sys.exit( 'Cannot create %s' % packageModPath )

      packModPackages = getPackageModules( '%s.%s' % ( diracPackage, modulePackage ) )

      packageModPathIndex = os.path.join( packageModPath, 'index.rst' )
      with open( packageModPathIndex, 'w' ) as packModFile:
        writeIndexHeader( packModFile, modulePackage )
                    
        for packModPackage in packModPackages:
          if 'lfc_dfc_copy' in packModPackage or "lfc_dfc_db_copy" in packModPackage:
            continue
          if 'CLI' in packModPackage:
            continue

          route = 'DIRAC/%s/%s/%s.py' % ( diracPackage, modulePackage, packModPackage )
        
          route2 = tmpDir + '/../../' + route

          if not os.path.isfile( route2 ):
            if not packModPackage in ['Helpers']:
              continue
            packModFile.write( '\n\n   %s/index.rst' % packModPackage )
            dir2 = 'DIRAC/%s/%s/%s' % ( diracPackage, modulePackage, packModPackage ) 
            subModPackages = getPackageModules( '%s.%s.%s' % ( diracPackage, modulePackage, packModPackage ) )
            subModPath = os.path.join( packageModPath, packModPackage )
            subModPathIndex = os.path.join( subModPath, 'index.rst' )
            os.mkdir( subModPath )
            with open( subModPathIndex, 'w' ) as subModFile:
              writeIndexHeader( subModFile, packModPackage )  
              for subModPackage in subModPackages:                
                subModFile.write( '\n\n   %s' % subModPackage )
                subModPackagePath = os.path.join( subModPath, '%s.rst' % subModPackage )
                f = open( subModPackagePath, 'w' )
                f.write( '=' * len( subModPackage ) )
                f.write( '\n%s\n' % subModPackage )
                f.write( '=' * len( subModPackage ) )
                f.write( '\n' )
                f.write( '\n.. automodule:: DIRAC.%s.%s.%s.%s' % ( diracPackage, modulePackage, packModPackage, subModPackage ) )
                f.write( '\n   :members:' )
                f.close() 
          else:    
            packModFile.write( '\n\n   %s' % packModPackage )
            packModPackagePath = os.path.join( packageModPath, '%s.rst' % packModPackage )
            f = open( packModPackagePath, 'w' )
            f.write( '=' * len( packModPackage ) )
            f.write( '\n%s\n' % packModPackage )
            f.write( '=' * len( packModPackage ) )
            f.write( '\n' )
            f.write( '\n.. automodule:: DIRAC.%s.%s.%s' % ( diracPackage, modulePackage, packModPackage ) )
            f.write( '\n   :members:' )
            f.close() 
#...............................................................................
# run

def run( diracVersion, tmpDir = None ):

  if tmpDir is None:
    tmpDir = getTmpDir()

  diracPackages         = getDIRACPackages()    
  codeDocumentationPath = getCodeDocumentationPath()
  writeCodeDocumentationIndexRST( codeDocumentationPath, diracPackages )
  for diracPackage in diracPackages:
    writePackageDocumentation( tmpDir, codeDocumentationPath, diracPackage )
  
#...............................................................................
# main

if __name__ == "__main__":

  try:
    tmpdir = sys.argv[ 1 ]
  except IndexError:  
    tmpdir = None

  try:
    diracVersion = sys.argv[ 2 ]
  except IndexError:  
    diracVersion = 'integration'
  
  run( diracVersion, tmpdir )
  
#...............................................................................  
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  
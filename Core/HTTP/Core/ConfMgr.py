
import imp
import os
from DIRAC import gConfig, gLogger
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals

BASECS = "WebApp"

def loadWebAppCFGFiles( mainExt="WebAppDIRAC" ):
  """
  Load WebApp/web.cfg definitions
  """
  exts = []
  for ext in CSGlobals.getCSExtensions():
    if ext == "DIRAC":
      continue
    if ext[-5:] != "DIRAC":
      ext = "%sDIRAC" % ext
    if ext != mainExt:
      exts.append( ext )
  exts.append( "DIRAC" )
  exts.append( mainExt )
  webCFG = CFG()
  for modName in reversed( exts ):
    try:
      modPath = imp.find_module( modName )[1]
    except ImportError:
      continue
    gLogger.verbose( "Found module %s at %s" % ( modName, modPath ) )
    cfgPath = os.path.join( modPath, "WebApp", "web.cfg" )
    if not os.path.isfile( cfgPath ):
      gLogger.verbose( "Inexistant %s" % cfgPath )
      continue
    try:
      modCFG = CFG().loadFromFile( cfgPath )
    except Exception, excp:
      gLogger.error( "Could not load %s: %s" % ( cfgPath, excp ) )
      continue
    gLogger.verbose( "Loaded %s" % cfgPath )
    expl = [ BASECS ]
    while len( expl ):
      current = expl.pop( 0 )
      if not modCFG.isSection( current ):
        continue
      if modCFG.getOption( "%s/AbsoluteDefinition" % current, False ):
        gLogger.verbose( "%s:%s is an absolute definition" % ( modName, current ) )
        try:
          webCFG.deleteKey( current )
        except:
          pass
        modCFG.deleteKey( "%s/AbsoluteDefinition" % current )
      else:
        for sec in modCFG[ current ].listSections():
          expl.append( "%s/%s" % ( current, sec ) )
    #Add the modCFG
    webCFG = webCFG.mergeWith( modCFG )
  gConfig.loadCFG( webCFG )

def getRawSchema():
  """
  Load the schema from the CS
  """
  base = "%s/Schema" % ( BASECS )
  schema = []
  explore = [ ( "", schema ) ]
  while len( explore ):
    parentName, parentData = explore.pop( 0 )
    fullName = "%s/%s" % ( base, parentName )
    result = gConfig.getSections( fullName )
    if not result[ 'OK' ]:
      continue
    sectionsList = result[ 'Value' ]
    for sName in sectionsList:
      sData = []
      parentData.append( ( "%s/%s" % ( parentName, sName ), sData ) )
      explore.append( ( sName, sData ) )
    result = gConfig.getOptions( fullName )
    if not result[ 'OK' ]:
      continue
    optionsList = result[ 'Value' ]
    for opName in optionsList:
      opVal = gConfig.getValue( "%s/%s" % ( fullName, opName ) )
      if opVal.find( "link|" ) == 0:
        parentData.append( ( "link", opName, opVal[5:] ) )
      else:
        parentData.append( ( "app", opName, opVal ) )
  return schema

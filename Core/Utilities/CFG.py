# $HeadURL$
__RCSID__ = "$Id$"

import types
import copy
import os
try:
  import zipfile
  gZipEnabled = True
except ImportError:
  gZipEnabled = False

try:
  from DIRAC.Core.Utilities import S_OK, S_ERROR
  from DIRAC.Core.Utilities import List, ThreadSafe

  gCFGSynchro = ThreadSafe.Synchronizer( recursive = True )
except Exception:
  #We're out of python, define required utilities
  import threading
  from types import StringTypes

  def S_ERROR( messageString = '' ):
    return { 'OK' : False, 'Message' : str( messageString )  }

  def S_OK( value = '' ):
    return { 'OK' : True, 'Value' : value }

  class ListDummy:
    def fromChar( self, inputString, sepChar = "," ):
      if not ( type( inputString ) in StringTypes and
               type( sepChar ) in StringTypes and
               sepChar ): # to prevent getting an empty String as argument
        return None

      return [ fieldString.strip() for fieldString in inputString.split( sepChar ) if len( fieldString.strip() ) > 0 ]

  List = ListDummy()

  class Synchronizer:
    """ Class enapsulating a lock
    allowing it to be used as a synchronizing
    decorator making the call thread-safe"""

    def __init__( self, lockName = "", recursive = False ):
      self.lockName = lockName
      if recursive:
        self.lock = threading.RLock()
      else:
        self.lock = threading.Lock()

    def __call__( self, funcToCall ):
      def lockedFunc( *args, **kwargs ):
        try:
          if self.lockName:
            print "LOCKING", self.lockName
          self.lock.acquire()
          return funcToCall( *args, **kwargs )
        finally:
          if self.lockName:
            print "UNLOCKING", self.lockName
          self.lock.release()
      return lockedFunc

  gCFGSynchro = Synchronizer( recursive = True )
  #END OF OUT OF DIRAC

#START OF CFG MODULE

class CFG:

  def __init__( self ):
    """
    Constructor
    """
    self.__orderedList = []
    self.__commentDict = {}
    self.__dataDict = {}
    self.reset()

  @gCFGSynchro
  def reset( self ):
    """
    Empty the CFG
    """
    self.__orderedList = []
    self.__commentDict = {}
    self.__dataDict = {}

  @gCFGSynchro
  def createNewSection( self, sectionName, comment = "", contents = False ):
    """
    Create a new section

    @type sectionName: string
    @param sectionName: Name of the section
    @type comment: string
    @param comment: Comment for the section
    @type contents: CFG
    @param contents: Optional cfg with the contents of the section.
    """
    if sectionName == "":
      raise Exception( "Creating a section with empty name! You shouldn't do that!" )
    if sectionName.find( "/" ) > -1:
      recDict = self.getRecursive( sectionName, -1 )
      parentSection = recDict[ 'value' ]
      if type( parentSection ) in ( types.StringType, types.UnicodeType ):
        raise Exception( "Entry %s doesn't seem to be a section" % recDict[ 'key' ] )
      return parentSection.createNewSection( recDict[ 'levelsBelow' ], comment, contents )
    self.__addEntry( sectionName, comment )
    if sectionName not in self.__dataDict:
      if not contents:
        self.__dataDict[ sectionName ] = CFG()
      else:
        self.__dataDict[ sectionName ] = contents
    else:
      raise Exception( "%s key already exists" % sectionName )
    return self.__dataDict[ sectionName ]

  def __overrideAndCloneSection( self, sectionName, oCFGToClone ):
    """
    Replace the contents of a section

    @type sectionName: string
    @params sectionName: Name of the section
    @type oCFGToClone: CFG
    @param oCFGToClone: CFG with the contents of the section
    """
    if sectionName not in self.listSections():
      raise Exception( "Section %s does not exist" % sectionName )
    self.__dataDict[ sectionName ] = oCFGToClone.clone()

  @gCFGSynchro
  def setOption( self, optionName, value, comment = "" ):
    """
    Create a new option.

    @type optionName: string
    @param optionName: Name of the option to create
    @type value: string
    @param value: Value of the option
    @type comment: string
    @param comment: Comment for the option
    """
    if optionName == "":
      raise Exception( "Creating an option with empty name! You shouldn't do that!" )
    if optionName.find( "/" ) > -1:
      recDict = self.getRecursive( optionName, -1 )
      parentSection = recDict[ 'value' ]
      if type( parentSection ) in ( types.StringType, types.UnicodeType ):
        raise Exception( "Entry %s doesn't seem to be a section" % recDict[ 'key' ] )
      return parentSection.setOption( recDict[ 'levelsBelow' ], value, comment )
    self.__addEntry( optionName, comment )
    self.__dataDict[ optionName ] = str( value )

  def __addEntry( self, entryName, comment ):
    """
    Add an entry and set the comment

    @type entryName: string
    @param entryName: Name of the entry
    @type comment: string
    @param comment: Comment for the entry
    """
    if not entryName in self.__orderedList:
      self.__orderedList.append( entryName )
    self.__commentDict[ entryName ] = comment

  def existsKey( self, key ):
    """
    Check if an option/section with that name exists

    @type key: string
    @param key: Name of the option/section to check
    @return: Boolean with the result
    """
    return key in self.__orderedList

  def sortAlphabetically( self, ascending = True ):
    """
    Order this cfg alphabetically
    returns true if modified
    """
    unordered = list( self.__orderedList )
    if ascending:
      self.__orderedList.sort()
    else:
      self.__orderedList.reverse()
    return unordered != self.__orderedList

  @gCFGSynchro
  def deleteKey( self, key ):
    """
    Delete an option/section

    @type key: string
    @param key: Name of the option/section to delete
    @return: Boolean with the result
    """
    if key in self.__orderedList:
      del( self.__commentDict[ key ] )
      del( self.__dataDict[ key ] )
      pos = self.__orderedList.index( key )
      del( self.__orderedList[ pos ] )
      return True
    return False

  @gCFGSynchro
  def copyKey( self, originalKey, newKey ):
    """
    Copy an option/section

    @type originalKey: string
    @param originalKey: Name of the option/section to copy
    @type newKey: string
    @param newKey: Destination name
    @return: Boolean with the result
    """
    if originalKey == newKey:
      return False
    if newKey in self.__orderedList:
      return False
    if originalKey in self.__orderedList:
      self.__dataDict[ newKey ] = copy.copy( self.__dataDict[ originalKey ] )
      self.__commentDict[ newKey ] = copy.copy( self.__commentDict[ originalKey ] )
      self.__orderedList.append( newKey )
      return True
    return False

  @gCFGSynchro
  def listOptions( self, ordered = True ):
    """
    List options

    @type ordered: boolean
    @param ordered: Return the options ordered. By default is False
    @return: List with the option names
    """
    if ordered:
      return [ sKey for sKey in self.__orderedList if type( self.__dataDict[ sKey ] ) == types.StringType ]
    else:
      return [ sKey for sKey in self.__dataDict.keys() if type( self.__dataDict[ sKey ] ) == types.StringType ]

  @gCFGSynchro
  def listSections( self, ordered = True ):
    """
    List subsections

    @type ordered: boolean
    @param ordered: Return the subsections ordered. By default is False
    @return: List with the subsection names
    """
    if ordered:
      return [ sKey for sKey in self.__orderedList if type( self.__dataDict[ sKey ] ) != types.StringType ]
    else:
      return [ sKey for sKey in self.__dataDict.keys() if type( self.__dataDict[ sKey ] ) != types.StringType ]

  @gCFGSynchro
  def isSection( self, key ):
    """
    Return if a section exists

    @type key: string
    @param key: Name to check
    @return: Boolean with the results
    """
    if key.find( "/" ) != -1:
      keyDict = self.getRecursive( key, -1 )
      if not keyDict:
        return False
      section = keyDict[ 'value' ]
      if type( section ) in ( types.StringType, types.UnicodeType ):
        return False
      secKey = keyDict[ 'levelsBelow' ]
      return section.isSection( secKey )
    return key in self.__dataDict and type( self.__dataDict[ key ] ) not in ( types.StringType, types.UnicodeType )

  @gCFGSynchro
  def isOption( self, key ):
    """
    Return if an option exists

    @type key: string
    @param key: Name to check
    @return: Boolean with the results
    """
    if key.find( "/" ) != -1:
      keyDict = self.getRecursive( key, -1 )
      if not keyDict:
        return False
      section = keyDict[ 'value' ]
      if type( section ) in ( types.StringType, types.UnicodeType ):
        return False
      secKey = keyDict[ 'levelsBelow' ]
      return section.isOption( secKey )
    return key in self.__dataDict and type( self.__dataDict[ key ] ) == types.StringType

  def listAll( self ):
    """
    List all sections and options

    @return: List with names of all options and subsections
    """
    return self.__orderedList

  def __recurse( self, pathList ):
    """
    Explore recursively a path

    @type pathList: list
    @param pathList: List containing the path to explore
    @return: Dictionary with the contents { key, value, comment }
    """
    if pathList[0] in self.__dataDict:
      if len( pathList ) == 1:
        return { 'key' : pathList[0],
                 'value' : self.__dataDict[ pathList[0] ],
                 'comment' : self.__commentDict[ pathList[0] ] }
      else:
        return self.__dataDict[ pathList[0] ].__recurse( pathList[1:] )
    else:
      return False

  @gCFGSynchro
  def getRecursive( self, path, levelsAbove = 0 ):
    """
    Get path contents

    @type path: string
    @param path: Path to explore recursively and get the contents
    @type levelsAbove: integer
    @param levelsAbove: Number of children levels in the path that won't be explored.
                        For instance, to explore all sections in a path except the last one use
                        levelsAbove = 1
    @return: Dictionary containing:
                key -> name of the entry
                value -> content of the key
                comment -> comment of the key
    """
    pathList = [ dirName.strip() for dirName in path.split( "/" ) if not dirName.strip() == "" ]
    levelsAbove = abs( levelsAbove )
    if len( pathList ) - levelsAbove < 0:
      return False
    if len( pathList ) - levelsAbove == 0:
      return { 'key' : "", 'value' : self, 'comment' : "", 'levelsBelow' : "" }
    levelsBelow = ""
    if levelsAbove > 0:
      levelsBelow = "/".join( pathList[-levelsAbove:] )
      pathList = pathList[:-levelsAbove]
    retDict = self.__recurse( pathList )
    if not retDict:
      return False
    retDict[ 'levelsBelow' ] = levelsBelow
    return retDict

  def getOption( self, opName, defaultValue = None ):
    """
    Get option value with default applied

    @type opName: string
    @param opName: Path to the option to retrieve
    @type defaultValue: optional (any python type)
    @param defaultValue: Default value for the option if the option is not defined.
                         If the option is defined, the value will be returned casted to
                         the type of defaultValue if it is defined.
    @return: Value of the option casted to defaultValue type, or defaultValue
    """
    levels = List.fromChar( opName, "/" )
    dataD = self.__dataDict
    while len( levels ) > 0:
      try:
        dataV = dataD[ levels.pop( 0 ) ]
      except KeyError:
        return defaultValue
      dataD = dataV

    if type( dataV ) != types.StringType:
      optionValue = defaultValue
    else:
      optionValue = dataV

    #Return value if existing, defaultValue if not
    if optionValue == defaultValue:
      if defaultValue == None or type( defaultValue ) == types.TypeType:
        return defaultValue
      return optionValue

    #Value has been returned from the configuration
    if defaultValue == None:
      return optionValue

    #Casting to defaultValue's type
    defaultType = defaultValue
    if not type( defaultValue ) == types.TypeType:
      defaultType = type( defaultValue )

    if defaultType == types.ListType:
      try:
        return List.fromChar( optionValue, ',' )
      except Exception:
        return defaultValue
    elif defaultType == types.BooleanType:
      try:
        return optionValue.lower() in ( "y", "yes", "true", "1" )
      except Exception:
        return defaultValue
    else:
      try:
        return defaultType( optionValue )
      except Exception:
        return defaultValue

  def getAsDict( self, path = "" ):
    """
    Get the contents below a give path as a dict

    @type path: string
    @param path: Path to retrieve as dict
    @return : Dictionary containing the data
    """
    resVal = {}
    if path:
      reqDict = self.getRecursive( path )
      if not reqDict:
        return resVal
      keyCfg = reqDict[ 'value' ]
      if type( keyCfg ) in ( types.StringType, types.UnicodeType ):
        return resVal
      return keyCfg.getAsDict()
    for op in self.listOptions():
      resVal[ op ] = self[ op ]
    for sec in self.listSections():
      resVal[ sec ] = self[ sec ].getAsDict()
    return resVal

  @gCFGSynchro
  def appendToOption( self, optionName, value ):
    """
    Append a value to an option prepending a comma

    @type optionName: string
    @param optionName: Name of the option to append the value
    @type value: string
    @param value: Value to append to the option
    """
    if optionName not in self.__dataDict:
      raise Exception( "Option %s has not been declared" % optionName )
    self.__dataDict[ optionName ] += str( value )

  @gCFGSynchro
  def addKey( self, key, value, comment, beforeKey = "" ):
    """
    Add a new entry (option or section)

    @type key: string
    @param key: Name of the option/section to add
    @type value: string/CFG
    @param value: Contents of the new option/section
    @type comment: string
    @param comment: Comment for the option/section
    @type beforeKey: string
    @param beforeKey: Name of the option/section to add the entry above. By default
                        the new entry will be added at the end.
    """
    if key in self.__dataDict:
      raise Exception( "%s already exists" % key )
    self.__dataDict[ key ] = value
    self.__commentDict[ key ] = comment
    if beforeKey == "":
      self.__orderedList.append( key )
    else:
      refKeyPos = self.__orderedList.index( beforeKey )
      self.__orderedList.insert( refKeyPos, key )

  @gCFGSynchro
  def renameKey( self, oldName, newName ):
    """
    Rename a option/section

    @type oldName: string
    @param oldName: Name of the option/section to change
    @type newName: string
    @param newName: New name of the option/section
    @return: Boolean with the result of the rename
    """
    if oldName == newName:
      return True
    if oldName in self.__dataDict:
      self.__dataDict[ newName ] = self.__dataDict[ oldName ]
      self.__commentDict[ newName ] = self.__commentDict[ oldName ]
      refKeyPos = self.__orderedList.index( oldName )
      self.__orderedList[ refKeyPos ] = newName
      del( self.__dataDict[ oldName ] )
      del( self.__commentDict[ oldName ] )
      return True
    else:
      return False

  def __getitem__( self, key ):
    """
    Get the contents of a section/option

    @type key: string
    @param key: Name of the section/option to retrieve
    @return: String/CFG with the contents
    """
    if key.find( "/" ) > -1:
      subDict = self.getRecursive( key )
      if not subDict:
        return False
      return subDict[ 'value' ]
    return self.__dataDict[ key ]

  def __iter__( self ):
    """
    Iterate though the contents in order
    """
    for key in self.__orderedList:
      yield key

  def __contains__( self, key ):
    """
    Check if a key is defined
    """
    return key in self.__orderedList

  def __str__( self ):
    """
    Get a print friendly representation of the CFG

    @return: String with the contents of the CFG
    """
    return self.serialize()

  def __repr__( self ):
    """
    Get a print friendly representation of the CFG

    @return: String with the contents of the CFG
    """
    return self.serialize()

  def __nonzero__( self ):
    """
    CFGs are not zeroes! ;)
    """
    return True

  def __eq__( self, cfg ):
    """
    Check CFGs
    """
    if not self.__orderedList == cfg.__orderedList:
      return False
    for key in self.__orderedList:
      if not self.__commentDict[ key ].strip() == cfg.__commentDict[ key ].strip():
        return False
      if not self.__dataDict[ key ] == cfg.__dataDict[ key ]:
        return False
    return True

  @gCFGSynchro
  def getComment( self, entryName ):
    """
    Get the comment for an option/section

    @type entryName: string
    @param entryName: Name of the option/section
    @return: String with the comment
    """
    try:
      return self.__commentDict[ entryName ]
    except:
      raise Exception( "%s does not have any comment defined" % entryName )

  @gCFGSynchro
  def setComment( self, entryName, comment ):
    """
    Set the comment for an option/section

    @type entryName: string
    @param entryName: Name of the option/section
    @type comment: string
    @param comment: Comment for the option/section
    """
    if entryName in self.__orderedList:
      self.__commentDict[ entryName ] = comment
      return True
    return False

  @gCFGSynchro
  def serialize( self, tabLevelString = "" ):
    """
    Generate a human readable serialization of a CFG

    @type tabLevelString: string
    @param tabLevelString: Tab string to apply to entries before representing them
    @return: String with the contents of the CFG
    """
    indentation = "  "
    cfgString = ""
    for entryName in self.__orderedList:
      if entryName in self.__commentDict:
        for commentLine in List.fromChar( self.__commentDict[ entryName ], "\n" ):
          cfgString += "%s#%s\n" % ( tabLevelString, commentLine )
      if entryName in self.listSections():
        cfgString += "%s%s\n%s{\n" % ( tabLevelString, entryName, tabLevelString )
        cfgString += self.__dataDict[ entryName ].serialize( "%s%s" % ( tabLevelString, indentation ) )
        cfgString += "%s}\n" % tabLevelString
      elif entryName in self.listOptions():
        valueList = List.fromChar( self.__dataDict[ entryName ] )
        if len( valueList ) == 0:
          cfgString += "%s%s = \n" % ( tabLevelString, entryName )
        else:
          cfgString += "%s%s = %s\n" % ( tabLevelString, entryName, valueList[0] )
          for value in valueList[1:]:
            cfgString += "%s%s += %s\n" % ( tabLevelString, entryName, value )
      else:
        raise Exception( "Oops. There is an entry in the order which is not a section nor an option" )
    return cfgString

  @gCFGSynchro
  def clone( self ):
    """
    Create a copy of the CFG

    @return: CFG copy
    """
    clonedCFG = CFG()
    clonedCFG.__orderedList = copy.deepcopy( self.__orderedList )
    clonedCFG.__commentDict = copy.deepcopy( self.__commentDict )
    for option in self.listOptions():
      clonedCFG.__dataDict[ option ] = self[ option ]
    for section in self.listSections():
      clonedCFG.__dataDict[ section ] = self[ section ].clone()
    return clonedCFG

  @gCFGSynchro
  def mergeWith( self, cfgToMergeWith ):
    """
    Generate a CFG by merging with the contents of another CFG.

    @type cfgToMergeWith: CFG
    @param cfgToMergeWith: CFG with the contents to merge with. This contents are more
                            preemtive than this CFG ones
    @return: CFG with the result of the merge
    """
    mergedCFG = CFG()
    for option in self.listOptions():
      mergedCFG.setOption( option,
                           self[ option ],
                           self.getComment( option ) )
    for option in cfgToMergeWith.listOptions():
      mergedCFG.setOption( option,
                           cfgToMergeWith[ option ],
                           cfgToMergeWith.getComment( option ) )
    for section in self.listSections():
      if section in cfgToMergeWith.listSections():
        oSectionCFG = self[ section ].mergeWith( cfgToMergeWith[ section ] )
        mergedCFG.createNewSection( section,
                                    cfgToMergeWith.getComment( section ),
                                    oSectionCFG )
      else:
        mergedCFG.createNewSection( section,
                                    self.getComment( section ),
                                    self[ section ].clone() )
    for section in cfgToMergeWith.listSections():
      if section not in self.listSections():
        mergedCFG.createNewSection( section,
                                    cfgToMergeWith.getComment( section ),
                                    cfgToMergeWith[ section ] )
    return mergedCFG

  def getModifications( self, newerCfg, ignoreMask = None, parentPath = "" ):
    """
    Compare two cfgs
    
    @type newerCfg: CFG
    @param newerCfg: Cfg to compare with
    @type prefix: string
    @param prefix: Internal use only
    @return: A list of modifications
    """
    modList = []
    #Options
    oldOptions = self.listOptions( True )
    newOptions = newerCfg.listOptions( True )
    for newOption in newOptions:
      iPos = newerCfg.__orderedList.index( newOption )
      newOptPath = "%s/%s" % ( parentPath, newOption )
      if ignoreMask and newOptPath in ignoreMask:
        continue
      if newOption not in oldOptions:
        modList.append( ( 'addOpt', newOption, iPos,
                          newerCfg[ newOption ],
                          newerCfg.getComment( newOption ) ) )
      else:
        modified = False
        if iPos != self.__orderedList.index( newOption ):
          modified = True
        elif newerCfg[ newOption ] != self[ newOption ]:
          modified = True
        elif newerCfg.getComment( newOption ) != self.getComment( newOption ):
          modified = True
        if modified:
          modList.append( ( 'modOpt', newOption, iPos,
                            newerCfg[ newOption ],
                            newerCfg.getComment( newOption ) ) )
    for oldOption in oldOptions:
      oldOptPath = "%s/%s" % ( parentPath, oldOption )
      if ignoreMask and oldOptPath in ignoreMask:
        continue
      if oldOption not in newOptions:
        modList.append( ( 'delOpt', oldOption, -1, '' ) )
    #Sections
    oldSections = self.listSections( True )
    newSections = newerCfg.listSections( True )
    for newSection in newSections:
      iPos = newerCfg.__orderedList.index( newSection )
      newSecPath = "%s/%s" % ( parentPath, newSection )
      if ignoreMask and newSecPath in ignoreMask:
        continue
      if newSection not in oldSections:
        modList.append( ( 'addSec', newSection, iPos,
                          str( newerCfg[ newSection ] ),
                          newerCfg.getComment( newSection ) ) )
      else:
        modified = False
        if iPos != self.__orderedList.index( newSection ):
          modified = True
        elif newerCfg.getComment( newSection ) != self.getComment( newSection ):
          modified = True
        subMod = self[ newSection ].getModifications( newerCfg[ newSection ],
                                                      ignoreMask, newSecPath )
        if subMod:
          modified = True
        if modified:
          modList.append( ( 'modSec', newSection, iPos,
                            subMod,
                            newerCfg.getComment( newSection ) ) )
    for oldSection in oldSections:
      oldSecPath = "%s/%s" % ( parentPath, oldSection )
      if ignoreMask and oldSecPath in ignoreMask:
        continue
      if oldSection not in newSections:
        modList.append( ( 'delSec', oldSection, -1, '' ) )
    return modList

  def applyModifications( self, modList, parentSection = "" ):
    """
    Apply modifications to a CFG
    
    @type modList: List
    @param modList: Modifications from a getModifications call
    @return: True/False
    """
    for modAction in modList:
      action = modAction[0]
      key = modAction[1]
      iPos = modAction[2]
      value = modAction[3]
      if action == 'addSec':
        if key in self.listSections():
          return S_ERROR( "Section %s/%s already exists" % ( parentSection, key ) )
        #key, value, comment, beforeKey = ""
        value = CFG().loadFromBuffer( value )
        comment = modAction[4].strip()
        if iPos < len( self.__orderedList ):
          beforeKey = self.__orderedList[ iPos ]
        else:
          beforeKey = ""
        self.addKey( key, value, comment, beforeKey )
      elif action == 'delSec':
        if key not in self.listSections():
          return S_ERROR( "Section %s/%s does not exist" % ( parentSection, key ) )
        self.deleteKey( key )
      elif action == 'modSec':
        if key not in self.listSections():
          return S_ERROR( "Section %s/%s does not exist" % ( parentSection, key ) )
        comment = modAction[4].strip()
        self.setComment( key, comment )
        if value:
          result = self[ key ].applyModifications( value, "%s/%s" % ( parentSection, key ) )
          if not result[ 'OK' ]:
            return result
        if iPos >= len( self.__orderedList ) or key != self.__orderedList[ iPos ]:
          prevPos = self.__orderedList.index( key )
          del( self.__orderedList[ prevPos ] )
          self.__orderedList.insert( iPos, key )
      elif action == "addOpt":
        if key in self.listOptions():
          return S_ERROR( "Option %s/%s exists already" % ( parentSection, key ) )
        #key, value, comment, beforeKey = ""
        comment = modAction[4].strip()
        if iPos < len( self.__orderedList ):
          beforeKey = self.__orderedList[ iPos ]
        else:
          beforeKey = ""
        self.addKey( key, value, comment, beforeKey )
      elif action == "modOpt":
        if key not in self.listOptions():
          return S_ERROR( "Option %s/%s does not exist" % ( parentSection, key ) )
        comment = modAction[4].strip()
        self.setOption( key , value, comment )
        if iPos >= len( self.__orderedList ) or key != self.__orderedList[ iPos ]:
          prevPos = self.__orderedList.index( key )
          del( self.__orderedList[ prevPos ] )
          self.__orderedList.insert( iPos, key )
      elif action == "delOpt":
        if key not in self.listOptions():
          return S_ERROR( "Option %s/%s does not exist" % ( parentSection, key ) )
        self.deleteKey( key )

    return S_OK()

  #Functions to load a CFG
  def loadFromFile( self, fileName ):
    """
    Load the contents of the CFG from a file

    @type fileName: string
    @param fileName: File name to load the contents from
    @return: This CFG
    """
    if gZipEnabled and fileName.find( ".zip" ) == len( fileName ) - 4:
      #Zipped file
      zipHandler = zipfile.ZipFile( fileName )
      nameList = zipHandler.namelist()
      fileToRead = nameList[0]
      fileData = zipHandler.read( fileToRead )
      zipHandler.close()
    else:
      fd = file( fileName )
      fileData = fd.read()
      fd.close()
    return self.loadFromBuffer( fileData )

  @gCFGSynchro
  def loadFromBuffer( self, data ):
    """
    Load the contents of the CFG from a string

    @type data: string
    @param data: Contents of the CFG
    @return: This CFG
    """
    self.reset()
    levelList = []
    currentLevel = self
    currentlyParsedString = ""
    currentComment = ""
    for line in data.split( "\n" ):
      line = line.strip()
      if len( line ) < 1:
        continue
      commentPos = line.find( "#" )
      if commentPos > -1:
        currentComment += "%s\n" % line[ commentPos: ].replace( "#", "" )
        line = line[ :commentPos ]
      for index in range( len( line ) ):
        if line[ index ] == "{":
          currentlyParsedString = currentlyParsedString.strip()
          currentLevel.createNewSection( currentlyParsedString, currentComment )
          levelList.append( currentLevel )
          currentLevel = currentLevel[ currentlyParsedString ]
          currentlyParsedString = ""
          currentComment = ""
        elif line[ index ] == "}":
          currentLevel = levelList.pop()
        elif line[ index ] == "=":
          lFields = line.split( "=" )
          currentLevel.setOption( lFields[0].strip(),
           "=".join( lFields[1:] ).strip(),
           currentComment )
          currentlyParsedString = ""
          currentComment = ""
          break
        elif line[ index: index + 2 ] == "+=":
          valueList = line.split( "+=" )
          currentLevel.appendToOption( valueList[0].strip(), ", %s" % "+=".join( valueList[1:] ).strip() )
          currentlyParsedString = ""
          currentComment = ""
          break
        else:
          currentlyParsedString += line[ index ]
    return self

  def writeToFile( self, fileName ):
    """
    Write the contents of the cfg to file

    @type fileName: string
    @param fileName: Name of the file to write the cfg to
    @return: True/False
    """
    try:
      directory = os.path.dirname( fileName )
      if directory and ( not os.path.exists( directory ) ):
        os.makedirs( directory )
      fd = file( fileName, "w" )
      fd.write( str( self ) )
      fd.close()
      return True
    except Exception:
      return False





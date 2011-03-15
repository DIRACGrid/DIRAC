#!/usr/bin/env python
# $HeadURL$
"""
Compile the externals
"""
__RCSID__ = "$Id$"

import sys, os, getopt, tarfile, urllib2, imp, signal, re, time, stat, types, copy

try:
  import zipfile
  zipEnabled = True
except:
  zipEnabled = False

executablePerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

try:
  from hashlib import md5
except:
  from md5 import md5

############
# Start of CFG
############


try:
  from DIRAC.Core.Utilities import S_OK, S_ERROR
  from DIRAC.Core.Utilities import List, ThreadSafe

  gCFGSynchro = ThreadSafe.Synchronizer( recursive = True )
except:
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
        return { 'key' : pathList[0], 'value' : self.__dataDict[ pathList[0] ], 'comment' : self.__commentDict[ pathList[0] ] }
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
    pathList = [ dir.strip() for dir in path.split( "/" ) if not dir.strip() == "" ]
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
        dataD = dataD[ levels.pop( 0 ) ]
      except KeyError:
        return defaultValue
    try:
      optionValue = dataD
      if type( optionValue ) != types.StringType:
        optionValue = defaultValue
    except KeyError:
      optionValue = defaultValue

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
      except Exception, v:
        return defaultValue
    elif defaultType == types.BooleanType:
      try:
        return optionValue.lower() in ( "y", "yes", "true", "1" )
      except Exception, v:
        return defaultValue
    else:
      try:
        return defaultType( optionValue )
      except:
        return defaultValue

  def getAsDict( self, path = "" ):
    """
    Get the contents below a give path as a dict

    @type secPath: string
    @param secPath: Path to retrieve as dict
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
    CFGSTring = ""
    for entryName in self.__orderedList:
      if entryName in self.__commentDict:
        for commentLine in List.fromChar( self.__commentDict[ entryName ], "\n" ):
          CFGSTring += "%s#%s\n" % ( tabLevelString, commentLine )
      if entryName in self.listSections():
        CFGSTring += "%s%s\n%s{\n" % ( tabLevelString, entryName, tabLevelString )
        CFGSTring += self.__dataDict[ entryName ].serialize( "%s%s" % ( tabLevelString, indentation ) )
        CFGSTring += "%s}\n" % tabLevelString
      elif entryName in self.listOptions():
        valueList = List.fromChar( self.__dataDict[ entryName ] )
        if len( valueList ) == 0:
          CFGSTring += "%s%s = \n" % ( tabLevelString, entryName )
        else:
          CFGSTring += "%s%s = %s\n" % ( tabLevelString, entryName, valueList[0] )
          for value in valueList[1:]:
            CFGSTring += "%s%s += %s\n" % ( tabLevelString, entryName, value )
      else:
        raise Exception( "Oops. There is an entry in the order which is not a section nor an option" )
    return CFGSTring

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

  def getModifications( self, newerCfg, ignoreMask = [], parentPath = "" ):
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
      if newOptPath in ignoreMask:
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
      if oldOptPath in ignoreMask:
        continue
      if oldOption not in newOptions:
        modList.append( ( 'delOpt', oldOption, -1, '' ) )
    #Sections
    oldSections = self.listSections( True )
    newSections = newerCfg.listSections( True )
    for newSection in newSections:
      iPos = newerCfg.__orderedList.index( newSection )
      newSecPath = "%s/%s" % ( parentPath, newSection )
      if newSecPath in ignoreMask:
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
      if oldSecPath in ignoreMask:
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
    if zipEnabled and fileName.find( ".zip" ) == len( fileName ) - 4:
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
    except:
      return False


#############################################################################################################
# End of CFG module                                                                                         #
#############################################################################################################


class Params:

  def __init__( self ):
    self.packagesToInstall = [ 'DIRAC' ]
    self.release = 'HEAD'
    self.externalsType = 'client'
    self.pythonVersion = '25'
    self.platform = False
    self.targetPath = os.getcwd()
    self.buildExternals = False
    self.buildIfNotAvailable = False
    self.debug = False
    self.lcgVer = ''
    self.useVersionsDir = False
    self.downBaseURL = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3'
    self.vo = ''

cliParams = Params()

#platformAlias = { 'Darwin_i386_10.6' : 'Darwin_i386_10.5' }
platformAlias = {}

####
# Start of helper functions
####

def logDEBUG( msg ):
  if cliParams.debug:
    for line in msg.split( "\n" ):
      print "%s UTC dirac-install [DEBUG] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
    sys.stdout.flush()

def logERROR( msg ):
  for line in msg.split( "\n" ):
    print "%s UTC dirac-install [ERROR] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
  sys.stdout.flush()

def logNOTICE( msg ):
  for line in msg.split( "\n" ):
    print "%s UTC dirac-install [NOTICE]  %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
  sys.stdout.flush()

def alarmTimeoutHandler( *args ):
  raise Exception( 'Timeout' )

def urlretrieveTimeout( url, fileName, timeout = 0 ):
  """
   Retrieve remote url to local file, with timeout wrapper
  """
  # NOTE: Not thread-safe, since all threads will catch same alarm.
  #       This is OK for dirac-install, since there are no threads.
  logDEBUG( 'Retrieving remote file "%s"' % url )

  if timeout:
    signal.signal( signal.SIGALRM, alarmTimeoutHandler )
    # set timeout alarm
    signal.alarm( timeout )
  try:
    remoteFD = urllib2.urlopen( url )
    expectedBytes = long( remoteFD.info()[ 'Content-Length' ] )
    localFD = open( fileName, "wb" )
    receivedBytes = 0L
    data = remoteFD.read( 16384 )
    while data:
      receivedBytes += len( data )
      localFD.write( data )
      data = remoteFD.read( 16384 )
    localFD.close()
    remoteFD.close()
    if receivedBytes != expectedBytes:
      logERROR( "File should be %s bytes but received %s" % ( expectedBytes, receivedBytes ) )
      return False
  except urllib2.HTTPError, x:
    if x.code == 404:
      logERROR( "%s does not exist" % url )
      return False
  except Exception, x:
    if x == 'TimeOut':
      logERROR( 'Timeout after %s seconds on transfer request for "%s"' % ( str( timeout ), url ) )
    if timeout:
      signal.alarm( 0 )
    raise x

  if timeout:
    signal.alarm( 0 )
  return True

def downloadFileFromSVN( filePath, destPath, isExecutable = False, filterLines = [] ):
  fileName = os.path.basename( filePath )
  logNOTICE( "Downloading %s" % fileName )
  viewSVNLocation = "http://svnweb.cern.ch/world/wsvn/dirac/%s?op=dl&rev=0" % filePath
  anonymousLocation = 'http://svnweb.cern.ch/guest/dirac/%s' % filePath
  downOK = False
  localPath = os.path.join( destPath, fileName )
  for remoteLocation in ( anonymousLocation, viewSVNLocation ):
    try:
      remoteFile = urllib2.urlopen( remoteLocation )
    except urllib2.URLError:
      continue
    remoteData = remoteFile.read()
    remoteFile.close()
    if remoteData:
      localFile = open( localPath , "wb" )
      localFile.write( remoteData )
      localFile.close()
      downOK = True
      break
  if not downOK:
    osCmd = "svn cat 'http://svnweb.cern.ch/guest/dirac/DIRAC/trunk/%s' > %s" % ( filePath, localPath )
    if os.system( osCmd ):
      logERROR( "Could not retrieve %s from the web nor via SVN. Aborting..." % fileName )
      sys.exit( 1 )
  if filterLines:
    fd = open( localPath, "rb" )
    fileContents = fd.readlines()
    fd.close()
    fd = open( localPath, "wb" )
    for line in fileContents:
      isFiltered = False
      for filter in filterLines:
        if line.find( filter ) > -1:
          isFiltered = True
          break
      if not isFiltered:
        fd.write( line )
    fd.close()
  if isExecutable:
    os.chmod( localPath , executablePerms )

def downloadAndExtractTarball( pkgVer, targetPath, subDir = False, checkHash = True ):
  if not subDir:
    subDir = "tars"
  tarName = "%s.tar.gz" % ( pkgVer )
  tarPath = os.path.join( cliParams.targetPath, tarName )
  try:
    if not urlretrieveTimeout( "%s/%s/%s" % ( cliParams.downBaseURL, subDir, tarName ), tarPath, 300 ):
      logERROR( "Cannot download %s" % tarName )
      return False
  except Exception, e:
    logERROR( "Cannot download %s: %s" % ( tarName, str( e ) ) )
    sys.exit( 1 )
  if checkHash:
    md5Name = "%s.md5" % ( pkgVer )
    md5Path = os.path.join( cliParams.targetPath, md5Name )
    try:
      if not urlretrieveTimeout( "%s/%s/%s" % ( cliParams.downBaseURL, subDir, md5Name ), md5Path, 300 ):
        logERROR( "Cannot download %s" % tarName )
        return False
    except Exception, e:
      logERROR( "Cannot download %s: %s" % ( md5Name, str( e ) ) )
      sys.exit( 1 )
    #Read md5  
    fd = open( os.path.join( cliParams.targetPath, md5Name ), "r" )
    md5Expected = fd.read().strip()
    fd.close()
    #Calculate md5
    md5Calculated = md5()
    fd = open( os.path.join( cliParams.targetPath, tarName ), "r" )
    buf = fd.read( 4096 )
    while buf:
      md5Calculated.update( buf )
      buf = fd.read( 4096 )
    fd.close()
    #Check
    if md5Expected != md5Calculated.hexdigest():
      logERROR( "Oops... md5 for package %s failed!" % pkgVer )
      sys.exit( 1 )
    #Delete md5 file
    os.unlink( md5Path )
  #Extract
  #cwd = os.getcwd()
  #os.chdir(cliParams.targetPath)
  #tf = tarfile.open( tarPath, "r" )
  #for member in tf.getmembers():
  #  tf.extract( member )
  #os.chdir(cwd)
  tarCmd = "tar xzf '%s' -C '%s'" % ( tarPath, cliParams.targetPath )
  os.system( tarCmd )
  #Delete tar
  os.unlink( tarPath )
  return True

def fixBuildPaths():
  """
  At compilation time many scripts get the building directory inserted, 
  this needs to be changed to point to the current installation path: 
  cliParams.targetPath
"""

  # Locate build path (from header of pydoc)
  pydocPath = os.path.join( cliParams.targetPath, cliParams.platform, 'bin', 'pydoc' )
  try:
    fd = open( pydocPath )
    line = fd.readline()
    fd.close()
    buildPath = line[2:line.find( cliParams.platform ) - 1]
    replaceCmd = "grep -rIl '%s' %s | xargs sed -i'.org' 's:%s:%s:g'" % ( buildPath, cliParams.targetPath, buildPath, cliParams.targetPath )
    os.system( replaceCmd )

  except:
    pass


def runExternalsPostInstall():
  """
   If there are any postInstall in externals, run them
  """
  postInstallPath = os.path.join( cliParams.targetPath, cliParams.platform, "postInstall" )
  if not os.path.isdir( postInstallPath ):
    logDEBUG( "There's no %s directory. Skipping postInstall step" % postInstallPath )
    return
  postInstallSuffix = "-postInstall"
  for scriptName in os.listdir( postInstallPath ):
    suffixFindPos = scriptName.find( postInstallSuffix )
    if suffixFindPos == -1 or not suffixFindPos == len( scriptName ) - len( postInstallSuffix ):
      logDEBUG( "%s does not have the %s suffix. Skipping.." % ( scriptName, postInstallSuffix ) )
      continue
    scriptPath = os.path.join( postInstallPath, scriptName )
    os.chmod( scriptPath , executablePerms )
    logNOTICE( "Executing %s..." % scriptPath )
    if os.system( "'%s' > '%s.out' 2> '%s.err'" % ( scriptPath, scriptPath, scriptPath ) ):
      logERROR( "Post installation script %s failed. Check %s.err" % ( scriptPath, scriptPath ) )
      sys.exit( 1 )

def checkPlatformAliasLink():
  """
  Make a link if there's an alias
  """
  if cliParams.platform in platformAlias:
    os.symlink( os.path.join( cliParams.targetPath, platformAlias[ cliParams.platform ] ),
                os.path.join( cliParams.targetPath, cliParams.platform ) )

####
# End of helper functions
####

cmdOpts = ( ( 'r:', 'release=', 'Release version to install' ),
            ( 'e:', 'extraPackages=', 'Extra packages to install (comma separated)' ),
            ( 't:', 'installType=', 'Installation type (client/server)' ),
            ( 'i:', 'pythonVersion=', 'Python version to compile (25/24)' ),
            ( 'p:', 'platform=', 'Platform to install' ),
            ( 'P:', 'installationPath=', 'Path where to install (default current working dir)' ),
            ( 'b', 'build', 'Force local compilation' ),
            ( 'g:', 'grid=', 'lcg tools package version' ),
            ( 'B', 'buildIfNotAvailable', 'Build if not available' ),
            ( 'v', 'useVersionsDir', 'Use versions directory' ),
            ( 'd', 'debug', 'Show debug messages' ),
            ( 'h', 'help', 'Show this help' ),
            ( 'u:', 'baseURL=', 'Change base URL for Tar Download repository' ),
            ( 'V:', 'virtualOrganization=', 'Install for this Virtual Organization (using remote defaults)' )
          )

optList, args = getopt.getopt( sys.argv[1:],
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )

def usage():
  print "Usage %s <opts> <cfgFile>" % sys.argv[0]
  for cmdOpt in cmdOpts:
    print " %s %s : %s" % ( cmdOpt[0].ljust( 3 ), cmdOpt[1].ljust( 20 ), cmdOpt[2] )
  print
  print "Known options and default values from /LocalInstallation section of local or remote cfgFile "
  for options in [ ( 'Release', cliParams.release ),
                   ( 'Extensions', [] ),
                   ( 'InstallType', cliParams.externalsType ),
                   ( 'PythonVersion', cliParams.pythonVersion ),
                   ( 'Platform', cliParams.platform ),
                   ( 'TargetPath', cliParams.targetPath ),
                   ( 'LcgVer', cliParams.lcgVer ),
                   ( 'BaseURL', cliParams.downBaseURL ),
                   ( 'UseVersionsDir', cliParams.useVersionsDir ) ]:
    print " %s = %s" % options

  sys.exit( 1 )

# First check if -V option is set to attempt retrieval of defaults.cfg

for o, v in optList:
  if o in ( '-h', '--help' ):
    usage()
  elif o in ( '-V', '--virtualOrganization' ):
    cliParams.vo = v

#Load CFG  
#downloadFileFromSVN( "DIRAC/trunk/DIRAC/Core/Utilities/CFG.py", cliParams.targetPath, False, [ '@gCFGSynchro' ] )
#cfgPath = os.path.join( cliParams.targetPath , "CFG.py" )
#cfgFD = open( cfgPath, "r" )
#CFG = imp.load_module( "CFG", cfgFD, cfgPath, ( "", "r", imp.PY_SOURCE ) )
#cfgFD.close()

optCfg = CFG()

defCfgFile = "defaults.cfg"
defaultsURL = "%s/%s" % ( cliParams.downBaseURL, defCfgFile )
logNOTICE( "Getting defaults from %s" % defaultsURL )
try:
  urlretrieveTimeout( defaultsURL, defCfgFile, 30 )
  # when all defaults are move to use LocalInstallation Section the next 2 lines can be removed
  defCfg = CFG().loadFromFile( defCfgFile )
  optCfg = defCfg
  if defCfg.isSection( 'LocalInstallation' ):
    optCfg = optCfg.mergeWith( defCfg['LocalInstallation'] )
except Exception, e:
  logNOTICE( "Cannot download default release version: %s" % ( str( e ) ) )

if cliParams.vo:
  voCfgFile = '%s_defaults.cfg' % cliParams.vo
  voURL = "%s/%s" % ( cliParams.downBaseURL, voCfgFile )
  logNOTICE( "Getting defaults from %s" % voURL )
  try:
    urlretrieveTimeout( voURL, voCfgFile, 30 )
    voCfg = CFG().loadFromFile( voCfgFile )
    # when all defaults are move to use LocalInstallation Section the next 5 lines can be removed
    if not optCfg:
      optCfg = voCfg
    else:
      optCfg = optCfg.mergeWith( voCfg )
    if voCfg.isSection( 'LocalInstallation' ):
      optCfg = optCfg.mergeWith( voCfg['LocalInstallation'] )
  except Exception, e:
    logNOTICE( "Cannot download VO default release version: %s" % ( str( e ) ) )

for arg in args:
  if not arg[-4:] == ".cfg":
    continue
  cfg = CFG().loadFromFile( arg )
  if not cfg.isSection( 'LocalInstallation' ):
    continue
  if not optCfg:
    optCfg = cfg['LocalInstallation']
    continue
  optCfg = optCfg.mergeWith( cfg['LocalInstallation'] )

cliParams.release = optCfg.getOption( 'Release', cliParams.release )
cliParams.packagesToInstall.extend( optCfg.getOption( 'Extensions', [] ) )
cliParams.externalsType = optCfg.getOption( 'InstallType', cliParams.externalsType )
cliParams.pythonVersion = optCfg.getOption( 'PythonVersion', cliParams.pythonVersion )
cliParams.platform = optCfg.getOption( 'Platform', cliParams.platform )
cliParams.targetPath = optCfg.getOption( 'TargetPath', cliParams.targetPath )
cliParams.buildExternals = optCfg.getOption( 'BuildExternals', cliParams.buildExternals )
cliParams.lcgVer = optCfg.getOption( 'LcgVer', cliParams.lcgVer )
cliParams.downBaseURL = optCfg.getOption( 'BaseURL', cliParams.downBaseURL )
cliParams.useVersionsDir = optCfg.getOption( 'UseVersionsDir', cliParams.useVersionsDir )


for o, v in optList:
  if o in ( '-r', '--release' ):
    cliParams.release = v
  elif o in ( '-e', '--extraPackages' ):
    for pkg in [ p.strip() for p in v.split( "," ) if p.strip() ]:
      cliParams.packagesToInstall.append( pkg )
  elif o in ( '-t', '--installType' ):
    cliParams.externalsType = v
  elif o in ( '-y', '--pythonVersion' ):
    cliParams.pythonVersion = v
  elif o in ( '-p', '--platform' ):
    cliParams.platform = v
  elif o in ( '-d', '--debug' ):
    cliParams.debug = True
  elif o in ( '-g', '--grid' ):
    cliParams.lcgVer = v
  elif o in ( '-u', '--baseURL' ):
    cliParams.downBaseURL = v
  elif o in ( '-P', '--installationPath' ):
    cliParams.targetPath = v
    try:
      os.makedirs( v )
    except:
      pass
  elif o in ( '-v', '--useVersionsDir' ):
    cliParams.useVersionsDir = True
  elif o in ( '-b', '--build' ):
    cliParams.buildExternals = True


# Make sure Extensions are not duplicated and have the full name
pkgList = cliParams.packagesToInstall
cliParams.packagesToInstall = []
for pkg in pkgList:
  pl = pkg.split( '@' )
  if pl[0] != 'Web':
    iPos = pl[0].find( "DIRAC" )
    if iPos == -1 or iPos != len( pl[0] ) - 5:
      pl[0] = "%sDIRAC" % pl[0]
  pkg = "@".join( pl )
  if pkg not in cliParams.packagesToInstall:
    cliParams.packagesToInstall.append( pkg )

if cliParams.useVersionsDir:
  # install under <installPath>/versions/<version>_<timestamp>
  cliParams.basePath = cliParams.targetPath
  cliParams.targetPath = os.path.join( cliParams.targetPath, 'versions', '%s_%s' % ( cliParams.release, int( time.time() ) ) )
  try:
    os.makedirs( cliParams.targetPath )
  except:
    pass

#Get the list of tarfiles
tarsURL = "%s/tars/tars.list" % cliParams.downBaseURL
logDEBUG( "Getting the tar list from %s" % tarsURL )
tarListPath = os.path.join( cliParams.targetPath, "tars.list" )
try:
  urlretrieveTimeout( tarsURL, tarListPath, 300 )
except Exception, e:
  logERROR( "Cannot download list of tars: %s" % ( str( e ) ) )
  sys.exit( 1 )
fd = open( tarListPath, "r" )
availableTars = [ line.strip() for line in fd.readlines() if line.strip() ]
fd.close()
os.unlink( tarListPath )

#Load releases
cfgURL = "%s/%s/%s" % ( cliParams.downBaseURL, "tars", "releases-%s.cfg" % cliParams.release )
cfgLocation = os.path.join( cliParams.targetPath, "releases.cfg" )
if not urlretrieveTimeout( cfgURL, cfgLocation, 300 ):
  logERROR( "Release %s doesn't seem to have been distributed" % cliParams.release )
  sys.exit( 1 )
mainCFG = CFG().loadFromFile( cfgLocation )

if 'Releases' not in mainCFG.listSections():
  logERROR( " There's no Releases section in releases.cfg" )
  sys.exit( 1 )

if cliParams.release not in mainCFG[ 'Releases' ].listSections():
  logERROR( " There's no release %s" % cliParams.release )
  sys.exit( 1 )

#Tar fest!

moduleDIRACRe = re.compile( "^.*DIRAC$" )

releaseCFG = mainCFG[ 'Releases' ][ cliParams.release ]
for package in cliParams.packagesToInstall:
  pl = package.split( '@' )
  packageVersion = False
  #Explicit version can be defined for packages using pkgName@version
  if len( pl ) > 1 :
    package = pl[0]
    packageVersion = "@".join( pl[1:] )
  else:
    #Try to get the defined package version
    if package not in releaseCFG.listOptions():
      logERROR( " Package %s is not defined for the release" % package )
      sys.exit( 1 )
    packageVersion = releaseCFG.getOption( package, "trunk" )
  packageTar = "%s-%s.tar.gz" % ( package, packageVersion )
  if packageTar not in availableTars:
    logERROR( "%s is not registered" % packageTar )
    sys.exit( 1 )
  logNOTICE( "Installing package %s version %s" % ( package, packageVersion ) )
  if not downloadAndExtractTarball( "%s-%s" % ( package, packageVersion ), cliParams.targetPath ):
    sys.exit( 1 )
  if moduleDIRACRe.match( package ):
    initFilePath = os.path.join( cliParams.targetPath, package, "__init__.py" )
    if not os.path.isfile( initFilePath ):
      fd = open( initFilePath, "w" )
      fd.write( "#Generated by dirac-install\n" )
      fd.close()
  postInstallScript = os.path.join( cliParams.targetPath, package, 'dirac-postInstall.py' )
  if os.path.isfile( postInstallScript ):
    os.chmod( postInstallScript , executablePerms )
    logNOTICE( "Executing %s..." % postInstallScript )
    if os.system( "python '%s' > '%s.out' 2> '%s.err'" % ( postInstallScript,
                                                           postInstallScript,
                                                           postInstallScript ) ):
      logERROR( "Post installation script %s failed. Check %s.err" % ( postInstallScript,
                                                                       postInstallScript ) )
      sys.exit( 1 )

#Deploy scripts :)
os.system( os.path.join( cliParams.targetPath, "DIRAC", "Core", "scripts", "dirac-deploy-scripts.py" ) )

#Do we have a platform defined?
if not cliParams.platform:
  platformPath = os.path.join( cliParams.targetPath, "DIRAC", "Core", "Utilities", "Platform.py" )
  platFD = open( platformPath, "r" )
  Platform = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
  platFD.close()
  cliParams.platform = Platform.getPlatformString()

logNOTICE( "Using platform: %s" % cliParams.platform )

#Externals stuff
extVersion = releaseCFG.getOption( 'Externals', "trunk" )
if cliParams.platform in platformAlias:
  effectivePlatform = platformAlias[ cliParams.platform ]
else:
  effectivePlatform = cliParams.platform
extDesc = "-".join( [ cliParams.externalsType, extVersion,
                          effectivePlatform, 'python%s' % cliParams.pythonVersion ] )

logDEBUG( "Externals version is %s" % extDesc )
extTar = "Externals-%s" % extDesc
extAvailable = "%s.tar.gz" % ( extTar ) in availableTars

buildCmd = os.path.join( cliParams.targetPath, "DIRAC", "Core", "scripts", "dirac-compile-externals.py" )
buildCmd = "%s -t '%s' -D '%s' -v '%s' -i '%s'" % ( buildCmd, cliParams.externalsType,
                                                    os.path.join( cliParams.targetPath, cliParams.platform ),
                                                    extVersion,
                                                    cliParams.pythonVersion )
if cliParams.buildExternals:
  if os.system( buildCmd ):
    logERROR( "Could not compile binaries" )
    sys.exit( 1 )
else:
  if extAvailable:
    if not downloadAndExtractTarball( extTar, cliParams.targetPath ):
      sys.exit( 1 )
    fixBuildPaths()
    runExternalsPostInstall()
    checkPlatformAliasLink()
  else:
    if cliParams.buildIfNotAvailable:
      if os.system( buildCmd ):
        logERROR( "Could not compile binaries" )
        sys.exit( 1 )
    else:
      logERROR( "%s.tar.gz is not registered" % extTar )
      sys.exit( 1 )

#LCG utils if required
if cliParams.lcgVer:
  tarBallName = "DIRAC-lcg-%s-%s-python%s" % ( cliParams.lcgVer, cliParams.platform, cliParams.pythonVersion )
  if not downloadAndExtractTarball( tarBallName, cliParams.targetPath, "lcgBundles", False ):
    logERROR( "Check that there is a release for your platform: %s" % tarBallName )

for file in ( "releases.cfg", "CFG.py", "CFG.pyc", "CFG.pyo" ):
  dirs = [ cliParams.targetPath, os.getcwd() ]
  if cliParams.useVersionsDir:
    dirs.append( cliParams.basePath )
  for dir in dirs:
    filePath = os.path.join( dir, file )
    if os.path.isfile( filePath ):
      os.unlink( filePath )


proPath = cliParams.targetPath
if cliParams.useVersionsDir:
  oldPath = os.path.join( cliParams.basePath, 'old' )
  proPath = os.path.join( cliParams.basePath, 'pro' )
  try:
    if os.path.exists( proPath ):
      if os.path.exists( oldPath ):
        os.unlink( oldPath )
      os.rename( proPath, oldPath )
    os.symlink( cliParams.targetPath, proPath )
    for dir in ['startup', 'runit', 'data', 'work', 'control', 'sbin', 'etc']:
      fake = os.path.join( cliParams.targetPath, dir )
      real = os.path.join( cliParams.basePath, dir )
      if not os.path.exists( real ):
        os.makedirs( real )
      os.symlink( real, fake )
  except Exception, x:
    logERROR( str( x ) )
    sys.exit( 1 )

# Now create bashrc at basePath
try:
  bashrcFile = os.path.join( cliParams.targetPath, 'bashrc' )
  if cliParams.useVersionsDir:
    bashrcFile = os.path.join( cliParams.basePath, 'bashrc' )
  logNOTICE( 'Creating %s' % bashrcFile )
  if not os.path.exists( bashrcFile ):
    lines = [ '# DIRAC bashrc file, used by service and agent run scripts to set environment',
              'export PYTHONUNBUFFERED=yes',
              'export PYTHONOPTIMIZE=x' ]
    if 'HOME' in os.environ:
      lines.append( '[ -z "$HOME" ] && export HOME=%s' % os.environ['HOME'] )
    if 'X509_CERT_DIR' in os.environ:
      lines.append( 'export X509_CERT_DIR=%s' % os.environ['X509_CERT_DIR'] )
    lines.append( 'export X509_VOMS_DIR=%s' % os.path.join( os.path.join( cliParams.targetPath, 'etc', 'grid-security', 'vomsdir' ) ) )
    lines.extend( ['# Some DIRAC locations',
                   'export DIRAC=%s' % proPath,
                   'export DIRACBIN=%s' % os.path.join( proPath, cliParams.platform, 'bin' ),
                   'export DIRACSCRIPTS=%s' % os.path.join( proPath, 'scripts' ),
                   'export DIRACLIB=%s' % os.path.join( proPath, cliParams.platform, 'lib' ),
                   'export TERMINFO=%s' % os.path.join( proPath, cliParams.platform, 'share', 'terminfo' ),
                   'export RRD_DEFAULT_FONT=%s' % os.path.join( proPath, cliParams.platform, 'share', 'rrdtool', 'fonts', 'DejaVuSansMono-Roman.ttf' ) ] )

    lines.extend( ['# Clear the PYTHONPATH and the LD_LIBRARY_PATH',
                  'PYTHONPATH=""',
                  'LD_LIBRARY_PATH=""'] )

    lines.extend( ['( echo $PATH | grep -q $DIRACBIN ) || export PATH=$DIRACBIN:$PATH',
                   '( echo $PATH | grep -q $DIRACSCRIPTS ) || export PATH=$DIRACSCRIPTS:$PATH',
                   'export LD_LIBRARY_PATH=$DIRACLIB:$DIRACLIB/mysql',
                   'export PYTHONPATH=$DIRAC'] )
    lines.append( '' )
    f = open( bashrcFile, 'w' )
    f.write( '\n'.join( lines ) )
    f.close()
except Exception, x:
 logERROR( str( x ) )
 sys.exit( 1 )

logNOTICE( "DIRAC release %s successfully installed" % cliParams.release )
sys.exit( 0 )

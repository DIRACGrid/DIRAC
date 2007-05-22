# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/Attic/CFG.py,v 1.5 2007/05/22 18:49:38 acasajus Exp $
__RCSID__ = "$Id: CFG.py,v 1.5 2007/05/22 18:49:38 acasajus Exp $"

import types
import copy

from DIRAC.Core.Utilities import List

class CFG:

  def __init__( self ):
    self.reset()

  def reset( self ):
    self.__orderedList = []
    self.__commentDict = {}
    self.__dataDict = {}

  def createNewSection( self, sectionName, comment = "", oCFG = False ):
    if sectionName == "":
      raise Exception( "Creating a section with empty name! You shouldn't do that!" )
    if sectionName.find( "/" ) > -1:
      raise Exception( "Sections and options can't contain '/' character. Correct %s" % sectionName )
    self.__addEntry( sectionName, comment )
    if sectionName not in self.__dataDict:
      if not oCFG:
        self.__dataDict[ sectionName ] = CFG()
      else:
        self.__dataDict[ sectionName ] = oCFG
    else:
      raise Exception( "%s key is already a section"  % sectionName )

  def __overrideAndCloneSection( self, sectionName, oCFGToClone ):
    if sectionName not in self.listSections():
      raise Exception( "Section %s does not exist" % sectionName )
    self.__dataDict[ sectionName ] = oCFGToClone.clone()

  def setOption( self, optionName, value, comment = "" ):
    if optionName == "":
      raise Exception( "Creating an option with empty name! You shouldn't do that!" )
    if optionName.find( "/" ) > -1:
      raise Exception( "Sections and options can't contain '/' character. Correct %s" % optionName )
    if optionName in self.listSections():
      raise Exception( "%s key is already defined as section" % optionName )
    self.__addEntry( optionName, comment )
    self.__dataDict[ optionName ] = str( value )


  def __addEntry( self, entryName, comment ):
    if not entryName in self.__orderedList:
      self.__orderedList.append( entryName )
    self.__commentDict[ entryName ] = comment

  def deleteEntry( self, entryName ):
    if entryName in self.__orderedList:
      del( self.__commentDict[ entryName ] )
      del( self.__dataDict[ entryName ] )
      pos = self.__orderedList.index( entryName )
      del( self.__orderedList[ pos ] )
      return True
    return False

  def listOptions( self ):
    return [ sKey for sKey in self.__dataDict.keys() if type( self.__dataDict[ sKey ] ) == types.StringType ]

  def listSections( self ):
    return [ sKey for sKey in self.__dataDict.keys() if type( self.__dataDict[ sKey ] ) != types.StringType ]

  def appendToOption( self, optionName, value ):
    if optionName not in self.__dataDict:
      raise Exception( "Option %s has not been declared" % optionName )
    self.__dataDict[ optionName ] += str( value )

  def __getitem__( self, key ):
    return self.__getattr__( key )

  def __getattr__( self, key ):
    return self.__dataDict[ key ]

  def __str__( self ):
    return self.serialize()

  def __nonzero__( self ):
    return True

  def getComment( self, entryName ):
    try:
      return self.__commentDict[ entryName ]
    except:
      raise Exception( "%s does not have any comment defined" % entryName )

  def setComment( self, entryName, comment ):
    if entryName in self.__orderedList:
      self.__commentDict[ entryName ] = comment
      return True
    return False

  def serialize( self, tabLevelString = "" ):
    CFGSTring = ""
    for entryName in self.__orderedList:
      if entryName in self.__commentDict:
        for commentLine in List.fromChar( self.__commentDict[ entryName ], "\n" ):
          CFGSTring += "%s#%s\n" % ( tabLevelString, commentLine )
      if entryName in self.listSections():
        CFGSTring += "%s%s\n%s{\n" % ( tabLevelString, entryName, tabLevelString )
        # FIXME: I have change the tab by spaces (we may want to put 4)
        CFGSTring += self.__dataDict[ entryName ].serialize( "%s  " % tabLevelString )
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

  def clone( self ):
    clonedCFG = CFG()
    clonedCFG.__orderedList = copy.deepcopy( self.__orderedList )
    clonedCFG.__commentDict = copy.deepcopy( self.__commentDict )
    for option in self.listOptions():
      clonedCFG.__dataDict[ option ] = self[ option ]
    for section in self.listSections():
      clonedCFG.__dataDict[ section ] = self[ section ].clone()
    return clonedCFG

  def mergeWith( self, cfgToMergeWith ):
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

  #Functions to load a CFG
  def loadFromFile( self, fileName ):
    fd = file( fileName )
    fileData = fd.read()
    fd.close()
    return self.loadFromBuffer( fileData )

  def loadFromBuffer( self, data ):
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
          currentLevel.appendToOption( valueList[0].strip(), ",%s" % "+=".join( valueList[1:] ).strip() )
          currentlyParsedString = ""
          currentComment = ""
          break
        else:
          currentlyParsedString += line[ index ]






# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/Attic/CFG.py,v 1.1 2007/03/09 15:20:22 rgracian Exp $
__RCSID__ = "$Id: CFG.py,v 1.1 2007/03/09 15:20:22 rgracian Exp $"

import sys
import types
import copy

class CFG:
  
  def __init__( self ):
    self.reset()
    
  def reset( self ):
    self.__lOrder = []
    self.__dComments = {}
    self.__dData = {}
    
  def createNewSection( self, sSectionName, sComment = "", oCFG = False ):
    if sSectionName == "":
      raise Exception( "Creating a section with empty name! You shouldn't do that!" )
    if sSectionName.find( "/" ) > -1:
      raise Exception( "Sections and options can't contain '/' character. Correct %s" % sSectionName )
    self.__addEntry( sSectionName, sComment )
    if sSectionName not in self.__dData:
      if not oCFG:
        self.__dData[ sSectionName ] = CFG()
      else:
        self.__dData[ sSectionName ] = oCFG
    else:
      raise Exception( "%s key is already a section" )
    
  def __overrideAndCloneSection( self, sSectionName, oCFGToClone ):
    if sSectionName not in self.listSections():
      raise Exception( "Section %s does not exist" % sSectionName )
    self.__dData[ sSectionName ] = oCFGToClone.clone()

  def setOption( self, sOptionName, sValue, sComment = "" ):
    if sOptionName == "":
      raise Exception( "Creating an option with empty name! You shouldn't do that!" )
    if sOptionName.find( "/" ) > -1:
      raise Exception( "Sections and options can't contain '/' character. Correct %s" % sOptionName )
    if sOptionName in self.listSections():
      raise Exception( "%s key is already defined as section" )
    self.__addEntry( sOptionName, sComment )
    self.__dData[ sOptionName ] = sValue
      
    
  def __addEntry( self, sEntry, sComment ):
    if not sEntry in self.__lOrder:
      self.__lOrder.append( sEntry )
    self.__dComments[ sEntry ] = sComment
    
  def listOptions( self ):
    return [ sKey for sKey in self.__dData.keys() if type( self.__dData[ sKey ] ) == types.StringType ]
  
  def listSections( self ):
    return [ sKey for sKey in self.__dData.keys() if type( self.__dData[ sKey ] ) != types.StringType ]

  def appendToOption( self, sOptionName, sValue ):
    if sOptionName not in self.__dData:
      raise Exception( "Option %s has not been declared" % sOptionName )
    self.__dData[ sOptionName ] += sValue
  
  def __getitem__( self, sKey ):
    return self.__getattr__( sKey )
  
  def __getattr__( self, sKey ):
    return self.__dData[ sKey ]
  
  def __str__( self ):
    return self.serialize()
  
  def __nonzero__( self ):
    return True
  
  def getComment( self, sEntry ):
    try:
      return self.__dComments[ sEntry ]
    except:
      raise Exception( "%s does not have any comment defined" % sEntry )
    
  def listFromChar( self, sInfo, sChar = "," ):
    return [ sField.strip() for sField in sInfo.split( sChar ) if len( sField.strip() ) > 0 ]
    
  def serialize( self, sTabLevel = "" ):
    sString = ""
    lSections = self.listSections()
    lOptions = self.listOptions()
    for sEntry in self.__lOrder:
      if sEntry in self.__dComments:
        for sCommentLine in self.listFromChar( self.__dComments[ sEntry ] ):
          sString += "%s#%s\n" % ( sTabLevel, sCommentLine )
      if sEntry in lSections:
        sString += "%s%s\n%s{\n" % ( sTabLevel, sEntry, sTabLevel )
        sString += self.__dData[ sEntry ].serialize( "%s\t" % sTabLevel )
        sString += "%s}\n" % sTabLevel
      elif sEntry in lOptions:
        lValue = self.listFromChar( self.__dData[ sEntry ] )
        sString += "%s%s = %s\n" % ( sTabLevel, sEntry, lValue[0] )
        for sField in lValue[1:]:
          sString += "%s%s += %s\n" % ( sTabLevel, sEntry, sField )          
      else:
        raise Exception( "Oops. There is an entry in the order which is not a section nor an option" )
    return sString
    
  def clone( self ):
    return copy.copy( self )
    
  def mergeWith( self, oCFGToMergeWith ):
    oMergedCFG = CFG()
    for sOption in self.listOptions():
      oMergedCFG.setOption( sOption, 
                               self[ sOption ],
                               self.getComment( sOption ) )
    for sOption in oCFGToMergeWith.listOptions():
      oMergedCFG.setOption( sOption, 
                               oCFGToMergeWith[ sOption ],
                               oCFGToMergeWith.getComment( sOption ) )      
    for sSection in self.listSections():
      if sSection in oCFGToMergeWith.listSections():
        oSectionCFG = self[ sSection ].mergeWith( oCFGToMergeWith[ sSection ] )
        oMergedCFG.createNewSection( sSection, 
                                     oCFGToMergeWith.getComment( sSection ),
                                     oSectionCFG )
      else:
        oMergedCFG.createNewSection( sSection, 
                                     self.getComment( sSection ),
                                     self[ sSection ].clone() )
    for sSection in oCFGToMergeWith.listSections():
      if sSection not in self.listSections():
        oMergedCFG.createNewSection( sSection, 
                                     oCFGToMergeWith.getComment( sSection ),
                                     oCFGToMergeWith[ sSection ] )
    return oMergedCFG

  #Functions to load a CFG    
  def loadFromFile( self, sFileName ):
    oFD = file( sFileName )
    sFileData = oFD.read()
    oFD.close()
    return self.loadFromBuffer( sFileData )
  
  def loadFromBuffer( self, sData ):
    self.reset()
    lLevels = []
    oCurrentLevel = self
    sCurrentlyParsedString = ""
    sCurrentComment = ""
    for sLine in sData.split( "\n" ):
      sLine = sLine.strip()
      if len( sLine ) < 1:
        continue
      if sLine[0] == "#":
        while sLine[0] == "#":
          sLine = sLine[1:]
        sCurrentComment += "%s\n" % sLine
        continue
      for iIndex in range( len( sLine ) ):
        if sLine[ iIndex ] == "{":
          sCurrentlyParsedString = sCurrentlyParsedString.strip()
          oCurrentLevel.createNewSection( sCurrentlyParsedString, sCurrentComment )
          lLevels.append( oCurrentLevel )
          oCurrentLevel = oCurrentLevel[ sCurrentlyParsedString ]
          sCurrentlyParsedString = ""
          sCurrentComment = ""
        elif sLine[ iIndex ] == "}":
          oCurrentLevel = lLevels.pop()
        elif sLine[ iIndex ] == "=":
          lFields = sLine.split( "=" )
          oCurrentLevel.setOption( lFields[0].strip(), 
           "=".join( lFields[1:] ).strip(),
           sCurrentComment )
          sCurrentlyParsedString = ""
          sCurrentComment = ""
          break
        elif sLine[ iIndex: iIndex + 2 ] == "+=":
          lFields = sLine.split( "+=" )
          oCurrentLevel.appendToOption( lFields[0].strip(), ",%s" % "+=".join( lFields[1:] ).strip() )
          sCurrentlyParsedString = ""
          sCurrentComment = ""
          break
        else:
          sCurrentlyParsedString += sLine[ iIndex ]

  
if __name__=="__main__":
  oCFG1 = CFG()
  oCFG1.loadFromFile( "testinfo1.cfg" )
  oCFG2 = CFG()
  oCFG2.loadFromBuffer( oCFG1.serialize() )
  oCFG3 = oCFG1.mergeWith( oCFG2 )
  print oCFG1
  print oCFG2
  print oCFG3
  oCFG4 = CFG()
  oCFG4.loadFromFile( "testinfo2.cfg" )
  oCFGM = oCFG1.mergeWith( oCFG4 )
  print oCFGM
      
     

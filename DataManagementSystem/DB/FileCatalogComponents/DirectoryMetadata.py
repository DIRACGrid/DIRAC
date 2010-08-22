########################################################################
# $HeadURL:  $
########################################################################

""" DIRAC FileCatalog mix-in class to manage directory metadata
"""

__RCSID__ = "$Id:  $"

import time, os, types
from DIRAC import S_OK, S_ERROR

class DirectoryMetadata:
  
##############################################################################
#
#  Manage Metadata fields
#
##############################################################################  
  def addMetadataField(self,pname,ptype,credDict):
    """ Add a new metadata parameter to the Metadata Database.
        pname - parameter name, ptype - parameter type in the MySQL notation
    """
    
    req = "CREATE TABLE FC_Meta_%s ( DirID INTEGER NOT NULL, Value %s, PRIMARY KEY (DirID), INDEX (Value) )" % (pname,ptype)
    result = self._query(req)
    if not result['OK']:
      return result
    
    result = self._insert('FC_Meta_Fields',['MetaName','MetaType'],[pname,ptype])
    if not result['OK']:
      return result
    
    return S_OK(result['lastRowId']) 
  
  def deleteMetadataField(self,pname,credDict):
    """ Remove metadata field
    """
    
    req = "DROP TABLE FC_Meta_%s" % pname
    result = self._update(req)
    return result
  
  def getMetadataFields(self,credDict):
    """ Get all the defined metadata fields
    """
    
    req = "SELECT MetaName,MetaType FROM FC_Meta_Fields"
    result = self._query(req)
    if not result['OK']:
      return result
    
    metaDict = {}
    for row in result['Value']:
      metaDict[row[0]] = row[1]
      
    return S_OK(metaDict)  

#############################################################################################  
#
# Set and get directory metadata
#
#############################################################################################  
  def setMetadata(self,dpath,metaName,metaValue,credDict):
    """ Set the value of a given metadata field for the the given directory path
    """
    result = self.getMetadataFields(credDict)
    if not result['OK']:
      return result
    metaFields = result['Value']
    
    if not metaName in metaFields:
      result = self.setMetaParameter(dpath,metaName,metaValue,credDict)
      result['Warning'] = "Added metadata is not searchable"
      return result
    
    result = self.dtree.findDir(dpath)
    if not result['OK']:
      return result
    dirID = result['Value']
    if not dirID:
      return S_ERROR('%s: directory not found' % dpath)
    
    # Check that the metadata is not defined for the parent directories
    result = self.getDirectoryMetadata(dpath,credDict,owndata=False)
    if not result['OK']:
      return result
    if metaName in result['Value']:
      return S_ERROR('Metadata conflict detected for %s for directory %s' % (metaName,dpath) )
    result = self._insert('FC_Meta_%s' % metaName,['DirID','Value'],[dirID,metaValue])
    if not result['OK']:
      if result['Message'].find('Duplicate') != -1:
        req = "UPDATE FC_Meta_%s SET Value='%s' WHERE DirID=%d" % (metaName,metaValue,dirID)
        result = self._update(req)
        if not result['OK']:
          return result       
      else:
        return result       
        
    return S_OK() 
  
  def setMetaParameter(self,dpath,metaName,metaValue,credDict):
    """ Set an meta parameter - metadata which is not used in the the data
        search operations
    """
    result = self.dtree.findDir(dpath)
    if not result['OK']:
      return result
    dirID = result['Value']
    if not dirID:
      return S_ERROR('%s: directory not found' % dpath)
    
    result = self._insert('FC_DirMeta',
                          ['DirID','MetaKey','MetaValue'],
                          [dirID,metaName,str(metaValue)])
    return result
  
  def getDirectoryMetaParameters(self,dpath,credDict,inherited=True,owndata=True):
    """ Get meta parameters for the given directory
    """
    if inherited:
      result = self.dtree.getPathIDs(dpath)
      if not result['OK']:
        return result
      pathIDs = result['Value']
      dirID = pathIDs[-1]
    else:
      result = self.dtree.findDir(dpath)
      if not result['OK']:
        return result
      dirID = result['Value']
      if not dirID:
        return S_ERROR('%s: directory not found' % dpath)  
      pathIDs = [dirID]
      
    if len(pathIDs) > 1:  
      pathString = ','.join( [ str(x) for x in pathIDs ] )
      req = "SELECT DirID,MetaKey,MetaValue from FC_DirMeta where DirID in (%s)" % pathString
    else:
      req = "SELECT DirID,MetaKey,MetaValue from FC_DirMeta where DirID=%d " % dirID
    result = self._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK({})
    metaDict = {}
    for dID,key,value in result['Value']:
      if metaDict.has_key(key):
        if type(metaDict[key]) == ListType:
          metaDict[key].append(value)
        else:
          metaDict[key] = [metaDict[key]].append(value) 
      else:
        metaDict[key] = value
        
    return S_OK(metaDict)             
  
  def getDirectoryMetadata(self,path,credDict,inherited=True,owndata=True):
    """ Get metadata for the given directory aggregating metadata for the directory itself
        and for all the parent directories if inherited flag is True
    """
    
    result = self.dtree.getPathIDs(path)
    if not result['OK']:
      return result
    pathIDs = result['Value']
    
    result = self.getMetadataFields(credDict)
    if not result['OK']:
      return result
    metaFields = result['Value']
    
    metaDict = {}
    if not inherited:
      pathIDs = pathIDs[-1:]
    if not owndata:
      pathIDs = pathIDs[:-1]  
    pathString = ','.join([ str(x) for x in pathIDs ])
    for meta in metaFields:
      req = "SELECT Value,DirID FROM FC_Meta_%s WHERE DirID in (%s)" % (meta,pathString)
      result = self._query(req)
      if not result['OK']:
        return result
      if len(result['Value']) > 1:
        return S_ERROR('Metadata conflict for directory %s' % path)
      if result['Value']:
        metaDict[meta] = result['Value'][0][0]
      
    # Get also non-searchable data  
    result = self.getDirectoryMetaParameters(path,credDict,inherited,owndata) 
    if result['OK']:
      metaDict.update(result['Value'])
       
    return S_OK(metaDict)  

############################################################################################
#
# Find directories corresponding to the metadata 
#
############################################################################################  
  def __findSubdirByMeta(self,meta,value,subdirFlag=True):
    """ Find directories for the given meta datum. If the the meta datum type is a list,
        combine values in OR. In case the meta datum is 'Any', finds all the subdirectories
        for which the meta datum is defined at all.
    """
  
    if type(value) == types.ListType:
      vString = ','.join( [ "'"+str(x)+"'" for x in value] )
      req = " SELECT DirID FROM FC_Meta_%s WHERE Value IN (%s) " % (meta,vString)
    else:  
      if value == "Any":
        req = " SELECT DirID FROM FC_Meta_%s " % meta
      else:  
        req = " SELECT DirID FROM FC_Meta_%s WHERE Value='%s' " % (meta,value)
    result = self._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK([])
    
    dirList = []
    for row in result['Value']:
      dirID = row[0]
      dirList.append(dirID)
      if subdirFlag:
        result = self.dtree.getSubdirectoriesByID(dirID)
        if not result['OK']:
          return result
        dirList += result['Value']
      
    return S_OK(dirList)  
  
  def __findSubdirMissingMeta(self,meta):
    """ Find directories not having the given meta datum defined
    """
    result = self.__findSubdirByMeta(meta,'Any')
    if not result['OK']:
      return result
    dirList = result['Value']
    table = self.dtree.getTreeTable()
    dirString = ','.join( [ str(x) for x in dirList ] )
    req = 'SELECT DirID FROM %s WHERE DirID NOT IN ( %s )' % (table,dirString)
    result = self._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK([])
    
    dirList = [ x[0] for x in result['Value'] ]
    return S_OK(dirList)        
  
  def findDirectoriesByMetadata(self,metaDict,credDict):
    """ Find Directories satisfying the given metadata
    """
    
    dirList = []
    first = True
    for meta,value in metaDict.items():
      if value == "Missing":
        result = self.__findSubdirMissingMeta(meta)
      else:  
        result = self.__findSubdirByMeta(meta,value)
      if not result['OK']:
        return result
      mList = result['Value']
      if first:
        dirList = mList
        first = False
      else:
        newList = []
        for d in dirList:
          if d in mList:
            newList.append(d)
        dirList = newList
        
    dirNameList = [ ]  
    for dir in dirList:
      result = self.dtree.getDirectoryPath(dir)  
      if not result['OK']:
        return result
      dirNameList.append(result['Value'])
    return S_OK(dirNameList)  
  
  def findFilesByMetadata(self,metaDict,credDict):
    """ Find Files satisfying the given metadata
    """
    
    result = self.findDirectoriesByMetadata(metaDict,credDict)
    if not result['OK']:
      return result
    
    dirList = result['Value']
    fileList = []
    result = self.listDirectory(dirList,credDict)
    if not result['OK']:
      return result
    
    for dir in result['Value']['Successful']:
      for fname in result['Value']['Successful'][dir]['Files']:
        fileList.append(dir+'/'+os.path.basename(fname))
        
    return S_OK(fileList)    
  
################################################################################################
#
# Find metadata compatible with other metadata in order to organize dynamically updated
# metadata selectors 
#
################################################################################################  
  def __findCompatibleDirectories(self,meta,value,fromDirs=None):
    """ Find directories compatible with the given meta datum.
        Optionally limit the list of compatible directories to only those in the
        fromDirs list 
    """
    
    # The directories compatible with the given meta datum are:
    # - directory for which the datum is defined
    # - all the subdirectories of the above directory
    # - all the directories in the parent hierarchy of the above directory
    
    # Find directories defining the meta datum and their subdirectories
    subdirs = []
    result = self.__findSubdirByMeta(meta,value)
    if not result['OK']:
      return result
    subdirs = result['Value']

    # Find parent directories of the directories defining the meta datum
    pdirs= []
    if subdirs:
      # The first element is the directory for which the meta datum is defined
      result = self.__findSubdirByMeta(meta,value,False)
      if not result['OK']:
        return result
      psubdirs = result['Value']
      for psub in psubdirs:
        result = self.dtree.getPathIDsByID(subdirs[0])
        if not result['OK']:
          return result
        pdirs += result['Value']
      
    # Constrain the output to only those that are present in the input list  
    resDirs = pdirs+subdirs  
    if fromDirs:
      resDirs = []
      for dir in pdirs+subdirs:
        if dir in fromDirs:
          resDirs.append(dir)  
      
    return S_OK(resDirs)  
  
  def __findDistinctMetadata(self,metaList,dList):
    """ Find distinct metadata values defined for the list of the input directories.
        Limit the search for only metadata in the input list
    """
    
    if dList:
      dString = ','.join([ str(x) for x in dList ])
    else:
      dString = None
    metaDict = {}
    for meta in metaList:
      req = "SELECT DISTINCT(Value) FROM FC_Meta_%s" % meta
      if dString:
        req += " WHERE DirID in (%s)" % dString
      result = self._query(req)
      if not result['OK']:
        return result
      metaDict[meta] = []
      for row in result['Value']:
        metaDict[meta].append(row[0])
      
    return S_OK(metaDict)
          
  def getCompatibleMetadata(self,metaDict,credDict):
    """ Get distinct metadata values compatible with the given already defined metadata
    """    
    
    # Get the list of metadata fields to inspect
    result = self.getMetadataFields(credDict)
    if not result['OK']:
      return result
    metaFields = result['Value']
    comFields = metaFields.keys()
    for m in metaDict:
      if m in comFields:
        del comFields[comFields.index(m)]
    
    fromList = []
    any = True
    if metaDict:
      any = False
      for meta,value in metaDict.items():
        result = self.__findCompatibleDirectories(meta,value,fromList)
        if not result['OK']:
          return result  
        cdirList = result['Value']
        if cdirList:
          fromList = cdirList
        else: 
          fromList = []
          break

    if any:  
      result = self.__findDistinctMetadata(comFields,[])
    elif fromList:
      result = self.__findDistinctMetadata(comFields,fromList)
    else:
      result = S_OK({})      
    return result  
  

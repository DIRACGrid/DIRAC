# $HeadURL$
"""
The Data Management Request contains all the necessary information for
a data management operation.

    :deprecated:
"""
import os, xml.dom.minidom, copy, datetime, time
from types import DictType, ListType, NoneType, StringTypes

from DIRAC.Core.Utilities.File import makeGuid
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Utilities import Time
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest

__RCSID__ = "$Id$"

class RequestContainer:
  """
  .. class:: RequestContainer

  Bag object holding all information about Request.
  """


  def __init__( self, request = None, init = True ):

    # This is a list of attributes - mandatory parameters
    self.attributeNames = ['Status', 'RequestName', 'RequestID', 'DIRACSetup', 'OwnerDN',
                           'OwnerGroup', 'SourceComponent', 'CreationTime', 'LastUpdate', 'JobID']

    # This dictionary contains all the request attributes
    self.attributes = {}

    # Subrequests are represented as a dictionary. The subrequests of similar types are stored together in a list.
    # The dictionary named Attributes must be present and must have the following mandatory names
    self.subAttributeNames = ['Status', 'SubRequestID', 'Operation', 'CreationTime', 'Catalogue', "TargetSE",
                              'LastUpdate', 'ExecutionOrder', 'Error']
    self.subRequests = {}

    if init:
      self.initialize( request )

  def initialize( self, request ):
    """ Set default values to attributes and parameters
    """
    if type( request ) == NoneType:
      # Set some defaults
      for name in self.attributeNames:
        self.attributes[name] = 'Unknown'
      self.attributes['CreationTime'] = str( Time.dateTime() )
      self.attributes['Status'] = "New"
      result = getProxyInfo()
      if result['OK']:
        proxyDict = result[ 'Value' ]
        self.attributes['OwnerDN'] = proxyDict[ 'identity' ]
        if 'group' in proxyDict:
          self.attributes['OwnerGroup'] = proxyDict[ 'group' ]
      self.attributes['DIRACSetup'] = gConfig.getValue( '/DIRAC/Setup', 'Unknown' )
    elif isinstance( request, RequestContainer ):
      for attr in self.attributeNames:
        self.attributes[attr] = request.attributes[attr]

    # initialize request from an XML string
    if type( request ) in StringTypes:
      for name in self.attributeNames:
        self.attributes[name] = 'Unknown'
      self.parseRequest( request )

    # Initialize request from another request
    elif isinstance( request, RequestContainer ):
      self.subRequests = copy.deepcopy( request.subrequests )

  #####################################################################
  #
  #  Attribute access methods
  #

  def __getattr__( self, name ):
    """ Generic method to access request attributes or parameters
    """

    if name.find( 'getSubRequest' ) == 0:
      item = name[13:]
      self.item_called = item
      if item in self.subAttributeNames:
        return self.__get_subattribute
      else:
        raise AttributeError, name
    if name.find( 'setSubRequest' ) == 0:
      item = name[13:]
      self.item_called = item
      if item in self.subAttributeNames:
        return self.__set_subattribute
      else:
        raise AttributeError, name
    if name.find( 'get' ) == 0:
      item = name[3:]
      self.item_called = item
      if item in self.attributeNames:
        return self.__get_attribute
      else:
        raise AttributeError, name
    elif name.find( 'set' ) == 0:
      item = name[3:]
      self.item_called = item
      if item in self.attributeNames:
        return self.__set_attribute
      else:
        raise AttributeError, name
    else:
      raise AttributeError, name

  def getRequestAttributes( self ):
    """ Get the dictionary of the request attributes
    """
    return S_OK( self.attributes )

  def setRequestAttributes( self, attributeDict ):
    """ Set the attributes associated to this request
    """
    self.attributes.update( attributeDict )
    return S_OK()

  def setCreationTime( self, rTime = 'now' ):
    """ Set the creation time to the current data and time
    """
    if rTime.lower() == "now":
      self.attributes['CreationTime'] = time.strftime( '%Y-%m-%d %H:%M:%S' )
    else:
      self.attributes['CreationTime'] = rTime
    return S_OK()

  def setLastUpdate( self, rTime = 'now' ):
    """ Set the last update to the current data and time
    """
    if rTime.lower() == "now":
      self.attributes['LastUpdate'] = time.strftime( '%Y-%m-%d %H:%M:%S' )
    else:
      self.attributes['LastUpdate'] = rTime
    return S_OK()

  def getAttribute( self, aname ):
    """ Get the attribute specified by its name aname
    """
    attributeValue = self.attributes[aname]
    return S_OK( attributeValue )

  def setAttribute( self, aname, value ):
    """ Set the attribute specified by its name aname
    """
    self.attributes[aname] = value
    return S_OK()

  def __get_attribute( self ):
    """ Generic method to get attributes
    """
    return S_OK( self.attributes[self.item_called] )

  def __set_attribute( self, value ):
    """ Generic method to set attribute value
    """
    self.attributes[self.item_called] = value
    return S_OK()

  def __get_subattribute( self, ind, rType ):
    """ Generic method to get attributes
    """
    return S_OK( self.subRequests[rType][ind]['Attributes'][self.item_called] )

  def __set_subattribute( self, ind, rType, value ):
    """ Generic method to set attribute value
    """
    self.subRequests[rType][ind]['Attributes'][self.item_called] = value
    return S_OK()

  #####################################################################
  #
  #  Sub request manipulation methods
  #
  def update( self, request ):
    """ Add subrequests from another request
    """
    requestTypes = request.getSubRequestTypes()['Value']
    for requestType in requestTypes:
      subRequests = request.getSubRequests( requestType )['Value']
      self.setSubRequests( requestType, subRequests )
    return S_OK()

  def initiateSubRequest( self, rType ):
    """ Add dictionary to list of requests and return the list index
    """
    defaultAttributes = {}
    for attr in self.subAttributeNames:
      defaultAttributes[attr] = ''
      if attr == 'ExecutionOrder':
        defaultAttributes[attr] = 0
      if attr == "Status":
        defaultAttributes[attr] = 'Waiting'
    defaultDict = {'Attributes':defaultAttributes, 'Files':[], 'Datasets':[]}
    if not self.subRequests.has_key( rType ):
      self.subRequests[rType] = []
    self.subRequests[rType].append( defaultDict )
    length = len( self.subRequests[rType] )
    return S_OK( length - 1 )

  def getSubRequestTypes( self ):
    """ Get the list of subrequest types
    """
    subRequestTypes = self.subRequests.keys()
    return S_OK( subRequestTypes )

  def getSubRequests( self, rType ):
    """ Get the the sub-requests of a particular type
    """
    if self.subRequests.has_key( rType ):
      return S_OK( self.subRequests[rType] )
    else:
      return S_OK( [] )

  def setSubRequests( self, rType, subRequests ):
    """ Set the sub-requests of a particular type associated to this request
    """
    if not self.subRequests.has_key( rType ):
      self.subRequests[rType] = []
    for subRequest in subRequests:
      self.addSubRequest( subRequest, rType )
    return S_OK()

  def _getLastOrder( self, fileIn = None ):
    """ just returns the last ExecutionOrder of ALL the sub requests, or for file (if present)
    """
    execOrdersSoFar = []

    for subReqTypes in self.subRequests:
      for operationDict in self.subRequests[subReqTypes]:
        if fileIn is None:  # gets the last in total
          execOrdersSoFar.append( operationDict['Attributes']['ExecutionOrder'] )
        else:
          for fileDict in operationDict['Files']:
            if fileIn == fileDict['LFN']:
              execOrdersSoFar.append( operationDict['Attributes']['ExecutionOrder'] )

    try:
      last = max( execOrdersSoFar )
    except ValueError:
      last = 0
    return last


  def addSubRequest( self, requestDict, rType ):
    """  Add a new sub-requests of specified type
        The 'Attributes':ExecutionOrder can be set to 'last' to always be the last to be executed.
    """
    # Initialise the sub-request
    index = self.initiateSubRequest( rType )['Value']
    # Stuff the sub-request with the attributes
    attributeDict = {'Status':'Waiting', 'SubRequestID':makeGuid(),
                     'CreationTime': str( datetime.datetime.utcnow() ),
                     'ExecutionOrder':0}
    for attr, value in requestDict['Attributes'].items():
      if attr == 'ExecutionOrder' and str( value ).lower() == 'last':
        value = self._getLastOrder() + 1
      attributeDict[attr] = value
    for attr in self.subAttributeNames:
      if not attr in attributeDict.keys():
        attributeDict[attr] = ''
        if attr == "ExecutionOrder":
          attributeDict[attr] = 0
        if attr == "Status":
          attributeDict[attr] = "Waiting"
    self.setSubRequestAttributes( index, rType, attributeDict )

    if requestDict.has_key( 'Files' ):
      files = []
      for rFile in requestDict['Files']:
        fileDict = {'Status':'Waiting', 'FileID':makeGuid(), 'Attempt':1}
        for attr, value in rFile.items():
          fileDict[attr] = value
        files.append( fileDict )
      self.setSubRequestFiles( index, rType, files )

    if requestDict.has_key( 'Datasets' ):
      datasets = []
      for dataset in requestDict['Datasets']:
        datasetDict = {'Status':'Waiting'}
        for attr, value in rFile.items():
          fileDict[attr] = value
        datasets.append( datasetDict )
      self.setSubRequestDatasets( index, rType, datasets )
    return S_OK( index )

  def getSubRequest( self, ind, rType ):
    """ Get the sub-request as specified by its index
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      return S_OK( self.subRequests[rType][ind] )

  def removeSubRequest( self, ind, rType ):
    """ Remove sub-request as specified by its index
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      return S_OK( self.subRequests[rType].pop( ind ) )

  def getNumSubRequests( self, rType ):
    """ Get the number of sub-requests for a given request type
    """
    if not self.subRequests.has_key( rType ):
      return S_OK( 0 )
    else:
      return S_OK( len( self.subRequests[rType] ) )

  def setSubRequestStatus( self, ind, rType, status ):
    """ Set the status of the sub request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      self.subRequests[rType][ind]['Attributes']['Status'] = status
    return S_OK()

  def getSubRequestAttributes( self, ind, rType ):
    """ Get the sub-request attributes
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      attributes = self.subRequests[rType][ind]['Attributes']
      return S_OK( attributes )

  def setSubRequestAttributes( self, ind, rType, attributeDict ):
    """ Set the sub-request attributes
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      self.subRequests[rType][ind]['Attributes'].update( attributeDict )
      return S_OK()

  def setSubRequestAttributeValue( self, ind, rType, attribute, value ):
    """ Set the attribute value associated to a sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      if not self.subRequests[rType][ind].has_key( 'Attributes' ):
        self.subRequests[rType][ind]['Attributes'] = {}
      self.subRequests[rType][ind]['Attributes'][attribute] = value
      return S_OK()

  def getSubRequestAttributeValue( self, ind, rType, attribute ):
    """ Get the attribute value associated to a sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      requestAttrValue = self.subRequests[rType][ind]['Attributes'][attribute]
      return S_OK( requestAttrValue )

  ###########################################################
  #
  # File manipulation methods
  #

  def getSubRequestNumFiles( self, ind, rType ):
    """ Get the number of files in the sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    elif not self.subRequests[rType][ind].has_key( 'Files' ):
      return S_OK( 0 )
    else:
      numFiles = len( self.subRequests[rType][ind]['Files'] )
      return S_OK( numFiles )

  def getSubRequestFiles( self, ind, rType ):
    """ Get the files associated to a sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    elif not self.subRequests[rType][ind].has_key( 'Files' ):
      return S_OK( [] )
    else:
      files = self.subRequests[rType][ind]['Files']
      return S_OK( files )

  def setSubRequestFiles( self, ind, rType, files ):
    """ Set the files associated to a sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      if not self.subRequests[rType][ind].has_key( 'Files' ):
        # Make deep copy
        self.subRequests[rType][ind]['Files'] = copy.deepcopy( files )
      else:
        for fDict in files:
          self.subRequests[rType][ind]['Files'].append( copy.deepcopy( fDict ) )
      return S_OK()

  def setSubRequestFileAttributeValue( self, ind, rType, lfn, attribute, value ):
    """ Set the file status
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      numFiles = self.getSubRequestNumFiles( ind, rType )['Value']
      for rFile in range ( numFiles ):
        if self.subRequests[rType][ind]['Files'][rFile]['LFN'] == lfn:
          self.subRequests[rType][ind]['Files'][rFile][attribute] = value
          return S_OK()
      return S_ERROR( "File not found" )

  def getSubRequestFileAttributeValue( self, ind, rType, lfn, attribute ):
    """ Get the file attribute value associated to a LFN and sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      numFiles = self.getSubRequestNumFiles( ind, rType )['Value']
      for rFile in range ( numFiles ):
        if self.subRequests[rType][ind]['Files'][rFile]['LFN'] == lfn:
          value = self.subRequests[rType][ind]['Files'][rFile][attribute]
          return S_OK( value )
      return S_ERROR( "File not found" )

  def getSubRequestFileAttributes( self, ind, rType, lfn ):
    """ Get the file attributes associated to a LFN and sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      numFiles = self.getSubRequestNumFiles( ind, rType )['Value']
      for rFile in range ( numFiles ):
        if self.subRequests[rType][ind]['Files'][rFile]['LFN'] == lfn:
          attributes = self.subRequests[rType][ind]['Files'][rFile]['LFN']
          return S_OK( attributes )
      return S_ERROR( "File not found" )

  ###########################################################
  #
  # Dataset manipulation methods
  #

  def getSubRequestNumDatasets( self, ind, rType ):
    """ Get the number of files in the sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    elif not self.subRequests[rType][ind].has_key( 'Datasets' ):
      return S_OK( 0 )
    else:
      numDatasets = len( self.subRequests[rType][ind]['Datasets'] )
      return S_OK( numDatasets )

  def getSubRequestDatasets( self, ind, rType ):
    """ Get the files associated to a sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    elif not self.subRequests[rType][ind].has_key( 'Datasets' ):
      return S_OK( [] )
    else:
      datasets = self.subRequests[rType][ind]['Datasets']
      return S_OK( datasets )

  def setSubRequestDatasets( self, ind, rType, datasets ):
    """ Set the datasets associated to a sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      if not self.subRequests[rType][ind].has_key( 'Datasets' ):
        self.subRequests[rType][ind]['Datasets'] = []
      self.subRequests[rType][ind]['Datasets'].extend( datasets )
      return S_OK()

  def setSubRequestDatasetAttributeValue( self, ind, rType, handle, attribute, value ):
    """ Set the attribute of the given dataset
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      numDatasets = self.getSubRequestNumDatasets( ind, rType )['Value']
      for dataset in range ( numDatasets ):
        if self.subRequests[rType][ind]['Datasets'][dataset]['Handle'] == handle:
          self.subRequests[rType][ind]['Files'][dataset][attribute] = value
          return S_OK()
      return S_ERROR( "Dataset not found" )

  def getSubRequestDatasetAttributeValue( self, ind, rType, handle, attribute ):
    """ Get the attribute value associated to a dataset and sub-request
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) < ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      numDatasets = self.getSubRequestNumDatasets( ind, rType )['Value']
      for dataset in range( numDatasets ):
        if self.subRequests[rType][ind]['Datasets'][dataset]['Handle'] == handle:
          value = self.subRequests[rType][ind]['Datasets'][dataset][attribute]
          return S_OK( value )
      return S_ERROR( "Dataset not found" )

  ###########################################################
  #
  # Methods for determining whether things are finished
  #

  def isSubRequestEmpty( self, ind, rType ):
    """ Check if the request contains more operations to be performed
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) <= ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      status = self.getSubRequestAttributeValue( ind, rType, "Status" )['Value']
      if status == 'Done':
        return S_OK( 1 )
      files = self.getSubRequestFiles( ind, rType )['Value']
      for rFile in files:
        if not rFile.has_key( 'Status' ):
          gLogger.info( 'file=', '%s' % rFile )
          gLogger.error( "!!! The file has no status information !!!" )
          gLogger.error( "Ind:%s Type:%s" % ( ind, rType ), self.toXML()['Value'] )
        elif rFile['Status'] == 'Waiting':
          gLogger.info( 'Found Waiting File' )
          return S_OK( 0 )
      datasets = self.getSubRequestDatasets( ind, rType )['Value']
      for dataset in datasets:
        if dataset['Status'] == 'Waiting':
          return S_OK( 0 )

    if files or datasets:
      return S_OK( 1 )
    else:
      return S_OK( 0 )

  def isRequestTypeEmpty( self, rType ):
    """ Check whether the requests of given type are complete
    """
    numSubRequests = self.getNumSubRequests( rType )['Value']
    for subRequestInd in range( numSubRequests ):
      if not self.isSubRequestEmpty( subRequestInd, rType )['Value']:
        return S_OK( False )
    return S_OK( True )

  def isRequestEmpty( self ):
    """ Check whether all sub-requests are complete
    """
    requestTypes = self.getSubRequestTypes()['Value']
    for requestType in requestTypes:
      if not self.isRequestTypeEmpty( requestType )['Value']:
        return S_OK( False )
    return S_OK( True )

  def isEmpty( self ):
    """ Included for compatibility not sure if it is used
    """
    return self.isRequestEmpty()

  def isSubRequestDone( self, ind, rType ):
    """ Check if the request contains more operations to be performed
    """
    if not self.subRequests.has_key( rType ):
      return S_ERROR( "No requests of type specified found." )
    elif len( self.subRequests[rType] ) <= ind:
      return S_ERROR( "Subrequest index is out of range." )
    else:
      status = self.getSubRequestAttributeValue( ind, rType, "Status" )['Value']
      if status == 'Done':
        return S_OK( 1 )
      files = self.getSubRequestFiles( ind, rType )['Value']
      for rFile in files:
        if not rFile.has_key( 'Status' ):
          gLogger.error( "!!! The file has no status information !!!" )
          gLogger.error( "Ind:%s Type:%s" % ( ind, rType ), self.toXML()['Value'] )
        elif rFile['Status'] not in ( 'Done', 'Failed' ):
          gLogger.verbose( 'Found file in a non-Done or non-Failed state' )
          return S_OK( 0 )
      datasets = self.getSubRequestDatasets( ind, rType )['Value']
      for dataset in datasets:
        if dataset['Status'] != 'Done':
          return S_OK( 0 )

    if files or datasets:
      return S_OK( 1 )
    else:
      return S_OK( 0 )

  def isRequestTypeDone( self, rType ):
    """ Check whether the requests of given type are complete
    """
    numSubRequests = self.getNumSubRequests( rType )['Value']
    for subRequestInd in range( numSubRequests ):
      if not self.isSubRequestDone( subRequestInd, rType )['Value']:
        return S_OK( False )
    return S_OK( True )

  def isRequestDone( self ):
    """ Check whether all sub-requests are complete
    """
    requestTypes = self.getSubRequestTypes()['Value']
    for requestType in requestTypes:
      if not self.isRequestTypeDone( requestType )['Value']:
        return S_OK( False )
    return S_OK( True )

  def isDone( self ):
    """ Included for compatibility not sure if it is used
    """
    return self.isRequestDone()

  ###########################################################
  #
  # Methods for setting specific requests
  #

  def setDISETRequest( self, rpcStub, executionOrder = 0 ):
    """ Add DISET subrequest from the DISET rpcStub
    """

    result = self.addSubRequest( DISETSubRequest( rpcStub, executionOrder ).getDictionary(), 'diset' )
    return result

  ###########################################################
  #
  # Parsing methods
  #

  def toFile( self, fname ):
    res = self.toXML()
    if not res['OK']:
      return res
    reqfile = open( fname, 'w' )
    reqfile.write( res['Value'] )
    reqfile.close()
    return S_OK()


  def __listToXML_new( self, xmldoc, tagName, aList ):
    # # create
    tag = xmldoc.createElement( tagName )
    tag.setAttribute( "element_type", "list" )
    encodedStringTag = xmldoc.createElement( "EncodedString" )
    encodedStringTag.setAttribute( "element_type", "leaf" )
    encodedStringCDATA = xmldoc.createCDATASection( DEncode.encode( aList ) )
    # # appending
    encodedStringTag.appendChild( encodedStringCDATA )
    tag.appendChild( encodedStringTag )
    # # returning
    return tag

  def __dictToXML( self, xmldoc, tagName, aDict ):
    tag = xmldoc.createElement( tagName )
    tag.setAttribute( "element_type", "dictionary" )
    for key, value in aDict.items():
      if type( value ) == DictType:
        tag.appendChild( self.__dictToXML( xmldoc, key, value ) )
      elif type( value ) == ListType:
        tag.appendChild( self.__listToXML_new( xmldoc, key, value ) )
      else:
        # # creating
        childTag = xmldoc.createElement( str( key ) )
        childTag.setAttribute( "element_type", "leaf" )
        childText = xmldoc.createCDATASection( str( value ) )
        # # appending
        childTag.appendChild( childText )
        tag.appendChild( childTag )
    return tag

  def toXML_new( self, desiredType = None ):
    # # create new doc
    xmlDoc = xml.dom.minidom.Document()
    # # <DIRAC_REQUEST />
    requestTag = xmlDoc.createElement( "DIRAC_REQUEST" )
    # # <Header/>
    headerTag = xmlDoc.createElement( "Header" )
    # # <Header> attrs
    for attrName, attrValue in self.attributes.items():
      headerTag.setAttribute( str( attrName ), str( attrValue ) )
    requestTag.appendChild( headerTag )

    requestTypes = self.getSubRequestTypes()['Value']
    if desiredType:
      if self.getNumSubRequests( desiredType )["Value"]:
        requestTypes = [ desiredType ]
      else:
        return S_ERROR( "toXML: sub-requests of type=%s not found in this request" % desiredType )

    for requestType in requestTypes:
      for i in range( self.getNumSubRequests( requestType )['Value'] ):
        # # <REQUESTTYPE_SUBREQUEST />
        requestTag.appendChild ( self.__dictToXML( xmlDoc,
                                                   "%s_SUBREQUEST" % requestType.upper(),
                                                   self.subRequests[requestType][i] ) )

    xmlDoc.appendChild( requestTag )
    return S_OK( xmlDoc.toprettyxml( " ", encoding = "UTF-8" ) )

  def toXML( self, desiredType = '' ):
    """ Output the request to XML
    """

    out = '<?xml version="1.0" encoding="UTF-8" ?>\n\n'
    out = '%s<DIRAC_REQUEST>\n\n' % out

    xml_attributes = ''
    for attr, value in self.attributes.items():
      xml_attributes += '             %s="%s"\n' % ( attr, str( value ) )
    out = '%s<Header \n%s/>\n\n' % ( out, xml_attributes )

    for requestType in self.getSubRequestTypes()['Value']:
      # This allows us to supply a request type
      useType = False
      if not desiredType:
        useType = True
      elif desiredType == requestType:
        if not self.getNumSubRequests( desiredType )['Value']:
          # You have requested a request type and there are no sub requests of this type
          return S_OK()
        else:
          useType = True
      if useType:
        numSubReqs = self.getNumSubRequests( requestType )['Value']
        for subReqInd in range( numSubReqs ):
          out = "%s%s" % ( out, self.createSubRequestXML( subReqInd, requestType )['Value'] )
    out = '%s</DIRAC_REQUEST>\n' % out
    return S_OK( str( out ) )

  def createSubRequestXML( self, ind, rType ):
    """ A simple subrequest representation assuming the subrequest is just
        a dictionary of subrequest attributes
    """
    name = rType.upper() + '_SUBREQUEST'
    out = self.__dictionaryToXML( name, self.subRequests[rType][ind] )
    return S_OK( out )

  def __dictionaryToXML( self, name, dictIn, indent = 0, attributes = {} ):
    """ Utility to convert a dictionary to XML
    """
    xml_attributes = ''
    xml_elements = []
    for attr, value in dictIn.items():
      if type( value ) is DictType:
        xml_elements.append( self.__dictionaryToXML( attr, value, indent + 1 ) )
      elif type( value ) is ListType:
        xml_elements.append( self.__listToXML( attr, value, indent + 1 ) )
      else:
        xml_attributes += ' ' * ( indent + 1 ) * 8 + '<%s element_type="leaf"><![CDATA[%s]]></%s>\n' % ( attr, str( value ), attr )

    for attr, value in attributes.items():
      xml_attributes += ' ' * ( indent + 1 ) * 8 + '<%s element_type="leaf"><![CDATA[%s]]></%s>\n' % ( attr, str( value ), attr )

    out = ' ' * indent * 8 + '<%s element_type="dictionary">\n%s\n' % ( name, xml_attributes[:-1] )
    for el in xml_elements:
      out += ' ' * indent * 8 + el
    out += ' ' * indent * 8 + '</%s>\n' % name
    return out

  def __listToXML( self, name, aList, indent = 0, attributes = {} ):
    """ Utility to convert a list to XML
    """
    """
    xml_attributes = ''
    xml_elements = []
    for element in list:
      if type(element) is DictType:
        xml_elements.append(self.__dictionaryToXML(name[:-1],element,indent+1))
      elif type(value) is ListType:
        xml_elements.append(self.__listToXML(name[:-1],element,indent+1))
      else:
        xml_attributes += ' '*(indent+1)*8+'<%s element_type="leaf"><![CDATA[%s]]></%s>\n' % (name[:-1],str(element),name[:-1])

    for attr,value in attributes.items():
      xml_attributes += ' '*(indent+1)*8+'<%s element_type="leaf"><![CDATA[%s]]></%s>\n' % (attr,str(value),attr)

    out = ' '*indent*8+'<%s element_type="list">\n%s\n' % (name,xml_attributes[:-1])
    for el in xml_elements:
      out += ' '*indent*8+el
    out += ' '*indent*8+'</%s>\n' % name
    """
    out = ''
    if aList:
      den = DEncode.encode( aList )
      out += ' ' * indent * 8 + '<%s element_type="list">\n' % ( name )
      out += ' ' * ( indent + 1 ) * 8 + '<EncodedString element_type="leaf"><![CDATA[%s]]></EncodedString>\n' % ( den )
      out += ' ' * indent * 8 + '</%s>\n' % name
    return out

  def parseRequest( self, request ):
    """ Create request from the XML string or file
    """
    if os.path.exists( request ):
      dom = xml.dom.minidom.parse( request )
    else:
      dom = xml.dom.minidom.parseString( request )

    header = dom.getElementsByTagName( 'Header' )[0]
    for name in self.attributeNames:
      self.attributes[name] = header.getAttribute( name )

    request = dom.getElementsByTagName( 'DIRAC_REQUEST' )[0]
    dom_subrequests = request.childNodes
    for dom_subrequest in dom_subrequests:
      if dom_subrequest.nodeName.find( '_SUBREQUEST' ) != -1:
        subrequest = self.parseSubRequest( dom_subrequest )
        requestType = dom_subrequest.nodeName.split( '_' )[0].lower()
        self.addSubRequest( subrequest, requestType )

  def parseSubRequest( self, dom ):
    """ A simple subrequest parser from the dom object. This is to be overloaded
        in more complex request types
    """
    subDict = self.__dictionaryFromXML( dom )
    return subDict

  def __dictionaryFromXML( self, dom ):
    """ Utility to get a dictionary from the XML element
    """
    resultDict = {}
    for child in dom.childNodes:
      if child.nodeType == child.ELEMENT_NODE:
        dname = child.nodeName
        dom_dict = dom.getElementsByTagName( dname )[0]
        if dom_dict.getAttribute( 'element_type' ) == 'dictionary':
          ddict = self.__dictionaryFromXML( child )
          resultDict[dname] = ddict
        elif dom_dict.getAttribute( 'element_type' ) == 'list':
          resultDict[dname] = self.__listFromXML( child )
        elif dom_dict.getAttribute( 'element_type' ) == 'leaf':
          value = self.__getCharacterData( child )
          resultDict[dname] = value
    return resultDict

  def __listFromXML( self, dom ):
    resultList = []
    """
    for child in dom.childNodes:
      if child.nodeType == child.ELEMENT_NODE:
        dname = child.nodeName
        dom_dict = dom.getElementsByTagName(dname)[0]
        if dom_dict.getAttribute('element_type') == 'dictionary':
          ddict = self.__dictionaryFromXML(child)
          resultList.append(ddict)
        elif dom_dict.getAttribute('element_type') == 'list':
          resultList = self.__listFromXML(child)
        elif dom_dict.getAttribute('element_type') == 'leaf':
          value = self.__getCharacterData(child)
          resultList.append(value)
    """
    for child in dom.childNodes:
      if child.nodeType == child.ELEMENT_NODE:
        dname = child.nodeName
        dom_dict = dom.getElementsByTagName( dname )[0]
        if dom_dict.getAttribute( 'element_type' ) == 'leaf':
          value = self.__getCharacterData( child )
          resultList, ignored = DEncode.decode( value )
    return resultList

  def __getCharacterData( self, node ):
    out = ''
    for child in node.childNodes:
      if child.nodeType == child.TEXT_NODE or \
         child.nodeType == child.CDATA_SECTION_NODE:
        out = out + child.data
    return str( out.strip() )

  def getDigest( self ):
    """ Get the request short description string
    """

    digestStrings = []
    for sType in self.subRequests.keys():
      for ind in range( len( self.subRequests[sType] ) ):
        digestList = []
        digestList.append( sType )
        digestList.append( self.subRequests[sType][ind]['Attributes']['Operation'] )
        digestList.append( self.subRequests[sType][ind]['Attributes']['Status'] )
        digestList.append( str( self.subRequests[sType][ind]['Attributes']['ExecutionOrder'] ) )
        if self.subRequests[sType][ind]['Attributes'].has_key( 'TargetSE' ):
          digestList.append( str( self.subRequests[sType][ind]['Attributes']['TargetSE'] ) )
        if self.subRequests[sType][ind]['Attributes'].has_key( 'Catalogue' ):
          digestList.append( str( self.subRequests[sType][ind]['Attributes']['Catalogue'] ) )
        if self.subRequests[sType][ind].has_key( 'Files' ):
          if self.subRequests[sType][ind]['Files']:
            fname = os.path.basename( self.subRequests[sType][ind]['Files'][0]['LFN'] )
            if len( self.subRequests[sType][ind]['Files'] ) > 1:
              fname += ',...<%d files>' % len( self.subRequests[sType][ind]['Files'] )
            digestList.append( fname )
        digestStrings.append( ":".join( digestList ) )

    digest = '\n'.join( digestStrings )
    return S_OK( digest )

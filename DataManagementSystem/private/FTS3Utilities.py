from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.DErrno import DError
import json
import datetime

def _checkSourceReplicas( ftsFiles ):
  """ Check the active replicas
  """

  lfns = list( set( [f.lfn for f in ftsFiles] ) )
  res = DataManager().getActiveReplicas( lfns )

  return res



def selectUniqueSourceforTransfers( multipleSourceTransfers ):
  """
      :param multipleSourceTransfers : { sourceSE : [FTSFiles] }
                           

      :return { source SE : [ FTSFiles] } where each LFN appears only once
  """
  # the more an SE has files, the more likely it is that it is a big good old T1 site.
  # So we start packing with these SEs
  orderedSources = sorted( multipleSourceTransfers, key = lambda srcSE: len( multipleSourceTransfers[srcSE] ), reverse = True )

  transfersBySource = {}
  usedLFNs = set()

  for sourceSE in orderedSources:
    transferList = []
    for ftsFile in multipleSourceTransfers[sourceSE]:
      if ftsFile.lfn not in usedLFNs:
        transferList.append( ftsFile )
        usedLFNs.add( ftsFile.lfn )

    if transferList:
      transfersBySource[sourceSE] = transferList

  return S_OK( transfersBySource )



def generatePossibleTransfersBySources( ftsFiles, allowedSources = None ):
  """
      For a list of FTS3files object, group the transfer possible sources
      CAUTION ! a given LFN can be in multiple source
                You still have to choose your source !

      :param allowedSources : list of allowed sources
      :param ftsFiles : list of FTS3File object
      :return  S_OK({ sourceSE: [ FTS3Files] })


  """

  _log = gLogger.getSubLogger( "generatePossibleTransfersBySources", True )


  # destGroup will contain for each target SE a dict { possible source : transfer metadata }
  groupBySource = {}

  # For all files, check which possible sources they have
  res = _checkSourceReplicas( ftsFiles )
  if not res['OK']:
    return res

  filteredReplicas = res['Value']


  for ftsFile in ftsFiles:

    if ftsFile.lfn in filteredReplicas['Failed']:
      _log.error( "Failed to get active replicas", "%s,%s" % ( ftsFile.lfn , filteredReplicas['Failed'][ftsFile.lfn] ) )
      continue

    replicaDict = filteredReplicas['Successful'][ftsFile.lfn]

    for se in replicaDict:

      # if we are imposed a source, respect it
      if allowedSources and se not in allowedSources:
        continue


      groupBySource.setdefault( se, [] ).append( ftsFile )


  return S_OK( groupBySource )


def groupFilesByTarget(ftsFiles):
  """
        For a list of FTS3files object, group the Files by target

        :param ftsFiles : list of FTS3File object
        :return {targetSE : [ ftsFiles] } }


    """

  # destGroup will contain for each target SE a dict { possible source : transfer metadata }
  destGroup = {}

  for ftsFile in ftsFiles:
    destGroup.setdefault(ftsFile.targetSE, []).append(ftsFile)

  return S_OK( destGroup )


class FTS3Serializable( object ):
  """ This is the base class for all the FTS3 objects that
      needs to be serialized, so FTS3Operation, FTS3File
      and FTS3Job

      The inheriting classes just have to define a class
      attribute called _attrToSerialize, which is a list of
      strings, which correspond to the name of the attribute
      they want to serialize
  """
  _datetimeFormat = '%Y-%m-%d %H:%M:%S'

  # MUST BE OVERWRITTEN IN THE CHILD CLASS
  _attrToSerialize = []

  def toJSON( self, forPrint = False ):
    """ Returns the JSON formated string

        :param forPrint: if set to True, we don't include
               the 'magic' arguments used for rebuilding the
               object
    """

    jsonStr = json.dumps( self, cls = FTS3JSONEncoder, forPrint = forPrint )
    return jsonStr

  def __str__( self ):
    import pprint
    js = json.loads( self.toJSON( forPrint = True ) )
    return pprint.pformat( js )


  def _getJSONData( self, forPrint = False ):
    """ Returns the data that have to be serialized by JSON

        :param forPrint: if set to True, we don't include
               the 'magic' arguments used for rebuilding the
               object

        :return dictionary to be transformed into json
    """
    jsonData = {}
    datetimeAttributes = []
    for attrName in self._attrToSerialize :
      value = getattr( self, attrName, None )
      if isinstance( value, datetime.datetime ):
        # We convert date time to a string
        jsonData[attrName] = value.strftime( self._datetimeFormat )
        datetimeAttributes.append( attrName )
      else:
        jsonData[attrName] = value

    if not forPrint:
      jsonData['__type__'] = self.__class__.__name__
      jsonData['__module__'] = self.__module__
      jsonData['__datetime__'] = datetimeAttributes


    return jsonData


class FTS3JSONEncoder( json.JSONEncoder ):
  """ This class is an encoder for the FTS3 objects
  """

  def __init__( self, *args, **kwargs ):
    if 'forPrint' in kwargs:
      self._forPrint = kwargs.pop( 'forPrint' )
    else:
      self._forPrint = False

    super( FTS3JSONEncoder, self ).__init__( *args, **kwargs )

  def default( self, obj ):

    if hasattr( obj, '_getJSONData' ):
      return obj._getJSONData( forPrint = self._forPrint )
    else:
      return json.JSONEncoder.default( self, obj )



class FTS3JSONDecoder( json.JSONDecoder ):
  """ This class is an decoder for the FTS3 objects
  """


  def __init__( self, *args, **kargs ):
    json.JSONDecoder.__init__( self, object_hook = self.dict_to_object,
                         *args, **kargs )

  def dict_to_object( self, dataDict ):
    """ Convert the dictionary into an object """
    import importlib
    # If it is not an FTS3 object, just return the structure as is
    if not ( '__type__' in dataDict and '__module__' in dataDict ):
      return dataDict

    # Get the class and module
    className = dataDict.pop( '__type__' )
    modName = dataDict.pop( '__module__' )
    datetimeAttributes = dataDict.pop( '__datetime__', [] )
    datetimeSet = set(datetimeAttributes)
    try:
      # Load the module
      mod = importlib.import_module( modName )
      # import the class
      cl = getattr( mod, className )
      # Instantiate the object
      obj = cl()

      # Set each attribute
      for attrName, attrValue in dataDict.iteritems():
        if attrName in datetimeSet:
          attrValue = datetime.datetime.strptime( attrValue, FTS3Serializable._datetimeFormat )
        setattr( obj, attrName, attrValue )

      return obj

    except Exception as e:
      gLogger.error( 'exception in FTS3JSONDecoder %s for type %s' % ( e, className ) )
      dataDict['__type__'] = className
      dataDict['__module__'] = modName
      dataDict['__datetime__'] = datetimeAttributes
      return dataDict

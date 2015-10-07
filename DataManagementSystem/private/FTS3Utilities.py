from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno, DError

def __checkSourceReplicas( ftsFiles ):
  """ Check the active replicas
  """

  lfns = list( set( [f.lfn for f in ftsFiles] ) )
  res = DataManager().getActiveReplicas( lfns )

  return res


# def selectUniqueSourceforTransfers( multipleSourceTransfers ):
#   """
#       :param multipleSourceTransfers : { sourceSE : [transfer metadata] }
#                            transfer metadata = { lfn, ftsFileID, sourceSURL, checksum, size}
#
#       :return { source SE : [ transfer metadata] } where each LFN appears only once
#   """
#   # the more an SE has files, the more likely it is that it is a big good old T1 site.
#   # So we start packing with these SEs
#   orderedSources = sorted( multipleSourceTransfers, key = lambda srcSE: len( multipleSourceTransfers[srcSE] ), reverse = True )
#
#   transfersBySource = {}
#   usedLFNs = set()
#
#   for sourceSE in orderedSources:
#     transferList = []
#     for transfer in multipleSourceTransfers[sourceSE]:
#       if transfer['lfn'] not in usedLFNs:
#         transferList.append( transfer )
#         usedLFNs.add( transfer['lfn'] )
#     if transferList:
#       transfersBySource[sourceSE] = transferList
#
#   return transfersBySource

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

  return transfersBySource



def generateTransfersByTarget( ftsFiles, allowedSources = None ):
  """
      For a list of FTS3files object, group the transfer by target
      and all the possible sources
      CAUTION ! for a given target, an LFN can be in multiple source
                You still have to choose your source !

      :param allowedSources : list of allowed sources
      :param ftsFiles : list of FTS3File object
      :return {targetSE : { sourceSE: [ <metadata dict>] } }
              metadata dict : { lfn, ftsFileID, sourceSURl, checksum, size }

  """

  _log = gLogger.getSubLogger( "groupTransfersByTarget", True )


  # destGroup will contain for each target SE a dict { possible source : transfer metadata }
  destGroup = {}


  # For all files, check which possible sources they have
  res = __checkSourceReplicas( ftsFiles )
  if not res['OK']:
    return res

  filteredReplicas = res['Value']


  for ftsFile in ftsFiles:

    if ftsFile.lfn in filteredReplicas['Failed']:
      _log.error( "Failed to get active replicas", "%s,%s" % ( ftsFile.lfn , filteredReplicas['Failed'][ftsFile.LFN] ) )
      continue

    replicaDict = filteredReplicas['Successful'][ftsFile.lfn]

    for se in replicaDict:

      # if we are imposed a source, respect it
      if allowedSources and se not in allowedSources:
        continue

      transferDict = {'lfn' : ftsFile.lfn,
                      'ftsFileID' : getattr( ftsFile, 'ftsFileID' ),
                      'sourceSURL' : replicaDict[se],
                      'checksum' : ftsFile.checksum,
                      'size' : ftsFile.size
                      }
      destGroup.setdefault( ftsFile.targetSE, {} ).setdefault( se, [] ).append( transferDict )


  return S_OK( destGroup )


def generatePossibleTransfersBySources( ftsFiles, allowedSources = None ):
  """
      For a list of FTS3files object, group the transfer possible sources
      CAUTION ! a given LFN can be in multiple source
                You still have to choose your source !

      :param allowedSources : list of allowed sources
      :param ftsFiles : list of FTS3File object
      :return  { sourceSE: [ FTS3Files] }


  """

  _log = gLogger.getSubLogger( "groupTransfersByTarget", True )


  # destGroup will contain for each target SE a dict { possible source : transfer metadata }
  groupBySource = {}


  # For all files, check which possible sources they have
  res = __checkSourceReplicas( ftsFiles )
  if not res['OK']:
    return res

  filteredReplicas = res['Value']


  for ftsFile in ftsFiles:

    if ftsFile.lfn in filteredReplicas['Failed']:
      _log.error( "Failed to get active replicas", "%s,%s" % ( ftsFile.lfn , filteredReplicas['Failed'][ftsFile.LFN] ) )
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

def generateTransfersByTargetAndSource( ftsFiles, allowedSources = None ):
  """ For a list of FTS3Files object, generate unique combination of
      transfers by targets and sources

      :param allowedSources : list of allowed sources
      :param ftsFiles : list of FTS3File object
      :return {targetSE : { sourceSE: [ <metadata dict>] } }
              metadata dict : { lfn, ftsFileID, sourceSURl, checksum, size }
  """

  res = generateTransfersByTarget( ftsFiles, allowedSources = allowedSources )
  if not res['OK']:
    return res

  transferDict = {}

  destGroup = res['Value']

  # We now have for each target SE possible source.
  # make a choice !
  for targetSE in destGroup:

    transferBySource = selectUniqueSourceforTransfers( destGroup[targetSE] )

    if transferBySource:
      transferDict[targetSE] = transferBySource

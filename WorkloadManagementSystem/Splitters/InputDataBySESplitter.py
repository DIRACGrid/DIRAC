from DIRAC import S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Splitters.BaseSplitter import BaseSplitter

class InputDataBySESplitter( BaseSplitter ):

  AFTER_OPTIMIZER = "InputDataResolution"

  def splitJob( self, jobState ):
    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return result
    jobManifest = result[ 'Value' ]
    maxIDPerJob = max( 1, jobManifest.getOption( "SplitterMaxFilesPerJob", 1 ) )
    result = jobState.getInputData( )
    if not result[ 'OK' ]:
      self.jobLog.error( "Could not retrieve input data: %s" % result[ 'Message' ] )
      return result
    data = result[ 'Value' ]
    if not data:
      self.jobLog.error( "No input data defined" )
      return S_ERROR( "No input data defined" )
    seContents = {}
    for lfn in data:
      for seName in data[ lfn ][ 'Replicas' ]:
        if seName not in seContents:
          seContents[ seName ] = set()
        seContents[ seName ].add( lfn )

    manifests = []
    seCounters = dict( [ ( seName, 0 ) for seName in seContents ] )
    while seContents:
      seName = sorted( [ ( seCounters[ seName ], seName ) for seName in seCounters ] )[0][-1]
      seData = seContents[ seName ]
      lfns = []
      for i in range( maxIDPerJob ):
        try:
          lfn = seData.pop()
        except KeyError:
          break
        lfns.append( lfn )
        for otherSE in seContents:
          try:
            seContents[ otherSE ].remove( lfn )
          except:
            pass
      seCounters[ seName ] += len( lfns )
      if len( lfns ) < maxIDPerJob :
        seContents.pop( seName )
        seCounters.pop( seName )
      if lfns:
        self.jobLog.info( "Generated manifest to %s with %s lfns" % ( seName, len( lfns ) ) )
        manifest = jobManifest.clone()
        manifest.setOption( "InputData", ",".join( lfns ) )
        manifest.setOption( "SplitterChosenSE", seName )
        manifests.append( manifest )

    return S_OK( manifests )

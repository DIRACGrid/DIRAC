
from DIRAC import S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Splitters.BaseSplitter import BaseSplitter

class ParametricSplitter( BaseSplitter ):

  def splitJob( self, jobState ):
    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return result
    manifest = result[ 'Value' ]
    params = manifest.getOption( "Parameters", [] )

    if len( params ) == 1:
      try:
        numParams = int( params[0] )
      except ValueError:
        return S_ERROR( "Illegal value for Parameters option in the manifest" )
      pStart = manifest.getOption( 'ParameterStart', 1.0 )
      pStep = manifest.getOption( 'ParameterStep', 0.0 )
      pFactor = manifest.getOption( 'ParameterFactor', 1.0 )

      params = [ pStart ]
      for i in range( numParams -1 ):
        params.append( params[-1] * pFactor + pStep )

    parentManifest = manifest.clone()
    for k in ( 'Parameters', 'ParameterStart', 'ParameterStep', 'ParameterFactor' ):
      parentManifest.remove( k )

    manifestList = []
    fillLen = len( str( len( params ) ) )
    for iP in range( len( params ) ):
      param = params[ iP ]
      childManifest = parentManifest.clone()
      childManifest.setOption( "Parameter", param )
      childManifest.expand()
      manifestList.append( childManifest )

    return S_OK( manifestList )

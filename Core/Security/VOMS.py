# $HeadURL$
__RCSID__ = "$Id$"

import os, stat, tempfile, shutil
from DIRAC import S_OK, S_ERROR, gConfig, rootPath, gLogger
import DIRAC.Core.Security.Locations as Locations
import DIRAC.Core.Security.File as File
from DIRAC.Core.Security.BaseSecurity import BaseSecurity
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities import List


class VOMS( BaseSecurity ):

  def getVOMSAttributes( self, proxy, switch = "all" ):
    """
    Return VOMS proxy attributes as list elements if switch="all" (default) OR
    return the string prepared to be stored in DB if switch="db" OR
    return the string of elements to be used as the option string in voms-proxy-init
    if switch="option".
    If a given proxy is a grid proxy, then function will return an empty list.
    """

    # Get all possible info from voms proxy
    result = self.getVOMSProxyInfo( proxy, "all" )
    if not result["OK"]:
      return S_ERROR( 'Failed to extract info from proxy: %s' % result[ 'Message' ] )

    vomsInfoOutput = List.fromChar( result["Value"], "\n" )

    #Get a list of known VOMS attributes
    validVOMSAttrs = []
    result = gConfig.getOptions( "/Registry/VOMS/Mapping" )
    if result[ 'OK' ]:
      for group in result[ 'Value' ]:
        vA = gConfig.getValue( "/Registry/VOMS/Mapping/%s" % group, "" )
        if vA and vA not in validVOMSAttrs:
          validVOMSAttrs.append( vA )
    result = gConfig.getSections( "/Registry/Groups" )
    if result[ 'OK' ]:
      for group in result[ 'Value' ]:
        vA = gConfig.getValue( "/Registry/Groups/%s/VOMSRole" % group, "" )
        if vA and vA not in validVOMSAttrs:
          validVOMSAttrs.append( vA )

    # Parse output of voms-proxy-info command
    attributes = []
    voName = ''
    nickName = ''
    for line in vomsInfoOutput:
      fields = List.fromChar( line, ":" )
      key = fields[0]
      value = " ".join( fields[1:] )
      if key == "VO":
        voName = value
      elif key == "attribute":
        # Cut off unsupported Capability selection part
        if value.find( "nickname" ) == 0:
          nickName = "=".join( List.fromChar( value, "=" )[ 1: ] )
        else:
          value = value.replace( "/Capability=NULL" , "" )
          value = value.replace( "/Role=NULL" , "" )
          if value and value not in attributes and value in validVOMSAttrs:
            attributes.append( value )

    # Sorting and joining attributes
    if switch == "db":
      returnValue = ":".join( attributes )
    elif switch == "option":
      if len( attributes ) > 1:
        returnValue = voName + " -order " + ' -order '.join( attributes )
      elif attributes:
        returnValue = voName + ":" + attributes[0]
      else:
        returnValue = voName
    elif switch == 'nickname':
      returnValue = nickName
    elif switch == 'all':
      returnValue = attributes

    return S_OK( returnValue )

  def getVOMSProxyFQAN( self, proxy ):
    """ Get the VOMS proxy fqan attributes
    """
    return self.getVOMSProxyInfo( proxy, "fqan" )

  def getVOMSProxyInfo( self, proxy, option = False ):
    """ Returns information about a proxy certificate (both grid and voms).
        Available information is:
          1. Full (grid)voms-proxy-info output
          2. Proxy Certificate Timeleft in seconds (the output is an int)
          3. DN
          4. voms group (if any)
        @type  proxy: a string
        @param proxy: the proxy certificate location.
        @type  option: a string
        @param option: None is the default value. Other option available are:
          - timeleft
          - actimeleft
          - identity
          - fqan
          - all
        @rtype:   tuple
        @return:  status, output, error, pyerror.
    """

    validOptions = ['actimeleft', 'timeleft', 'identity', 'fqan', 'all']
    if option:
      if option not in validOptions:
        S_ERROR( 'Non valid option %s' % option )

    retVal = File.multiProxyArgument( proxy )
    if not retVal[ 'OK' ]:
      return retVal
    proxyDict = retVal[ 'Value' ]
    # chain = proxyDict[ 'chain' ]
    proxyLocation = proxyDict[ 'file' ]

    cmd = 'voms-proxy-info -dont-verify-ac -file %s' % proxyLocation
    if option:
      cmd += ' -%s' % option

    result = shellCall( self._secCmdTimeout, cmd )

    if proxyDict[ 'tempFile' ]:
      self._unlinkFiles( proxyLocation )

    if not result['OK']:
      return S_ERROR( 'Failed to call voms-proxy-info' )

    status, output, error = result['Value']
    # FIXME: if the local copy of the voms server certificate is not up to date the command returns 0.
    # the stdout needs to be parsed.
    if status:
      gLogger.warn( 'Failed to execute:', cmd )
      gLogger.warn( 'Exit code:', status )
      gLogger.warn( 'StdOut' , output )
      gLogger.warn( 'StdErr' , error )
      if error.find( 'VOMS extension not found' ) == -1 and \
         not error.find( 'WARNING: Unable to verify signature! Server certificate possibly not installed.' ) == 0:
        return S_ERROR( 'Failed to get proxy info. Command: %s; StdOut: %s; StdErr: %s' % ( cmd, output, error ) )

    if option == 'fqan':
      if output:
        output = output.split( '/Role' )[0]
      else:
        output = '/lhcb'

    return S_OK( output )


  def getVOMSESLocation( self ):
    #755
    requiredDirPerms = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    #644
    requiredFilePerms = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
    #777
    allPerms = stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO
    vomsesPaths = []
    if 'DIRAC_VOMSES' in os.environ:
      vomsesPaths.append( os.environ[ 'DIRAC_VOMSES' ] )
    vomsesPaths.append( os.path.join( rootPath, "etc", "grid-security", "vomses" ) )
    for vomsesPath in vomsesPaths:
      if not os.path.exists( vomsesPath ):
        continue
      if os.path.isfile( vomsesPath ):
        pathMode = os.stat( vomsesPath )[ stat.ST_MODE ]
        if ( pathMode & allPerms ) ^ requiredFilePerms == 0:
          return vomsesPath
        fd, tmpPath = tempfile.mkstemp( "vomses" )
        os.close( fd )
        shutil.copy( vomsesPath , tmpPath )
        os.chmod( tmpPath, requiredFilePerms )
        os.environ[ 'DIRAC_VOMSES' ] = tmpPath
        return tmpPath
      elif os.path.isdir( vomsesPath ):
        ok = True
        pathMode = os.stat( vomsesPath )[ stat.ST_MODE ]
        if ( pathMode & allPerms ) ^ requiredDirPerms:
          ok = False
        if ok:
          for fP in os.listdir( vomsesPath ):
            pathMode = os.stat( os.path.join( vomsesPath, fP ) )[ stat.ST_MODE ]
            if ( pathMode & allPerms ) ^ requiredFilePerms:
              ok = False
              break
        if ok:
          return vomsesPath
        tmpDir = tempfile.mkdtemp()
        tmpDir = os.path.join( tmpDir, "vomses" )
        shutil.copytree( vomsesPath, tmpDir )
        os.chmod( tmpDir, requiredDirPerms )
        for fP in os.listdir( tmpDir ):
          os.chmod( os.path.join( tmpDir, fP ), requiredFilePerms )
        os.environ[ 'DIRAC_VOMSES' ] = tmpDir
        return tmpDir


  def setVOMSAttributes( self, proxy, attribute = None, vo = None ):
    """ Sets voms attributes to a proxy
    """
    if not vo:
      return S_ERROR( "No vo specified, and can't get default in the configuration" )

    retVal = File.multiProxyArgument( proxy )
    if not retVal[ 'OK' ]:
      return retVal
    proxyDict = retVal[ 'Value' ]
    chain = proxyDict[ 'chain' ]
    proxyLocation = proxyDict[ 'file' ]

    secs = chain.getRemainingSecs()[ 'Value' ] - 300
    if secs < 0:
      return S_ERROR( "Proxy length is less that 300 secs" )
    hours = int( secs / 3600 )
    mins = int( ( secs - hours * 3600 ) / 60 )

    retVal = self._generateTemporalFile()
    if not retVal[ 'OK' ]:
      File.deleteMultiProxy( proxyDict )
      return retVal
    newProxyLocation = retVal[ 'Value' ]

    cmdArgs = []
    if chain.isLimitedProxy()[ 'Value' ]:
      cmdArgs.append( '-limited' )
    cmdArgs.append( '-cert "%s"' % proxyLocation )
    cmdArgs.append( '-key "%s"' % proxyLocation )
    cmdArgs.append( '-out "%s"' % newProxyLocation )
    if attribute and attribute != 'NoRole':
      cmdArgs.append( '-voms "%s:%s"' % ( vo, attribute ) )
    else:
      cmdArgs.append( '-voms "%s"' % vo )
    cmdArgs.append( '-valid "%s:%s"' % ( hours, mins ) )
    tmpDir = False
    vomsesPath = self.getVOMSESLocation()
    if vomsesPath:
      cmdArgs.append( '-vomses "%s"' % vomsesPath )

    cmd = 'voms-proxy-init %s' % " ".join( cmdArgs )
    result = shellCall( self._secCmdTimeout, cmd )
    if tmpDir:
      shutil.rmtree( tmpDir )

    File.deleteMultiProxy( proxyDict )

    if not result['OK']:
      self._unlinkFiles( newProxyLocation )
      return S_ERROR( 'Failed to call voms-proxy-init: %s' % result['Message'] )

    status, output, error = result['Value']

    if status:
      self._unlinkFiles( newProxyLocation )
      return S_ERROR( 'Failed to set VOMS attributes. Command: %s; StdOut: %s; StdErr: %s' % ( cmd, output, error ) )

    newChain = X509Chain()
    retVal = newChain.loadProxyFromFile( newProxyLocation )
    self._unlinkFiles( newProxyLocation )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't load new proxy: %s" % retVal[ 'Message' ] )

    return S_OK( newChain )

  def vomsInfoAvailable( self ):
    """
    Is voms info available?
    """
    cmd = 'voms-proxy-info -h'
    result = shellCall( self._secCmdTimeout, cmd )
    if not result['OK']:
      return False
    status, output, error = result['Value']
    if status:
      return False
    return True


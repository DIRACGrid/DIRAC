""" Module for dealing with VOMS (Virtual Organization Membership Service)
"""

__RCSID__ = "$Id$"

import os
import stat
import tempfile
import shutil

from DIRAC import S_OK, S_ERROR, gConfig, rootPath
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.ProxyFile import multiProxyArgument, deleteMultiProxy
from DIRAC.Core.Security.BaseSecurity import BaseSecurity
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities import List, Time, Os


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
      return S_ERROR( DErrno.EVOMS, 'Failed to extract info from proxy: %s' % result[ 'Message' ] )

    vomsInfoOutput = List.fromChar( result["Value"], "\n" )

    #Get a list of known VOMS attributes
    validVOMSAttrs = []
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
      key = fields[0].strip()
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
        :type  proxy: a string
        :param proxy: the proxy certificate location.
        :type  option: a string
        :param option: None is the default value. Other option available are:
          - timeleft
          - actimeleft
          - identity
          - fqan
          - all
        :rtype:   tuple
        :return:  status, output, error, pyerror.
    """
    validOptions = ['actimeleft', 'timeleft', 'identity', 'fqan', 'all']
    if option:
      if option not in validOptions:
        S_ERROR( DErrno.EVOMS, "valid option %s" % option )

    retVal = multiProxyArgument( proxy )
    if not retVal[ 'OK' ]:
      return retVal
    proxyDict = retVal[ 'Value' ]

    try:
      res = proxyDict[ 'chain' ].getVOMSData()
      if not res[ 'OK' ]:
        return res

      data = res[ 'Value' ]

      if option == 'actimeleft':
        now = Time.dateTime()
        left = data[ 'notAfter' ] - now
        return S_OK( "%d\n" % left.total_seconds() )
      if option == "timeleft":
        now = Time.dateTime()
        left = proxyDict[ 'chain' ].getNotAfterDate()[ 'Value' ] - now
        return S_OK( "%d\n" % left.total_seconds() )
      if option == "identity":
        return S_OK( "%s\n" % data[ 'subject' ] )
      if option == "fqan":
        return S_OK( "\n".join( [ f.replace( "/Role=NULL", "" ).replace( "/Capability=NULL", "" ) for f in data[ 'fqan' ] ] ) )
      if option == "all":
        lines = []
        creds = proxyDict[ 'chain' ].getCredentials()[ 'Value' ]
        lines.append( "subject : %s" % creds[ 'subject' ] )
        lines.append( "issuer : %s" % creds[ 'issuer' ] )
        lines.append( "identity : %s" % creds[ 'identity' ] )
        if proxyDict[ 'chain' ].isRFC().get( 'Value' ):
          lines.append( "type : RFC compliant proxy" )
        else:
          lines.append( "type : proxy" )
        left = creds[ 'secondsLeft' ]
        h = int( left / 3600 )
        m = int( left / 60 ) - h * 60
        s = int( left ) - m * 60 - h * 3600
        lines.append( "timeleft  : %s:%s:%s\nkey usage : Digital Signature, Key Encipherment, Data Encipherment" % (  h, m, s ) )
        lines.append( "== VO %s extension information ==" % data[ 'vo' ] )
        lines.append( "VO: %s" % data[ 'vo' ] )
        lines.append( "subject : %s" % data[ 'subject' ] )
        lines.append( "issuer : %s" % data[ 'issuer' ] )
        for fqan in data[ 'fqan' ]:
          lines.append( "attribute : %s" % fqan )
        if 'attribute' in data:
          lines.append( "attribute : %s" % data[ 'attribute' ] )
        now = Time.dateTime()
        left = ( data[ 'notAfter' ] - now ).total_seconds()
        h = int( left / 3600 )
        m = int( left / 60 ) - h * 60
        s = int( left ) - m * 60 - h * 3600
        lines.append( "timeleft : %s:%s:%s" % ( h, m , s ) )

        return S_OK( "\n".join( lines ) )
      else:
        return S_ERROR( DErrno.EVOMS, "NOT IMP" )

    finally:
      if proxyDict[ 'tempFile' ]:
        self._unlinkFiles( proxyDict[ 'file' ] )

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
      return S_ERROR( DErrno.EVOMS, "No vo specified, and can't get default in the configuration" )

    retVal = multiProxyArgument( proxy )
    if not retVal[ 'OK' ]:
      return retVal
    proxyDict = retVal[ 'Value' ]
    chain = proxyDict[ 'chain' ]
    proxyLocation = proxyDict[ 'file' ]

    secs = chain.getRemainingSecs()[ 'Value' ] - 300
    if secs < 0:
      return S_ERROR( DErrno.EVOMS, "Proxy length is less that 300 secs" )
    hours = int( secs / 3600 )
    mins = int( ( secs - hours * 3600 ) / 60 )

    retVal = self._generateTemporalFile()
    if not retVal[ 'OK' ]:
      deleteMultiProxy( proxyDict )
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
    if chain.isRFC().get( 'Value' ):
      cmdArgs.append( "-r" )

    if not Os.which('voms-proxy-init'):
      return S_ERROR( DErrno.EVOMS, "Missing voms-proxy-init" )

    cmd = 'voms-proxy-init %s' % " ".join( cmdArgs )
    result = shellCall( self._secCmdTimeout, cmd )
    if tmpDir:
      shutil.rmtree( tmpDir )

    deleteMultiProxy( proxyDict )

    if not result['OK']:
      self._unlinkFiles( newProxyLocation )
      return S_ERROR( DErrno.EVOMS, 'Failed to call voms-proxy-init: %s' % result['Message'] )

    status, output, error = result['Value']

    if status:
      self._unlinkFiles( newProxyLocation )
      return S_ERROR( DErrno.EVOMS, 'Failed to set VOMS attributes. Command: %s; StdOut: %s; StdErr: %s' % ( cmd, output, error ) )

    newChain = X509Chain()
    retVal = newChain.loadProxyFromFile( newProxyLocation )
    self._unlinkFiles( newProxyLocation )
    if not retVal[ 'OK' ]:
      return S_ERROR( DErrno.EVOMS, "Can't load new proxy: %s" % retVal[ 'Message' ] )

    return S_OK( newChain )

  def vomsInfoAvailable( self ):
    """
    Is voms info available?
    """
    if not Os.which("voms-proxy-info"):
      return S_ERROR( DErrno.EVOMS, "Missing voms-proxy-info" )
    cmd = 'voms-proxy-info -h'
    result = shellCall( self._secCmdTimeout, cmd )
    if not result['OK']:
      return False
    status, _output, _error = result['Value']
    if status:
      return False
    return True


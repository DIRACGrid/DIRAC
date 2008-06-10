
from DIRAC import S_OK, S_ERROR
import DIRAC.Core.Security.Locations as Locations
import DIRAC.Core.Security.File as File
from DIRAC.Core.Security.BaseSecurity import BaseSecurity
from DIRAC.Core.Utilities.Subprocess import shellCall

class VOMS( BaseSecurity ):

  def getVOMSAttributes( self, proxy, switch="all" ):
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
      return S_ERROR( 'Failed to extract info from proxy' )

    voms_info_output = result["Value"]
    voms_info_output = voms_info_output.split("\n")

    # Parse output of voms-proxy-info command
    attributes = []
    voName = ''
    nickName = ''
    for i in voms_info_output:
      j = i.split(":")
      if j[0].strip() == "VO":
        voName = j[1].strip()
      elif j[0].strip()=="attribute":
        # Cut off unsupported Capability selection part
        j[1] = j[1].replace("/Capability=NULL","")
        if j[1].find('Role=NULL') == -1 and j[1].find('Role') != -1:
          attributes.append(j[1].strip())
        if j[1].find('nickname') != -1:
          nickName = j[1].strip().split()[2]

    # Sorting and joining attributes
    if switch == "db":
      attributes.sort()
      returnValue = ":".join(attributes)
    elif switch == "option":
      if len(attributes)>1:
        returnValue = voName+" -order "+' -order '.join(attributes)
      elif attributes:
        returnValue = voName+":"+attributes[0]
      else:
        returnValue = voName
    elif switch == 'nickname':
      returnValue = nickName
    elif switch == 'all':
      returnValue = attributes

    return S_OK(returnValue)

  def getVOMSProxyFQAN( proxy ):
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
        @type  proxy_file: a string
        @param proxy_file: the proxy certificate location.
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

    validOptions = ['actimeleft','timeleft','identity','fqan','all']
    if option:
      if option not in validOptions:
        S_ERROR('Non valid option %s' % option)

    retVal = self._loadProxy( proxy )
    if not retVal[ 'OK' ]:
      return retVal
    proxyDict = retVal[ 'Value' ]
    chain = proxyDict[ 'chain' ]
    proxyLocation = proxyDict[ 'file' ]

    vomsEnv = self._getExternalCmdEnvironment()

    cmd = 'voms-proxy-info -file %s' % proxyLocation
    if option:
      cmd += ' -%s' % option

    result = shellCall( self._secCmdTimeout, cmd, env = vomsEnv )

    if proxyDict[ 'tempFile' ]:
        self._unlinkFiles( proxyLocation )

    if not result['OK']:
      return S_ERROR('Failed to call voms-proxy-info')

    status, output, error = result['Value']

    if status:
      if error.find('VOMS extension not found') == -1:
        return S_ERROR('Failed to get proxy info. Command: %s; StdOut: %s; StdErr: %s' % (cmd,output,error))

    if option == 'fqan':
      if output:
        output = output.split('/Role')[0]
      else:
        output = '/lhcb'

    return S_OK( output )

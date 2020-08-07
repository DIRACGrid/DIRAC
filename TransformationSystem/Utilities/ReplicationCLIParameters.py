"""
Command Line Parameters for creating the Replication transformations Script
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOMSVOForGroup


class Params(object):
  """Parameter Object"""

  def __init__(self):
    self.targetSE = []
    self.sourceSE = ''
    self.groupSize = 1
    self.groupName = None
    self.extraname = ''
    self.flavour = 'Replication'
    self.plugin = 'Broadcast'
    self.metaValues = []
    self.metaKey = None
    self.extraData = {}
    self.errorMessages = []
    self.enable = False

  def setMetaValues(self, values):
    if isinstance(values, list):
      self.metaValues = values
    else:
      self.metaValues = [val for val in values.split(",")]
    return S_OK()

  def setMetaKey(self, key):
    self.metaKey = key
    return S_OK()

  def setMetadata(self, metadata):
    for pair in metadata.split(','):
      splitPair = pair.strip().split(':')
      if len(splitPair) == 2:
        self.extraData[splitPair[0]] = splitPair[1].strip()
    return S_OK()

  def setSourceSE(self, sourceSE):
    self.sourceSE = [sSE.strip() for sSE in sourceSE.split(",")]
    return S_OK()

  def setTransFlavour(self, flavour):
    self.flavour = flavour
    return S_OK()

  def setTargetSE(self, targetSE):
    self.targetSE = [tSE.strip() for tSE in targetSE.split(",")]
    return S_OK()

  def setExtraname(self, extraname):
    self.extraname = extraname
    return S_OK()

  def setGroupSize(self, size):
    try:
      self.groupSize = int(size)
    except ValueError:
      return S_ERROR("Expected integer for groupsize")
    return S_OK()

  def setGroupName(self, name):
    self.groupName = name
    return S_OK()

  def setPlugin(self, plugin):
    self.plugin = plugin
    return S_OK()

  def setEnable(self, _):
    self.enable = True
    return S_OK()

  def registerSwitches(self, script):
    """ register command line arguments

    :param script: Dirac.Core.Base Script Class
    :type script: DIRAC.Core.Base.Script
    """

    script.registerSwitch("G:", "GroupSize=", "Number of Files per transformation task", self.setGroupSize)
    script.registerSwitch("R:", "GroupName=", "TransformationGroup Name", self.setGroupName)
    script.registerSwitch("S:", "SourceSEs=", "SourceSE(s) to use, comma separated list", self.setSourceSE)
    script.registerSwitch("N:", "Extraname=", "String to append to transformation name", self.setExtraname)
    script.registerSwitch("P:", "Plugin=", "Plugin to use for transformation", self.setPlugin)
    script.registerSwitch("T:", "Flavour=", "Flavour to create: Replication or Moving", self.setTransFlavour)
    script.registerSwitch("K:", "MetaKey=", "Meta Key to use: TransformationID", self.setMetaKey)
    script.registerSwitch("M:", "MetaData=", "MetaData to use Key/Value Pairs: 'DataType:REC,'", self.setMetadata)
    script.registerSwitch("x", "Enable", "Enable the transformation creation, otherwise dry-run", self.setEnable)

    useMessage = []
    useMessage.append("Create one replication transformation for each MetaValue given")
    useMessage.append("Is running in dry-run mode, unless enabled with -x")
    useMessage.append("MetaValue and TargetSEs can be comma separated lists")
    useMessage.append("Usage:")
    useMessage.append("  %s <MetaValue1[,val2,val3]> <TargetSEs> [-G<Files>] [-S<SourceSEs>]"
                      "[-N<ExtraName>] [-T<Type>] [-M<Key>] [-K...] -x" % script.scriptName)
    script.setUsageMessage('\n'.join(useMessage))

  def checkSettings(self, script, checkArguments=True):
    """check if all required parameters are set, print error message and return S_ERROR if not

    :param script: The script object
    :type script: DIRAC.Core.Base.Script
    :param bool checkArguments: if false do not check for the correct number of arguments, should only be
        changed if using derived class
    """

    if checkArguments:
      args = script.getPositionalArgs()
      if len(args) == 2:
        self.setMetaValues(args[0])
        self.setTargetSE(args[1])
      else:
        self.errorMessages.append("ERROR: Wrong number of arguments")

    self._checkProxy()

    # get default metadata key:
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    if self.metaKey is None:
      self.metaKey = Operations().getValue('Transformations/TransfIDMeta', 'TransformationID')

    if not self.errorMessages:
      return S_OK()
    gLogger.error("\n".join(self.errorMessages))
    script.showHelp()
    return S_ERROR()

  def _checkProxy(self):
    """checks if the proxy has the ProductionManagement property and belongs to a VO"""
    proxyInfo = getProxyInfo()
    if not proxyInfo['OK']:
      self.errorMessages.append("ERROR: No Proxy present")
      return False
    proxyValues = proxyInfo.get('Value', {})
    group = proxyValues.get('group', '')
    vomsvo = getVOMSVOForGroup(group)
    if not vomsvo:
      self.errorMessages.append("ERROR: ProxyGroup not associated to VOMS VO, get a different proxy")
      return False

    groupProperties = proxyValues.get('groupProperties', [])

    if groupProperties:
      if 'ProductionManagement' not in groupProperties:
        self.errorMessages.append("ERROR: Not allowed to create production, you need a ProductionManagement proxy.")
        return False
    else:
      self.errorMessages.append("ERROR: Could not determine Proxy properties, you do not have the right proxy.")
      return False
    return True

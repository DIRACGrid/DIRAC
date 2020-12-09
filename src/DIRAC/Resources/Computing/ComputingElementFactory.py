########################################################################
# File :   ComputingElementFactory.py
# Author : Stuart Paterson
########################################################################

"""  The Computing Element Factory has one method that instantiates a given Computing Element
     from the CEUnique ID specified in the JobAgent configuration section.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.Computing.ComputingElement import getCEConfigDict
from DIRAC.Core.Utilities import ObjectLoader

__RCSID__ = "$Id$"


class ComputingElementFactory(object):

  #############################################################################
  def __init__(self, ceType=''):
    """ Standard constructor
    """
    self.ceType = ceType
    self.log = gLogger.getSubLogger('ComputingElementFactory')

  #############################################################################
  def getCE(self, ceType='', ceName='', ceParametersDict={}):
    """This method returns the CE instance corresponding to the supplied
       CEUniqueID.  If no corresponding CE is available, this is indicated.
    """
    if ceType:
      self.log.verbose('Creating CE of type %s' % ceType)
    if ceName:
      self.log.verbose('Creating CE for name %s' % ceName)
    ceTypeLocal = ceType if ceType else self.ceType
    ceNameLocal = ceName if ceName else ceType
    ceConfigDict = getCEConfigDict(ceNameLocal)
    self.log.verbose('CEConfigDict', ceConfigDict)
    if 'CEType' in ceConfigDict:
      ceTypeLocal = ceConfigDict['CEType']
    if not ceTypeLocal:
      error = 'Can not determine CE Type'
      self.log.error(error)
      return S_ERROR(error)
    subClassName = "%sComputingElement" % (ceTypeLocal)

    objectLoader = ObjectLoader.ObjectLoader()
    result = objectLoader.loadObject('Resources.Computing.%s' % subClassName, subClassName)
    if not result['OK']:
      self.log.error('Failed to load object', '%s: %s' % (subClassName, result['Message']))
      return result

    ceClass = result['Value']
    try:
      computingElement = ceClass(ceNameLocal)
      # Always set the CEType parameter according to instantiated class
      ceDict = {'CEType': ceTypeLocal}
      if ceParametersDict:
        ceDict.update(ceParametersDict)
      result = computingElement.setParameters(ceDict)
      if not result['OK']:
        return result

    except Exception as x:
      msg = 'ComputingElementFactory could not instantiate %s object' % subClassName
      self.log.exception()
      self.log.warn(msg, repr(x))
      return S_ERROR(repr(x))

    return S_OK(computingElement)

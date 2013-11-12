########################################################################
# $HeadURL$
# File :   ComputingElementFactory.py
# Author : Stuart Paterson
########################################################################

"""  The Computing Element Factory has one method that instantiates a given Computing Element
     from the CEUnique ID specified in the JobAgent configuration section.
"""
from DIRAC.Resources.Computing.ComputingElement          import getCEConfigDict
from DIRAC                                               import S_OK, S_ERROR, gLogger

__RCSID__ = "$Id$"

class ComputingElementFactory( object ):

  #############################################################################
  def __init__(self, ceType=''):
    """ Standard constructor
    """
    self.ceType = ceType
    self.log = gLogger.getSubLogger( self.ceType )

  #############################################################################
  def getCE(self, ceType='', ceName='', ceParametersDict={}):
    """This method returns the CE instance corresponding to the supplied
       CEUniqueID.  If no corresponding CE is available, this is indicated.
    """
    self.log.verbose('Creating CE of %s type with the name %s' % (ceType, ceName) )
    ceTypeLocal = ceType
    if not ceTypeLocal:
      ceTypeLocal = self.ceType
    ceNameLocal = ceName
    if not ceNameLocal:
      ceNameLocal = self.ceType 
    ceConfigDict = getCEConfigDict( ceNameLocal )
    self.log.verbose('CEConfigDict', ceConfigDict)
    if 'CEType' in ceConfigDict:
      ceTypeLocal = ceConfigDict['CEType']
    if not ceTypeLocal:
      error = 'Can not determine CE Type'
      self.log.error( error )
      return S_ERROR( error )
    subClassName = "%sComputingElement" % (ceTypeLocal)

    try:
      ceSubClass = __import__('DIRAC.Resources.Computing.%s' % subClassName, globals(), locals(), [subClassName])
    except Exception, x:
      msg = 'ComputingElementFactory could not import DIRAC.Resources.Computing.%s' % ( subClassName )
      self.log.exception()
      self.log.warn( msg )
      return S_ERROR( msg )

    try:
      ceStr = 'ceSubClass.%s( "%s" )' % ( subClassName, ceNameLocal )
      computingElement = eval( ceStr )
      if ceParametersDict:
        computingElement.setParameters(ceParametersDict)
    except Exception, x:
      msg = 'ComputingElementFactory could not instantiate %s()' % (subClassName)
      self.log.exception()
      self.log.warn( msg )
      return S_ERROR( msg )

    computingElement._reset()
    return S_OK( computingElement )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

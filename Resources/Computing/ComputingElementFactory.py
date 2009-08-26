########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Resources/Computing/ComputingElementFactory.py,v 1.3 2009/08/26 16:33:29 rgracian Exp $
# File :   ComputingElementFactory.py
# Author : Stuart Paterson
########################################################################

"""  The Computing Element Factory has one method that instantiates a given Computing Element
     from the CEUnique ID specified in the JobAgent configuration section.
"""
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig

__RCSID__ = "$Id: ComputingElementFactory.py,v 1.3 2009/08/26 16:33:29 rgracian Exp $"

import sys,types

class ComputingElementFactory:

  #############################################################################
  def __init__(self,ceUniqueID):
    """ Standard constructor
    """
    self.log = gLogger
    self.ceUniqueID = ceUniqueID
    # Check if the given UniqueID is defined as CE in the /LocalSite section
    result = gConfig.getSections( '/LocalSite' )
    if result['OK'] and self.ceUniqueID in result['Value']:
      # If defined it should contain an Option CEType defining the Type of CE
      self.ceType = gConfig.getValue( '/LocalSite/%s/CEType' % self.ceUniqueID, 'None' )
      if self.ceType == 'None':
        self.log.error( 'CE %s does not define a CEType, will fail to instantiate' )
    else:
      # The UniqueID is assume to be the Type
      self.ceType = self.ceUniqueID
    #self.log.setLevel('debug')

  #############################################################################
  def getCE(self):
    """This method returns the CE instance corresponding to the supplied
       CEUniqueID.  If no corresponding CE is available, this is indicated.
    """
    try:
      subClassName = "%sComputingElement" % (self.ceType)
      ceSubClass = __import__('DIRAC.Resources.Computing.%s' % subClassName,globals(),locals(),[subClassName])
    except Exception, x:
      msg = 'ComputingElementFactory could not import DIRAC.Resources.Computing.%s' % ( subClassName )
      self.log.exception()
      self.log.warn( msg )
      return S_ERROR( msg )

    try:
      ceStr = 'ceSubClass.%s("%s")' % ( subClassName, self.ceUniqueID )
      computingElement = eval( ceStr )
    except Exception, x:
      msg = 'ComputingElementFactory could not instantiate %s()' %(subClassName)
      self.log.exception()
      self.log.warn( msg )
      return S_ERROR( msg )

    return S_OK( computingElement )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

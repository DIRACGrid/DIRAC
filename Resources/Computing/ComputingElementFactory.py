########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Resources/Computing/ComputingElementFactory.py,v 1.2 2009/04/18 18:26:57 rgracian Exp $
# File :   ComputingElementFactory.py
# Author : Stuart Paterson
########################################################################

"""  The Computing Element Factory has one method that instantiates a given Computing Element
     from the CEUnique ID specified in the JobAgent configuration section.
"""
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC                                               import S_OK, S_ERROR, gLogger

__RCSID__ = "$Id: ComputingElementFactory.py,v 1.2 2009/04/18 18:26:57 rgracian Exp $"

import sys,types

class ComputingElementFactory:

  #############################################################################
  def __init__(self,ceUniqueID):
    """ Standard constructor
    """
    self.ceUniqueID = ceUniqueID
    self.log = gLogger
    #self.log.setLevel('debug')

  #############################################################################
  def getCE(self):
    """This method returns the CE instance corresponding to the supplied
       CEUniqueID.  If no corresponding CE is available, this is indicated.
    """
    try:
      subClassName = "%sComputingElement" % (self.ceUniqueID)
      ceSubClass = __import__('DIRAC.Resources.Computing.%s' % subClassName,globals(),locals(),[subClassName])
    except Exception, x:
      msg = 'ComputingElementFactory could not import DIRAC.Resources.Computing.%s' %(subClassName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    try:
      ceStr = 'ceSubClass.%s()' % (subClassName)
      computingElement = eval(ceStr)
    except Exception, x:
      msg = 'ComputingElementFactory could not instantiate %s()' %(subClassName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(computingElement)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

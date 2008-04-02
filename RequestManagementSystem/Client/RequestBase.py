# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/RequestBase.py,v 1.1 2008/04/02 09:23:59 atsareg Exp $

""" Request base class. Defines the common general parameters that should be present in any
    request
"""

__RCSID__ = "$Id: RequestBase.py,v 1.1 2008/04/02 09:23:59 atsareg Exp $"

import commands
import DIRAC.Core.Utilities.Time as Time

class RequestBase:

  def __init__(self):

    # This is a list of mandatory parameters
    self.genParametersNames = ['SourceComponent','TargetComponent','RequestType',
                               'RequestTechnology','OwnerDN','OwnerGroup',
                               'CreationTime','RequestName','RequestID','JobID',
                               'DIRACSetup','Status']

    self.genParameters = {}
    for name in self.genParametersNames:
      self.genParameters[name] = 'Unknown'

    # Set some defaults
    self.genParameters['DIRACSetup'] = "LHCb-Development"
    status,self.genParameters['RequestID'] = commands.getstatusoutput('uuidgen')
    self.genParameters['CreationTime'] = Time.toString(Time.dateTime())
    self.genParameters['Status'] = "New"

  def getGenParameters(self):
    """ Get the dictionary of the generic parameters
    """

    return self.genParameters

######################################################################################
#
#  All the set/get methods should go here
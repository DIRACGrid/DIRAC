""" IdProvider base class for various identity providers
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import gLogger, S_OK

__RCSID__ = "$Id$"


class IdProvider(object):

  def __init__(self, *args, **kwargs):
    """ C'or
    """
    self.log = gLogger.getSubLogger(self.__class__.__name__)
    self.parameters = kwargs.get('parameters', {})
    self._initialization()

  def _initialization(self):
    """ Initialization """
    pass

  def setParameters(self, parameters):
    """ Set parameters

        :param dict parameters: parameters of the identity Provider
    """
    self.parameters = parameters

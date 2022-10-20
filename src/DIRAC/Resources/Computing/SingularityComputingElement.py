"""Legacy backwards compatibility class, use ApptainerComputingElement instead
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.ApptainerComputingElement import ApptainerComputingElement


class SingularityComputingElement(ApptainerComputingElement):
    pass

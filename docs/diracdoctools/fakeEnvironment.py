''' fakeEnvironment

   this module allows to create the documentation without having to do
   any kind of special installation. The list of mocked modules is:

   GSI

'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import mock


class MyMock(mock.Mock):

  def __len__(self):
    return 0


# Needed
# GSI
mockGSI = MyMock()
mockGSI.__version__ = "1"
mockGSI.version.__version__ = "1"
sys.modules['GSI'] = mockGSI

"""Tools to build the DIRAC documentation.


Scripts to build code documentation, and also some mocking, etc.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# list of packages that should be mocked in sphinx
# TODO: check if we can install more of these
DIRAC_DOC_MOCK_LIST = ['_arc',
                       'arc',
                       'cmreslogging',
                       'fts3',
                       'gfal2',
                       'git',
                       'irods',
                       'lcg_util',
                       'pylab',
                       'pythonjsonlogger',
                       'stomp',
                       ]

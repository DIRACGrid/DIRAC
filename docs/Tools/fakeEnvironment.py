''' fakeEnvironment
   
   this module allows to create the documentation without having to do
   any kind of special installation. The list of mocked modules is:
      
   GSI
   
'''

import mock
import sys

#...............................................................................
# mocks...

class MyMock(mock.Mock):
  
  def __len__(self):
    return 0

# GSI
mockGSI                     = MyMock()
mockGSI.__version__         = "1"
mockGSI.version.__version__ = "1"

# MySQLdb
mockMySQLdb = mock.Mock()

#...............................................................................
# sys.modules hacked

sys.modules[ 'GSI' ]     = mockGSI
sys.modules[ 'MySQLdb' ] = mockMySQLdb
sys.modules[ 'MySQLdb.cursors' ] = mock.Mock()

#FIXME: do we need all them ??

sys.modules[ 'suds' ]                            = mock.Mock()
sys.modules[ 'irods' ]                           = mock.Mock()
sys.modules[ 'pylab' ]                           = mock.Mock()
sys.modules[ 'pytz' ]                            = mock.Mock()
sys.modules[ 'numpy' ]                           = mock.Mock()
sys.modules[ 'numpy.random' ]                    = mock.Mock()
sys.modules[ 'matplotlib' ]                      = mock.Mock()
sys.modules[ 'matplotlib.ticker' ]               = mock.Mock()
sys.modules[ 'matplotlib.figure' ]               = mock.Mock()
sys.modules[ 'matplotlib.patches' ]              = mock.Mock()
sys.modules[ 'matplotlib.dates' ]                = mock.Mock()
sys.modules[ 'matplotlib.text' ]                 = mock.Mock()
sys.modules[ 'matplotlib.axes' ]                 = mock.Mock()
sys.modules[ 'matplotlib.pylab' ]                = mock.Mock()
sys.modules[ 'cx_Oracle' ]                       = mock.Mock()
sys.modules[ 'dateutil' ]                        = mock.Mock()
sys.modules[ 'dateutil.relativedelta' ]          = mock.Mock()
sys.modules[ 'matplotlib.backends' ]             = mock.Mock()
sys.modules[ 'matplotlib.backends.backend_agg' ] = mock.Mock()
sys.modules[ 'fts3' ]                            = mock.Mock()
sys.modules[ 'fts3.rest' ]                       = mock.Mock()
sys.modules[ 'fts3.rest.client' ]                = mock.Mock()
sys.modules[ 'fts3.rest.client.easy' ]           = mock.Mock()
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
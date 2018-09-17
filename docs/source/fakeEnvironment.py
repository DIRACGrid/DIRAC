''' fakeEnvironment

   this module allows to create the documentation without having to do
   any kind of special installation. The list of mocked modules is:

   GSI

'''

import mock
import sys

# ...............................................................................
# mocks...


class MyMock(mock.Mock):

  def __len__(self):
    return 0


# GSI
mockGSI = MyMock()
mockGSI.__version__ = "1"
mockGSI.version.__version__ = "1"

# MySQLdb
mockMySQLdb = mock.Mock()

# ...............................................................................
# sys.modules hacked

sys.modules['GSI'] = mockGSI
sys.modules['MySQLdb'] = mockMySQLdb
sys.modules['MySQLdb.cursors'] = mock.Mock()

# FIXME: do we need all them ?? We do not install these on readTheDocs, so yes

sys.modules['sqlalchemy'] = mock.Mock()
sys.modules['sqlalchemy.exc'] = mock.Mock()
sys.modules['sqlalchemy.orm'] = mock.Mock()
sys.modules['sqlalchemy.orm.exc'] = mock.Mock()
sys.modules['sqlalchemy.orm.query'] = mock.Mock()
sys.modules['sqlalchemy.engine'] = mock.Mock()
sys.modules['sqlalchemy.engine.reflection'] = mock.Mock()
sys.modules['sqlalchemy.ext'] = mock.Mock()
sys.modules['sqlalchemy.ext.declarative'] = mock.Mock()
sys.modules['sqlalchemy.schema'] = mock.Mock()
sys.modules['sqlalchemy.sql'] = mock.Mock()
sys.modules['sqlalchemy.sql.expression'] = mock.Mock()
sys.modules['lcg_util'] = mock.Mock()
sys.modules['suds'] = mock.Mock()
sys.modules['suds.client'] = mock.Mock()
sys.modules['suds.transport'] = mock.Mock()
sys.modules['irods'] = mock.Mock()
sys.modules['pylab'] = mock.Mock()
sys.modules['pytz'] = mock.Mock()
sys.modules['numpy'] = mock.Mock()
sys.modules['numpy.random'] = mock.Mock()
sys.modules['matplotlib'] = mock.Mock()
sys.modules['matplotlib.ticker'] = mock.Mock()
sys.modules['matplotlib.figure'] = mock.Mock()
sys.modules['matplotlib.patches'] = mock.Mock()
sys.modules['matplotlib.dates'] = mock.Mock()
sys.modules['matplotlib.text'] = mock.Mock()
sys.modules['matplotlib.axes'] = mock.Mock()
sys.modules['matplotlib.pylab'] = mock.Mock()
sys.modules['matplotlib.lines'] = mock.Mock()
sys.modules['matplotlib.cbook'] = mock.Mock()
sys.modules['matplotlib.colors'] = mock.Mock()
sys.modules['matplotlib.cm'] = mock.Mock()
sys.modules['matplotlib.colorbar'] = mock.Mock()
sys.modules['cx_Oracle'] = mock.Mock()
sys.modules['dateutil'] = mock.Mock()
sys.modules['dateutil.relativedelta'] = mock.Mock()
sys.modules['matplotlib.backends'] = mock.Mock()
sys.modules['matplotlib.backends.backend_agg'] = mock.Mock()
sys.modules['fts3'] = mock.Mock()
sys.modules['fts3.rest'] = mock.Mock()
sys.modules['fts3.rest.client'] = mock.Mock()
sys.modules['fts3.rest.client.easy'] = mock.Mock()
sys.modules['fts3.rest.client.exceptions'] = mock.Mock()
sys.modules['fts3.rest.client.request'] = mock.Mock()
sys.modules['pyparsing'] = mock.MagicMock()
sys.modules['stomp'] = mock.MagicMock()
sys.modules['psutil'] = mock.MagicMock()

sys.modules['_arc'] = mock.Mock()
sys.modules['arc'] = mock.Mock()
sys.modules['arc.common'] = mock.Mock()
sys.modules['gfal2'] = mock.Mock()
sys.modules['XRootD'] = mock.Mock()
sys.modules['XRootD.client'] = mock.Mock()
sys.modules['XRootD.client.flags'] = mock.Mock()

sys.modules['elasticsearch'] = mock.Mock()
sys.modules['elasticsearch.Elasticsearch'] = mock.Mock()
sys.modules['elasticsearch_dsl'] = mock.Mock()
sys.modules['elasticsearch.exceptions'] = mock.Mock()
sys.modules['elasticsearch.helpers'] = mock.Mock()

sys.modules['pythonjsonlogger'] = mock.Mock()
sys.modules['pythonjsonlogger.jsonlogger'] = mock.Mock()
sys.modules['cmreslogging'] = mock.Mock()
sys.modules['cmreslogging.handlers'] = mock.Mock()
sys.modules['git'] = mock.Mock()

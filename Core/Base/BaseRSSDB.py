
__RCSID__ = "$Id$"

from sqlalchemy import create_engine, exc
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters


class BaseRSSDB(object):
  def _initializeConnection(self, dbPath):
    """
    Collect from the CS all the info needed to connect to the DB.
    """

    result = getDBParameters(dbPath)
    if not result['OK']:
      raise Exception('Cannot get database parameters: %s' % result['Message'])

    dbParameters = result['Value']
    self.log.debug("db parameters: %s" % dbParameters)
    self.host = dbParameters['Host']
    self.port = dbParameters['Port']
    self.user = dbParameters['User']
    self.password = dbParameters['Password']
    self.dbName = dbParameters['DBName']

    self.engine = create_engine('mysql://%s:%s@%s:%s/%s' % (self.user,
                                                            self.password,
                                                            self.host,
                                                            self.port,
                                                            self.dbName),
                                pool_recycle=3600,
                                echo_pool=True,
                                echo=self.log.getLevel() == 'DEBUG')
    self.sessionMaker_o = sessionmaker(bind=self.engine)
    self.inspector = Inspector.from_engine(self.engine)

  def _createTablesIfNotThere(self, tablesList):
    """
    Adds each table in tablesList to the DB if not already present
    """
    tablesInDB = self.inspector.get_table_names()

    for table in self.tablesList:
      if table not in tablesInDB:
        found = False
        # is it in the extension? (fully or extended)
        for ext in self.extensions:
          try:
            getattr(
                __import__(
                    ext + __name__,
                    globals(),
                    locals(),
                    [table]),
                table).__table__.create(
                self.engine)  # pylint: disable=no-member
            found = True
            break
          except (ImportError, AttributeError):
            continue
        # If not found in extensions, import it from DIRAC base.
        if not found:
          getattr(
              __import__(
                  __name__,
                  globals(),
                  locals(),
                  [table]),
              table).__table__.create(
              self.engine)  # pylint: disable=no-member
      else:
        gLogger.debug("Table %s already exists" % table)

  def insert(self, table, params):
    """
    Inserts params in the DB.

    :param table: table where to insert
    :type table: str
    :param params: Dictionary to fill a single line
    :type params: dict

    :return: S_OK() || S_ERROR()
    """

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.sessionMaker_o(expire_on_commit=False)  # FIXME: should we use this flag elsewhere?

    found = False
    for ext in self.extensions:
      try:
        tableRow_o = getattr(__import__(ext + self.__class__.__module__, globals(), locals(), [table]), table)()
        found = True
        break
      except (ImportError, AttributeError):
        continue
    # If not found in extensions, import it from DIRAC base (this same module).
    if not found:
      tableRow_o = getattr(__import__(self.__class__.__module__, globals(), locals(), [table]), table)()

    tableRow_o.fromDict(params)

    try:
      session.add(tableRow_o)
      session.commit()
      return S_OK()
    except exc.IntegrityError as err:
      self.log.warn("insert: trying to insert a duplicate key? %s" % err)
      session.rollback()
    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception("insert: unexpected exception", lException=e)
      return S_ERROR("insert: unexpected exception %s" % e)
    finally:
      session.close()

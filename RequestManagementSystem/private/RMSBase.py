from sqlalchemy.ext.declarative import declarative_base

# This is the common class instance all the RMS objects (Request, Operation, Files) have to inherit from
RMSBase = declarative_base()
RMSBase._datetimeFormat = '%Y-%m-%d %H:%M:%S'
RMSBase.__table_args__ = {'mysql_engine':'InnoDB'}

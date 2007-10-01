from dirac import DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.LoggingSystem.DB.MsgLoggingDB import MsgLoggingDB
from DIRAC.Core.Utilities import Time, dateTime, hour, date, week, day

DBpoint=MsgLoggingDB()
#result=DBpoint.getMsgByDate(dateTime()-4*day,dateTime()-3*day-12*hour)
#print result['Value']
#result=DBpoint.getMsgByMainTxt( [ 'error message 1' , 'error message 4' ])
#print result['Value']
result=DBpoint.getMsgs( { 'FixtxtString': [ 'error message 1' ,
                                        'error message 4' ] ,
                      'OwnerDN': 'user1', 'LogLevelName': [ 'ERROR',
                                                            'ALWAYS' ] } )
if result['OK']:
  for res in result['Value']:
    print '%s\t%s' % ( res[0],'\t'.join(res[1:]))

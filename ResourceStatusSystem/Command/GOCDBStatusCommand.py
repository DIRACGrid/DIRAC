# $HeadURL:  $
''' GOCDBStatus_Command 
  The GOCDBStatus_Command class is a command class to know about 
  present downtimes
'''

import urllib2

from datetime import datetime

from DIRAC                                        import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping  import getGOCSiteName
from DIRAC.ResourceStatusSystem.Command.Command   import Command
#from DIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Utils   import convertTime
from DIRAC.Core.LCG.GOCDBClient                   import GOCDBClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient


__RCSID__ = '$Id:  $'

################################################################################
################################################################################

class GOCDBStatusCommand( Command ):
  
#  __APIs__ = [ 'GOCDBClient' ]

  def __init__( self, args = None, clients = None ):
    
    super( GOCDBStatusCommand, self ).__init__( args, clients )
    
    if 'GOCDBClient' in self.APIs:
      self.gClient = self.APIs[ 'GOCDBClient' ]
    else:
      self.gClient = GOCDBClient() 
  
  def doCommand( self ):
    """ 
    Return getStatus from GOC DB Client
    
    :attr:`args`: 
     - args[0]: string: should be a ValidElement

     - args[1]: string: should be the name of the ValidElement

     - args[2]: string: optional, number of hours in which 
     the down time is starting
    """
    
    timeFormat = "%Y-%m-%d %H:%M"
    
#    super(GOCDBStatus_Command, self).doCommand()
#    self.APIs = initAPIs( self.__APIs__, self.APIs )
    
#    try:

    granularity = self.args[0]
    name        = self.args[1]  
    if len( self.args ) > 2:
      hours = self.args[2]
    else:
      hours = None  
      
    if granularity == 'Site':
      name = getGOCSiteName( name )[ 'Value' ]
      
    res = self.gClient.getStatus( granularity, name, None, hours )

    if not res['OK']:
      return res     
        
    res = res['Value']
       
    if res is None or res == {}:
      return S_OK( { 'DT' : None } )
          
    DT_dict_result = {}
    now = datetime.utcnow().replace( microsecond = 0, second = 0 )
      
    if len( res ) > 1:
      #there's more than one DT
      resDT = None
          
      for dt_ID in res:
        #looking for an ongoing one
        startSTR = res[ dt_ID ][ 'FORMATED_START_DATE' ]
        start_datetime = datetime.strptime( startSTR, timeFormat )
        if start_datetime < now:
          resDT = res[ dt_ID ]
          break

      #if I'm here, there's no OnGoing DT
      if resDT is None:
        resDT = res[res.keys()[0]]
      res = resDT
            
    else:
      res = res[res.keys()[0]]

    DT_dict_result['DT']      = res['SEVERITY']
    DT_dict_result['EndDate'] = res['FORMATED_END_DATE']
    startSTR                  = res['FORMATED_START_DATE']
    start_datetime = datetime.strptime( startSTR, timeFormat )
          
    if start_datetime > now:
      diff = convertTime( start_datetime - now, 'hours' )
      DT_dict_result[ 'DT' ] = DT_dict_result['DT'] + " in " + str( diff ) + ' hours'
          
    res = S_OK( DT_dict_result )
        
#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return res

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class DTCachedCommand( Command ):
  
#  __APIs__ = [ 'ResourceManagementClient' ]


  def __init__( self, args = None, clients = None ):
    
    super( DTCachedCommand, self ).__init__( args, clients )
    
    if 'ResourceManagementClient' in self.APIs:
      self.rmClient = self.APIs[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient() 
  
  def doCommand( self ):
    """ 
    Returns DT Information that are cached.

    :attr:`args`: 
       - args[0]: string: should be a ValidElement
  
       - args[1]: string should be the name of the ValidElement

       - args[2]: string: optional, number of hours in which 
       the down time is starting
    """
    
    timeFormat = "%Y-%m-%d %H:%M"
    
#    super(DTCached_Command, self).doCommand()
#    self.APIs = initAPIs( self.__APIs__, self.APIs )    

#    try:

    granularity = self.args[0]
    name        = self.args[1]

    now = datetime.utcnow().replace(microsecond = 0, second = 0)
          
    if granularity == 'Site':
      commandName = 'DTEverySites'
    elif granularity == 'Resource':
      commandName = 'DTEveryResources'  

    meta = { 'columns': 'opt_ID' }
    res = self.rmClient.getClientCache( name = name, commandName = commandName, meta = meta)
    
    if not res[ 'OK' ]:
      return res
    
    res = res[ 'Value' ]
        
    if res is None or len( res ) == 0:
      return S_OK( { 'DT' : None } )
      
    #CachedResult
    clientDict = { 
                  'name'        : name,
                  'commandName' : commandName,
                  'value'       : None,
                  'opt_ID'      : None,
                  'meta'        : { 'columns' : 'Result' }
                 } 
      
    if len( res ) > 1:
      #there's more than one DT
      
      dt_ID_startingSoon     = res[0]
      clientDict[ 'value' ]  = 'StartDate'
      clientDict[ 'opt_ID' ] = dt_ID_startingSoon 
      clientDict[ 'meta' ]   = { 'columns' : 'Result' }  
        
      startSTR_startingSoon = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
      if startSTR_startingSoon:
        startSTR_startingSoon = startSTR_startingSoon[0][0]    
                 
      clientDict[ 'value' ]  = 'EndDate'
      clientDict[ 'opt_ID' ] = dt_ID_startingSoon
      clientDict[ 'meta' ]   = { 'columns' : 'Result' }
      endSTR_startingSoon = self.rmClient.getClientCache( **clientDict )[ 'Value' ]            
      if endSTR_startingSoon:
        endSTR_startingSoon = endSTR_startingSoon[0][0]
      start_datetime_startingSoon = datetime.strptime( startSTR_startingSoon, timeFormat )
        
      end_datetime_startingSoon   = datetime.strptime( endSTR_startingSoon, timeFormat )        

      DT_ID = None

      if start_datetime_startingSoon < now:
        if end_datetime_startingSoon > now:
            #ongoing downtime found!
          DT_ID = dt_ID_startingSoon
      
      if DT_ID is None:
          
        for dt_ID in res[1:]:
          #looking for an ongoing one
          clientDict[ 'value' ]  = 'StartDate'
          clientDict[ 'opt_ID' ] = dt_ID 
          clientDict[ 'meta' ]   = { 'columns' : 'Result' }
          startSTR = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
          if startSTR:
            startSTR = startSTR[0][0]
         
          clientDict[ 'value' ]  = 'EndDate'
          clientDict[ 'opt_ID' ] = dt_ID 
          clientDict[ 'meta' ]   = { 'columns' : 'Result' }
          endSTR = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
          if endSTR:
            endSTR = endSTR[0][0]
              
          start_datetime = datetime.strptime( startSTR, timeFormat )
          end_datetime   = datetime.strptime( endSTR, timeFormat )
            
          if start_datetime < now:
            if end_datetime > now:
              #ongoing downtime found!
              DT_ID = dt_ID
            break
          if start_datetime < start_datetime_startingSoon:
            #the DT starts before the former considered one
            dt_ID_startingSoon = dt_ID
          
        if DT_ID is None:
          #if I'm here, there's no OnGoing DT
          DT_ID = dt_ID_startingSoon
    else:
      DT_ID = res[0]
        
    DT_dict_result = {}
    clientDict[ 'value' ]  = 'StartDate'
    clientDict[ 'opt_ID' ] = DT_ID 
    clientDict[ 'meta' ]   = { 'columns' : 'Result' }
    startSTR = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
    if startSTR:
      startSTR = startSTR[0][0]
    
    clientDict[ 'value' ]  = 'EndDate'
    clientDict[ 'opt_ID' ] = DT_ID 
    clientDict[ 'meta' ]   = { 'columns' : 'Result' }
    endSTR = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
    if endSTR:
      endSTR = endSTR[0][0]
          
    start_datetime = datetime.strptime(startSTR, timeFormat )
    end_datetime   = datetime.strptime(endSTR, timeFormat )      
    
    if end_datetime < now:
      return S_OK( { 'DT' : None } ) 
      
    clientDict[ 'value' ]  = 'Severity'
    clientDict[ 'opt_ID' ] = DT_ID 
    clientDict[ 'meta' ]   = { 'columns' : 'Result' }
  
    DT_dict_result['DT'] = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
    if DT_dict_result['DT']:
      DT_dict_result['DT'] = DT_dict_result['DT'][0][0]
    DT_dict_result['EndDate'] = endSTR
    
    if start_datetime > now:
      self.args[2]
      diff = convertTime(start_datetime - now, 'hours')
      if diff > self.args[2]:
        return S_OK( { 'DT' : None } )
      
      DT_dict_result['DT'] = DT_dict_result['DT'] + " in " + str(diff) + ' hours'
      
    res = S_OK( DT_dict_result )
      
#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return res      
            
#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class DTInfoCachedCommand( Command ):
  
#  __APIs__ = [ 'ResourceManagementClient' ]

  def __init__( self, args = None, clients = None ):
    
    super( DTInfoCachedCommand, self ).__init__( args, clients )
    
    if 'ResourceManagementClient' in self.APIs:
      self.rmClient = self.APIs[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient() 
  
  def doCommand(self):
    """ 
    Returns DT info that are cached.

    :attr:`args`: 
       - args[0]: string: should be a ValidElement
  
       - args[1]: string should be the name of the ValidElement

       - args[2]: string: optional, number of hours in which 
       the down time is starting
    """
    
    timeFormat = "%Y-%m-%d %H:%M"
    
#    super(DTInfo_Cached_Command, self).doCommand()
#    self.APIs = initAPIs( self.__APIs__, self.APIs )     

#    try:

    granularity = self.args[0]
    name        = self.args[1]

    now = datetime.utcnow().replace( microsecond = 0, second = 0 )
    
    if granularity == 'Site':
      commandName = 'DTEverySites'
    elif granularity == 'Resource':
      commandName = 'DTEveryResources'

    meta = { 'columns': 'opt_ID' }
    res = self.rmClient.getClientCache( name = name, commandName = commandName, meta = meta )
      
    if not res[ 'OK' ]:
      return res
      
    res = res[ 'Value' ]    
      
    #CachedResult
    clientDict = { 
                  'name'        : name,
                  'commandName' : commandName,
                  'value'       : None,
                  'opt_ID'      : None,
                  'meta'        : { 'columns'     : 'Result' }
                  }
       
    if len(res) > 1:
      #there's more than one DT
      
      dt_ID_startingSoon    = res[0]
      clientDict[ 'value' ] = 'StartDate'
      clientDict[ 'optID' ] = dt_ID_startingSoon
      clientDict[ 'meta' ]  = { 'columns' : 'Result' }  
      startSTR_startingSoon = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
      if startSTR_startingSoon:
        startSTR_startingSoon = startSTR_startingSoon[0][0]
                                                          
      clientDict[ 'value' ] = 'EndDate'
      clientDict[ 'optID' ] = dt_ID_startingSoon 
      clientDict[ 'meta' ]  = { 'columns' : 'Result' }
      endSTR_startingSoon = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
      if endSTR_startingSoon:
        endSTR_startingSoon = endSTR_startingSoon[0][0]
      
      start_datetime_startingSoon = datetime.strptime(startSTR_startingSoon,
                                                              timeFormat )
      end_datetime_startingSoon = datetime.strptime(endSTR_startingSoon,
                                                             timeFormat )
        
      DT_ID = None
        
      if start_datetime_startingSoon < now:
        if end_datetime_startingSoon > now:
          #ongoing downtime found!
          DT_ID = dt_ID_startingSoon
      
      if DT_ID is None:
          
        for dt_ID in res[1:]:
          #looking for an ongoing one
          clientDict[ 'value' ] = 'StartDate'
          clientDict[ 'optID' ] = dt_ID 
          clientDict[ 'meta' ]  = { 'columns' : 'Result' }
          startSTR = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
          if startSTR:
            startSTR = startSTR[0][0]
          
          clientDict[ 'value' ] = 'EndDate'
          clientDict[ 'optID' ] = dt_ID
          clientDict[ 'meta' ]  = { 'columns' : 'Result' } 
          endSTR = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
          if endSTR:
            endSTR = endSTR[0][0]
         
          start_datetime = datetime.strptime( startSTR, timeFormat )
          end_datetime   = datetime.strptime( endSTR, timeFormat )
          if start_datetime < now:
            if end_datetime > now:
                #ongoing downtime found!
              DT_ID = dt_ID
            break
          if start_datetime < start_datetime_startingSoon:
            #the DT starts before the former considered one
            dt_ID_startingSoon = dt_ID
          
        if DT_ID is None:
          #if I'm here, there's no OnGoing DT
          DT_ID = dt_ID_startingSoon

    else:
      DT_ID = res[0]

    DT_dict_result = {}

    clientDict[ 'value' ] = 'EndDate'
    clientDict[ 'optID' ] = DT_ID
    clientDict[ 'meta' ]  = { 'columns' : 'Result' } 
    endSTR = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
    if endSTR:
      endSTR = endSTR[0][0]
    end_datetime = datetime.strptime( endSTR, timeFormat )
    if end_datetime < now:
      return S_OK( { 'DT' : None } )
    
    DT_dict_result['EndDate'] = endSTR
    
    clientDict[ 'value' ] = 'Severity'
    clientDict[ 'optID' ] = DT_ID 
    clientDict[ 'meta' ]  = { 'columns' : 'Result' }
    DT_dict_result['DT']  = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
    if DT_dict_result['DT']:
      DT_dict_result['DT'] = DT_dict_result['DT'][0][0]
     
    clientDict[ 'value' ] = 'StartDate'
    clientDict[ 'optID' ] = DT_ID 
    clientDict[ 'meta' ]  = { 'columns' : 'Result' }
    DT_dict_result['StartDate'] = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
    if DT_dict_result['StartDate']:
      DT_dict_result['StartDate'] = DT_dict_result['StartDate'][0][0] 
    
    clientDict[ 'value' ] = 'Description'
    clientDict[ 'optID' ] = DT_ID
    clientDict[ 'meta' ]  = { 'columns' : 'Result' } 
    DT_dict_result['Description'] = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
    if DT_dict_result['Description']:
      DT_dict_result['Description'] = DT_dict_result['Description'][0][0]
    
    clientDict[ 'value' ] = 'Link'
    clientDict[ 'optID' ] = DT_ID
    clientDict[ 'meta' ]  = { 'columns' : 'Result' } 
    DT_dict_result['Link'] = self.rmClient.getClientCache( **clientDict )[ 'Value' ]
    if DT_dict_result['Link']:
      DT_dict_result['Link'] = DT_dict_result['Link'][0][0]
    
    start_datetime = datetime.strptime( DT_dict_result['StartDate'], timeFormat )
    
    if start_datetime > now:
      self.args[2]
      diff = convertTime(start_datetime - now, 'hours')
      if diff > self.args[2]:
        return S_OK( { 'DT' : None } )
        
      DT_dict_result['DT'] = DT_dict_result['DT'] + " in " + str(diff) + ' hours'
        
    res = S_OK( DT_dict_result )

#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return res      

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
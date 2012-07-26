# $HeadURL:  $
''' GOCDBStatusCommand module 
'''

import urllib2

from datetime import datetime

from DIRAC                                        import S_OK, S_ERROR
from DIRAC.Core.LCG.GOCDBClient                   import GOCDBClient
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping  import getGOCSiteName
from DIRAC.ResourceStatusSystem.Command.Command   import Command
from DIRAC.ResourceStatusSystem.Utilities.Utils   import convertTime

__RCSID__ = '$Id:  $'

################################################################################

class GOCDBStatusCommand( Command ):
  '''
    The GOCDBStatus_Command class is a command class to know about downtimes
  '''
  
  def __init__( self, args = None, clients = None ):
    
    super( GOCDBStatusCommand, self ).__init__( args, clients )
    
    if 'GOCDBClient' in self.apis:
      self.gClient = self.apis[ 'GOCDBClient' ]
    else:
      self.gClient = GOCDBClient() 
  
  def doCommand( self ):
    ''' 
      Returns downtimes if any from GOCDB on the following format:
        { 'DT' : X, 'EndDate' : Y }
      
      It needs as input parameters a valid element: [ Site, Resource, Node ],
      a valid element name and optionally a number of hours in which the
      downtime will start.
    '''
    
    _timeFormat = "%Y-%m-%d %H:%M"
    
    ## INPUT PARAMETERS
    
    if not 'element' in self.args:
      return S_ERROR( 'GOCDBStatusCommand: "element" not found in self.args' )
    element = self.args[ 'element' ]
    if element is None:
      return S_ERROR( 'GOCDBStatusCommand: "element" should not be None')
    
    if not 'name' in self.args:
      return S_ERROR( 'GOCDBStatusCommand: "name" not found in self.args' )
    name = self.args[ 'name' ]
    if name is None:
      return S_ERROR( 'GOCDBStatusCommand: "name" should not be None' )
    
    hours = None
    if 'hours' in self.args:
      hours = self.args[ 'hours' ]
            
    if element == 'Site':
      name = getGOCSiteName( name )
      if not name[ 'OK' ]:
        return name
      name = name[ 'Value' ]
      
    #FIXME: check if that certainly works or not. Right now, no idea  
    try:
      resDTGOC = self.gClient.getStatus( element, name, None, hours )
    except urllib2.URLError:
      return S_ERROR( 'URLError on getStatus with %s, %s, %s' % ( element, name, hours ) )  

    if not resDTGOC[ 'OK' ]:
      return resDTGOC    
        
    resDTGOC = resDTGOC[ 'Value' ]
       
    if resDTGOC is None or resDTGOC == {}:
      return S_OK( { 'DT' : None } )
          
    now = datetime.utcnow().replace( microsecond = 0, second = 0 )
    
    selectedDTID = None
      
    # If there is more than one DownTime  
    if len( resDTGOC ) > 1:

      # Let's pick the first ongoing one ( startime < now )    
      for dtID in resDTGOC:
        
        _startSTR      = resDTGOC[ dtID ][ 'FORMATED_START_DATE' ]
        startDatetime = datetime.strptime( _startSTR, _timeFormat )
        if startDatetime < now:
          #resDT = resDTGOC[ dtID ]
          selectedDTID = dtID
          break

      #if I'm here, there's no OnGoing DT
      if selectedDTID is None:
        selectedDTID = resDTGOC.keys()[0]
                  
    else:
      selectedDTID = resDTGOC.keys()[0]

    dtResult = {}

    dtResult[ 'DT' ]      = resDTGOC[ selectedDTID ][ 'SEVERITY' ]
    dtResult[ 'EndDate' ] = resDTGOC[ selectedDTID ][ 'FORMATED_END_DATE' ]
    
    _startSTR              = resDTGOC[ selectedDTID ][ 'FORMATED_START_DATE' ]
    start_datetime         = datetime.strptime( _startSTR, _timeFormat )
          
    if start_datetime > now:
      diff = convertTime( start_datetime - now, 'hours' )
      dtResult[ 'DT' ] = dtResult[ 'DT' ] + " in " + str( diff ) + ' hours'
        
    return S_OK( dtResult )
   
################################################################################
#
# UNUSED COMMANDS !!! ( We keep them for the time being ! )
#
#
################################################################################
#
#class DTCachedCommand( Command ):
#
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( DTCachedCommand, self ).__init__( args, clients )
#    
#    if 'ResourceManagementClient' in self.apis:
#      self.rmClient = self.apis[ 'ResourceManagementClient' ]
#    else:
#      self.rmClient = ResourceManagementClient() 
#  
#  def doCommand( self ):
#    """ 
#    Returns DT Information that are cached.
#
#    :attr:`args`: 
#       - args[0]: string: should be a ValidElement
#  
#       - args[1]: string should be the name of the ValidElement
#
#       - args[2]: string: optional, number of hours in which 
#       the down time is starting
#    """
#    
#    _timeFormat = "%Y-%m-%d %H:%M"
#
#    ## INPUT PARAMETERS
#    
#    if not 'element' in self.args:
#      return S_ERROR( '"element" not found in self.args' )
#    element = self.args[ 'element' ]
#    
#    if not 'name' in self.args:
#      return S_ERROR( '"name" not found in self.args' )
#    name = self.args[ 'name' ]
#    
#    hours = None
#    if 'hours' in self.args:
#      hours = self.args[ 'hours' ]
#    
#    now = datetime.utcnow().replace( microsecond = 0, second = 0 )
#          
#    if element == 'Site':
#      commandName = 'DTEverySites'
#    elif element == 'Resource':
#      commandName = 'DTEveryResources'  
#
#    resDTID = self.rmClient.selectClientCache( name = name, commandName = commandName, 
#                                           meta = { 'columns': 'opt_ID' } )
#    
#    if not resDTID[ 'OK' ]:
#      return resDTID
#    
#    resDTID = resDTID[ 'Value' ]
# 
#    if not resDTID:         
##    if resDTID is None or len( resDTID ) == 0:
#      return S_OK( { 'DT' : None } )
#      
#    #CachedResult
#    clientDict = { 
#                  'name'        : name,
#                  'commandName' : commandName,
#                  'value'       : None,
#                  'opt_ID'      : None,
#                  'meta'        : { 'columns' : 'Result' }
#                 } 
#
#    selectedDTDic = {}
#
#    if len( resDTID ) > 1:
#      
#      for dtID in resDTID:
#        
#        clientDict[ 'opt_ID' ] = dtID[ 0 ]
#        
#        # Get startdate
#        clientDict[ 'value' ]  = 'StartDate'
#        
#        startDate = self.rmClient.selectClientCache( **clientDict )
#        if not startDate[ 'OK' ]:
#          return startDate
#        startDate = startDate[ 'Value' ]
#        
#        #FIXME: I want to see a crash if not there
#        #if startDate:
#        startDate = startDate[ 0 ][ 0 ]        
#        startDate = datetime.strptime( startDate, _timeFormat )
#        #else:
#        #  continue
#
#        # Get endDate
#        clientDict[ 'value' ]  = 'EndDate'
#        
#        endDate = self.rmClient.selectClientCache( **clientDict )
#        if not endDate[ 'OK' ]:
#          return endDate
#        endDate = endDate[ 'Value' ]
#        
#        #FIXME: I want to see a crash if it is not there
#        #if endDate:
#        endDate = endDate[ 0 ][ 0 ]        
#        endDate = datetime.strptime( endDate, _timeFormat )
#        #else:
#        #  continue
#
#        if ( startDate < now ) and ( endDate > now ):
#          selectedDTDic[ 'ID' ]        = dtID[ 0 ]
#          selectedDTDic[ 'StartDate' ] = startDate
#          selectedDTDic[ 'EndDate' ]   = endDate
#      
#      #FIXME: If none fulfills the conditions, we pick the first one, but why ???     
#      if selectedDTDic == {}:
#        selectedDTDic[ 'ID' ] = resDTID[ 0 ][ 0 ]
#    
#    else:
#      selectedDTDic[ 'ID' ] = resDTID[ 0 ][ 0 ]                 
#      
#
#    clientDict[ 'opt_ID' ] = selectedDTDic[ 'ID' ]
#    
#    if not 'EndDate' in selectedDTDic:
#      # Get enddate
#      clientDict[ 'value' ]  = 'EndDate'
#        
#      endDate = self.rmClient.selectClientCache( **clientDict )
#      if not endDate[ 'OK' ]:
#        return endDate
#      endDate = endDate[ 'Value' ]
#        
#      #if endDate:
#      endDate                    = endDate[ 0 ][ 0 ]        
#      endDate                    = datetime.strptime( endDate, _timeFormat )  
#      selectedDTDic[ 'EndDate' ] = endDate
#      
#      if endDate < now:
#        return S_OK( { 'DT' : None } )
#
#    if not 'StartDate' in selectedDTDic:
#      # Get enddate
#      clientDict[ 'value' ]  = 'StartDate'
#        
#      startDate = self.rmClient.selectClientCache( **clientDict )
#      if not startDate[ 'OK' ]:
#        return startDate
#      startDate = startDate[ 'Value' ]
#        
#      #if startDate:
#      startDate                    = startDate[ 0 ][ 0 ]        
#      startDate                    = datetime.strptime( startDate, _timeFormat )  
#      selectedDTDic[ 'StartDate' ] = startDate
#      
#    if selectedDTDic[ 'StartDate' ] > now:
#   
#      diffTime = convertTime( selectedDTDic[ 'StartDate' ] - now, 'hours' )
#      if diffTime > hours:
#        return S_OK( { 'DT' : None } )
#      
#      #FIXME
#      #DT_dict_result['DT'] = DT_dict_result['DT'] + ' in ' + str( diffTime ) + ' hours'   
#    
#    clientDict[ 'value' ]  = 'Severity'
#  
#    severity = self.rmClient.selectClientCache( **clientDict )
#    if not severity[ 'OK' ]:
#      return severity
#    severity = severity[ 'Value' ]
#  
#    if severity:
#      selectedDTDic[ 'DT' ] = severity[ 0 ][ 0 ]
#  
#    return S_OK( selectedDTDic )
#   
#################################################################################
#
#class DTInfoCachedCommand( Command ):
## FIXME: modify to get something like previous one !  
##  __APIs__ = [ 'ResourceManagementClient' ]
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( DTInfoCachedCommand, self ).__init__( args, clients )
#    
#    if 'ResourceManagementClient' in self.apis:
#      self.rmClient = self.apis[ 'ResourceManagementClient' ]
#    else:
#      self.rmClient = ResourceManagementClient() 
#  
#  def doCommand(self):
#    """ 
#    Returns DT info that are cached.
#
#    :attr:`args`: 
#       - args[0]: string: should be a ValidElement
#  
#       - args[1]: string should be the name of the ValidElement
#
#       - args[2]: string: optional, number of hours in which 
#       the down time is starting
#    """
#    
#    timeFormat = "%Y-%m-%d %H:%M"
#    
##    super(DTInfo_Cached_Command, self).doCommand()
##    self.apis = initAPIs( self.__APIs__, self.apis )     
#
##    try:
#
#    granularity = self.args[0]
#    name        = self.args[1]
#
#    now = datetime.utcnow().replace( microsecond = 0, second = 0 )
#    
#    if granularity == 'Site':
#      commandName = 'DTEverySites'
#    elif granularity == 'Resource':
#      commandName = 'DTEveryResources'
#
#    meta = { 'columns': 'opt_ID' }
#    res = self.rmClient.selectClientCache( name = name, commandName = commandName, meta = meta )
#      
#    if not res[ 'OK' ]:
#      return res
#      
#    res = res[ 'Value' ]    
#      
#    #CachedResult
#    clientDict = { 
#                  'name'        : name,
#                  'commandName' : commandName,
#                  'value'       : None,
#                  'opt_ID'      : None,
#                  'meta'        : { 'columns'     : 'Result' }
#                  }
#       
#    if len(res) > 1:
#      #there's more than one DT
#      
#      dt_ID_startingSoon    = res[0]
#      clientDict[ 'value' ] = 'StartDate'
#      clientDict[ 'optID' ] = dt_ID_startingSoon
#      clientDict[ 'meta' ]  = { 'columns' : 'Result' }  
#      startSTR_startingSoon = self.rmClient.selectClientCache( **clientDict )[ 'Value' ]
#      if startSTR_startingSoon:
#        startSTR_startingSoon = startSTR_startingSoon[0][0]
#                                                          
#      clientDict[ 'value' ] = 'EndDate'
#      clientDict[ 'optID' ] = dt_ID_startingSoon 
#      clientDict[ 'meta' ]  = { 'columns' : 'Result' }
#      endSTR_startingSoon = self.rmClient.selectClientCache( **clientDict )[ 'Value' ]
#      if endSTR_startingSoon:
#        endSTR_startingSoon = endSTR_startingSoon[0][0]
#      
#      start_datetime_startingSoon = datetime.strptime(startSTR_startingSoon,
#                                                              timeFormat )
#      end_datetime_startingSoon = datetime.strptime(endSTR_startingSoon,
#                                                             timeFormat )
#        
#      DT_ID = None
#        
#      if start_datetime_startingSoon < now:
#        if end_datetime_startingSoon > now:
#          #ongoing downtime found!
#          DT_ID = dt_ID_startingSoon
#      
#      if DT_ID is None:
#          
#        for dt_ID in res[1:]:
#          #looking for an ongoing one
#          clientDict[ 'value' ] = 'StartDate'
#          clientDict[ 'optID' ] = dt_ID 
#          clientDict[ 'meta' ]  = { 'columns' : 'Result' }
#          startSTR = self.rmClient.selectClientCache( **clientDict )[ 'Value' ]
#          if startSTR:
#            startSTR = startSTR[0][0]
#          
#          clientDict[ 'value' ] = 'EndDate'
#          clientDict[ 'optID' ] = dt_ID
#          clientDict[ 'meta' ]  = { 'columns' : 'Result' } 
#          endSTR = self.rmClient.selectClientCache( **clientDict )[ 'Value' ]
#          if endSTR:
#            endSTR = endSTR[0][0]
#         
#          start_datetime = datetime.strptime( startSTR, timeFormat )
#          end_datetime   = datetime.strptime( endSTR, timeFormat )
#          if start_datetime < now:
#            if end_datetime > now:
#                #ongoing downtime found!
#              DT_ID = dt_ID
#            break
#          if start_datetime < start_datetime_startingSoon:
#            #the DT starts before the former considered one
#            dt_ID_startingSoon = dt_ID
#          
#        if DT_ID is None:
#          #if I'm here, there's no OnGoing DT
#          DT_ID = dt_ID_startingSoon
#
#    else:
#      DT_ID = res[0]
#
#    DT_dict_result = {}
#
#    clientDict[ 'value' ] = 'EndDate'
#    clientDict[ 'optID' ] = DT_ID
#    clientDict[ 'meta' ]  = { 'columns' : 'Result' } 
#    endSTR = self.rmClient.selectClientCache( **clientDict )[ 'Value' ]
#    if endSTR:
#      endSTR = endSTR[0][0]
#    end_datetime = datetime.strptime( endSTR, timeFormat )
#    if end_datetime < now:
#      return S_OK( { 'DT' : None } )
#    
#    DT_dict_result['EndDate'] = endSTR
#    
#    clientDict[ 'value' ] = 'Severity'
#    clientDict[ 'optID' ] = DT_ID 
#    clientDict[ 'meta' ]  = { 'columns' : 'Result' }
#    DT_dict_result['DT']  = self.rmClient.selectClientCache( **clientDict )[ 'Value' ]
#    if DT_dict_result['DT']:
#      DT_dict_result['DT'] = DT_dict_result['DT'][0][0]
#     
#    clientDict[ 'value' ] = 'StartDate'
#    clientDict[ 'optID' ] = DT_ID 
#    clientDict[ 'meta' ]  = { 'columns' : 'Result' }
#    DT_dict_result['StartDate'] = self.rmClient.selectClientCache( **clientDict )[ 'Value' ]
#    if DT_dict_result['StartDate']:
#      DT_dict_result['StartDate'] = DT_dict_result['StartDate'][0][0] 
#    
#    clientDict[ 'value' ] = 'Description'
#    clientDict[ 'optID' ] = DT_ID
#    clientDict[ 'meta' ]  = { 'columns' : 'Result' } 
#    DT_dict_result['Description'] = self.rmClient.selectClientCache( **clientDict )[ 'Value' ]
#    if DT_dict_result['Description']:
#      DT_dict_result['Description'] = DT_dict_result['Description'][0][0]
#    
#    clientDict[ 'value' ] = 'Link'
#    clientDict[ 'optID' ] = DT_ID
#    clientDict[ 'meta' ]  = { 'columns' : 'Result' } 
#    DT_dict_result['Link'] = self.rmClient.selectClientCache( **clientDict )[ 'Value' ]
#    if DT_dict_result['Link']:
#      DT_dict_result['Link'] = DT_dict_result['Link'][0][0]
#    
#    start_datetime = datetime.strptime( DT_dict_result['StartDate'], timeFormat )
#    
#    if start_datetime > now:
#      self.args[2]
#      diff = convertTime(start_datetime - now, 'hours')
#      if diff > self.args[2]:
#        return S_OK( { 'DT' : None } )
#        
#      DT_dict_result['DT'] = DT_dict_result['DT'] + " in " + str(diff) + ' hours'
#        
#    res = S_OK( DT_dict_result )
#
##    except Exception, e:
##      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
##      gLogger.exception( _msg )
##      return S_ERROR( _msg )
#
#    return res      
#
##  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF